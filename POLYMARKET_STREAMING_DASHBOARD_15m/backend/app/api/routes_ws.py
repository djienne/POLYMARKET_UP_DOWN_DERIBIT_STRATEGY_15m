from __future__ import annotations

import asyncio
import json
import logging
import time
from collections import deque
from typing import Deque

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from ..config import settings
from ..envelope import envelope
from ..events.bus import bus
from ..models import ViewSource
from .auth import websocket_authorized
from .state_hub import get_hub

log = logging.getLogger(__name__)
router = APIRouter()

QUEUE_LIMIT = 256
DROPPABLE_TOPICS = {
    "leaderboard.update",
    "liveness.update",
    "liveness.tick",
    "edge.update",
    "calibration.start",
    "calibration.end",
    "market_context.update",
    "polymarket_status.update",
}


_AUTO_SWITCH_COOLDOWN_S = 5.0


def _slug_window_start(slug: str | None) -> int | None:
    if not slug:
        return None
    import re

    m = re.search(r"btc-updown-15m-(\d+)", slug)
    if not m:
        return None
    return int(m.group(1))


def normalize_terminal_payload_for_view(hub, payload: dict, source: ViewSource) -> dict:
    """Keep streamed terminal snapshots aligned with the active market window."""
    normalized = dict(payload)
    market = dict(normalized.get("market") or {})
    current_slug = hub._current_slug(source)
    current_start = _slug_window_start(current_slug)
    incoming_start = _slug_window_start(market.get("slug"))
    should_replace = (
        current_slug is not None
        and (
            incoming_start is None
            or (
                current_start is not None
                and incoming_start + 900 <= time.time()
                and incoming_start < current_start
            )
        )
    )
    if should_replace:
        market["slug"] = current_slug
        if current_start is not None:
            market["window_start_unix"] = current_start
            market["window_end_unix"] = current_start + 900
        normalized["market"] = market
    return normalized


def matches_view_source(data: dict, source: ViewSource) -> bool:
    return (data.get("run_source") or data.get("source")) == source


class ConnContext:
    def __init__(
        self,
        ws: WebSocket,
        initial_instance_id: int,
        initial_source: ViewSource | None = None,
    ) -> None:
        self.ws = ws
        self.instance_id: int = initial_instance_id
        self.source: ViewSource = initial_source or ("live" if settings.mode == "live" else "dry_run")
        self._user_selected: bool = False
        self._last_auto_switch: float = 0.0
        self.closed = False
        self._queue: Deque[tuple[str, dict]] = deque()
        self._queue_event = asyncio.Event()
        self._queue_lock = asyncio.Lock()

    async def send_now(self, topic: str, data: dict) -> None:
        if self.closed:
            return
        try:
            await self.ws.send_json(envelope(topic, data))
        except Exception:  # noqa: BLE001
            self.closed = True

    def _drop_oldest_droppable(self) -> bool:
        for idx, (topic, _) in enumerate(self._queue):
            if topic in DROPPABLE_TOPICS:
                del self._queue[idx]
                return True
        return False

    async def enqueue(self, topic: str, data: dict) -> None:
        if self.closed:
            return
        async with self._queue_lock:
            if len(self._queue) >= QUEUE_LIMIT and not self._drop_oldest_droppable():
                # A trade/state event cannot be silently discarded. Reconnect
                # gives the slow client a fresh bootstrap and full equity curve.
                self.closed = True
                self._queue.clear()
                self._queue_event.set()
            else:
                self._queue.append((topic, data))
                self._queue_event.set()
                return
        try:
            await asyncio.wait_for(self.ws.close(code=1013, reason="dashboard resync required"), timeout=1.0)
        except Exception:
            log.debug("slow WebSocket did not close promptly")

    async def clear_queue(self) -> None:
        async with self._queue_lock:
            self._queue.clear()
            self._queue_event.clear()

    async def sender_loop(self) -> None:
        while not self.closed:
            await self._queue_event.wait()
            while not self.closed:
                async with self._queue_lock:
                    if not self._queue:
                        self._queue_event.clear()
                        break
                    topic, data = self._queue.popleft()
                await self.send_now(topic, data)


@router.websocket("/ws")
async def ws_endpoint(ws: WebSocket) -> None:
    if not await websocket_authorized(ws):
        await ws.close(code=1008)
        return
    await ws.accept()
    hub = get_hub()
    ctx = ConnContext(ws, hub.recommended_instance_id())
    sender_task = asyncio.create_task(ctx.sender_loop())

    def instance_payload() -> dict:
        instance, position, equity, equity_series, today_summary = hub.instance_snapshot(
            ctx.instance_id,
            ctx.source,
        )
        return {
            "source": ctx.source,
            "instance": instance.model_dump() if instance else None,
            "position": position.model_dump(),
            "equity": equity,
            "equity_series": equity_series,
            "today_summary": today_summary.model_dump(),
        }

    async def send_instance_snapshot() -> None:
        await ctx.enqueue("instance.update", instance_payload())

    try:
        payload = hub.build_bootstrap(ctx.instance_id, ctx.source).model_dump()
        await ctx.send_now("bootstrap", payload)
    except Exception:
        log.exception("initial bootstrap failed")

    async def send_edge_update() -> None:
        edge_up, edge_down = hub.current_edges(ctx.instance_id, ctx.source)
        await ctx.enqueue(
            "edge.update",
            {
                "edge_up": edge_up.model_dump() if edge_up else None,
                "edge_down": edge_down.model_dump() if edge_down else None,
            },
        )

    def build_orderbook_snapshot() -> dict:
        slug = hub._current_slug()
        price_source = hub.polymarket.latest or hub.orderbook.latest
        return {
            "prices": price_source.model_dump() if price_source else None,
            "series_up": hub._scope_series(
                hub.polymarket.series("UP") or hub.orderbook.series("UP"), slug
            ),
            "series_down": hub._scope_series(
                hub.polymarket.series("DOWN") or hub.orderbook.series("DOWN"), slug
            ),
            "series_up_bid": hub._scope_series(
                hub.polymarket.series("UP", "bid") or hub.orderbook.series("UP", "bid"), slug
            ),
            "series_up_ask": hub._scope_series(
                hub.polymarket.series("UP", "ask") or hub.orderbook.series("UP", "ask"), slug
            ),
            "series_down_bid": hub._scope_series(
                hub.polymarket.series("DOWN", "bid") or hub.orderbook.series("DOWN", "bid"), slug
            ),
            "series_down_ask": hub._scope_series(
                hub.polymarket.series("DOWN", "ask") or hub.orderbook.series("DOWN", "ask"), slug
            ),
        }

    def chart_signature(payload: dict) -> tuple:
        def tail(key: str) -> tuple[int, str | None]:
            rows = payload.get(key) or []
            return len(rows), rows[-1].get("t") if rows else None

        prices = payload.get("prices") or {}
        return (
            tail("series_up"),
            tail("series_down"),
            tail("series_up_bid"),
            tail("series_up_ask"),
            tail("series_down_bid"),
            tail("series_down_ask"),
            prices.get("prob_up"),
            prices.get("prob_down"),
        )

    async def chart_snapshot_loop() -> None:
        last_sig: tuple | None = None
        interval = max(1.0, float(settings.polymarket_poll_interval_seconds))
        while not ctx.closed:
            payload = build_orderbook_snapshot()
            sig = chart_signature(payload)
            if sig != last_sig:
                last_sig = sig
                await ctx.enqueue("orderbook.update", payload)
                await send_edge_update()
            try:
                await asyncio.wait_for(asyncio.sleep(interval), timeout=interval + 0.5)
            except asyncio.TimeoutError:
                pass

    chart_task = asyncio.create_task(chart_snapshot_loop())

    async def on_event(topic: str, data) -> None:
        if ctx.closed:
            return
        if topic == "terminal.update":
            payload = normalize_terminal_payload_for_view(hub, dict(data), ctx.source)
            slug = (payload.get("market") or {}).get("slug") or hub._current_slug(ctx.source)
            payload["model_up"] = hub._scope_series(hub.terminal.model_series("UP"), slug)
            payload["model_down"] = hub._scope_series(hub.terminal.model_series("DOWN"), slug)
            await ctx.enqueue(topic, payload)
            await send_edge_update()
            return
        if topic == "trade.append":
            if not matches_view_source(data, ctx.source):
                return
            if data.get("instance_id") != ctx.instance_id:
                return
            await ctx.enqueue(topic, data)
            await send_instance_snapshot()
            await send_edge_update()
            return
        if topic in {"state.update", "equity.update"}:
            if not matches_view_source(data, ctx.source):
                return
            await send_instance_snapshot()
            return
        if topic == "market_context.update":
            await ctx.enqueue(topic, data)
            return
        if topic == "trades.reset":
            if not matches_view_source(data, ctx.source):
                return
            if data.get("events") is None:
                events = [
                    ev.model_dump()
                    for ev in reversed(hub.trades_for(ctx.source).recent(ctx.instance_id, n=200))
                ]
            else:
                events = [
                    ev
                    for ev in data.get("events", [])
                    if ev.get("run_source") == ctx.source and ev.get("instance_id") == ctx.instance_id
                ]
            await ctx.enqueue(
                topic,
                {
                    "source": ctx.source,
                    "reason": data.get("reason"),
                    "events": events,
                    "snapshot": instance_payload(),
                },
            )
            await send_edge_update()
            return
        if topic == "leaderboard.update":
            await ctx.enqueue(topic, data)
            if ctx.source == "dry_run" and not ctx._user_selected:
                now = time.time()
                if now - ctx._last_auto_switch < _AUTO_SWITCH_COOLDOWN_S:
                    await send_instance_snapshot()
                    await send_edge_update()
                    return
                try:
                    recommended = hub.recommended_instance_id(ctx.source)
                except Exception:
                    log.exception("auto-switch recommendation failed")
                    recommended = ctx.instance_id
                if recommended != ctx.instance_id:
                    ctx.instance_id = recommended
                    ctx._last_auto_switch = now
                    payload = hub.build_bootstrap(ctx.instance_id, ctx.source).model_dump()
                    await ctx.send_now("bootstrap", payload)
                    return
            await send_instance_snapshot()
            await send_edge_update()
            return
        if topic == "liveness.update":
            await ctx.enqueue(topic, data)
            if not ctx._user_selected:
                now = time.time()
                if now - ctx._last_auto_switch < _AUTO_SWITCH_COOLDOWN_S:
                    return
                try:
                    recommended = hub.recommended_instance_id(ctx.source)
                except Exception:
                    log.exception("auto-switch recommendation failed")
                    recommended = ctx.instance_id
                if recommended != ctx.instance_id:
                    ctx.instance_id = recommended
                    ctx._last_auto_switch = now
                    await ctx.clear_queue()
                    payload = hub.build_bootstrap(ctx.instance_id, ctx.source).model_dump()
                    await ctx.send_now("bootstrap", payload)
            return
        await ctx.enqueue(topic, data)
        if topic in ("terminal.update", "orderbook.update"):
            await send_edge_update()

    unsub = bus.subscribe("*", on_event)

    try:
        while True:
            msg = await ws.receive_text()
            try:
                obj = json.loads(msg)
            except json.JSONDecodeError:
                continue
            if obj.get("action") == "select_instance":
                try:
                    new_id = int(obj.get("instance_id"))
                except (TypeError, ValueError):
                    continue
                ctx._user_selected = True
                ctx.instance_id = hub.canonical_instance_id(new_id)
                await ctx.clear_queue()
                payload = hub.build_bootstrap(ctx.instance_id, ctx.source).model_dump()
                await ctx.send_now("bootstrap", payload)
            elif obj.get("action") == "select_view":
                source = obj.get("source")
                ctx.source = hub.normalize_source(source if source in ("dry_run", "live") else None)
                try:
                    new_id = int(obj.get("instance_id"))
                except (TypeError, ValueError):
                    new_id = hub.recommended_instance_id(ctx.source)
                ctx._user_selected = True
                ctx.instance_id = hub.canonical_instance_id(new_id)
                await ctx.clear_queue()
                payload = hub.build_bootstrap(ctx.instance_id, ctx.source).model_dump()
                await ctx.send_now("bootstrap", payload)
            elif obj.get("action") == "ping":
                await ctx.send_now("pong", {"t": time.time()})
    except WebSocketDisconnect:
        pass
    except Exception:
        log.exception("ws error")
    finally:
        ctx.closed = True
        unsub()
        ctx._queue_event.set()
        chart_task.cancel()
        sender_task.cancel()
        try:
            await chart_task
        except (asyncio.CancelledError, Exception):
            pass
        try:
            await sender_task
        except (asyncio.CancelledError, Exception):
            pass
