from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Optional

from ..config import settings
from ..events.bus import bus
from ..models import MarketContextInfo

log = logging.getLogger(__name__)


def _context_from_payload(raw: dict) -> MarketContextInfo:
    published_ms = raw.get("published_at_ms")
    age = None
    if published_ms is not None:
        try:
            age = max(0.0, time.time() - (int(published_ms) / 1000.0))
        except (TypeError, ValueError):
            age = None
    return MarketContextInfo(
        schema_version=int(raw.get("schema_version", 1)),
        contract_metadata=dict(raw.get("contract_metadata") or {}),
        source=raw.get("source"),
        market_id=raw.get("market_id"),
        slug=raw.get("slug") or raw.get("market_id"),
        title=raw.get("title"),
        window_start_iso=raw.get("window_start_iso"),
        window_end_iso=raw.get("window_end_iso"),
        window_start_unix=raw.get("window_start_unix"),
        window_end_unix=raw.get("window_end_unix"),
        barrier=raw.get("barrier"),
        barrier_source=raw.get("barrier_source"),
        barrier_confirmed=bool(raw.get("barrier_confirmed", False)),
        barrier_offset_s=raw.get("barrier_offset_s"),
        clob_token_ids=dict(raw.get("clob_token_ids") or {}),
        sequence=raw.get("sequence"),
        published_at_ms=raw.get("published_at_ms"),
        age_seconds=age,
    )


class RedisMarketContextReader:
    def __init__(
        self,
        *,
        redis_url: str,
        latest_key: str,
        channel: str,
    ) -> None:
        self.redis_url = redis_url
        self.latest_key = latest_key
        self.channel = channel
        self._latest: Optional[MarketContextInfo] = None
        self._last_sequence: Optional[int] = None

    @property
    def latest(self) -> Optional[MarketContextInfo]:
        return self._latest

    def current(self) -> Optional[MarketContextInfo]:
        ctx = self._latest
        if ctx is None or ctx.window_end_unix is None:
            return None
        if ctx.window_end_unix + settings.market_context_current_grace_seconds < time.time():
            return None
        return ctx

    def _client(self):
        import redis  # type: ignore

        return redis.Redis.from_url(self.redis_url, decode_responses=False)

    def load_latest(self) -> Optional[MarketContextInfo]:
        client = self._client()
        try:
            raw = client.get(self.latest_key)
        finally:
            client.close()
        if not raw:
            return None
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        ctx = _context_from_payload(json.loads(raw))
        self._record(ctx)
        return ctx

    def _record(self, ctx: MarketContextInfo) -> bool:
        seq = ctx.sequence
        if seq is not None and self._last_sequence == seq:
            self._latest = ctx
            return False
        self._latest = ctx
        self._last_sequence = seq
        return True

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            client = None
            pubsub = None
            try:
                ctx = await asyncio.to_thread(self.load_latest)
                if ctx is not None:
                    await bus.publish("market_context.update", ctx.model_dump())
                client = self._client()
                pubsub = client.pubsub(ignore_subscribe_messages=True)
                pubsub.subscribe(self.channel)
                log.info("subscribed to market context channel %s", self.channel)
                while not stop.is_set():
                    message = await asyncio.to_thread(pubsub.get_message, False, 1.0)
                    if not message or message.get("type") != "message":
                        continue
                    data = message.get("data")
                    if isinstance(data, bytes):
                        data = data.decode("utf-8")
                    ctx = _context_from_payload(json.loads(data))
                    if self._record(ctx):
                        await bus.publish("market_context.update", ctx.model_dump())
            except Exception as exc:  # noqa: BLE001
                if not stop.is_set():
                    log.warning("market context reader failed: %s", exc)
                    try:
                        await asyncio.wait_for(stop.wait(), timeout=2.0)
                    except asyncio.TimeoutError:
                        pass
            finally:
                try:
                    if pubsub is not None:
                        pubsub.close()
                except Exception:
                    pass
                try:
                    if client is not None:
                        client.close()
                except Exception:
                    pass


async def run_market_context_loop(reader: RedisMarketContextReader, stop: asyncio.Event) -> None:
    await reader.run(stop)


def build_default_market_context_reader() -> RedisMarketContextReader:
    return RedisMarketContextReader(
        redis_url=settings.redis_url,
        latest_key=settings.market_context_latest_key,
        channel=settings.market_context_channel,
    )
