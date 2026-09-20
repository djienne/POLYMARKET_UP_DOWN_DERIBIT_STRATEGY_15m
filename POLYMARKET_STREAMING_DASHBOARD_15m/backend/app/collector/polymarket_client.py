"""Live Polymarket price source for the dashboard.

Uses the public CLOB + Gamma REST APIs — identical to what the bot itself uses
(see `BTC_pricer_15m/scripts/trader/order_book.py:OrderBookClient`). No authentication
required. This decouples the PriceChart from the bot's Docker container: prices keep
flowing even if the bot is stopped/rotated.
"""
from __future__ import annotations

import asyncio
import json
import logging
import random
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Deque, Optional

import httpx

from ..config import settings
from ..events.bus import bus
from ..models import PolymarketPrices
from ..time_utils import iso_to_unix

log = logging.getLogger(__name__)

MAX_POINTS = 1000

# Circuit breaker: skip an endpoint for one poll cycle after this many consecutive failures.
BREAKER_THRESHOLD = 5
RETRY_ATTEMPTS = 3
RETRY_BASE_DELAY = 0.25  # seconds


@dataclass(frozen=True)
class PolymarketClobHealth:
    slug: Optional[str]
    healthy: bool
    reason: str
    consecutive_healthy_polls: int = 0
    accepting_orders: Optional[bool] = None
    enable_order_book: Optional[bool] = None
    checked_at: Optional[str] = None


class _EndpointUnavailable(Exception):
    """Raised when the circuit breaker is open for the called endpoint."""


def _bool_or_none(value: Any) -> Optional[bool]:
    if isinstance(value, bool):
        return value
    return None


def _best_bid_ask(book: dict) -> tuple[Optional[float], Optional[float]]:
    """Extract best_bid (highest) and best_ask (lowest) from a CLOB /book response."""
    bids = [(float(b["price"]), float(b["size"])) for b in book.get("bids", []) if b.get("price")]
    asks = [(float(a["price"]), float(a["size"])) for a in book.get("asks", []) if a.get("price")]
    best_bid = max((p for p, s in bids if s > 0), default=None)
    best_ask = min((p for p, s in asks if s > 0), default=None)
    return best_bid, best_ask


class PolymarketClient:
    def __init__(self, slug_fn: Callable[[], Optional[str]]) -> None:
        self._slug_fn = slug_fn
        self._client: Optional[httpx.AsyncClient] = None

        self._cur_slug: Optional[str] = None
        self._history_seeded_slug: Optional[str] = None
        self._up_token: Optional[str] = None
        self._down_token: Optional[str] = None
        self._accepting_orders: Optional[bool] = None
        self._enable_order_book: Optional[bool] = None
        self._token_lookup_failures = 0

        self._series_up: Deque[tuple[str, float]] = deque(maxlen=MAX_POINTS)
        self._series_down: Deque[tuple[str, float]] = deque(maxlen=MAX_POINTS)
        self._series_up_bid: Deque[tuple[str, float]] = deque(maxlen=MAX_POINTS)
        self._series_up_ask: Deque[tuple[str, float]] = deque(maxlen=MAX_POINTS)
        self._series_down_bid: Deque[tuple[str, float]] = deque(maxlen=MAX_POINTS)
        self._series_down_ask: Deque[tuple[str, float]] = deque(maxlen=MAX_POINTS)
        self._up_bid: Optional[float] = None
        self._up_ask: Optional[float] = None
        self._down_bid: Optional[float] = None
        self._down_ask: Optional[float] = None
        self._endpoint_failures: dict[str, int] = {}
        self._endpoint_degraded: set[str] = set()
        self._health = PolymarketClobHealth(
            slug=None,
            healthy=False,
            reason="not checked",
        )
        self._consecutive_healthy_polls = 0

    async def _ensure_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=settings.polymarket_request_timeout_seconds,
                headers={"User-Agent": "polymarket-streaming-dashboard/0.1"},
            )
        return self._client

    def _on_endpoint_success(self, endpoint: str) -> None:
        if endpoint in self._endpoint_degraded:
            log.warning("polymarket endpoint %s recovered", endpoint)
            self._endpoint_degraded.discard(endpoint)
        self._endpoint_failures[endpoint] = 0

    def _on_endpoint_failure(self, endpoint: str) -> None:
        n = self._endpoint_failures.get(endpoint, 0) + 1
        self._endpoint_failures[endpoint] = n
        if n >= BREAKER_THRESHOLD and endpoint not in self._endpoint_degraded:
            self._endpoint_degraded.add(endpoint)
            log.warning(
                "polymarket endpoint %s degraded after %d consecutive failures; "
                "skipping for next poll cycle",
                endpoint,
                n,
            )

    async def _request_with_retry(
        self,
        endpoint: str,
        url: str,
        *,
        params: Optional[dict[str, Any]] = None,
        attempts: int = RETRY_ATTEMPTS,
        base_delay: float = RETRY_BASE_DELAY,
    ) -> httpx.Response:
        """GET with bounded exponential backoff. Retries timeouts, network errors,
        and 429/5xx responses. Raises the last exception (or _EndpointUnavailable
        if the breaker is open) on final failure."""
        if endpoint in self._endpoint_degraded:
            raise _EndpointUnavailable(endpoint)

        client = await self._ensure_client()
        last_exc: Optional[Exception] = None
        for i in range(attempts):
            try:
                r = await client.get(url, params=params)
                if r.status_code == 429 or 500 <= r.status_code < 600:
                    last_exc = httpx.HTTPStatusError(
                        f"{r.status_code} from {endpoint}", request=r.request, response=r
                    )
                else:
                    r.raise_for_status()
                    self._on_endpoint_success(endpoint)
                    return r
            except (httpx.TimeoutException, httpx.TransportError) as e:
                last_exc = e
            except httpx.HTTPStatusError as e:
                # 4xx other than 429 — don't retry
                self._on_endpoint_failure(endpoint)
                raise e
            if i < attempts - 1:
                delay = base_delay * (2 ** i) + random.uniform(0, base_delay)
                await asyncio.sleep(delay)
        self._on_endpoint_failure(endpoint)
        assert last_exc is not None
        raise last_exc

    async def close(self) -> None:
        if self._client is not None:
            try:
                await self._client.aclose()
            finally:
                self._client = None

    @property
    def latest(self) -> Optional[PolymarketPrices]:
        up_mid = _mid(self._up_bid, self._up_ask)
        down_mid = _mid(self._down_bid, self._down_ask)
        if up_mid is None and down_mid is None:
            return None
        prob_up = up_mid
        prob_down = down_mid if down_mid is not None else (
            1.0 - up_mid if up_mid is not None else None
        )
        return PolymarketPrices(
            best_bid=self._up_bid,
            best_ask=self._up_ask,
            mid=up_mid,
            prob_up=prob_up,
            prob_down=prob_down,
        )

    @property
    def health(self) -> PolymarketClobHealth:
        return self._health

    def _record_health(self, healthy: bool, reason: str) -> None:
        if healthy:
            self._consecutive_healthy_polls += 1
        else:
            self._consecutive_healthy_polls = 0
        self._health = PolymarketClobHealth(
            slug=self._cur_slug,
            healthy=healthy,
            reason=reason,
            consecutive_healthy_polls=self._consecutive_healthy_polls,
            accepting_orders=self._accepting_orders,
            enable_order_book=self._enable_order_book,
            checked_at=datetime.now(timezone.utc).isoformat(),
        )

    def series(self, side: str, kind: str = "mid") -> list[dict]:
        if side == "UP":
            dq = {
                "mid": self._series_up,
                "bid": self._series_up_bid,
                "ask": self._series_up_ask,
            }.get(kind, self._series_up)
        else:
            dq = {
                "mid": self._series_down,
                "bid": self._series_down_bid,
                "ask": self._series_down_ask,
            }.get(kind, self._series_down)
        return [{"t": t, "v": v} for (t, v) in dq]

    @staticmethod
    def _window_bounds(slug: str) -> Optional[tuple[int, int]]:
        import re
        m = re.search(r"btc-updown-15m-(\d+)", slug)
        if not m:
            return None
        start = int(m.group(1))
        return start, start + 900

    @staticmethod
    def _first_ts(dq: Deque[tuple[str, float]]) -> Optional[int]:
        if not dq:
            return None
        ts = iso_to_unix(dq[0][0])
        return int(ts) if ts is not None else None

    @staticmethod
    def _merge_history(
        dq: Deque[tuple[str, float]],
        history: list[tuple[str, float]],
    ) -> None:
        merged = {t: v for t, v in history}
        for t, v in dq:
            merged[t] = v
        dq.clear()
        for t, v in sorted(merged.items()):
            dq.append((t, v))

    async def _resolve_tokens(self, slug: str) -> bool:
        """Gamma API ? extract UP/DOWN token_ids for this slug. Returns True if resolved."""
        url = f"{settings.polymarket_gamma_url}/markets"
        try:
            r = await self._request_with_retry("gamma", url, params={"slug": slug})
            data = r.json()
        except _EndpointUnavailable:
            return False
        except (httpx.HTTPError, ValueError) as e:
            self._token_lookup_failures += 1
            if self._token_lookup_failures <= 2:
                log.warning("polymarket gamma lookup failed for %s: %s", slug, e)
            return False

        if isinstance(data, list):
            if not data:
                self._record_health(False, "gamma market not found")
                return False
            market = data[0]
        elif isinstance(data, dict):
            market = data
        else:
            self._record_health(False, "gamma market payload invalid")
            return False

        self._accepting_orders = _bool_or_none(market.get("acceptingOrders"))
        self._enable_order_book = _bool_or_none(market.get("enableOrderBook"))

        raw_tokens = market.get("clobTokenIds")
        if not raw_tokens:
            self._record_health(False, "gamma market has no CLOB token ids")
            return False
        try:
            tokens = json.loads(raw_tokens) if isinstance(raw_tokens, str) else raw_tokens
        except (TypeError, ValueError):
            self._record_health(False, "CLOB token ids invalid")
            return False

        raw_outcomes = market.get("outcomes")
        try:
            outcomes = json.loads(raw_outcomes) if isinstance(raw_outcomes, str) else raw_outcomes
        except (TypeError, ValueError):
            outcomes = []

        if not isinstance(tokens, list) or len(tokens) < 2:
            self._record_health(False, "CLOB token ids incomplete")
            return False

        # Map outcome ? token, with "Up"/"Down" labels (falls back to index 0=UP).
        up_token: Optional[str] = None
        down_token: Optional[str] = None
        if isinstance(outcomes, list) and len(outcomes) == len(tokens):
            for o, t in zip(outcomes, tokens):
                if isinstance(o, str) and o.lower().startswith("up"):
                    up_token = str(t)
                elif isinstance(o, str) and o.lower().startswith("down"):
                    down_token = str(t)
        if up_token is None:
            up_token = str(tokens[0])
        if down_token is None:
            down_token = str(tokens[1]) if len(tokens) > 1 else None

        self._up_token = up_token
        self._down_token = down_token
        self._token_lookup_failures = 0
        log.info(
            "polymarket_client: resolved slug %s ? UP=%s... DOWN=%s...",
            slug, (up_token or "")[:8], (down_token or "")[:8],
        )
        return up_token is not None and down_token is not None

    async def _fetch_book(self, token: str) -> Optional[dict]:
        url = f"{settings.polymarket_clob_url}/book"
        try:
            r = await self._request_with_retry("clob_book", url, params={"token_id": token})
            return r.json()
        except _EndpointUnavailable:
            return None
        except (httpx.HTTPError, ValueError) as e:
            log.debug("clob book fetch failed: %s", e)
            return None

    async def _fetch_prices_history(
        self,
        token: str,
        start_ts: int,
        end_ts: int,
    ) -> Optional[list[tuple[str, float]]]:
        url = f"{settings.polymarket_clob_url}/prices-history"
        try:
            r = await self._request_with_retry(
                "clob_history",
                url,
                params={
                    "market": token,
                    "startTs": start_ts,
                    "endTs": end_ts,
                    "fidelity": 1,
                },
            )
            data = r.json()
        except _EndpointUnavailable:
            return None
        except (httpx.HTTPError, ValueError) as e:
            log.warning("polymarket prices-history fetch failed for %s: %s", token[:8], e)
            return None

        points: list[tuple[str, float]] = []
        for row in data.get("history", []):
            try:
                ts = int(float(row["t"]))
                price = float(row["p"])
            except (KeyError, TypeError, ValueError):
                continue
            # Polymarket reports p=0 when the book was empty at that timestamp;
            # treat as missing rather than a real $0 mid.
            if price <= 0:
                continue
            iso = datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
            points.append((iso, price))
        return points

    async def _maybe_backfill_history(self, slug: str) -> bool:
        if self._history_seeded_slug == slug:
            return False
        bounds = self._window_bounds(slug)
        if bounds is None or self._up_token is None or self._down_token is None:
            return False

        start_ts, end_ts = bounds
        now_ts = int(datetime.now(timezone.utc).timestamp())
        if now_ts <= start_ts + 15:
            return False

        up_first = self._first_ts(self._series_up)
        down_first = self._first_ts(self._series_down)
        if (
            up_first is not None and up_first <= start_ts + 5 and
            down_first is not None and down_first <= start_ts + 5
        ):
            self._history_seeded_slug = slug
            return False

        history_end = min(now_ts, end_ts)
        up_history, down_history = await asyncio.gather(
            self._fetch_prices_history(self._up_token, start_ts, history_end),
            self._fetch_prices_history(self._down_token, start_ts, history_end),
        )
        if up_history is None or down_history is None:
            return False

        self._merge_history(self._series_up, up_history)
        self._merge_history(self._series_down, down_history)
        self._history_seeded_slug = slug
        log.info(
            "polymarket_client: backfilled %s window history (UP=%d DOWN=%d)",
            slug,
            len(up_history),
            len(down_history),
        )
        return bool(up_history or down_history)

    async def poll(self) -> bool:
        # Allow each poll cycle to retry endpoints that tripped the breaker
        # last cycle. Failure counters persist so the warning only fires on
        # transition; recovery is logged from _on_endpoint_success.
        self._endpoint_degraded.clear()

        slug = self._slug_fn()
        if not slug:
            self._record_health(False, "no active market slug")
            return False
        if slug != self._cur_slug:
            # New market — reset
            self._cur_slug = slug
            self._history_seeded_slug = None
            self._up_token = None
            self._down_token = None
            self._accepting_orders = None
            self._enable_order_book = None
            self._series_up.clear()
            self._series_down.clear()
            self._series_up_bid.clear()
            self._series_up_ask.clear()
            self._series_down_bid.clear()
            self._series_down_ask.clear()
            self._up_bid = self._up_ask = None
            self._down_bid = self._down_ask = None
            self._record_health(False, "market changed")

        if self._up_token is None or self._down_token is None:
            if not await self._resolve_tokens(slug):
                return False

        changed = await self._maybe_backfill_history(slug)

        up_book, down_book = await asyncio.gather(
            self._fetch_book(self._up_token),  # type: ignore[arg-type]
            self._fetch_book(self._down_token),  # type: ignore[arg-type]
        )
        if up_book is None and down_book is None:
            self._record_health(False, "both CLOB books unavailable")
            return changed

        ts = datetime.now(timezone.utc).isoformat()
        up_has_mid = False
        down_has_mid = False
        if up_book is not None:
            b, a = _best_bid_ask(up_book)
            self._up_bid, self._up_ask = b, a
            m = _mid(b, a)
            if m is not None:
                up_has_mid = True
                self._series_up.append((ts, m))
                changed = True
            if b is not None:
                self._series_up_bid.append((ts, b))
            if a is not None:
                self._series_up_ask.append((ts, a))
        if down_book is not None:
            b, a = _best_bid_ask(down_book)
            self._down_bid, self._down_ask = b, a
            m = _mid(b, a)
            if m is not None:
                down_has_mid = True
                self._series_down.append((ts, m))
                changed = True
            if b is not None:
                self._series_down_bid.append((ts, b))
            if a is not None:
                self._series_down_ask.append((ts, a))
        if self._accepting_orders is not True:
            self._record_health(False, "market is not accepting orders")
        elif self._enable_order_book is not True:
            self._record_health(False, "order book disabled for market")
        elif up_book is None or down_book is None:
            self._record_health(False, "one CLOB book unavailable")
        elif not (up_has_mid or down_has_mid):
            self._record_health(False, "CLOB books are empty")
        else:
            self._record_health(True, "live CLOB books reachable")
        return changed


def _mid(bid: Optional[float], ask: Optional[float]) -> Optional[float]:
    if bid is None or ask is None:
        return None
    if bid <= 0 or ask <= 0:
        return None
    return (bid + ask) / 2.0


async def run_polymarket_loop(client: "PolymarketClient", stop: asyncio.Event) -> None:
    try:
        while not stop.is_set():
            try:
                if await client.poll():
                    await bus.publish(
                        "orderbook.update",
                        {
                            "prices": client.latest.model_dump() if client.latest else None,
                            "series_up": client.series("UP"),
                            "series_down": client.series("DOWN"),
                            "series_up_bid": client.series("UP", "bid"),
                            "series_up_ask": client.series("UP", "ask"),
                            "series_down_bid": client.series("DOWN", "bid"),
                            "series_down_ask": client.series("DOWN", "ask"),
                        },
                    )
            except Exception:  # noqa: BLE001
                log.exception("polymarket poll error")
            try:
                await asyncio.wait_for(
                    stop.wait(), timeout=settings.polymarket_poll_interval_seconds
                )
            except asyncio.TimeoutError:
                pass
    finally:
        await client.close()
