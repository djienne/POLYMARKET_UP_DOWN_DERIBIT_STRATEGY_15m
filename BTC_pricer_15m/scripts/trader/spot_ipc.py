"""Shared BTC spot IPC over Redis.

The spot daemon is the single owner of the current BTC/USD spot feed.  It
publishes accepted ticks here; calc, grid, live, and dashboard consume the same
payload instead of opening their own current-spot feeds.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Optional

from btc_pricer.utils.price_sanity import is_sane_btc_price

log = logging.getLogger(__name__)

DEFAULT_REDIS_URL = "redis://redis:6379/0"
DEFAULT_LATEST_KEY = "market:spot:btc_usd:latest"
DEFAULT_STREAM = "market.spot.btc_usd"
DEFAULT_CHANNEL = "market.spot.btc_usd"
DEFAULT_MAX_AGE_SECONDS = 10.0


def _now_ms() -> int:
    return int(time.time() * 1000)


def _source_label(source: str) -> str:
    if source == "polymarket_rtds":
        return "Polymarket RTDS Chainlink"
    if source.startswith("ws_median"):
        return "Multi-exchange websocket fallback"
    if source == "binance_rest_last_resort":
        return "REST fallback"
    return source or "Unknown"


@dataclass(frozen=True)
class SpotTick:
    schema_version: int
    symbol: str
    price: float
    source: str
    source_label: str
    source_timestamp_ms: int
    received_at_ms: int
    published_at_ms: int
    source_age_seconds: float
    receive_age_seconds: float
    sequence: int
    stale_after_seconds: float
    details: dict = field(default_factory=dict)

    @classmethod
    def create(
        cls,
        *,
        price: float,
        source: str,
        sequence: int,
        source_timestamp_ms: Optional[int] = None,
        received_at_ms: Optional[int] = None,
        stale_after_seconds: float = DEFAULT_MAX_AGE_SECONDS,
        details: Optional[dict] = None,
    ) -> "SpotTick":
        published_at_ms = _now_ms()
        source_ts = int(source_timestamp_ms if source_timestamp_ms is not None else published_at_ms)
        received = int(received_at_ms if received_at_ms is not None else published_at_ms)
        return cls(
            schema_version=1,
            symbol="BTC/USD",
            price=float(price),
            source=str(source),
            source_label=_source_label(str(source)),
            source_timestamp_ms=source_ts,
            received_at_ms=received,
            published_at_ms=published_at_ms,
            source_age_seconds=max(0.0, (published_at_ms - source_ts) / 1000.0),
            receive_age_seconds=max(0.0, (published_at_ms - received) / 1000.0),
            sequence=int(sequence),
            stale_after_seconds=float(stale_after_seconds),
            details=dict(details or {}),
        )

    @classmethod
    def from_dict(cls, raw: dict) -> "SpotTick":
        return cls(
            schema_version=int(raw.get("schema_version", 1)),
            symbol=str(raw.get("symbol") or "BTC/USD"),
            price=float(raw["price"]),
            source=str(raw.get("source") or ""),
            source_label=str(raw.get("source_label") or _source_label(str(raw.get("source") or ""))),
            source_timestamp_ms=int(raw.get("source_timestamp_ms", raw.get("published_at_ms", 0))),
            received_at_ms=int(raw.get("received_at_ms", raw.get("published_at_ms", 0))),
            published_at_ms=int(raw.get("published_at_ms") or _now_ms()),
            source_age_seconds=float(raw.get("source_age_seconds") or 0.0),
            receive_age_seconds=float(raw.get("receive_age_seconds") or 0.0),
            sequence=int(raw.get("sequence") or 0),
            stale_after_seconds=float(raw.get("stale_after_seconds") or DEFAULT_MAX_AGE_SECONDS),
            details=dict(raw.get("details") or {}),
        )

    @classmethod
    def from_json(cls, text: str | bytes) -> "SpotTick":
        if isinstance(text, bytes):
            text = text.decode("utf-8")
        return cls.from_dict(json.loads(text))

    def to_dict(self) -> dict:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), separators=(",", ":"), sort_keys=True)

    def age_seconds(self, now: Optional[float] = None) -> float:
        now_ms = int((time.time() if now is None else now) * 1000)
        return max(0.0, (now_ms - self.published_at_ms) / 1000.0,
                   (now_ms - self.source_timestamp_ms) / 1000.0,
                   (now_ms - self.received_at_ms) / 1000.0)

    def is_fresh(self, max_age_s: Optional[float] = None) -> bool:
        limit = self.stale_after_seconds if max_age_s is None else float(max_age_s)
        return is_sane_btc_price(self.price) and self.age_seconds() <= limit


def redis_url_from_env(default: Optional[str] = None) -> Optional[str]:
    return (
        os.environ.get("BTC_SPOT_REDIS_URL")
        or os.environ.get("REDIS_URL")
        or default
    )


def _redis_from_url(redis_url: str):
    try:
        import redis  # type: ignore
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("redis-py is required for shared spot IPC") from exc
    return redis.Redis.from_url(redis_url, decode_responses=False)


class RedisSpotPublisher:
    def __init__(
        self,
        *,
        redis_url: str = DEFAULT_REDIS_URL,
        latest_key: str = DEFAULT_LATEST_KEY,
        stream: str = DEFAULT_STREAM,
        channel: str = DEFAULT_CHANNEL,
        maxlen: int = 20_000,
    ) -> None:
        self.redis_url = redis_url
        self.latest_key = latest_key
        self.stream = stream
        self.channel = channel
        self.maxlen = int(maxlen)
        self._client = _redis_from_url(redis_url)

    def publish(self, tick: SpotTick) -> None:
        payload = tick.to_json()
        pipe = self._client.pipeline(transaction=False)
        pipe.set(self.latest_key, payload)
        pipe.xadd(
            self.stream,
            {"sequence": tick.sequence, "source": tick.source, "payload": payload},
            maxlen=self.maxlen,
            approximate=True,
        )
        pipe.publish(self.channel, payload)
        pipe.execute()

    def publish_twap(self, observation: dict) -> None:
        """Separate transport for oracle averages; never overwrites the spot key."""
        payload = json.dumps(observation, separators=(",", ":"), sort_keys=True)
        pipe = self._client.pipeline(transaction=False)
        pipe.set("market:twap:btc_usd:60s:latest", payload)
        pipe.xadd("market.twap.btc_usd.60s", {"payload": payload}, maxlen=self.maxlen, approximate=True)
        pipe.publish("market.twap.btc_usd.60s", payload)
        pipe.execute()

    def close(self) -> None:
        try:
            self._client.close()
        except Exception:  # noqa: BLE001
            pass


class RedisSpotCache:
    """Small synchronous latest-tick reader used by calc/grid/live."""

    def __init__(
        self,
        *,
        redis_url: str = DEFAULT_REDIS_URL,
        latest_key: str = DEFAULT_LATEST_KEY,
        stream: str = DEFAULT_STREAM,
    ) -> None:
        self.redis_url = redis_url
        self.latest_key = latest_key
        self.stream = stream
        self._client = _redis_from_url(redis_url)

    def latest(self, max_age_s: Optional[float] = DEFAULT_MAX_AGE_SECONDS) -> Optional[SpotTick]:
        try:
            raw = self._client.get(self.latest_key)
        except Exception as exc:  # noqa: BLE001
            log.debug("redis spot latest failed: %s", exc)
            return None
        if not raw:
            return None
        try:
            tick = SpotTick.from_json(raw)
        except Exception as exc:  # noqa: BLE001
            log.warning("invalid redis spot payload: %s", exc)
            return None
        if max_age_s is not None and not tick.is_fresh(float(max_age_s)):
            return None
        return tick

    @property
    def is_healthy(self) -> bool:
        return self.latest(max_age_s=30.0) is not None

    def get_first_at_or_after(
        self,
        target_utc: datetime,
        *,
        max_offset_s: float = 10.0,
        count: int = 10_000,
    ) -> tuple[Optional[float], Optional[float]]:
        """Return first RTDS tick at-or-after ``target_utc`` from Redis history."""
        target_ms = int(target_utc.timestamp() * 1000)
        best_tick: Optional[SpotTick] = None
        try:
            rows = self._client.xrevrange(self.stream, count=count)
        except Exception as exc:  # noqa: BLE001
            log.debug("redis spot history read failed: %s", exc)
            return None, None
        for _message_id, fields in rows:
            raw = fields.get(b"payload") or fields.get("payload")
            if not raw:
                continue
            try:
                tick = SpotTick.from_json(raw)
            except Exception:
                continue
            if tick.source != "polymarket_rtds":
                continue
            offset_ms = tick.source_timestamp_ms - target_ms
            if offset_ms < 0 or offset_ms > max_offset_s * 1000:
                continue
            if best_tick is None or tick.source_timestamp_ms < best_tick.source_timestamp_ms:
                best_tick = tick
        if best_tick is None:
            return None, None
        return float(best_tick.price), (best_tick.source_timestamp_ms - target_ms) / 1000.0

    def close(self) -> None:
        try:
            self._client.close()
        except Exception:  # noqa: BLE001
            pass


class RedisSpotConsumer(RedisSpotCache):
    """Latest spot cache maintained by Redis Pub/Sub for low-latency consumers."""

    def __init__(
        self,
        *,
        redis_url: str = DEFAULT_REDIS_URL,
        latest_key: str = DEFAULT_LATEST_KEY,
        stream: str = DEFAULT_STREAM,
        channel: str = DEFAULT_CHANNEL,
    ) -> None:
        super().__init__(redis_url=redis_url, latest_key=latest_key, stream=stream)
        self.channel = channel
        self._lock = threading.Lock()
        self._latest: Optional[SpotTick] = None
        self._event = threading.Event()
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._pubsub = None

    def start(self) -> None:
        if self._running:
            return
        self._latest = super().latest(max_age_s=None)
        self._running = True
        self._thread = threading.Thread(target=self._run, name="redis-spot-consumer", daemon=True)
        self._thread.start()

    def _run(self) -> None:
        try:
            self._pubsub = self._client.pubsub(ignore_subscribe_messages=True)
            self._pubsub.subscribe(self.channel)
            while self._running:
                message = self._pubsub.get_message(timeout=1.0)
                if not message or message.get("type") != "message":
                    continue
                try:
                    tick = SpotTick.from_json(message.get("data"))
                except Exception as exc:  # noqa: BLE001
                    log.debug("invalid spot pubsub payload: %s", exc)
                    continue
                with self._lock:
                    if self._latest is None or tick.sequence >= self._latest.sequence:
                        self._latest = tick
                        self._event.set()
        except Exception as exc:  # noqa: BLE001
            if self._running:
                log.warning("redis spot pubsub stopped: %s", exc)

    def latest(self, max_age_s: Optional[float] = DEFAULT_MAX_AGE_SECONDS) -> Optional[SpotTick]:
        with self._lock:
            tick = self._latest
        if tick is None or (max_age_s is not None and not tick.is_fresh(float(max_age_s))):
            tick = super().latest(max_age_s=None)
            if tick is not None:
                with self._lock:
                    self._latest = tick
        if tick is None:
            return None
        if max_age_s is not None and not tick.is_fresh(float(max_age_s)):
            return None
        return tick

    def wait_for_update(self, last_sequence: Optional[int], timeout_s: float) -> Optional[SpotTick]:
        deadline = time.time() + max(0.0, timeout_s)
        while time.time() <= deadline:
            tick = self.latest(max_age_s=None)
            if tick is not None and (last_sequence is None or tick.sequence != last_sequence):
                return tick
            remaining = max(0.0, deadline - time.time())
            self._event.wait(min(remaining, 0.5))
            self._event.clear()
        return self.latest(max_age_s=None)

    def close(self) -> None:
        self._running = False
        try:
            if self._pubsub is not None:
                self._pubsub.close()
        except Exception:  # noqa: BLE001
            pass
        if self._thread is not None:
            self._thread.join(timeout=2.0)
        super().close()


_GLOBAL_SPOT_CACHE: Optional[RedisSpotCache] = None
_GLOBAL_SPOT_LOCK = threading.Lock()


def get_shared_spot_price(max_age_s: float = DEFAULT_MAX_AGE_SECONDS) -> Optional[float]:
    redis_url = redis_url_from_env()
    if not redis_url:
        return None
    global _GLOBAL_SPOT_CACHE
    with _GLOBAL_SPOT_LOCK:
        if _GLOBAL_SPOT_CACHE is None:
            _GLOBAL_SPOT_CACHE = RedisSpotCache(redis_url=redis_url)
        cache = _GLOBAL_SPOT_CACHE
    tick = cache.latest(max_age_s=max_age_s)
    return float(tick.price) if tick is not None else None
