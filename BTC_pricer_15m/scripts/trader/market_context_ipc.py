"""Shared 15m market-context IPC over Redis.

Calc is the single owner of active market metadata: slug, window bounds,
price-to-beat/barrier, and token ids. This module publishes that context so
grid, live, and dashboard consumers can share the same value without
re-scanning or inferring it independently.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Optional

log = logging.getLogger(__name__)

DEFAULT_REDIS_URL = "redis://redis:6379/0"
DEFAULT_LATEST_KEY = "market:context:btc_updown_15m:latest"
DEFAULT_STREAM = "market.context.btc_updown_15m"
DEFAULT_CHANNEL = "market.context.btc_updown_15m"


def _now_ms() -> int:
    return int(time.time() * 1000)


def _parse_iso_seconds(value: Optional[str]) -> Optional[int]:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.astimezone(timezone.utc).timestamp())


@dataclass(frozen=True)
class MarketContextTick:
    schema_version: int
    source: str
    market_id: str
    slug: str
    title: Optional[str]
    window_start_iso: Optional[str]
    window_end_iso: str
    window_start_unix: Optional[int]
    window_end_unix: int
    barrier: float
    barrier_source: str
    barrier_confirmed: bool
    barrier_offset_s: Optional[float]
    clob_token_ids: dict = field(default_factory=dict)
    sequence: int = 0
    published_at_ms: int = 0
    contract_metadata: Optional[dict] = None

    @classmethod
    def create(
        cls,
        *,
        source: str,
        market_id: str,
        slug: Optional[str],
        title: Optional[str],
        window_start_iso: Optional[str],
        window_end_iso: str,
        barrier: float,
        barrier_source: str,
        barrier_offset_s: Optional[float],
        clob_token_ids: Optional[dict],
        sequence: int,
        contract_metadata: Optional[dict] = None,
    ) -> "MarketContextTick":
        window_start_unix = _parse_iso_seconds(window_start_iso)
        window_end_unix = _parse_iso_seconds(window_end_iso)
        if window_end_unix is None:
            raise ValueError("window_end_iso must parse to a timestamp")
        confirmed = str(barrier_source or "").lower() not in {"", "unknown", "chain_broken"}
        return cls(
            schema_version=2,
            source=str(source),
            market_id=str(market_id),
            slug=str(slug or market_id),
            title=title,
            window_start_iso=window_start_iso,
            window_end_iso=window_end_iso,
            window_start_unix=window_start_unix,
            window_end_unix=window_end_unix,
            barrier=float(barrier),
            barrier_source=str(barrier_source or "unknown"),
            barrier_confirmed=confirmed,
            barrier_offset_s=barrier_offset_s,
            clob_token_ids=dict(clob_token_ids or {}),
            sequence=int(sequence),
            published_at_ms=_now_ms(),
            contract_metadata=contract_metadata,
        )

    @classmethod
    def from_dict(cls, raw: dict) -> "MarketContextTick":
        return cls(
            schema_version=int(raw.get("schema_version", 1)),
            source=str(raw.get("source") or ""),
            market_id=str(raw["market_id"]),
            slug=str(raw.get("slug") or raw["market_id"]),
            title=raw.get("title"),
            window_start_iso=raw.get("window_start_iso"),
            window_end_iso=str(raw["window_end_iso"]),
            window_start_unix=(
                int(raw["window_start_unix"])
                if raw.get("window_start_unix") is not None
                else None
            ),
            window_end_unix=int(raw["window_end_unix"]),
            barrier=float(raw["barrier"]),
            barrier_source=str(raw.get("barrier_source") or "unknown"),
            barrier_confirmed=bool(raw.get("barrier_confirmed", False)),
            barrier_offset_s=(
                float(raw["barrier_offset_s"])
                if raw.get("barrier_offset_s") is not None
                else None
            ),
            clob_token_ids=dict(raw.get("clob_token_ids") or {}),
            sequence=int(raw.get("sequence") or 0),
            published_at_ms=int(raw.get("published_at_ms") or _now_ms()),
            contract_metadata=raw.get("contract_metadata") if isinstance(raw.get("contract_metadata"), dict) else None,
        )

    @classmethod
    def from_json(cls, text: str | bytes) -> "MarketContextTick":
        if isinstance(text, bytes):
            text = text.decode("utf-8")
        return cls.from_dict(json.loads(text))

    def to_dict(self) -> dict:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), separators=(",", ":"), sort_keys=True)

    def age_seconds(self, now: Optional[float] = None) -> float:
        now_ms = int((time.time() if now is None else now) * 1000)
        return max(0.0, (now_ms - self.published_at_ms) / 1000.0)

    def is_current(self, *, grace_seconds: float = 30.0) -> bool:
        return self.window_end_unix + float(grace_seconds) >= time.time()


class RedisMarketContextPublisher:
    def __init__(
        self,
        *,
        redis_url: str = DEFAULT_REDIS_URL,
        latest_key: str = DEFAULT_LATEST_KEY,
        stream: str = DEFAULT_STREAM,
        channel: str = DEFAULT_CHANNEL,
        maxlen: int = 10_000,
    ) -> None:
        self.redis_url = redis_url
        self.latest_key = latest_key
        self.stream = stream
        self.channel = channel
        self.maxlen = int(maxlen)
        self._client = _redis_from_url(redis_url)

    def publish(self, tick: MarketContextTick) -> None:
        payload = tick.to_json()
        pipe = self._client.pipeline(transaction=False)
        pipe.set(self.latest_key, payload)
        pipe.xadd(
            self.stream,
            {
                "sequence": tick.sequence,
                "market_id": tick.market_id,
                "payload": payload,
            },
            maxlen=self.maxlen,
            approximate=True,
        )
        pipe.publish(self.channel, payload)
        pipe.execute()

    def close(self) -> None:
        try:
            self._client.close()
        except Exception:  # noqa: BLE001
            pass


class RedisMarketContextCache:
    def __init__(
        self,
        *,
        redis_url: str = DEFAULT_REDIS_URL,
        latest_key: str = DEFAULT_LATEST_KEY,
    ) -> None:
        self.redis_url = redis_url
        self.latest_key = latest_key
        self._client = _redis_from_url(redis_url)

    def latest(self, *, current_only: bool = True) -> Optional[MarketContextTick]:
        try:
            raw = self._client.get(self.latest_key)
        except Exception as exc:  # noqa: BLE001
            log.debug("redis market context latest failed: %s", exc)
            return None
        if not raw:
            return None
        try:
            tick = MarketContextTick.from_json(raw)
        except Exception as exc:  # noqa: BLE001
            log.warning("invalid redis market context payload: %s", exc)
            return None
        if current_only and not tick.is_current():
            return None
        return tick

    def close(self) -> None:
        try:
            self._client.close()
        except Exception:  # noqa: BLE001
            pass


def _redis_from_url(redis_url: str):
    try:
        import redis  # type: ignore
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("redis-py is required for market context IPC") from exc
    return redis.Redis.from_url(redis_url, decode_responses=False)
