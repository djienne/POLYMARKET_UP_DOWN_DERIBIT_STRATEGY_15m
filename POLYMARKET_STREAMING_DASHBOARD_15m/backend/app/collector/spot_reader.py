from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Optional

from ..config import settings
from ..events.bus import bus
from ..models import SpotInfo

log = logging.getLogger(__name__)


def _spot_from_payload(raw: dict) -> SpotInfo:
    published_ms = raw.get("published_at_ms")
    age = None
    if published_ms is not None:
        try:
            age = max(0.0, time.time() - (int(published_ms) / 1000.0))
        except (TypeError, ValueError):
            age = None
    return SpotInfo(
        schema_version=int(raw.get("schema_version", 1)),
        symbol=str(raw.get("symbol") or "BTC/USD"),
        price=float(raw["price"]) if raw.get("price") is not None else None,
        source=raw.get("source"),
        source_label=raw.get("source_label"),
        source_timestamp_ms=raw.get("source_timestamp_ms"),
        received_at_ms=raw.get("received_at_ms"),
        published_at_ms=raw.get("published_at_ms"),
        source_age_seconds=raw.get("source_age_seconds"),
        receive_age_seconds=raw.get("receive_age_seconds"),
        sequence=raw.get("sequence"),
        stale_after_seconds=raw.get("stale_after_seconds"),
        age_seconds=age,
        details=dict(raw.get("details") or {}),
    )


class RedisSpotReader:
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
        self._latest: Optional[SpotInfo] = None
        self._last_sequence: Optional[int] = None
        self._connected: bool = False
        self._reconnects: int = 0
        self._last_pubsub_at: Optional[float] = None

    @property
    def latest(self) -> Optional[SpotInfo]:
        if self._latest is None:
            return None
        return self._with_fresh_age(self._latest)

    @staticmethod
    def _with_fresh_age(spot: SpotInfo) -> SpotInfo:
        if spot.published_at_ms is None:
            return spot
        age = max(0.0, time.time() - (int(spot.published_at_ms) / 1000.0))
        return spot.model_copy(update={"age_seconds": age})

    def health(self) -> dict:
        latest = self.latest
        return {
            "spot_pubsub_connected": self._connected,
            "spot_age_seconds": latest.age_seconds if latest is not None else None,
            "spot_sequence": latest.sequence if latest is not None else None,
            "spot_source": latest.source if latest is not None else None,
            "spot_reader_reconnects": self._reconnects,
            "spot_last_pubsub_age_seconds": (
                max(0.0, time.time() - self._last_pubsub_at)
                if self._last_pubsub_at is not None
                else None
            ),
        }

    def _client(self):
        import redis  # type: ignore

        return redis.Redis.from_url(self.redis_url, decode_responses=False)

    def load_latest(self) -> Optional[SpotInfo]:
        client = self._client()
        try:
            raw = client.get(self.latest_key)
        finally:
            client.close()
        if not raw:
            return None
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        spot = _spot_from_payload(json.loads(raw))
        self._record(spot)
        return spot

    def _record(self, spot: SpotInfo, *, from_pubsub: bool = False) -> bool:
        spot = self._with_fresh_age(spot)
        if from_pubsub:
            self._last_pubsub_at = time.time()
        seq = spot.sequence
        if seq is not None and self._last_sequence == seq:
            self._latest = spot
            return False
        self._latest = spot
        self._last_sequence = seq
        return True

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            client = None
            pubsub = None
            try:
                spot = await asyncio.to_thread(self.load_latest)
                if spot is not None:
                    await bus.publish("spot.update", spot.model_dump())
                client = self._client()
                pubsub = client.pubsub(ignore_subscribe_messages=True)
                pubsub.subscribe(self.channel)
                self._connected = True
                log.info("subscribed to shared spot channel %s", self.channel)
                while not stop.is_set():
                    message = await asyncio.to_thread(pubsub.get_message, False, 1.0)
                    if not message or message.get("type") != "message":
                        continue
                    data = message.get("data")
                    if isinstance(data, bytes):
                        data = data.decode("utf-8")
                    spot = _spot_from_payload(json.loads(data))
                    if self._record(spot, from_pubsub=True):
                        await bus.publish("spot.update", spot.model_dump())
            except Exception as exc:  # noqa: BLE001
                self._connected = False
                self._reconnects += 1
                if not stop.is_set():
                    log.warning("shared spot reader failed: %s", exc)
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


async def run_spot_loop(reader: RedisSpotReader, stop: asyncio.Event) -> None:
    await reader.run(stop)


def build_default_spot_reader() -> RedisSpotReader:
    return RedisSpotReader(
        redis_url=settings.redis_url,
        latest_key=settings.spot_latest_key,
        channel=settings.spot_channel,
    )
