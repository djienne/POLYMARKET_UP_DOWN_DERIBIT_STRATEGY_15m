from __future__ import annotations

import asyncio
import json
import logging

from ..config import settings
from ..events.bus import bus
from ..models import LeaderboardRow
from .leaderboard_reader import LeaderboardReader

log = logging.getLogger(__name__)


def _rows_from_payload(raw: dict) -> list[LeaderboardRow]:
    rows = raw.get("top") or []
    if not isinstance(rows, list):
        return []
    parsed: list[LeaderboardRow] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        try:
            parsed.append(LeaderboardRow.model_validate(row))
        except Exception as exc:  # noqa: BLE001
            log.warning("bad redis leaderboard row skipped: %s", exc)
    return parsed


class RedisLeaderboardReader:
    def __init__(
        self,
        *,
        redis_url: str,
        latest_key: str,
        channel: str,
        reader: LeaderboardReader,
    ) -> None:
        self.redis_url = redis_url
        self.latest_key = latest_key
        self.channel = channel
        self.reader = reader
        self._last_sequence = None

    def _client(self):
        import redis  # type: ignore

        return redis.Redis.from_url(self.redis_url, decode_responses=False)

    def _apply_payload(self, payload: str | bytes) -> bool:
        if isinstance(payload, bytes):
            payload = payload.decode("utf-8")
        raw = json.loads(payload)
        sequence = raw.get("sequence")
        if sequence is not None and sequence == self._last_sequence:
            return False
        rows = _rows_from_payload(raw)
        self.reader.apply_rows(rows)
        self._last_sequence = sequence
        return True

    def load_latest(self) -> bool:
        client = self._client()
        try:
            raw = client.get(self.latest_key)
        finally:
            client.close()
        if not raw:
            return False
        return self._apply_payload(raw)

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            client = None
            pubsub = None
            try:
                if await asyncio.to_thread(self.load_latest):
                    await bus.publish(
                        "leaderboard.update",
                        {"top": [r.model_dump() for r in self.reader.top(15)]},
                    )
                client = self._client()
                pubsub = client.pubsub(ignore_subscribe_messages=True)
                pubsub.subscribe(self.channel)
                log.info("subscribed to leaderboard channel %s", self.channel)
                while not stop.is_set():
                    message = await asyncio.to_thread(pubsub.get_message, False, 1.0)
                    if not message or message.get("type") != "message":
                        continue
                    if self._apply_payload(message.get("data")):
                        await bus.publish(
                            "leaderboard.update",
                            {"top": [r.model_dump() for r in self.reader.top(15)]},
                        )
            except Exception as exc:  # noqa: BLE001
                if not stop.is_set():
                    log.warning("redis leaderboard reader failed: %s", exc)
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


async def run_redis_leaderboard_loop(reader: RedisLeaderboardReader, stop: asyncio.Event) -> None:
    await reader.run(stop)


def build_default_redis_leaderboard_reader(
    leaderboard: LeaderboardReader,
) -> RedisLeaderboardReader:
    return RedisLeaderboardReader(
        redis_url=settings.redis_url,
        latest_key=settings.leaderboard_latest_key,
        channel=settings.leaderboard_channel,
        reader=leaderboard,
    )
