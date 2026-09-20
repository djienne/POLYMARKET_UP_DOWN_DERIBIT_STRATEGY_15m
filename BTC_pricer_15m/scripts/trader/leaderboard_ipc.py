"""Redis publisher for 15m grid leaderboard snapshots."""

from __future__ import annotations

import csv
import json
import logging
import math
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import yaml

log = logging.getLogger(__name__)

DEFAULT_REDIS_URL = "redis://redis:6379/0"
DEFAULT_LATEST_KEY = "grid:leaderboard:15m:latest"
DEFAULT_STREAM = "grid.leaderboard.15m"
DEFAULT_CHANNEL = "grid.leaderboard.15m"


def _now_ms() -> int:
    return int(time.time() * 1000)


def _to_float(value: object) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _to_int(value: object) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _optional_float(value: object) -> float | None:
    try:
        number = float(value)
        return number if math.isfinite(number) else None
    except (TypeError, ValueError):
        return None


def _rows_from_csv(path: Path, *, top_n: int = 15) -> list[dict]:
    if not path.exists():
        return []
    rows: list[dict] = []
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for raw in reader:
            rows.append({
                "rank": _to_int(raw.get("rank")),
                "instance_id": _to_int(raw.get("instance_id")),
                "total_pnl": _to_float(raw.get("total_pnl")),
                "pnl_mean_to_sd": _optional_float(raw.get("pnl_mean_to_sd")),
                "max_drawdown": _optional_float(raw.get("max_drawdown")),
                "max_drawdown_pct": _optional_float(raw.get("max_drawdown_pct")),
                "wins": _to_int(raw.get("wins")),
                "losses": _to_int(raw.get("losses")),
                "win_rate": _to_float(raw.get("win_rate")),
                "trades": _to_int(raw.get("trades")),
                "params": {
                    "alpha_up": _to_float(raw.get("alpha_up")),
                    "alpha_down": _to_float(raw.get("alpha_down")),
                    "floor_up": _to_float(raw.get("floor_up")),
                    "floor_down": _to_float(raw.get("floor_down")),
                    "tp_pct": _to_float(raw.get("tp_pct")),
                    "sl_pct": _to_float(raw.get("sl_pct")),
                },
                "liquidity_mode": raw.get("liquidity_mode") or "independent",
                "experiment_id": raw.get("experiment_id") or "legacy",
                "as_of": raw.get("as_of") or None,
                "written_at": raw.get("written_at") or None,
            })
            if len(rows) >= top_n:
                break
    return rows


class RedisLeaderboardPublisher:
    def __init__(
        self,
        *,
        redis_url: str = DEFAULT_REDIS_URL,
        latest_key: str = DEFAULT_LATEST_KEY,
        stream: str = DEFAULT_STREAM,
        channel: str = DEFAULT_CHANNEL,
        maxlen: int = 1_000,
    ) -> None:
        self.redis_url = redis_url
        self.latest_key = latest_key
        self.stream = stream
        self.channel = channel
        self.maxlen = int(maxlen)
        self._sequence = 0
        self._client = _redis_from_url(redis_url)

    def publish_from_csv(self, path: Path, *, top_n: int = 15) -> None:
        rows = _rows_from_csv(path, top_n=top_n)
        self._sequence += 1
        payload = {
            "schema_version": 1,
            "source": "grid",
            "sequence": self._sequence,
            "published_at_ms": _now_ms(),
            "top": rows,
        }
        text = json.dumps(payload, separators=(",", ":"), sort_keys=True)
        pipe = self._client.pipeline(transaction=False)
        pipe.set(self.latest_key, text)
        pipe.xadd(
            self.stream,
            {"sequence": self._sequence, "payload": text},
            maxlen=self.maxlen,
            approximate=True,
        )
        pipe.publish(self.channel, text)
        pipe.execute()

    def close(self) -> None:
        try:
            self._client.close()
        except Exception:  # noqa: BLE001
            pass


@dataclass(frozen=True)
class LeaderboardIPCConfig:
    enabled: bool = False
    redis_url: str = DEFAULT_REDIS_URL
    latest_key: str = DEFAULT_LATEST_KEY
    stream: str = DEFAULT_STREAM
    channel: str = DEFAULT_CHANNEL
    redis_maxlen: int = 1_000

    @classmethod
    def from_file(cls, path: Path, *, fallback_redis_url: Optional[str] = None) -> "LeaderboardIPCConfig":
        try:
            if path.suffix.lower() in {".yaml", ".yml"}:
                loaded = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
            else:
                loaded = json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:  # noqa: BLE001
            log.warning("leaderboard IPC config unavailable from %s: %s", path, exc)
            return cls(redis_url=fallback_redis_url or DEFAULT_REDIS_URL)
        raw = dict((loaded or {}).get("leaderboard_ipc") or {})
        return cls(
            enabled=bool(raw.get("enabled", False)),
            redis_url=str(raw.get("redis_url") or fallback_redis_url or DEFAULT_REDIS_URL),
            latest_key=str(raw.get("latest_key") or DEFAULT_LATEST_KEY),
            stream=str(raw.get("stream") or DEFAULT_STREAM),
            channel=str(raw.get("channel") or DEFAULT_CHANNEL),
            redis_maxlen=int(raw.get("redis_maxlen") or 1_000),
        )


def build_leaderboard_publisher(
    path: Path,
    *,
    fallback_redis_url: Optional[str] = None,
) -> Optional[RedisLeaderboardPublisher]:
    cfg = LeaderboardIPCConfig.from_file(path, fallback_redis_url=fallback_redis_url)
    if not cfg.enabled:
        return None
    try:
        return RedisLeaderboardPublisher(
            redis_url=cfg.redis_url,
            latest_key=cfg.latest_key,
            stream=cfg.stream,
            channel=cfg.channel,
            maxlen=cfg.redis_maxlen,
        )
    except Exception as exc:  # noqa: BLE001
        log.warning("leaderboard Redis publisher disabled: %s", exc)
        return None


def _redis_from_url(redis_url: str):
    try:
        import redis  # type: ignore
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("redis-py is required for leaderboard IPC") from exc
    return redis.Redis.from_url(
        redis_url, decode_responses=False, socket_connect_timeout=2.0, socket_timeout=2.0,
    )
