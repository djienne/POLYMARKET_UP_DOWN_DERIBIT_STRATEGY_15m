"""Tails `docker logs -t <container>` for the bot's model-probability ticks.

Polymarket prices are sourced from `polymarket_client.py` (direct CLOB API) — this
collector is intentionally narrow: it only extracts the `Model: UP=X% DOWN=Y%` portion
of each `grid_trader:` tick line so the chart's model line refreshes on every tick
without waiting for the next `terminal_data.json` write.
"""
from __future__ import annotations

import asyncio
import logging
import re
import httpx
from collections import deque
from datetime import datetime, timezone
from typing import Deque, Optional

from ..config import settings
from ..events.bus import bus
from ..time_utils import iso_to_unix

log = logging.getLogger(__name__)

MAX_POINTS = 1000

# With `docker logs -t`, every line is prefixed with an RFC3339Nano UTC timestamp:
#   2026-04-20T17:28:43.123456789Z grid_trader: Tick ...  Model: UP=15.9% DOWN=83.9%  |  ...
MODEL_RE = re.compile(r"Model:\s*UP=([\d.]+)%\s*DOWN=([\d.]+)%")
SLUG_RE = re.compile(r"btc-updown-15m-(\d+)")


def _parse_rfc3339_prefix(line: str) -> Optional[datetime]:
    head = line.split(" ", 1)[0]
    if not head:
        return None
    s = head.rstrip("Z")
    if "." in s:
        main, frac = s.split(".", 1)
        frac = frac[:6].ljust(6, "0") if frac else "000000"
        s = f"{main}.{frac}"
    try:
        return datetime.fromisoformat(s).replace(tzinfo=timezone.utc)
    except ValueError:
        return None


class DockerLogTail:
    """Streams model UP/DOWN probabilities from the bot's Docker container logs."""

    def __init__(
        self,
        container: str,
        slug_fn: Optional[object] = None,
    ) -> None:
        self.container = container
        # Callable returning the current market slug. When slug changes between polls,
        # model series auto-reset. tick lines don't include the slug so we can't rely
        # on SLUG_RE in log lines alone.
        self._slug_fn = slug_fn
        self._model_up: Deque[tuple[str, float]] = deque(maxlen=MAX_POINTS)
        self._model_down: Deque[tuple[str, float]] = deque(maxlen=MAX_POINTS)
        self._cur_slug: Optional[str] = None
        self._history_seeded_slug: Optional[str] = None
        self._seen: set[str] = set()
        self._last_up: Optional[float] = None
        self._last_down: Optional[float] = None
        self._disabled = False

    @property
    def disabled(self) -> bool:
        return self._disabled

    def model_series(self, side: str) -> list[dict]:
        dq = self._model_up if side == "UP" else self._model_down
        return [{"t": t, "v": v} for (t, v) in dq]

    @property
    def latest_model(self) -> tuple[Optional[float], Optional[float]]:
        return self._last_up, self._last_down

    def reset_for_slug(self, slug: Optional[str]) -> None:
        if slug and slug != self._cur_slug:
            self._cur_slug = slug
            self._history_seeded_slug = None
            self._model_up.clear()
            self._model_down.clear()
            self._seen.clear()

    @staticmethod
    def _window_start(slug: Optional[str]) -> Optional[int]:
        if not slug:
            return None
        m = SLUG_RE.search(slug)
        if not m:
            return None
        return int(m.group(1))

    @staticmethod
    def _first_ts(dq: Deque[tuple[str, float]]) -> Optional[int]:
        if not dq:
            return None
        ts = iso_to_unix(dq[0][0])
        return int(ts) if ts is not None else None

    def _parse_line(self, line: str) -> bool:
        m_slug = SLUG_RE.search(line)
        if m_slug:
            self.reset_for_slug(f"btc-updown-15m-{m_slug.group(1)}")
        m = MODEL_RE.search(line)
        if not m:
            return False
        up_pct = float(m.group(1)) / 100.0
        down_pct = float(m.group(2)) / 100.0
        stamp = _parse_rfc3339_prefix(line)
        if stamp is None:
            stamp = datetime.now(timezone.utc)
        ts = stamp.isoformat()
        key = f"{ts}|{up_pct:.4f}|{down_pct:.4f}"
        if key in self._seen:
            return False
        self._seen.add(key)
        self._model_up.append((ts, up_pct))
        self._model_down.append((ts, down_pct))
        self._last_up = up_pct
        self._last_down = down_pct
        return True

    async def poll(self, since_seconds: float) -> bool:
        if self._disabled or not self.container:
            return False
        # Auto-reset on market boundary — tick lines don't carry the slug, so we rely
        # on the external slug_fn (state_hub._current_slug) to detect the transition.
        if self._slug_fn is not None:
            try:
                cur = self._slug_fn()
            except Exception:
                cur = None
            if cur:
                self.reset_for_slug(cur)
        changed = False
        if self._cur_slug and self._history_seeded_slug != self._cur_slug:
            start_ts = self._window_start(self._cur_slug)
            first_up = self._first_ts(self._model_up)
            first_down = self._first_ts(self._model_down)
            have_window_start = (
                start_ts is not None
                and first_up is not None and first_up <= start_ts + 5
                and first_down is not None and first_down <= start_ts + 5
            )
            if not have_window_start and start_ts is not None:
                now = int(datetime.now(timezone.utc).timestamp())
                backfill_changed = await self._read_logs(max(since_seconds, now - start_ts + 1))
                changed = changed or backfill_changed
            self._history_seeded_slug = self._cur_slug

        recent_changed = await self._read_logs(since_seconds)
        return changed or recent_changed

    async def _read_logs(self, since_seconds: float) -> bool:
        try:
            async with httpx.AsyncClient(timeout=8.0) as client:
                response = await client.get(
                    f"{settings.docker_logs_url.rstrip('/')}/logs",
                    params={"since_seconds": min(1800, max(1, int(since_seconds)))},
                )
                response.raise_for_status()
        except Exception:  # noqa: BLE001
            log.warning("model log adapter unavailable", exc_info=False)
            return False
        changed = False
        for line in response.text.splitlines():
            if self._parse_line(line):
                changed = True
        return changed


async def run_docker_log_loop(tail: "DockerLogTail", stop: asyncio.Event) -> None:
    if not tail.container:
        return
    interval = settings.docker_poll_interval_seconds
    # Pick a `since` that overlaps the prior poll so we don't miss a tick.
    since = max(3.0, interval * 3)
    while not stop.is_set():
        try:
            if await tail.poll(since):
                await bus.publish(
                    "model.update",
                    {
                        "series_up": tail.model_series("UP"),
                        "series_down": tail.model_series("DOWN"),
                    },
                )
        except Exception:
            log.exception("docker log loop")
        if tail.disabled:
            return
        try:
            await asyncio.wait_for(stop.wait(), timeout=interval)
        except asyncio.TimeoutError:
            pass
