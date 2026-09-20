from __future__ import annotations

import asyncio
import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from ..config import settings
from ..events.bus import bus
from ..models import CalibrationStatus
from ..time_utils import parse_utc_iso

log = logging.getLogger(__name__)

START_PATTERNS = [
    re.compile(r"calibrat", re.IGNORECASE),
    re.compile(r"SSVI\s*(surface\s*)?fit", re.IGNORECASE),
    re.compile(r"deribit.*atm.*iv", re.IGNORECASE),
    re.compile(r"fetching\s+options", re.IGNORECASE),
]


def _any_start(line: str) -> bool:
    return any(p.search(line) for p in START_PATTERNS)


class CalibrationWatcher:
    def __init__(self, log_paths: list[Path], terminal_reader) -> None:
        self._log_paths = log_paths
        self._terminal = terminal_reader
        self._offsets: dict[Path, int] = {}
        self._status = CalibrationStatus(active=False)
        self._last_terminal_ts: Optional[str] = None

    @property
    def status(self) -> CalibrationStatus:
        if self._status.active and self._status.started_at is not None:
            start = parse_utc_iso(self._status.started_at)
            if start is not None:
                now = datetime.now(timezone.utc)
                self._status.elapsed_s = max(0.0, (now - start).total_seconds())
        return self._status

    async def _emit(self, topic: str) -> None:
        await bus.publish(topic, self.status.model_dump())

    def _tail_logs(self) -> list[str]:
        new_lines: list[str] = []
        for p in self._log_paths:
            try:
                size = p.stat().st_size
            except FileNotFoundError:
                self._offsets.pop(p, None)
                continue
            off = self._offsets.get(p)
            if off is None:
                # First time — skip to EOF to avoid flood on startup
                self._offsets[p] = size
                continue
            if size < off:
                # Rotated or truncated
                off = 0
            if size == off:
                continue
            try:
                with p.open("r", encoding="utf-8", errors="replace") as f:
                    f.seek(off)
                    chunk = f.read()
                    self._offsets[p] = f.tell()
                for line in chunk.splitlines():
                    if line.strip():
                        new_lines.append(line)
            except OSError:
                continue
        return new_lines

    async def poll(self) -> None:
        # 1) Check logs for start markers
        for line in self._tail_logs():
            if _any_start(line) and not self._status.active:
                self._status = CalibrationStatus(
                    active=True,
                    started_at=datetime.now(timezone.utc).isoformat(),
                    elapsed_s=0.0,
                    last_timing=self._status.last_timing,
                )
                await self._emit("calibration.start")
                break

        # 2) If terminal_data.json has a new timestamp, calibration just ended
        snap = self._terminal.latest
        if snap and snap.timestamp and snap.timestamp != self._last_terminal_ts:
            self._last_terminal_ts = snap.timestamp
            if self._status.active:
                self._status = CalibrationStatus(
                    active=False,
                    started_at=None,
                    elapsed_s=None,
                    last_timing=snap.timing,
                )
                await self._emit("calibration.end")
            else:
                # First observation; just record last_timing
                self._status.last_timing = snap.timing

        # 3) Heuristic fallback — if terminal hasn't updated for too long, assume calibration
        if not self._status.active and snap is not None and snap.age_seconds is not None:
            if snap.age_seconds > settings.calibration_timeout_seconds:
                self._status = CalibrationStatus(
                    active=True,
                    started_at=datetime.now(timezone.utc).isoformat(),
                    elapsed_s=0.0,
                    last_timing=self._status.last_timing,
                )
                await self._emit("calibration.start")


async def run_calibration_loop(watcher: "CalibrationWatcher", stop: asyncio.Event) -> None:
    while not stop.is_set():
        try:
            await watcher.poll()
        except Exception:  # noqa: BLE001
            log.exception("calibration poll error")
        try:
            await asyncio.wait_for(stop.wait(), timeout=1.0)
        except asyncio.TimeoutError:
            pass
