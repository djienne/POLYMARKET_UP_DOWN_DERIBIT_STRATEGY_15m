from __future__ import annotations

import asyncio
import ctypes
import json
import logging
import os
import time
from pathlib import Path
from typing import Optional

from ..config import settings
from ..events.bus import bus
from ..models import LivenessInfo
from .location_probe import probe as location_probe

log = logging.getLogger(__name__)


class _FileTime(ctypes.Structure):
    _fields_ = [
        ("dwLowDateTime", ctypes.c_ulong),
        ("dwHighDateTime", ctypes.c_ulong),
    ]


def _filetime_to_int(ft: _FileTime) -> int:
    return (ft.dwHighDateTime << 32) | ft.dwLowDateTime


def _read_windows_cpu_times() -> Optional[tuple[int, int]]:
    idle = _FileTime()
    kernel = _FileTime()
    user = _FileTime()
    try:
        ok = ctypes.windll.kernel32.GetSystemTimes(  # type: ignore[attr-defined]
            ctypes.byref(idle),
            ctypes.byref(kernel),
            ctypes.byref(user),
        )
    except Exception:
        return None
    if ok == 0:
        return None
    idle_i = _filetime_to_int(idle)
    total_i = _filetime_to_int(kernel) + _filetime_to_int(user)
    return idle_i, total_i


def _read_linux_cpu_times() -> Optional[tuple[int, int]]:
    try:
        with open("/proc/stat", "r", encoding="utf-8") as f:
            line = f.readline()
    except OSError:
        return None
    parts = line.split()
    if len(parts) < 5 or parts[0] != "cpu":
        return None
    values = [int(v) for v in parts[1:]]
    idle = values[3] + (values[4] if len(values) > 4 else 0)
    total = sum(values)
    return idle, total


def _read_cpu_times() -> Optional[tuple[int, int]]:
    if os.name == "nt":
        return _read_windows_cpu_times()
    if os.name == "posix":
        return _read_linux_cpu_times()
    return None


class CpuSampler:
    def __init__(self) -> None:
        self._last: Optional[tuple[int, int]] = None
        self._last_pct: Optional[float] = None

    def sample(self) -> Optional[float]:
        cur = _read_cpu_times()
        if cur is None:
            return self._last_pct
        if self._last is None:
            self._last = cur
            return self._last_pct
        prev_idle, prev_total = self._last
        idle, total = cur
        self._last = cur
        delta_total = total - prev_total
        delta_idle = idle - prev_idle
        if delta_total <= 0:
            return self._last_pct
        pct = max(0.0, min(100.0, (1.0 - (delta_idle / delta_total)) * 100.0))
        self._last_pct = pct
        return pct


_cpu_sampler = CpuSampler()


def _mtime(path: Path) -> Optional[float]:
    try:
        return path.stat().st_mtime
    except FileNotFoundError:
        return None


def _active_lock_paths() -> list[Path]:
    if settings.mode == "live":
        return [settings.resolved_results_dir / "single_trader.lock"]
    return [settings.lock_path()]


def _active_lock_exists() -> bool:
    return any(path.exists() for path in _active_lock_paths())


def _heartbeat_paths() -> list[Path]:
    # Each trader touches its own lock in its main loop, including while idle.
    # Shared calc/market output says nothing about a stalled execution process.
    return _active_lock_paths()


def _freshest_mtime(paths: list[Path]) -> Optional[float]:
    mtimes = [_mtime(path) for path in paths]
    existing = [mtime for mtime in mtimes if mtime is not None]
    if not existing:
        return None
    return max(existing)


def _live_position_status() -> tuple[bool, Optional[str], Optional[str]]:
    try:
        raw = json.loads(settings.live_state_path().read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, json.JSONDecodeError):
        return False, None, None
    if not isinstance(raw, dict):
        return False, None, None
    positions = raw.get("open_positions")
    if not isinstance(positions, list) or not positions:
        return False, None, None
    pos = next((item for item in positions if isinstance(item, dict)), None)
    if pos is None:
        return False, None, None
    direction = pos.get("direction")
    if direction not in {"UP", "DOWN"}:
        direction = None
    market_id = pos.get("market_id")
    if not isinstance(market_id, str) or not market_id:
        market_id = None
    return True, direction, market_id


def current_liveness() -> LivenessInfo:
    lock_exists = _active_lock_exists()
    term_mtime = _freshest_mtime(_heartbeat_paths())
    age = None
    if term_mtime is not None:
        age = max(0.0, time.time() - term_mtime)
    fresh = age is not None and age < 60.0
    location = location_probe.read_location()
    ping_ms, ping_age_s, label = location_probe.active_ping()
    if settings.mode == "dry_run" and location == "stopped":
        location = "local"

    cpu_pct = _cpu_sampler.sample()
    (
        live_position_open,
        live_position_direction,
        live_position_market_id,
    ) = _live_position_status()

    return LivenessInfo(
        bot_live=lock_exists and fresh,
        lock_exists=lock_exists,
        terminal_age_s=age,
        cpu_pct=cpu_pct,
        live_position_open=live_position_open,
        live_position_direction=live_position_direction,
        live_position_market_id=live_position_market_id,
        execution_location=location,
        execution_label=label,
        polymarket_ping_ms=ping_ms,
        polymarket_ping_age_s=ping_age_s,
    )


async def run_liveness_loop(stop: asyncio.Event) -> None:
    last: Optional[bool] = None
    while not stop.is_set():
        info = current_liveness()
        if info.bot_live != last:
            last = info.bot_live
            await bus.publish("liveness.update", info.model_dump())
        # Always publish terminal_age tick so UI can show seconds since last tick
        await bus.publish("liveness.tick", info.model_dump())
        try:
            await asyncio.wait_for(stop.wait(), timeout=settings.state_poll_interval_seconds)
        except asyncio.TimeoutError:
            pass
