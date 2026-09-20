"""Local Polymarket latency probe.

The 15m live trader is local-Docker-only. This module reports the local
execution marker plus a warm trader CLOB latency when available, falling back
to a dashboard-side HTTP probe.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Optional

import httpx

from ..config import settings

log = logging.getLogger(__name__)


@dataclass
class PingResult:
    ms: Optional[float] = None
    measured_at: Optional[float] = None

    def age_s(self) -> Optional[float]:
        if self.measured_at is None:
            return None
        return max(0.0, time.time() - self.measured_at)


class LocationProbe:
    def __init__(self) -> None:
        self._local = PingResult()

    def read_location(self) -> str:
        try:
            val = settings.live_location_path().read_text(encoding="utf-8").strip()
        except OSError:
            return "unknown"
        return val if val in ("local", "stopped") else "unknown"

    def _read_trader_measured(self) -> Optional[PingResult]:
        path = settings.resolved_results_dir / ".clob_latency_ms"
        try:
            content = path.read_text(encoding="utf-8").strip()
        except OSError:
            return None
        parts = content.split()
        if not parts:
            return None
        try:
            ms = float(parts[0])
            epoch = int(parts[2]) if len(parts) > 2 else None
        except (ValueError, IndexError):
            return None
        return PingResult(ms=ms, measured_at=float(epoch) if epoch else None)

    def active_ping(self) -> tuple[Optional[float], Optional[float], Optional[str]]:
        trader = self._read_trader_measured()
        if trader is not None and trader.ms is not None:
            age = trader.age_s()
            if age is None or age <= 900:
                return trader.ms, age, "local"
        return self._local.ms, self._local.age_s(), "local"

    async def measure_local(self) -> None:
        url = f"{settings.polymarket_clob_url}/markets?limit=1"
        try:
            t0 = time.perf_counter()
            async with httpx.AsyncClient(timeout=settings.polymarket_request_timeout_seconds) as client:
                resp = await client.get(url)
            elapsed_ms = (time.perf_counter() - t0) * 1000.0
            if resp.status_code == 200:
                self._local = PingResult(ms=elapsed_ms, measured_at=time.time())
        except Exception as exc:  # noqa: BLE001
            log.debug("local polymarket probe failed: %s", exc)

probe = LocationProbe()


async def run_location_probe_loop(stop: asyncio.Event) -> None:
    interval = settings.polymarket_probe_interval_seconds
    while not stop.is_set():
        try:
            await probe.measure_local()
        except Exception:  # noqa: BLE001
            log.exception("location probe iteration failed")
        try:
            await asyncio.wait_for(stop.wait(), timeout=interval)
        except asyncio.TimeoutError:
            pass
