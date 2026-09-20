from __future__ import annotations

import asyncio
import logging
import time
from typing import Optional


log = logging.getLogger(__name__)


class EventLoopLagMonitor:
    def __init__(self, *, interval_s: float = 1.0, warn_ms: float = 500.0) -> None:
        self.interval_s = interval_s
        self.warn_ms = warn_ms
        self.last_lag_ms: Optional[float] = None
        self.max_lag_ms: Optional[float] = None
        self.last_warn_at: Optional[float] = None

    def snapshot(self) -> dict:
        return {
            "event_loop_lag_ms": self.last_lag_ms,
            "event_loop_max_lag_ms": self.max_lag_ms,
        }

    async def run(self, stop: asyncio.Event) -> None:
        expected = time.perf_counter() + self.interval_s
        while not stop.is_set():
            try:
                await asyncio.wait_for(stop.wait(), timeout=self.interval_s)
                break
            except asyncio.TimeoutError:
                pass
            now = time.perf_counter()
            lag_ms = max(0.0, (now - expected) * 1000.0)
            self.last_lag_ms = lag_ms
            self.max_lag_ms = lag_ms if self.max_lag_ms is None else max(self.max_lag_ms, lag_ms)
            expected = now + self.interval_s
            if lag_ms >= self.warn_ms:
                wall_now = time.time()
                if self.last_warn_at is None or wall_now - self.last_warn_at >= 10.0:
                    log.warning("dashboard event loop lag %.1fms", lag_ms)
                    self.last_warn_at = wall_now


loop_lag_monitor = EventLoopLagMonitor()


async def run_event_loop_monitor(stop: asyncio.Event) -> None:
    await loop_lag_monitor.run(stop)
