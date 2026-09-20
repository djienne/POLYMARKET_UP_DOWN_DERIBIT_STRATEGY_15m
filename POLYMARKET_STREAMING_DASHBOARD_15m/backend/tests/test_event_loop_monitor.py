import asyncio
import pytest
from app.collector import event_loop_monitor as module


@pytest.mark.asyncio
async def test_event_loop_monitor_measures_lag_and_retains_maximum(monkeypatch):
    monitor = module.EventLoopLagMonitor(interval_s=1)
    stop = asyncio.Event()
    now = 0.0
    wakes = iter([2.2, 3.4, None])
    async def wake(awaitable, timeout):
        nonlocal now
        awaitable.close()
        next_time = next(wakes)
        if next_time is None:
            stop.set()
            return True
        now = next_time
        raise asyncio.TimeoutError
    monkeypatch.setattr(module.time, "perf_counter", lambda: now)
    monkeypatch.setattr(module.asyncio, "wait_for", wake)
    await monitor.run(stop)
    assert monitor.snapshot()["event_loop_lag_ms"] == pytest.approx(200)
    assert monitor.snapshot()["event_loop_max_lag_ms"] == pytest.approx(1200)
