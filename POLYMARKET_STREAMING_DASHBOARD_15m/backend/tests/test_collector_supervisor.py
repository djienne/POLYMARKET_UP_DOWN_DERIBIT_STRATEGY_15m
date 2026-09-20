from __future__ import annotations

import asyncio
import logging

import pytest

from app.main import _collector_supervisor


@pytest.mark.asyncio
async def test_supervisor_logs_when_collector_task_dies(caplog) -> None:
    stop = asyncio.Event()

    async def boom() -> None:
        raise RuntimeError("collector exploded")

    task = asyncio.create_task(boom(), name="boom_loop")
    task.add_done_callback(_collector_supervisor(stop))
    with caplog.at_level(logging.ERROR, logger="app.main"):
        with pytest.raises(RuntimeError):
            await task
        await asyncio.sleep(0)  # let the done-callback run

    assert any("boom_loop" in rec.message and "died" in rec.message for rec in caplog.records)


@pytest.mark.asyncio
async def test_supervisor_silent_on_cancel_and_shutdown(caplog) -> None:
    stop = asyncio.Event()

    async def forever() -> None:
        await asyncio.Event().wait()

    task = asyncio.create_task(forever(), name="forever_loop")
    task.add_done_callback(_collector_supervisor(stop))
    with caplog.at_level(logging.ERROR, logger="app.main"):
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        await asyncio.sleep(0)

    assert not caplog.records

    # graceful exit after stop is set: also silent
    stop.set()

    async def graceful() -> None:
        return None

    task2 = asyncio.create_task(graceful(), name="graceful_loop")
    task2.add_done_callback(_collector_supervisor(stop))
    await task2
    await asyncio.sleep(0)

    assert not caplog.records
