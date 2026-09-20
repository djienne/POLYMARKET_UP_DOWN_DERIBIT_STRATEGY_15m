from __future__ import annotations

import asyncio

import pytest

from app.api import routes_ws
from app.api.routes_ws import (
    ConnContext,
    QUEUE_LIMIT,
    matches_view_source,
    normalize_terminal_payload_for_view,
)


class _Ws:
    def __init__(self) -> None:
        self.sent = []
        self.closed_codes = []

    async def send_json(self, payload):
        self.sent.append(payload)

    async def close(self, code, reason=""):
        self.closed_codes.append(code)


@pytest.mark.asyncio
async def test_ws_queue_requires_reconnect_instead_of_losing_critical_trades() -> None:
    ws = _Ws()
    ctx = ConnContext(ws, initial_instance_id=1, initial_source="live")

    for idx in range(QUEUE_LIMIT + 5):
        await ctx.enqueue("trade.append", {"idx": idx})

    assert ctx.closed
    assert len(ctx._queue) == 0
    assert ws.closed_codes == [1013]
    assert ctx.source == "live"


def test_ws_source_match_uses_trade_or_reset_source() -> None:
    assert matches_view_source({"run_source": "dry_run"}, "dry_run")
    assert matches_view_source({"source": "live"}, "live")
    assert not matches_view_source({"run_source": "dry_run"}, "live")


def test_ws_terminal_update_normalizes_missing_or_expired_slug(monkeypatch) -> None:
    monkeypatch.setattr(routes_ws.time, "time", lambda: 1_778_780_810)

    class _Hub:
        def _current_slug(self, source):
            assert source == "live"
            return "btc-updown-15m-1778780700"

    missing = normalize_terminal_payload_for_view(_Hub(), {"market": {}}, "live")
    expired = normalize_terminal_payload_for_view(
        _Hub(),
        {"market": {"slug": "btc-updown-15m-1778779800"}},
        "live",
    )

    for payload in (missing, expired):
        assert payload["market"]["slug"] == "btc-updown-15m-1778780700"
        assert payload["market"]["window_start_unix"] == 1778780700
        assert payload["market"]["window_end_unix"] == 1778781600


@pytest.mark.asyncio
@pytest.mark.parametrize("incoming", ["trade.append", "orderbook.update"])
async def test_ws_queue_drops_oldest_droppable_before_critical_message(incoming) -> None:
    ctx = ConnContext(_Ws(), initial_instance_id=1)

    await ctx.enqueue("liveness.tick", {"idx": "drop"})
    for idx in range(QUEUE_LIMIT - 1):
        await ctx.enqueue("trade.append", {"idx": idx})
    await ctx.enqueue(incoming, {"idx": "keep"})

    assert len(ctx._queue) == QUEUE_LIMIT
    assert all(topic != "liveness.tick" for topic, _ in ctx._queue)
    assert ctx._queue[-1][1]["idx"] == "keep"


@pytest.mark.asyncio
async def test_ws_sender_loop_drains_deque_queue() -> None:
    ws = _Ws()
    ctx = ConnContext(ws, initial_instance_id=1)
    task = asyncio.create_task(ctx.sender_loop())

    try:
        await ctx.enqueue("orderbook.update", {"idx": "chart"})
        await asyncio.wait_for(_wait_for_sent(ws, expected=1), timeout=1)
    finally:
        ctx.closed = True
        ctx._queue_event.set()
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    assert len(ws.sent) == 1
    assert ws.sent[0]["type"] == "orderbook.update"
    assert ws.sent[0]["data"] == {"idx": "chart"}


async def _wait_for_sent(ws: _Ws, expected: int) -> None:
    while len(ws.sent) < expected:
        await asyncio.sleep(0)
