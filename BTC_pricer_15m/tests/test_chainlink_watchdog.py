"""Tests for ChainlinkPriceFeed's unhealthy-socket watchdog."""

from __future__ import annotations

import asyncio
import time
from types import SimpleNamespace

import pytest

from scripts.trader.chainlink_feed import (
    ChainlinkPriceFeed,
    UNHEALTHY_CHECK_INTERVAL,
    UNHEALTHY_RECONNECT_SECONDS,
)


class _FakeWS:
    def __init__(self):
        self.closed = False

    async def close(self):
        self.closed = True


@pytest.mark.asyncio
async def test_watchdog_closes_socket_when_prices_go_stale(monkeypatch):
    """Force reconnect when no price lands for > UNHEALTHY_RECONNECT_SECONDS."""
    feed = ChainlinkPriceFeed()
    feed._running = True
    # Simulate a session that saw an early price, then nothing for too long.
    feed._last_price_time = time.time() - (UNHEALTHY_RECONNECT_SECONDS + 5)

    ws = _FakeWS()
    # Speed up the watchdog loop so the test doesn't wait 10s.
    monkeypatch.setattr("scripts.trader.chainlink_feed.UNHEALTHY_CHECK_INTERVAL", 0.01)

    # Also override the module constant inside the coroutine (closure-resolved
    # via module attribute, so monkeypatch above is sufficient).
    await asyncio.wait_for(feed._unhealthy_watchdog(ws), timeout=2.0)
    assert ws.closed is True


@pytest.mark.asyncio
async def test_watchdog_noop_before_first_price(monkeypatch):
    """Don't tear the socket down during the connect grace before first price."""
    feed = ChainlinkPriceFeed()
    feed._running = True
    feed._last_price_time = 0  # never seen a price yet

    ws = _FakeWS()
    monkeypatch.setattr("scripts.trader.chainlink_feed.UNHEALTHY_CHECK_INTERVAL", 0.01)

    # Stop the feed after a short spin — the watchdog should never close.
    async def _stop_soon():
        await asyncio.sleep(0.05)
        feed._running = False

    await asyncio.gather(feed._unhealthy_watchdog(ws), _stop_soon())
    assert ws.closed is False


@pytest.mark.asyncio
async def test_watchdog_noop_when_prices_fresh(monkeypatch):
    """Don't tear the socket down while prices are landing normally."""
    feed = ChainlinkPriceFeed()
    feed._running = True
    feed._last_price_time = time.time()  # price landed right now

    ws = _FakeWS()
    monkeypatch.setattr("scripts.trader.chainlink_feed.UNHEALTHY_CHECK_INTERVAL", 0.01)

    async def _stop_soon():
        await asyncio.sleep(0.05)
        feed._running = False

    await asyncio.gather(feed._unhealthy_watchdog(ws), _stop_soon())
    assert ws.closed is False


@pytest.mark.asyncio
async def test_watchdog_swallows_close_errors(monkeypatch):
    """A raising close() should not propagate — we just want the session to end."""

    class _BrokenWS:
        def __init__(self):
            self.attempted = False

        async def close(self):
            self.attempted = True
            raise RuntimeError("already closed")

    feed = ChainlinkPriceFeed()
    feed._running = True
    feed._last_price_time = time.time() - (UNHEALTHY_RECONNECT_SECONDS + 5)

    ws = _BrokenWS()
    monkeypatch.setattr("scripts.trader.chainlink_feed.UNHEALTHY_CHECK_INTERVAL", 0.01)

    await asyncio.wait_for(feed._unhealthy_watchdog(ws), timeout=2.0)
    assert ws.attempted is True  # attempted to close; exception swallowed
