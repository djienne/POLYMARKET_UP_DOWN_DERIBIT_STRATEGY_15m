"""Tests for ChainlinkPriceFeed lookups.

Focused on the new ``get_first_at_or_after`` helper and the legacy
``get_price_at`` it complements. We stub the buffer directly to avoid
spinning up the real WebSocket.
"""

from __future__ import annotations

from datetime import datetime, timezone

from scripts.trader.chainlink_feed import ChainlinkPriceFeed


def _seed(feed: ChainlinkPriceFeed, points: list[tuple[float, float]]) -> None:
    """Seed the feed buffer with ``(timestamp_seconds, price)`` tuples."""
    with feed._lock:
        for ts_s, price in points:
            feed._buffer.append((int(ts_s * 1000), price))


def _ts(seconds: float) -> datetime:
    return datetime.fromtimestamp(seconds, tz=timezone.utc)


def test_get_first_at_or_after_returns_earliest_future_point():
    feed = ChainlinkPriceFeed()
    _seed(
        feed,
        [
            (1000.0, 70_000.0),  # 5s before target — must be skipped
            (1006.0, 70_100.0),  # 1s after target — winner
            (1015.0, 70_200.0),  # 10s after target — loser
        ],
    )
    price, offset = feed.get_first_at_or_after(_ts(1005.0))
    assert price == 70_100.0
    assert offset is not None and abs(offset - 1.0) < 1e-9


def test_get_first_at_or_after_returns_zero_offset_on_exact_match():
    feed = ChainlinkPriceFeed()
    _seed(feed, [(1000.0, 70_500.0)])
    price, offset = feed.get_first_at_or_after(_ts(1000.0))
    assert price == 70_500.0
    assert offset == 0.0


def test_get_first_at_or_after_skips_pre_window_points():
    feed = ChainlinkPriceFeed()
    _seed(
        feed,
        [
            (995.0, 70_000.0),  # 5s before — skip
            (998.0, 70_100.0),  # 2s before — skip
        ],
    )
    price, offset = feed.get_first_at_or_after(_ts(1000.0))
    assert price is None
    assert offset is None


def test_get_first_at_or_after_respects_max_offset():
    feed = ChainlinkPriceFeed()
    _seed(feed, [(1015.0, 70_000.0)])  # 15s after target
    # Default max_offset_s=10 → reject.
    assert feed.get_first_at_or_after(_ts(1000.0)) == (None, None)
    # Loose tolerance accepts the same point.
    price, offset = feed.get_first_at_or_after(_ts(1000.0), max_offset_s=20.0)
    assert price == 70_000.0
    assert offset is not None and abs(offset - 15.0) < 1e-9


def test_get_first_at_or_after_empty_buffer():
    feed = ChainlinkPriceFeed()
    assert feed.get_first_at_or_after(_ts(1000.0)) == (None, None)


def test_get_price_at_unchanged_takes_closest_in_either_direction():
    """Regression: the legacy spot-lookup helper still picks the closest
    point on either side, distinct from get_first_at_or_after."""
    feed = ChainlinkPriceFeed()
    _seed(
        feed,
        [
            (995.0, 70_000.0),  # 5s before — closest
            (1010.0, 70_100.0),  # 10s after
        ],
    )
    feed._last_price_time = 1_000_000  # bypass health check internals
    # Don't actually exercise health here — get_price_at doesn't gate on it.
    price = feed.get_price_at(_ts(1000.0))
    assert price == 70_000.0
