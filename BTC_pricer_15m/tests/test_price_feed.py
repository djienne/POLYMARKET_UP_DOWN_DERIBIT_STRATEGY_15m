"""Tests for Chainlink price feed and price sanity checks."""

import json
import time
import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import patch, MagicMock

from scripts.polymarket_btc_15m import (
    _CHAINLINK_SNAPSHOT_SUB,
    _is_sane_btc_price,
    get_binance_price_at,
    get_chainlink_btc_price,
    get_chainlink_price_at,
)
from scripts.trader.chainlink_feed import (
    CHAINLINK_SUBSCRIPTION,
    ChainlinkPriceFeed,
    _is_sane_price,
)


class TestChainlinkSubscription:
    """Lock the Polymarket RTDS live-update subscription shape."""

    def test_uses_distinct_spot_and_sixty_second_twap_subscriptions(self):
        assert CHAINLINK_SUBSCRIPTION["action"] == "subscribe"
        subscriptions = CHAINLINK_SUBSCRIPTION["subscriptions"]
        assert len(subscriptions) == 2
        assert subscriptions[0] == {
            "topic": "crypto_prices_chainlink",
            "type": "update",
        }
        assert "filters" not in subscriptions[0]
        assert subscriptions[1] == {"topic": "crypto_prices_twap_sixty", "type": "update", "filters": '{"symbol":"btc/usd"}'}


class TestSanityCheck:
    """Price sanity bounds validation."""

    def test_valid_price(self):
        assert _is_sane_btc_price(70000)
        assert _is_sane_btc_price(50000.5)
        assert _is_sane_btc_price(100000)

    def test_zero(self):
        assert not _is_sane_btc_price(0)

    def test_negative(self):
        assert not _is_sane_btc_price(-100)

    def test_too_low(self):
        assert not _is_sane_btc_price(5000)

    def test_too_high(self):
        assert not _is_sane_btc_price(1_500_000)

    def test_none(self):
        assert not _is_sane_btc_price(None)

    def test_string(self):
        assert not _is_sane_btc_price("70000")

    def test_boundary_low(self):
        assert not _is_sane_btc_price(10_000)  # Exclusive
        assert _is_sane_btc_price(10_001)

    def test_boundary_high(self):
        assert not _is_sane_btc_price(1_000_000)  # Exclusive
        assert _is_sane_btc_price(999_999)

    def test_binance_kline_insane_price_returns_none(self, monkeypatch):
        class Resp:
            def raise_for_status(self):
                return None

            def json(self):
                return [[0, "5.00"]]

        monkeypatch.setattr(
            "scripts.polymarket_btc_15m._SESSION.get",
            lambda *args, **kwargs: Resp(),
        )

        assert get_binance_price_at(datetime.now(timezone.utc)) is None


class TestChainlinkFeedProcessMessage:
    """Test message parsing and buffer management."""

    def test_valid_message(self):
        feed = ChainlinkPriceFeed()
        msg = json.dumps({
            "topic": "crypto_prices",
            "type": "update",
            "timestamp": int(time.time() * 1000),
            "payload": {
                "data": [
                    {"timestamp": int(time.time() * 1000), "value": 71000.0},
                    {"timestamp": int(time.time() * 1000) + 1000, "value": 71001.5},
                ]
            }
        })
        feed._process_message(msg)
        assert feed.buffer_size == 2
        assert feed.get_latest_price() == 71001.5

    def test_empty_message(self):
        feed = ChainlinkPriceFeed()
        feed._process_message("")
        assert feed.buffer_size == 0

    def test_invalid_json(self):
        feed = ChainlinkPriceFeed()
        feed._process_message("not json")
        assert feed.buffer_size == 0

    def test_missing_payload(self):
        feed = ChainlinkPriceFeed()
        feed._process_message(json.dumps({"topic": "test"}))
        assert feed.buffer_size == 0

    def test_insane_price_rejected(self):
        feed = ChainlinkPriceFeed()
        msg = json.dumps({
            "payload": {
                "data": [
                    {"timestamp": int(time.time() * 1000), "value": 5.0},  # Insane
                    {"timestamp": int(time.time() * 1000), "value": 71000.0},  # OK
                ]
            }
        })
        feed._process_message(msg)
        assert feed.buffer_size == 1  # Only sane price kept
        assert feed.get_latest_price() == 71000.0

    def test_missing_keys_in_data_points(self):
        feed = ChainlinkPriceFeed()
        msg = json.dumps({
            "payload": {
                "data": [
                    {"timestamp": int(time.time() * 1000)},  # Missing value
                    {"value": 71000.0},  # Missing timestamp
                    {"timestamp": int(time.time() * 1000), "value": 71500.0},  # OK
                ]
            }
        })
        feed._process_message(msg)
        assert feed.buffer_size == 1

    def test_live_update_format(self):
        """Live updates have value/timestamp directly in payload (no data array)."""
        feed = ChainlinkPriceFeed()
        msg = json.dumps({
            "topic": "crypto_prices_chainlink",
            "type": "update",
            "timestamp": int(time.time() * 1000),
            "payload": {
                "symbol": "btc/usd",
                "timestamp": int(time.time() * 1000),
                "value": 71500.0,
            }
        })
        feed._process_message(msg)
        assert feed.buffer_size == 1
        assert feed.get_latest_price() == 71500.0

    def test_wrong_symbol_ignored(self):
        feed = ChainlinkPriceFeed()
        msg = json.dumps({
            "topic": "crypto_prices_chainlink",
            "payload": {
                "symbol": "eth/usd",
                "timestamp": int(time.time() * 1000),
                "value": 3500.0,
            },
        })
        assert feed._process_message(msg)
        assert feed.buffer_size == 0

    def test_live_update_without_symbol_ignored(self):
        feed = ChainlinkPriceFeed()
        msg = json.dumps({
            "topic": "crypto_prices_chainlink",
            "payload": {
                "timestamp": int(time.time() * 1000),
                "value": 71500.0,
            },
        })
        assert feed._process_message(msg)
        assert feed.buffer_size == 0

    def test_latest_price_info_marks_stale_source(self):
        feed = ChainlinkPriceFeed()
        old_ms = int((time.time() - 11) * 1000)
        feed._buffer_point(old_ms, 71500.0)
        info = feed.get_latest_price_info()
        assert info is not None
        assert info["source_age_seconds"] >= 10
        assert feed.get_latest_price_if_fresh(max_age_seconds=10) is None

    def test_both_formats_combined(self):
        """Initial snapshot (data[]) then live update (direct payload)."""
        feed = ChainlinkPriceFeed()
        now_ms = int(time.time() * 1000)
        # Snapshot
        feed._process_message(json.dumps({
            "payload": {"data": [{"timestamp": now_ms, "value": 71000.0}]}
        }))
        assert feed.buffer_size == 1
        # Live update
        feed._process_message(json.dumps({
            "payload": {"symbol": "btc/usd", "timestamp": now_ms + 5000, "value": 71050.0}
        }))
        assert feed.buffer_size == 2
        assert feed.get_latest_price() == 71050.0

    def test_buffer_trimming(self):
        feed = ChainlinkPriceFeed()
        now_ms = int(time.time() * 1000)
        # Add old point (15 minutes ago) and recent point
        msg = json.dumps({
            "payload": {
                "data": [
                    {"timestamp": now_ms - 900_000, "value": 70000.0},  # 15 min old
                    {"timestamp": now_ms, "value": 71000.0},  # Now
                ]
            }
        })
        feed._process_message(msg)
        assert feed.buffer_size == 1  # Old one trimmed (>180s)
        assert feed.get_latest_price() == 71000.0


class TestChainlinkFeedGetPriceAt:
    """Test price lookup at specific timestamps."""

    def _feed_with_data(self):
        feed = ChainlinkPriceFeed()
        now_ms = int(time.time() * 1000)
        # Add points at -30s, -20s, -10s, now
        msg = json.dumps({
            "payload": {
                "data": [
                    {"timestamp": now_ms - 30_000, "value": 70900.0},
                    {"timestamp": now_ms - 20_000, "value": 70950.0},
                    {"timestamp": now_ms - 10_000, "value": 71000.0},
                    {"timestamp": now_ms, "value": 71050.0},
                ]
            }
        })
        feed._process_message(msg)
        return feed, now_ms

    def test_exact_match(self):
        feed, now_ms = self._feed_with_data()
        dt = datetime.fromtimestamp(now_ms / 1000, tz=timezone.utc)
        price = feed.get_price_at(dt)
        assert price == 71050.0

    def test_closest_match(self):
        feed, now_ms = self._feed_with_data()
        # 22 seconds ago — closest to -20s point
        dt = datetime.fromtimestamp((now_ms - 22_000) / 1000, tz=timezone.utc)
        price = feed.get_price_at(dt)
        assert price == 70950.0

    def test_too_stale(self):
        feed, now_ms = self._feed_with_data()
        # 2 minutes ago — no point within 60s
        dt = datetime.fromtimestamp((now_ms - 120_000) / 1000, tz=timezone.utc)
        price = feed.get_price_at(dt)
        assert price is None

    def test_empty_buffer(self):
        feed = ChainlinkPriceFeed()
        dt = datetime.now(timezone.utc)
        assert feed.get_price_at(dt) is None


class TestOneShotChainlinkHelpers:
    def test_get_chainlink_btc_price_ignores_non_btc_live_update(self, monkeypatch):
        now_ms = int(time.time() * 1000)

        def fake_fetch(subscribe_msg, parse_fn, timeout=10):
            assert subscribe_msg == CHAINLINK_SUBSCRIPTION
            assert parse_fn({
                "payload": {
                    "symbol": "eth/usd",
                    "timestamp": now_ms,
                    "value": 3500.0,
                },
            }) is None
            return parse_fn({
                "payload": {
                    "symbol": "btc/usd",
                    "timestamp": now_ms,
                    "value": 71500.0,
                },
            })

        monkeypatch.setattr("scripts.polymarket_btc_15m._fetch_chainlink_ws", fake_fetch)
        assert get_chainlink_btc_price() == pytest.approx(71500.0)

    def test_get_chainlink_price_at_filters_non_btc_points(self, monkeypatch):
        target = datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc)
        target_ms = int(target.timestamp() * 1000)

        def fake_fetch(subscribe_msg, parse_fn, timeout=10):
            assert subscribe_msg == _CHAINLINK_SNAPSHOT_SUB
            return parse_fn({
                "payload": {
                    "data": [
                        {"symbol": "eth/usd", "timestamp": target_ms, "value": 3500.0},
                        {"symbol": "btc/usd", "timestamp": target_ms + 1000, "value": 71500.0},
                    ]
                },
            })

        monkeypatch.setattr("scripts.polymarket_btc_15m._fetch_chainlink_ws", fake_fetch)
        monkeypatch.setattr("scripts.polymarket_btc_15m.get_binance_price_at", lambda _ts: 71505.0)
        assert get_chainlink_price_at(target) == pytest.approx(71500.0)


class TestChainlinkFeedHealth:
    """Test health and staleness detection."""

    def test_healthy_after_price_point(self):
        feed = ChainlinkPriceFeed()
        feed._last_price_time = time.time()
        feed._last_message_time = time.time()
        assert feed.is_healthy

    def test_unhealthy_when_no_recent_price(self):
        # 30s is_healthy window, even if a socket message arrived just now.
        feed = ChainlinkPriceFeed()
        feed._last_price_time = time.time() - 31
        feed._last_message_time = time.time()
        assert not feed.is_healthy

    def test_unhealthy_with_only_keepalive_messages(self):
        """Fix #7: socket keepalives bump _last_message_time but not
        _last_price_time; is_healthy must gate on the latter."""
        feed = ChainlinkPriceFeed()
        feed._last_message_time = time.time()
        feed._last_price_time = 0.0  # No price ever buffered
        assert not feed.is_healthy

    def test_buffer_point_sets_last_price_time(self):
        feed = ChainlinkPriceFeed()
        before = time.time()
        feed._buffer_point(int(time.time() * 1000), 71000.0)
        assert feed._last_price_time >= before

    # Note: is_stale property was removed in favor of an active watchdog
    # (_unhealthy_watchdog) that tears down the socket when prices stop
    # flowing despite a live connection. See tests/test_chainlink_watchdog.py.


@pytest.mark.slow
class TestChainlinkFeedLive:
    """Integration tests requiring live network. Run with: pytest -m slow"""

    def test_connect_and_receive(self):
        """Connect to real WebSocket, verify we get valid BTC prices."""
        feed = ChainlinkPriceFeed()
        feed.start()
        try:
            # Wait for data
            for _ in range(20):
                time.sleep(0.5)
                if feed.buffer_size > 0:
                    break
            assert feed.buffer_size > 0, "No data received from WebSocket"
            price = feed.get_latest_price()
            assert _is_sane_price(price), f"Insane price: {price}"
            assert feed.is_healthy
        finally:
            feed.stop()

    def test_price_matches_binance(self):
        """Chainlink and Binance should agree within 0.5%."""
        from scripts.polymarket_btc_15m import _get_binance_rest_current_btc_price
        feed = ChainlinkPriceFeed()
        feed.start()
        try:
            for _ in range(20):
                time.sleep(0.5)
                if feed.buffer_size > 0:
                    break
            chainlink = feed.get_latest_price()
            binance = _get_binance_rest_current_btc_price()
            assert chainlink and binance
            diff_pct = abs(chainlink - binance) / binance * 100
            assert diff_pct < 0.5, f"Divergence: {diff_pct:.2f}% (CL={chainlink}, BN={binance})"
        finally:
            feed.stop()
