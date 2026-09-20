"""Tests for the RTDS-first BTC spot provider."""

from __future__ import annotations

import json
import time
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

from scripts.trader.spot_aggregator import (
    BinanceWebSocketFeed,
    BitfinexWebSocketFeed,
    BitstampWebSocketFeed,
    CoinbaseWebSocketFeed,
    ExchangeWebSocketFeed,
    GeminiWebSocketFeed,
    KrakenWebSocketFeed,
    SpotAggregator,
)


class TestTopOfBookParsing:
    def test_binance_book_ticker_mid(self):
        feed = BinanceWebSocketFeed()
        msg = json.dumps({"s": "BTCUSDT", "E": 1714000000000, "b": "97000", "a": "97010"})
        assert feed._parse_price_from_message(msg) == (1714000000000, 97005.0)

    def test_coinbase_best_bid_ask_mid(self):
        feed = CoinbaseWebSocketFeed()
        msg = json.dumps({
            "type": "ticker",
            "time": "2026-05-02T12:00:00.123456Z",
            "product_id": "BTC-USD",
            "best_bid": "97200",
            "best_ask": "97210",
            "price": "97205",
        })
        ts_ms, price = feed._parse_price_from_message(msg)
        assert price == 97205.0
        expected = datetime.fromisoformat("2026-05-02T12:00:00.123456+00:00")
        assert abs(ts_ms - int(expected.timestamp() * 1000)) < 2

    def test_kraken_ticker_mid(self):
        feed = KrakenWebSocketFeed()
        msg = json.dumps({
            "channel": "ticker",
            "type": "update",
            "data": [{
                "symbol": "BTC/USD",
                "bid": 97100.0,
                "ask": 97120.0,
                "timestamp": "2026-05-02T12:00:01.000000Z",
            }],
        })
        ts_ms, price = feed._parse_price_from_message(msg)
        assert price == 97110.0
        expected = datetime.fromisoformat("2026-05-02T12:00:01.000000+00:00")
        assert abs(ts_ms - int(expected.timestamp() * 1000)) < 2

    def test_bitstamp_order_book_mid(self):
        feed = BitstampWebSocketFeed()
        msg = json.dumps({
            "event": "data",
            "channel": "order_book_btcusd",
            "data": {
                "timestamp": "1714000001",
                "bids": [["97200.00", "0.1"]],
                "asks": [["97220.00", "0.2"]],
            },
        })
        assert feed._parse_price_from_message(msg) == (1714000001000, 97210.0)

    def test_gemini_top_of_book_mid(self):
        feed = GeminiWebSocketFeed()
        msg = json.dumps({
            "type": "update",
            "timestampms": 1714000001500,
            "events": [
                {"type": "change", "side": "bid", "price": "97000.00"},
                {"type": "change", "side": "ask", "price": "97020.00"},
            ],
        })
        assert feed._parse_price_from_message(msg) == (1714000001500, 97010.0)

    def test_bitfinex_ticker_mid(self):
        feed = BitfinexWebSocketFeed()
        feed._parse_price_from_message(json.dumps({
            "event": "subscribed",
            "channel": "ticker",
            "chanId": 42,
        }))
        result = feed._parse_price_from_message(json.dumps([42, [97000, 1, 97030, 2]]))
        assert result is not None
        _, price = result
        assert price == 97015.0


class TestFeedHealth:
    def test_unhealthy_when_no_data(self):
        feed = BinanceWebSocketFeed()
        assert not feed.is_healthy
        assert feed.get_latest_price() is None

    def test_healthy_after_buffer(self):
        feed = BinanceWebSocketFeed()
        feed._buffer_point(int(time.time() * 1000), 97000.0)
        assert feed.is_healthy
        assert feed.get_latest_price() == 97000.0

    def test_insane_price_rejected(self):
        feed = BinanceWebSocketFeed()
        assert not feed._buffer_point(int(time.time() * 1000), 5.0)
        assert feed.get_latest_price() is None


def _make_healthy_feed(name: str, price: float) -> MagicMock:
    feed = MagicMock(spec=ExchangeWebSocketFeed)
    feed.name = name
    feed.is_healthy = True
    feed.get_latest_price.return_value = price
    return feed


def _make_unhealthy_feed(name: str) -> MagicMock:
    feed = MagicMock(spec=ExchangeWebSocketFeed)
    feed.name = name
    feed.is_healthy = False
    feed.get_latest_price.return_value = None
    return feed


def _chainlink(price=97100.0, source_age=1.0, receive_age=1.0):
    chainlink = MagicMock()
    chainlink.get_latest_price_info.return_value = {
        "price": price,
        "source_age_seconds": source_age,
        "receive_age_seconds": receive_age,
    }
    return chainlink


class TestAggregation:
    def test_fresh_rtds_wins_over_ws(self):
        agg = SpotAggregator(chainlink_feed=_chainlink(), enabled_feeds=[])
        agg._feeds = {
            "coinbase": _make_healthy_feed("coinbase", 97000.0),
            "kraken": _make_healthy_feed("kraken", 97020.0),
            "bitstamp": _make_healthy_feed("bitstamp", 97040.0),
        }
        price, source, details = agg.get_aggregated_spot()
        assert price == 97100.0
        assert source == "polymarket_rtds"
        assert details["rtds"]["source_age_seconds"] == 1.0

    def test_stale_rtds_uses_ws_median(self):
        agg = SpotAggregator(
            chainlink_feed=_chainlink(source_age=11.0, receive_age=1.0),
            enabled_feeds=[],
            rest_last_resort_enabled=False,
        )
        agg._feeds = {
            "coinbase": _make_healthy_feed("coinbase", 97000.0),
            "kraken": _make_healthy_feed("kraken", 97020.0),
            "bitstamp": _make_healthy_feed("bitstamp", 97040.0),
        }
        price, source, details = agg.get_aggregated_spot()
        assert price == 97020.0
        assert source == "ws_median(bitstamp,coinbase,kraken)"
        assert details["fallback_reason"] == "polymarket_rtds_stale"

    def test_outlier_rejected_at_20_bps(self):
        agg = SpotAggregator(
            chainlink_feed=_chainlink(source_age=30.0, receive_age=30.0),
            enabled_feeds=[],
            outlier_bps=20.0,
            rest_last_resort_enabled=False,
        )
        prices = {"coinbase": 97000.0, "kraken": 97010.0, "bitstamp": 97400.0}
        filtered = agg._reject_outliers(prices)
        assert set(filtered) == {"coinbase", "kraken"}

    def test_ws_requires_three_after_outlier_filter(self):
        agg = SpotAggregator(
            chainlink_feed=_chainlink(source_age=30.0, receive_age=30.0),
            enabled_feeds=[],
            outlier_bps=20.0,
            rest_last_resort_enabled=False,
        )
        agg._feeds = {
            "coinbase": _make_healthy_feed("coinbase", 97000.0),
            "kraken": _make_healthy_feed("kraken", 97010.0),
            "bitstamp": _make_healthy_feed("bitstamp", 97400.0),
        }
        assert agg.get_aggregated_spot() is None

    def test_rest_last_resort_after_rtds_and_ws_fail(self):
        agg = SpotAggregator(
            chainlink_feed=_chainlink(source_age=30.0, receive_age=30.0),
            enabled_feeds=[],
            rest_fallback_fn=lambda: 97200.0,
        )
        agg._feeds = {"coinbase": _make_unhealthy_feed("coinbase")}
        price, source, details = agg.get_aggregated_spot()
        assert price == 97200.0
        assert source == "binance_rest_last_resort"
        assert details["fallback"] == "binance_rest_last_resort"

    def test_all_sources_down_returns_none_when_rest_disabled(self):
        agg = SpotAggregator(
            chainlink_feed=_chainlink(source_age=30.0, receive_age=30.0),
            enabled_feeds=[],
            rest_last_resort_enabled=False,
        )
        agg._feeds = {"coinbase": _make_unhealthy_feed("coinbase")}
        assert agg.get_aggregated_spot() is None

    def test_health_summary(self):
        agg = SpotAggregator(enabled_feeds=[], rest_last_resort_enabled=False)
        agg._feeds = {
            "binance": _make_healthy_feed("binance", 97000.0),
            "coinbase": _make_unhealthy_feed("coinbase"),
        }
        assert agg.health_summary() == {"binance": True, "coinbase": False}


class TestCalcDaemonSpotAggregator:
    def test_run_once_passes_provider_spot(self, tmp_path):
        from scripts.trader.calc_daemon import CalcConfig, CalcDaemon
        from scripts.trader.hybrid_calibration import MarketContext

        config = CalcConfig(
            inbox_targets=[tmp_path / "inbox"],
            snapshot_path=None,
            delay_min_seconds=0,
            delay_max_seconds=0,
            inbox_ttl_seconds=300,
            source_label="test",
            market_scan_interval_seconds=30,
        )
        end_time = datetime.now(timezone.utc) + timedelta(minutes=5)
        context = MarketContext(
            market_id="test-market",
            barrier=97000.0,
            market_end_time_iso=end_time.isoformat(),
        )

        aggregator = MagicMock()
        aggregator.get_aggregated_spot.return_value = (97100.0, "polymarket_rtds", {})

        daemon = CalcDaemon(
            config,
            terminal_path=tmp_path / "terminal.json",
            spot_aggregator=aggregator,
        )

        terminal_payload = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "timing": {"surface_fit_s": 1.0, "mc_s": 0.5},
        }
        captured_kwargs = {}

        def fake_run_terminal(**kwargs):
            captured_kwargs.update(kwargs)
            json_path = kwargs.get("json_path")
            if json_path:
                json_path.parent.mkdir(parents=True, exist_ok=True)
                json_path.write_text(json.dumps(terminal_payload), encoding="utf-8")
            return {
                "prob_above": 0.6,
                "prob_below": 0.4,
                "model_used": "ssvi_surface",
                "spot_price": 97100.0,
            }

        with patch(
            "scripts.trader.calc_daemon._resolve_active_market_context",
            return_value=context,
        ), patch(
            "scripts.trader.calc_daemon.run_terminal_script",
            side_effect=fake_run_terminal,
        ):
            event = daemon.run_once()

        assert captured_kwargs["spot"] == 97100.0
        assert event is not None
        assert event.spot_price == 97100.0
        assert event.spot_source == "polymarket_rtds"
