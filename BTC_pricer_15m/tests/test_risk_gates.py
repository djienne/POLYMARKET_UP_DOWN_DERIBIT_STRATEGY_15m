"""Tests for the pre-trade risk gates added in Phase 3 of the live-robustness refactor."""

from __future__ import annotations

import csv
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Optional

import pytest

from scripts.trader.data_feed import TickData
from scripts.trader.executor import OrderResult
from scripts.trader.order_book import OrderBookSnapshot
from scripts.trader.position import Direction, MicroPosition
from scripts.trader.single_trader import SingleTrader
from tests.conftest import make_order_book, make_test_single_trader


def _book(best_ask=0.55, best_ask_size=1000.0, best_bid=0.53) -> OrderBookSnapshot:
    book = make_order_book(best_bid=best_bid, best_ask=best_ask, ask_size=best_ask_size)
    return OrderBookSnapshot(
        token_id="tok",
        best_bid=book.best_bid,
        best_ask=book.best_ask,
        bids=[(book.best_bid, 1000.0)],
        asks=[(book.best_ask, best_ask_size), (book.best_ask + 0.01, 1000.0)],
        timestamp=book.timestamp,
    )


class _StubExecutor:
    is_live = True

    def __init__(self):
        self.buys: list[tuple[str, float]] = []

    def get_usdc_balance(self):
        return 100.0

    def buy_market(self, token_id, usd_amount):
        self.buys.append((token_id, usd_amount))
        return OrderResult(
            success=True,
            order_id="buy-1",
            shares_filled=10.0,
            avg_fill_price=0.55,
            total_cost=usd_amount,
        )

    def sell_limit(self, *args, **kwargs):
        return OrderResult(success=True, order_id="tp-1", shares_filled=0.0)

    def wait_for_order_on_book(self, order_id, timeout_seconds=15.0, verify_live=True):
        return None

    def get_order_status(self, order_id):
        return {"status": "live"}

    def get_position(self, token_id, strict=False):
        return 10.0

    def get_best_bid(self, token_id):
        return 0.53

    def get_best_ask(self, token_id):
        return 0.55

    def cancel_order(self, order_id):
        return True


class _StubOrderBookClient:
    def __init__(self, book: Optional[OrderBookSnapshot]):
        self._book = book

    def fetch_order_book(self, token_id, **kwargs):
        return self._book

    def simulate_market_buy(self, order_book, usd_amount):
        if order_book is None or not order_book.asks:
            return 0.0, 0.0, 0.0
        remaining = usd_amount
        shares = 0.0
        cost = 0.0
        for price, size in order_book.asks:
            if remaining <= 0:
                break
            level_cost = price * size
            if level_cost <= remaining:
                shares += size
                cost += level_cost
                remaining -= level_cost
            else:
                shares += remaining / price
                cost += remaining
                remaining = 0.0
        return shares, (cost / shares if shares > 0 else 0.0), cost


def _make_trader(tmp_path: Path, book: Optional[OrderBookSnapshot], **config_overrides) -> SingleTrader:
    executor = _StubExecutor()
    trader = make_test_single_trader(
        tmp_path,
        executor=executor,
        order_book_client=_StubOrderBookClient(book),
        **config_overrides,
    )
    trader.state.current_market = {
        "title": "BTC test",
        "reference_price": 83000.0,
        "clob_token_ids": {"UP": "tok"},
    }
    trader.state.current_capital = 100.0
    trader._current_market_id = "mkt-1"
    return trader


def _read_risk_events(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with open(path, "r", newline="") as f:
        reader = csv.DictReader(f)
        return list(reader)


class TestConcurrencyGate:

    def test_rejects_when_concurrent_positions_at_cap(self, tmp_path):
        trader = _make_trader(tmp_path, _book(), max_concurrent_positions=1)
        existing = MicroPosition(
            id="existing",
            direction=Direction.UP,
            token_id="existing-token",
            entry_price=0.55,
            shares=10.0,
            cost_basis=5.50,
            tp_price=0.63,
            stop_loss_price=0.0,
            peak_price=0.55,
            opened_at=datetime.now(timezone.utc),
            market_id="mkt-0",
            market_title="prior",
            reference_price=83000.0,
        )
        existing.tp_order_id = "tp-existing"
        trader.state.open_positions.append(existing)

        ok, reason = trader._check_concurrency_gate()
        assert ok is False
        assert "open_positions=1>=max=1" in reason

    def test_rejects_when_existing_position_has_no_tp(self, tmp_path):
        trader = _make_trader(tmp_path, _book(),
                              max_concurrent_positions=2,
                              require_tp_confirmed_before_new_entry=True)
        existing = MicroPosition(
            id="existing",
            direction=Direction.UP,
            token_id="existing-token",
            entry_price=0.55,
            shares=10.0,
            cost_basis=5.50,
            tp_price=0.63,
            stop_loss_price=0.0,
            peak_price=0.55,
            opened_at=datetime.now(timezone.utc),
            market_id="mkt-0",
            market_title="prior",
            reference_price=83000.0,
        )
        # No tp_order_id set.
        trader.state.open_positions.append(existing)

        ok, reason = trader._check_concurrency_gate()
        assert ok is False
        assert "no TP order_id" in reason

    def test_allows_when_below_cap_and_tps_confirmed(self, tmp_path):
        trader = _make_trader(tmp_path, _book(), max_concurrent_positions=2)
        ok, _ = trader._check_concurrency_gate()
        assert ok is True


class TestSlippageLiquidityGate:

    def test_rejects_on_high_slippage(self, tmp_path):
        # Thin top level: $1 depth at 0.55. Order of $50 will walk deep and
        # blow through max_slippage_estimate_pct.
        thin = OrderBookSnapshot(
            token_id="tok",
            best_bid=0.50,
            best_ask=0.55,
            bids=[(0.50, 1000.0)],
            asks=[(0.55, 2.0), (0.70, 1000.0)],
            timestamp="",
        )
        trader = _make_trader(
            tmp_path, thin,
            max_slippage_estimate_pct=0.02,
            min_liquidity_depth_usd=0.0,
        )
        slip, depth, ask = trader._estimate_slippage_from_book("tok", 50.0)
        assert slip > 0.02
        assert ask == 0.55

    def test_rejects_on_thin_depth(self, tmp_path):
        # Plenty of slippage budget, but depth at best ask is only $1.10 —
        # below $50 min.
        thin = OrderBookSnapshot(
            token_id="tok",
            best_bid=0.50,
            best_ask=0.55,
            bids=[(0.50, 1000.0)],
            asks=[(0.55, 2.0), (0.56, 1000.0)],
            timestamp="",
        )
        trader = _make_trader(
            tmp_path, thin,
            max_slippage_estimate_pct=0.50,  # very permissive
            min_liquidity_depth_usd=50.0,
        )
        slip, depth, ask = trader._estimate_slippage_from_book("tok", 50.0)
        assert depth < 50.0

    def test_none_when_book_missing(self, tmp_path):
        trader = _make_trader(tmp_path, None)
        est = trader._estimate_slippage_from_book("tok", 50.0)
        assert est is None


class TestRiskEventCsv:

    def test_event_row_schema(self, tmp_path):
        trader = _make_trader(tmp_path, _book())
        trader.state.log_risk_event(
            kind="unit_test_event",
            details={"foo": "bar", "n": 42},
        )
        path = trader.config.risk_events_file
        assert path.exists()
        rows = _read_risk_events(path)
        assert len(rows) == 1
        row = rows[0]
        assert row["kind"] == "unit_test_event"
        assert row["mode"] == "live"
        # Details are semi-colon separated key=value pairs, sorted by key.
        assert "foo=bar" in row["details"]
        assert "n=42" in row["details"]
        current_rows = _read_risk_events(trader.config.risk_events_current_file)
        assert len(current_rows) == 1
        assert current_rows[0]["kind"] == "unit_test_event"

    def test_on_tick_logs_live_tick_eval_when_blocked_before_window(self, tmp_path):
        trader = _make_trader(tmp_path, _book())
        trader.check_tp_fills = lambda: None
        trader._print_heartbeat = lambda: None
        trader._reconcile_positions = lambda: None
        trader._verify_tp_orders = lambda: None
        trader._detect_orphan_positions = lambda: None
        trader._settle_positions = lambda tick: None
        trader.execute_entry = lambda opportunity: None

        tick = TickData(
            timestamp=datetime.now(timezone.utc),
            market_id="mkt-1",
            market_title="BTC test",
            barrier=83000.0,
            spot_price=82950.0,
            time_remaining_seconds=840.0,
            elapsed_seconds=60.0,
            window_duration_seconds=900.0,
            prob_above=0.72,
            prob_below=0.28,
            poly_prob_up=0.61,
            poly_prob_down=0.39,
            up_order_book=_book(best_ask=0.62, best_bid=0.60),
            down_order_book=_book(best_ask=0.40, best_bid=0.38),
            up_token_id="tok-up",
            down_token_id="tok-down",
            model_used="ssvi_surface",
            barrier_confirmed=True,
        )

        trader.on_tick(tick)

        path = trader.config.live_tick_eval_file
        assert path.exists()
        with open(path, "r", newline="") as f:
            rows = list(csv.DictReader(f))
        assert len(rows) == 1
        row = rows[0]
        assert row["market_id"] == "mkt-1"
        assert row["skipped_before_eval"] == "1"
        assert row["pre_eval_skip_reason"] == "blocked_first_window"
        assert row["up_model_prob"] == "0.720000"
        assert row["up_required_prob"] != ""
