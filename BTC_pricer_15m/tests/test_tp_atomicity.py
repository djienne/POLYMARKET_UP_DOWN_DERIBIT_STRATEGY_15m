"""Tests for the TP-placement atomicity rewrite added in Phase 2."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from scripts.trader.executor import OrderResult
from scripts.trader.order_book import OrderBookSnapshot
from scripts.trader.position import Direction, MicroPosition
from scripts.trader.single_trader import SingleTrader
from tests.conftest import make_order_book, make_test_single_trader


def _book() -> OrderBookSnapshot:
    book = make_order_book(best_bid=0.53, best_ask=0.55)
    return OrderBookSnapshot(
        token_id="tok",
        best_bid=book.best_bid,
        best_ask=book.best_ask,
        bids=[(book.best_bid, 1000.0)],
        asks=[(book.best_ask, 1000.0), (book.best_ask + 0.01, 1000.0)],
        timestamp=book.timestamp,
    )


class _StubOrderBookClient:
    def fetch_order_book(self, token_id, **kwargs):
        return _book()

    def simulate_market_buy(self, ob, usd_amount):
        price = 0.55
        shares = usd_amount / price
        return shares, price, usd_amount


class _ExecutorBase:
    is_live = True

    def __init__(
        self,
        sell_limit_success: bool = True,
        wait_on_book: str = "live",       # "live", "timeout", "runtime"
        order_status_name: str = "live",  # "live", "matched", "cancelled", "expired"
        position_shares: float = 10.0,
    ):
        self.sell_limit_success = sell_limit_success
        self.wait_on_book = wait_on_book
        self.order_status_name = order_status_name
        self.position_shares = position_shares
        self.calls: list[str] = []

    # Capital / position --------------------------------------------------
    def get_usdc_balance(self):
        return 100.0

    def get_position(self, token_id, strict=False):
        self.calls.append(f"get_position({token_id})")
        return self.position_shares

    def wait_for_position(self, token_id, target, comparison="gte", timeout_seconds=15.0):
        return max(self.position_shares, target)

    # Buy / sell ----------------------------------------------------------
    def buy_market(self, token_id, usd_amount):
        return OrderResult(
            success=True,
            order_id="buy-1",
            shares_filled=10.0,
            avg_fill_price=0.55,
            total_cost=usd_amount,
        )

    def sell_limit(self, token_id, price, shares, order_type="GTC"):
        if not self.sell_limit_success:
            return OrderResult(success=False, error_message="place failed")
        return OrderResult(success=True, order_id="tp-1")

    # Order state ---------------------------------------------------------
    def wait_for_order_on_book(self, order_id, timeout_seconds=15.0, verify_live=True):
        if self.wait_on_book == "live":
            return {"status": "live"}
        if self.wait_on_book == "timeout":
            raise TimeoutError("not on book")
        raise RuntimeError("boom")

    def get_order_status(self, order_id):
        self.calls.append(f"get_order_status({order_id})")
        return {"status": self.order_status_name}

    def cancel_order(self, order_id):
        return True

    def get_best_bid(self, token_id):
        return 0.53

    def get_best_ask(self, token_id):
        return 0.55


def _make_trader(
    tmp_path: Path,
    executor: _ExecutorBase,
    **config_overrides,
) -> SingleTrader:
    trader = make_test_single_trader(
        tmp_path,
        executor=executor,
        order_book_client=_StubOrderBookClient(),
        max_slippage_estimate_pct=0.0,
        min_liquidity_depth_usd=0.0,
        require_tp_confirmed_before_new_entry=False,
        risk_events_file=tmp_path / "risk.csv",
        friction_realized_file=tmp_path / "friction.csv",
        **config_overrides,
    )
    trader.state.current_market = {
        "market_id": "mkt-1",
        "title": "BTC test",
        "reference_price": 83000.0,
        "clob_token_ids": {"UP": "tok"},
    }
    trader.state.current_capital = 100.0
    trader._current_market_id = "mkt-1"

    # Track calls to the fallback paths.
    trader.backup_calls: list[str] = []
    trader.close_calls: list[tuple[str, float]] = []

    def fake_backup(position, reason):
        trader.backup_calls.append(reason)
        position.tp_order_id = "backup-tp"

    def fake_close(position, exit_price):
        trader.close_calls.append((position.id, exit_price))

    trader._place_backup_tp_order = fake_backup  # type: ignore[assignment]
    trader.close_position_tp = fake_close        # type: ignore[assignment]

    # Also stub _build_entry_context_from_tick, _compute_cost_basis_from_trades
    trader._build_entry_context_from_tick = lambda opp: None
    trader._compute_cost_basis_from_trades = lambda pos: None

    return trader


def _opportunity():
    tick = SimpleNamespace(
        market_id="mkt-1",
        elapsed_seconds=400.0,
        time_remaining_seconds=500.0,
        prob_above=0.9,
        prob_below=0.1,
    )
    return {
        "direction": "UP",
        "polymarket_prob": 0.55,
        "tick": tick,
        "token_id": "tok",
        "edge": 1.10,
    }


class TestTPAtomicity:

    def test_happy_path_tp_confirmed_live(self, tmp_path):
        executor = _ExecutorBase(wait_on_book="live")
        trader = _make_trader(tmp_path, executor)
        result = trader.execute_entry(_opportunity())
        position = result.position

        assert position is not None
        assert position.tp_order_id == "tp-1"
        assert trader.backup_calls == []
        assert trader.close_calls == []

    def test_timeout_and_tp_filled_immediately_closes(self, tmp_path):
        # wait_for_order_on_book times out, status=matched → close_position_tp.
        executor = _ExecutorBase(
            wait_on_book="timeout",
            order_status_name="matched",
            position_shares=0.0,
        )
        trader = _make_trader(tmp_path, executor)
        result = trader.execute_entry(_opportunity())
        assert result.position is None
        assert trader.close_calls, "close_position_tp should have been called"
        assert trader.backup_calls == []

    def test_timeout_and_tp_cancelled_schedules_backup(self, tmp_path):
        executor = _ExecutorBase(
            wait_on_book="timeout",
            order_status_name="cancelled",
            position_shares=10.0,
        )
        trader = _make_trader(tmp_path, executor)
        result = trader.execute_entry(_opportunity())
        position = result.position
        assert position is not None
        assert position.tp_order_id == "backup-tp"
        assert trader.backup_calls, "backup TP placement should have been called"
        assert "initial TP cancelled" in trader.backup_calls[0]
        assert trader.close_calls == []

    def test_timeout_but_status_live_trusts_original_order(self, tmp_path):
        """status=live after timeout means the venue has the order; don't churn it."""
        executor = _ExecutorBase(
            wait_on_book="timeout",
            order_status_name="live",
            position_shares=10.0,
        )
        trader = _make_trader(tmp_path, executor)
        result = trader.execute_entry(_opportunity())
        position = result.position
        assert position is not None
        # Trusts the original tp_result.order_id; does NOT invoke the backup path.
        assert position.tp_order_id == "tp-1"
        assert trader.backup_calls == []
        assert trader.close_calls == []
        assert "tp_initial_status_trusted" in trader.config.risk_events_file.read_text()

    def test_timeout_unknown_status_schedules_backup(self, tmp_path):
        """Empty / unknown status is truly ambiguous — backup placement runs."""
        executor = _ExecutorBase(
            wait_on_book="timeout",
            order_status_name="",
            position_shares=10.0,
        )
        trader = _make_trader(tmp_path, executor)
        result = trader.execute_entry(_opportunity())
        position = result.position
        assert position is not None
        assert position.tp_order_id == "backup-tp"
        assert trader.backup_calls, "backup TP placement should have been called"
        assert "initial TP verify timeout" in trader.backup_calls[0]
        assert trader.close_calls == []
        assert "tp_initial_ambiguous" in trader.config.risk_events_file.read_text()

    def test_sell_limit_failure_schedules_backup(self, tmp_path):
        executor = _ExecutorBase(sell_limit_success=False)
        trader = _make_trader(tmp_path, executor)
        position = trader.execute_entry(_opportunity()).position
        assert position is not None
        assert trader.backup_calls, "backup TP should have been placed on sell_limit failure"
        assert "initial TP placement failed" in trader.backup_calls[0]
