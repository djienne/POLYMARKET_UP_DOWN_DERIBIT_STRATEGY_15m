from __future__ import annotations

import csv
import json
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts.trader.config import TraderConfig
from scripts.trader.executor import OrderResult
from scripts.trader.order_book import OrderBookSnapshot
from scripts.trader.persistence import SqliteStore
from scripts.trader.position import Direction, MicroPosition, Trade, TradeResult
from scripts.trader.single_trader import SingleTrader
from scripts.trader.state import StateManager
from tests.conftest import make_tick


class _BookClient:
    def __init__(self, asks=None):
        self.asks = asks or [(0.55, 1000.0)]

    def fetch_order_book(self, token_id, **kwargs):
        best_ask = self.asks[0][0]
        return OrderBookSnapshot(
            token_id=token_id,
            best_bid=best_ask - 0.02,
            best_ask=best_ask,
            bids=[(best_ask - 0.02, 1000.0)],
            asks=list(self.asks),
            timestamp="",
        )

    def simulate_market_buy(self, book, usd_amount):
        remaining = usd_amount
        shares = 0.0
        cost = 0.0
        for price, size in book.asks:
            take_cost = min(remaining, price * size)
            shares += take_cost / price
            cost += take_cost
            remaining -= take_cost
            if remaining <= 1e-9:
                break
        return shares, cost / shares if shares else 0.0, cost


class _LiveExec:
    is_live = True

    def __init__(self, buy_result: OrderResult | None = None):
        self.buy_result = buy_result
        self.positions: dict[str, float] = {}
        self.orders: dict[str, dict] = {}

    def get_usdc_balance(self):
        return None

    def get_position(self, token_id, strict=False):
        return self.positions.get(token_id, 0.0)

    def get_order_status(self, order_id):
        return self.orders.get(order_id, {"status": "cancelled"})

    def cancel_order(self, order_id):
        self.orders[order_id] = {"status": "cancelled"}
        return True

    def get_open_orders(self):
        return []

    def cancel_all_orders(self):
        return True

    def get_best_bid(self, token_id):
        return 0.53

    def get_best_ask(self, token_id):
        return 0.55

    def get_trades(self, token_id, since_timestamp=None):
        return [{"side": "BUY", "size": 10.0, "price": 0.55}]

    def buy_market(self, token_id, usd_amount):
        return self.buy_result or OrderResult(success=False)

    def buy_limit(self, token_id, price, shares, order_type="GTC", wait_for_fill_seconds=None):
        return self.buy_result or OrderResult(success=False)

    def sell_market(self, token_id, shares):
        return OrderResult(success=True, shares_filled=shares, avg_fill_price=0.5, total_cost=shares * 0.5)

    def sell_limit(self, token_id, price, shares, order_type="GTC"):
        oid = "tp-adopted"
        self.orders[oid] = {"status": "live", "token_id": token_id, "side": "SELL", "price": price, "size": shares}
        return OrderResult(success=True, order_id=oid)

    def wait_for_position(self, token_id, target, comparison="gte", timeout_seconds=15.0):
        return self.positions.get(token_id, target)

    def wait_for_order_on_book(self, order_id, timeout_seconds=15.0, verify_live=True):
        return self.orders.get(order_id, {"status": "live"})


def _config(tmp_path: Path, **overrides) -> TraderConfig:
    live_recheck_delay_seconds = overrides.pop("live_recheck_delay_seconds", 0.0)
    live_entry_spot_recheck_enabled = overrides.pop("live_entry_spot_recheck_enabled", False)
    return TraderConfig(
        paper_mode=False,
        live_trading_enabled=True,
        require_confirmation=False,
        max_slippage_estimate_pct=0.0,
        min_liquidity_depth_usd=0.0,
        live_recheck_delay_seconds=live_recheck_delay_seconds,
        live_entry_spot_recheck_enabled=live_entry_spot_recheck_enabled,
        reconciliation_enabled=False,
        state_file=tmp_path / "paper_state.json",
        live_state_file=tmp_path / "live_state.json",
        trades_file=tmp_path / "paper_trades.csv",
        live_trades_file=tmp_path / "live_trades.csv",
        equity_file=tmp_path / "paper_equity.csv",
        live_equity_file=tmp_path / "live_equity.csv",
        risk_events_file=tmp_path / "risk.csv",
        risk_events_current_file=tmp_path / "risk.current.csv",
        live_tick_eval_file=tmp_path / "live_tick_eval.csv",
        friction_realized_file=tmp_path / "friction.csv",
        orderbook_file=tmp_path / "orderbook.csv",
        **overrides,
    )


def _position(market_id="btc-updown-15m-1000", token_id="old-token") -> MicroPosition:
    return MicroPosition(
        direction=Direction.UP,
        token_id=token_id,
        entry_price=0.55,
        shares=10.0,
        cost_basis=5.5,
        tp_price=0.72,
        stop_loss_price=0.44,
        opened_at=datetime.now(timezone.utc),
        market_id=market_id,
        peak_price=0.55,
        market_title="old market",
        reference_price=83000.0,
    )


def _trader(tmp_path: Path, executor: _LiveExec | None = None, **config_overrides) -> SingleTrader:
    cfg = _config(tmp_path, **config_overrides)
    trader = SingleTrader.__new__(SingleTrader)
    trader.config = cfg
    trader.verbose = False
    trader.state = StateManager(cfg)
    trader.executor = executor or _LiveExec()
    trader.order_book_client = _BookClient()
    trader.log = lambda message: None
    trader._current_market_id = "btc-updown-15m-1000"
    trader._last_tick = None
    trader._last_heartbeat = None
    trader._last_reconciliation = None
    trader._tp_status_failures = {}
    trader._tp_backup_failures = {}
    trader._recently_settled = {}
    trader._start_time = datetime.now(timezone.utc)
    return trader


def test_live_tick_eval_header_matches_expanded_diagnostic_row(tmp_path):
    trader = _trader(tmp_path)
    base_tick = make_tick()
    tick = replace(
        base_tick,
        entry_signals=({
            "instance_id": "live",
            "market_id": base_tick.market_id,
            "direction": "UP",
            "executable_price": 0.55,
        },),
        entry_signal_age_s=3.25,
        entry_signal_market_id=base_tick.market_id,
    )

    trader._log_live_tick_diagnostics(
        tick,
        skipped_before_eval=False,
        chosen_direction="UP",
        entry_attempt=type(
            "_Attempt",
            (),
            {
                "attempted": True,
                "entered": False,
                "rejection_reason": "spot_drift_rejected",
                "rejection_details": {"fresh_spot": "82900.00"},
                "order_size_usd": 5.75,
                "raw_order_size_usd": 5.37,
                "capital_before_entry": 1000.0,
                "capital_source": "live_usdc",
                "order_id": "entry-1",
                "ambiguous": True,
                "token_id": "test_up_token",
                "rechecked_price": 0.56,
                "order_metadata": {"final_reason": "cancel_not_confirmed"},
            },
        )(),
    )

    with open(trader.config.live_tick_eval_file, newline="", encoding="utf-8") as f:
        rows = list(csv.reader(f))

    assert len(rows) == 2
    assert len(rows[0]) == len(rows[1])
    header = rows[0]
    row = dict(zip(header, rows[1]))
    assert row["entry_signal_present"] == "1"
    assert row["entry_signal_direction"] == "UP"
    assert row["entry_signal_market_id"] == tick.market_id
    assert row["post_eval_rejection_reason"] == "spot_drift_rejected"
    assert row["post_eval_capital_source"] == "live_usdc"
    assert row["post_eval_raw_order_size_usd"] == "5.370000"
    assert row["post_eval_order_id"] == "entry-1"
    assert row["post_eval_order_ambiguous"] == "1"
    assert "cancel_not_confirmed" in row["post_eval_order_metadata"]


def test_live_tick_with_pending_entry_does_not_crash_and_logs_blocker(tmp_path):
    trader = _trader(tmp_path, history_reconcile_interval_seconds=0)
    trader.state.pending_entry = {
        "order_id": "pending-buy",
        "token_id": "up-token",
        "market_id": "btc-updown-15m-1776004200",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "reason": "order_ambiguous",
    }
    trader._get_reconciliation_service = lambda: SimpleNamespace(
        reconcile_pending_entry=lambda: None,
        maybe_run_history_reconciler=lambda: None,
        detect_orphans=lambda: None,
        should_run_periodic=lambda: False,
        reconcile_positions=lambda mark_now=False: None,
    )
    trader._get_position_tracker = lambda: SimpleNamespace(check_fills=lambda: None)

    trader.on_tick(make_tick())

    with open(trader.config.live_tick_eval_file, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    assert len(rows) == 1
    assert rows[0]["skipped_before_eval"] == "1"
    assert rows[0]["pre_eval_skip_reason"].startswith("pending_entry:")
    assert rows[0]["post_eval_attempted"] == "0"


def test_live_zero_usdc_balance_overwrites_stale_capital_and_blocks_entry(tmp_path):
    class _ZeroBalanceExec(_LiveExec):
        def __init__(self):
            super().__init__(
                OrderResult(
                    success=True,
                    order_id="should-not-place",
                    shares_filled=10.0,
                    avg_fill_price=0.55,
                    total_cost=5.5,
                )
            )
            self.buy_calls = 0

        def get_usdc_balance(self):
            return 0.0

        def buy_limit(self, *args, **kwargs):
            self.buy_calls += 1
            return super().buy_limit(*args, **kwargs)

    executor = _ZeroBalanceExec()
    trader = _trader(tmp_path, executor)
    trader.state.current_capital = 50.0
    trader.state.current_market = {
        "market_id": "btc-updown-15m-1777642500",
        "title": "current",
        "reference_price": 83000.0,
        "clob_token_ids": {"UP": "up-token"},
    }

    result = trader.execute_entry({
        "direction": "UP",
        "polymarket_prob": 0.55,
        "model_prob": 0.8,
        "required_prob": 0.6,
        "edge": 1.45,
        "token_id": "up-token",
        "tick": make_tick(market_id="btc-updown-15m-1777642500"),
    })

    assert result.position is None
    assert result.rejection_reason == "insufficient_capital"
    assert result.capital_before_entry == 0.0
    assert result.capital_source == "live_usdc"
    assert trader.state.current_capital == 0.0
    assert executor.buy_calls == 0


def test_edge_skip_reason_logged_when_live_signal_missing(tmp_path):
    trader = _trader(tmp_path)
    tick = replace(
        make_tick(),
        entry_signals=({"instance_id": "0", "direction": "UP"},),
        entry_signal_age_s=2.0,
        entry_signal_market_id="btc-updown-15m-1776004200",
    )

    assert trader._evaluate_edge(tick) is None
    trader._log_live_tick_diagnostics(tick, skipped_before_eval=False)

    with open(trader.config.live_tick_eval_file, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    assert rows[0]["edge_skip_reason"] == "no_live_signal"
    assert rows[0]["entry_signal_present"] == "0"
    assert "signal_count" in rows[0]["edge_skip_details"]


def test_live_shadow_applies_fresh_spot_recheck(tmp_path, monkeypatch):
    class _PaperExec:
        is_live = False

        def __init__(self):
            self.buy_calls = 0

        def buy_limit(self, *args, **kwargs):
            self.buy_calls += 1
            return OrderResult(success=True, shares_filled=6.0, avg_fill_price=0.55, total_cost=3.30)

    cfg = TraderConfig(
        paper_mode=True,
        live_trading_enabled=False,
        live_shadow_mode=True,
        live_entry_spot_recheck_enabled=True,
        live_entry_spot_recheck_buffer_usd=5.0,
        max_slippage_estimate_pct=0.0,
        min_liquidity_depth_usd=0.0,
        live_recheck_delay_seconds=0.0,
        state_file=tmp_path / "shadow_state.json",
        trades_file=tmp_path / "shadow_trades.csv",
        equity_file=tmp_path / "shadow_equity.csv",
        risk_events_file=tmp_path / "risk.csv",
        orderbook_file=tmp_path / "orderbook.csv",
    )
    executor = _PaperExec()
    trader = SingleTrader.__new__(SingleTrader)
    trader.config = cfg
    trader.state = StateManager(cfg)
    trader.executor = executor
    trader.order_book_client = _BookClient()
    trader.log = lambda message: None
    trader._current_market_id = None
    monkeypatch.setattr("scripts.trader.single_trader.get_current_btc_price", lambda: 82900.0)

    trader.state.current_market = {
        "market_id": "btc-updown-15m-1777652400",
        "title": "current",
        "reference_price": 83000.0,
        "clob_token_ids": {"UP": "up-token"},
    }
    result = trader.execute_entry({
        "direction": "UP",
        "polymarket_prob": 0.55,
        "model_prob": 0.99,
        "required_prob": 0.6,
        "edge": 1.8,
        "token_id": "up-token",
        "tick": make_tick(
            market_id="btc-updown-15m-1777652400",
            barrier=83000.0,
            spot_price=83020.0,
        ),
    })

    assert result.position is None
    assert result.rejection_reason == "spot_drift_rejected"
    assert executor.buy_calls == 0


def test_live_rollover_does_not_settle_old_position_with_new_market_tick(tmp_path, monkeypatch):
    trader = _trader(tmp_path)
    pos = _position()
    trader.state.open_positions = [pos]
    trader.state.current_market = {"market_id": pos.market_id, "clob_token_ids": {"UP": pos.token_id}}
    monkeypatch.setattr("scripts.trader.single_trader.get_chainlink_price_at", lambda end: None)
    monkeypatch.setattr("scripts.trader.single_trader.get_binance_price_at", lambda end: None)

    trader.on_tick(make_tick(market_id="btc-updown-15m-1300", final_spot=99999.0, is_new_market=True))

    assert trader.state.open_positions == [pos]
    assert trader.state.closed_positions == []
    assert pos.pending_settlement is True
    assert pos.pending_settlement_market_id == "btc-updown-15m-1000"


def test_pending_settlement_resolves_with_positions_own_final_spot(tmp_path, monkeypatch):
    trader = _trader(tmp_path)
    pos = _position()
    pos.pending_settlement = True
    pos.pending_settlement_market_id = pos.market_id
    pos.pending_settlement_end_time = trader._market_end_time_from_id(pos.market_id)
    trader.state.open_positions = [pos]
    trader.executor.positions[pos.token_id] = pos.shares
    monkeypatch.setattr("scripts.trader.single_trader.get_chainlink_price_at", lambda end: 83100.0)

    trader._retry_pending_settlements()

    assert trader.state.open_positions == []
    assert len(trader.state.closed_positions) == 1
    assert trader.state.closed_positions[0].result == TradeResult.WIN_EXPIRY
    assert trader.state.closed_positions[0].final_btc_price == 83100.0


def test_live_csv_recovery_preserves_open_state(tmp_path):
    cfg = _config(tmp_path)
    state = StateManager(cfg)
    pos = _position()
    state.open_positions = [pos]
    state.pending_entry = {"order_id": "pending-buy", "token_id": "tok"}
    state.current_market = {"market_id": "mkt-current"}
    state.known_token_ids = {"tok"}
    state.last_entered_market_id = "mkt-current"

    trade = Trade(
        id="closed-1",
        direction=Direction.DOWN,
        shares=5.0,
        entry_price=0.5,
        exit_price=0.6,
        cost_basis=2.5,
        proceeds=3.0,
        pnl=0.5,
        pnl_pct=20.0,
        result=TradeResult.TP_FILLED,
        opened_at=datetime.now(timezone.utc),
        closed_at=datetime.now(timezone.utc),
        entry_order_id="entry-closed",
        exit_order_id="exit-closed",
        token_id="closed-token",
        proceeds_source="order_status",
    )
    cfg.live_trades_file.write_text(
        Trade.csv_header() + "\n" + trade.to_csv_row(mode="live") + "\n",
        encoding="utf-8",
    )

    assert state.recover_from_csv(preserve_open_positions=True)
    assert state.open_positions == [pos]
    assert state.pending_entry["order_id"] == "pending-buy"
    assert state.current_market["market_id"] == "mkt-current"
    assert state.known_token_ids == {"tok"}
    assert state.last_entered_market_id == "mkt-current"
    assert len(state.closed_positions) == 1
    recovered = state.closed_positions[0]
    assert recovered.entry_order_id == "entry-closed"
    assert recovered.exit_order_id == "exit-closed"
    assert recovered.token_id == "closed-token"
    assert recovered.proceeds_source == "order_status"


def test_current_market_orphan_is_adopted_not_sold(tmp_path):
    executor = _LiveExec()
    executor.positions["up-token"] = 10.0
    trader = _trader(tmp_path, executor)
    trader.state.current_market = {
        "market_id": "btc-updown-15m-1300",
        "title": "current market",
        "reference_price": 83000.0,
        "clob_token_ids": {"UP": "up-token", "DOWN": "down-token"},
    }
    sold = []
    trader._sell_residual_shares = lambda token_id, shares: sold.append((token_id, shares))

    trader._detect_orphan_positions()

    assert sold == []
    assert len(trader.state.open_positions) == 1
    assert trader.state.open_positions[0].token_id == "up-token"
    assert trader.state.open_positions[0].tp_order_id == "tp-adopted"


def test_non_ambiguous_fok_failure_does_not_create_pending_entry(tmp_path):
    executor = _LiveExec(
        OrderResult(success=False, order_id="fok-1", error_message="not filled", ambiguous=False)
    )
    trader = _trader(tmp_path, executor)
    trader.state.current_market = {
        "market_id": "btc-updown-15m-1300",
        "title": "current",
        "reference_price": 83000.0,
        "clob_token_ids": {"UP": "up-token"},
    }

    result = trader.execute_entry({
        "direction": "UP",
        "polymarket_prob": 0.55,
        "model_prob": 0.8,
        "required_prob": 0.6,
        "edge": 1.45,
        "token_id": "up-token",
        "tick": make_tick(market_id="btc-updown-15m-1300"),
    })

    assert result.position is None
    assert trader.state.pending_entry is None


def test_live_fill_above_max_entry_price_creates_manual_intervention_record(tmp_path):
    executor = _LiveExec(
        OrderResult(
            success=True,
            order_id="entry-over-cap",
            shares_filled=9.0,
            avg_fill_price=0.71,
            total_cost=6.39,
        )
    )
    trader = _trader(tmp_path, executor, max_entry_price=0.60)
    trader.state.current_market = {
        "market_id": "btc-updown-15m-1777642500",
        "title": "Bitcoin Up or Down",
        "reference_price": 78423.29,
        "clob_token_ids": {"UP": "up-token"},
    }
    events = []
    trader.state.log_risk_event = lambda kind, details=None: events.append((kind, details or {}))

    result = trader.execute_entry({
        "direction": "UP",
        "polymarket_prob": 0.55,
        "model_prob": 0.81,
        "required_prob": 0.55,
        "edge": 1.40,
        "token_id": "up-token",
        "tick": make_tick(market_id="btc-updown-15m-1777642500"),
    })

    assert result.position is None
    assert trader.state.open_positions == []
    assert trader.state.pending_entry["manual_intervention_required"] is True
    assert trader.state.pending_entry["reason"] == "entry_fill_above_cap"
    assert trader.state.pending_entry["order_id"] == "entry-over-cap"
    assert trader.state.pending_entry["fill_price"] == pytest.approx(0.71)
    assert events[0][0] == "entry_fill_above_cap"


def test_tiny_live_partial_fill_creates_manual_intervention_record(tmp_path):
    executor = _LiveExec(
        OrderResult(
            success=True,
            order_id="entry-dust",
            shares_filled=1.0,
            avg_fill_price=0.55,
            total_cost=0.55,
        )
    )
    trader = _trader(tmp_path, executor)
    trader.state.current_market = {
        "market_id": "btc-updown-15m-1300",
        "title": "current",
        "reference_price": 83000.0,
        "clob_token_ids": {"UP": "up-token"},
    }

    result = trader.execute_entry({
        "direction": "UP",
        "polymarket_prob": 0.55,
        "model_prob": 0.8,
        "required_prob": 0.6,
        "edge": 1.45,
        "token_id": "up-token",
        "tick": make_tick(market_id="btc-updown-15m-1300"),
    })

    assert result.position is None
    assert trader.state.open_positions == []
    assert trader.state.pending_entry["manual_intervention_required"] is True
    assert trader.state.pending_entry["reason"] == "entry_partial_below_minimum"
    assert trader.state.pending_entry["order_id"] == "entry-dust"
    assert trader.state.pending_entry["shares_filled"] == pytest.approx(1.0)


def test_near_minimum_live_fill_is_adopted_as_position(tmp_path):
    executor = _LiveExec(
        OrderResult(
            success=True,
            order_id="entry-near-min",
            shares_filled=6.05,
            avg_fill_price=0.95,
            total_cost=5.7475,
        )
    )
    executor.positions["up-token"] = 6.05
    trader = _trader(
        tmp_path,
        executor,
        max_entry_price=0.95,
        min_order_size_usd=5.75,
        min_position_shares=5.75,
    )
    trader.state.current_market = {
        "market_id": "btc-updown-15m-1300",
        "title": "current",
        "reference_price": 83000.0,
        "clob_token_ids": {"UP": "up-token"},
    }

    result = trader.execute_entry({
        "direction": "UP",
        "polymarket_prob": 0.95,
        "model_prob": 0.99,
        "required_prob": 0.98,
        "edge": 1.01,
        "token_id": "up-token",
        "tick": make_tick(market_id="btc-updown-15m-1300"),
    })

    assert result.position is not None
    assert trader.state.pending_entry is None
    assert len(trader.state.open_positions) == 1
    assert trader.state.open_positions[0].shares == pytest.approx(6.05)
    assert trader.state.open_positions[0].cost_basis == pytest.approx(5.7475)


def test_current_market_manual_near_minimum_pending_entry_is_adopted(tmp_path):
    executor = _LiveExec()
    executor.positions["up-token"] = 6.05
    executor.orders["entry-near-min"] = {
        "status": "matched",
        "size_matched": 6.05,
        "price": 0.95,
    }
    trader = _trader(
        tmp_path,
        executor,
        max_entry_price=0.95,
        min_order_size_usd=5.75,
        min_position_shares=5.75,
    )
    trader.state.current_market = {
        "market_id": "btc-updown-15m-1300",
        "title": "current",
        "reference_price": 83000.0,
        "clob_token_ids": {"UP": "up-token"},
    }
    trader.state.pending_entry = {
        "manual_intervention_required": True,
        "reason": "entry_partial_below_minimum",
        "order_id": "entry-near-min",
        "token_id": "up-token",
        "direction": "UP",
        "market_id": "btc-updown-15m-1300",
        "market_title": "current",
        "reference_price": 83000.0,
        "shares": 6.05,
        "shares_filled": 6.05,
        "fill_price": 0.95,
        "avg_fill_price": 0.95,
        "total_cost": 5.7475,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }

    trader._reconcile_pending_entry()

    assert trader.state.pending_entry is None
    assert len(trader.state.open_positions) == 1
    assert trader.state.open_positions[0].order_id == "entry-near-min"


def test_previous_market_manual_pending_entry_remains_tracked(tmp_path):
    executor = _LiveExec()
    trader = _trader(tmp_path, executor)
    trader.state.current_market = {
        "market_id": "btc-updown-15m-1400",
        "title": "new market",
        "reference_price": 83100.0,
        "clob_token_ids": {"UP": "new-up"},
    }
    events = []
    trader.state.log_risk_event = lambda kind, details=None: events.append((kind, details or {}))
    trader.state.pending_entry = {
        "manual_intervention_required": True,
        "reason": "entry_partial_below_minimum",
        "order_id": "old-entry",
        "token_id": "old-token",
        "direction": "UP",
        "market_id": "btc-updown-15m-1300",
        "shares": 6.05,
        "total_cost": 5.7475,
        "created_at": datetime(2026, 1, 1, tzinfo=timezone.utc).isoformat(),
    }

    trader._reconcile_pending_entry()

    assert trader.state.pending_entry["order_id"] == "old-entry"
    assert trader.state.open_positions == []
    assert "old-token" in trader.state.known_token_ids
    assert events[0][0] == "manual_pending_entry_preserved_after_market_transition"
    assert events[0][1]["pending_market_id"] == "btc-updown-15m-1300"
    assert events[0][1]["current_market_id"] == "btc-updown-15m-1400"


def test_on_tick_preserves_previous_market_unresolved_entry(tmp_path):
    trader = _trader(tmp_path, _LiveExec(), history_reconcile_interval_seconds=0)
    trader._current_market_id = "btc-updown-15m-1300"
    trader.state.current_market = {
        "market_id": "btc-updown-15m-1300",
        "title": "old market",
        "reference_price": 83000.0,
        "clob_token_ids": {"UP": "old-up"},
    }
    trader.state.pending_entry = {
        "manual_intervention_required": True,
        "reason": "entry_partial_below_minimum",
        "order_id": "old-entry",
        "token_id": "old-token",
        "direction": "UP",
        "market_id": "btc-updown-15m-1300",
        "shares": 6.05,
        "total_cost": 5.7475,
        "created_at": datetime(2026, 1, 1, tzinfo=timezone.utc).isoformat(),
    }

    trader.on_tick(make_tick(
        market_id="btc-updown-15m-1400",
        barrier=83100.0,
        elapsed_seconds=120.0,
        time_remaining_seconds=600.0,
    ))

    assert trader.state.pending_entry["order_id"] == "old-entry"
    assert trader.state.open_positions == []


def test_ambiguous_fok_failure_creates_pending_entry(tmp_path):
    executor = _LiveExec(
        OrderResult(success=False, order_id="fok-2", error_message="maybe live", ambiguous=True)
    )
    trader = _trader(tmp_path, executor)
    trader.state.current_market = {
        "market_id": "btc-updown-15m-1300",
        "title": "current",
        "reference_price": 83000.0,
        "clob_token_ids": {"UP": "up-token"},
    }

    trader.execute_entry({
        "direction": "UP",
        "polymarket_prob": 0.55,
        "model_prob": 0.8,
        "required_prob": 0.6,
        "edge": 1.45,
        "token_id": "up-token",
        "tick": make_tick(market_id="btc-updown-15m-1300"),
    })

    assert trader.state.pending_entry["order_id"] == "fok-2"


def test_stale_pending_entry_cancel_failure_keeps_pending_entry(tmp_path):
    class _Executor(_LiveExec):
        def get_order_status(self, order_id):
            return {"status": "live"}

        def get_position(self, token_id, strict=False):
            assert strict is True
            return 0.0

        def cancel_order(self, order_id):
            return False

    trader = _trader(tmp_path, _Executor(), pending_entry_max_age_seconds=1)
    trader.state.pending_entry = {
        "order_id": "pending-live",
        "token_id": "up-token",
        "direction": "UP",
        "market_id": "btc-updown-15m-1300",
        "created_at": datetime(2026, 1, 1, tzinfo=timezone.utc).isoformat(),
    }

    trader._reconcile_pending_entry()

    assert trader.state.pending_entry is not None
    assert trader.state.pending_entry["order_id"] == "pending-live"
    assert trader.state.pending_entry["cancel_attempts"] == 1
    assert trader.state.pending_entry["last_seen_order_status"] == "live"


def test_execute_entry_uses_tick_barrier_not_stale_current_market(tmp_path):
    executor = _LiveExec(
        OrderResult(
            success=True,
            order_id="entry-1",
            shares_filled=10.0,
            avg_fill_price=0.55,
            total_cost=5.5,
        )
    )
    trader = _trader(tmp_path, executor)
    trader.state.current_market = {
        "market_id": "btc-updown-15m-1300",
        "title": "stale current market",
        "reference_price": 78434.94,
        "clob_token_ids": {"UP": "test_up_token", "DOWN": "test_down_token"},
    }
    tick = make_tick(
        market_id="btc-updown-15m-1300",
        barrier=78423.28662262042,
    )

    result = trader.execute_entry({
        "direction": "UP",
        "polymarket_prob": 0.55,
        "model_prob": 0.8,
        "required_prob": 0.6,
        "edge": 1.45,
        "token_id": "test_up_token",
        "tick": tick,
    })

    assert result.position is not None
    assert result.position.reference_price == pytest.approx(78423.28662262042)
    assert result.position.entry_context["reference_price"] == pytest.approx(78423.28662262042)


def test_pending_entry_uses_tick_barrier_not_stale_current_market(tmp_path):
    executor = _LiveExec(
        OrderResult(
            success=False,
            order_id="entry-ambiguous",
            error_message="residual may still be live",
            ambiguous=True,
        )
    )
    trader = _trader(tmp_path, executor)
    trader.state.current_market = {
        "market_id": "btc-updown-15m-1300",
        "title": "stale current market",
        "reference_price": 78434.94,
        "clob_token_ids": {"UP": "test_up_token", "DOWN": "test_down_token"},
    }
    tick = make_tick(
        market_id="btc-updown-15m-1300",
        barrier=78423.28662262042,
    )

    result = trader.execute_entry({
        "direction": "UP",
        "polymarket_prob": 0.55,
        "model_prob": 0.8,
        "required_prob": 0.6,
        "edge": 1.45,
        "token_id": "test_up_token",
        "tick": tick,
    })

    assert result.position is None
    assert trader.state.pending_entry["reference_price"] == pytest.approx(78423.28662262042)


def test_same_market_confirmed_barrier_refreshes_current_market(tmp_path):
    trader = _trader(tmp_path, history_reconcile_interval_seconds=0)
    trader._current_market_id = "btc-updown-15m-1300"
    trader.state.current_market = {
        "market_id": "btc-updown-15m-1300",
        "title": "stale current market",
        "reference_price": 78434.94,
        "clob_token_ids": {"UP": "test_up_token", "DOWN": "test_down_token"},
    }

    trader.on_tick(make_tick(
        market_id="btc-updown-15m-1300",
        barrier=78423.28662262042,
        time_remaining_seconds=20.0,
    ))

    assert trader.state.current_market["reference_price"] == pytest.approx(78423.28662262042)


def test_pending_settlement_prefers_settlement_file_over_chainlink(tmp_path, monkeypatch):
    trader = _trader(tmp_path)
    pos = _position()
    pos.pending_settlement = True
    pos.pending_settlement_market_id = pos.market_id
    pos.pending_settlement_end_time = trader._market_end_time_from_id(pos.market_id)
    trader.state.open_positions = [pos]
    trader.executor.positions[pos.token_id] = pos.shares
    settlements = tmp_path / "settlements"
    settlements.mkdir()
    (settlements / f"{pos.market_id}.json").write_text(
        json.dumps({
            "market_id": pos.market_id,
            "settlement_price": 82900.0,
            "settlement_source": "polymarket",
        }),
        encoding="utf-8",
    )
    monkeypatch.setattr("scripts.trader.single_trader.get_chainlink_price_at", lambda end: 99999.0)
    monkeypatch.setattr("scripts.trader.single_trader.get_binance_price_at", lambda end: 99999.0)

    trader._retry_pending_settlements()

    assert trader.state.open_positions == []
    assert len(trader.state.closed_positions) == 1
    assert trader.state.closed_positions[0].result == TradeResult.LOSS_EXPIRY
    assert trader.state.closed_positions[0].final_btc_price == 82900.0


def test_pending_settlement_reads_sqlite_market_without_json_export(tmp_path, monkeypatch):
    market_db = tmp_path / "markets.sqlite"
    trader = _trader(tmp_path, market_db_path=market_db)
    pos = _position()
    pos.pending_settlement = True
    pos.pending_settlement_market_id = pos.market_id
    pos.pending_settlement_end_time = trader._market_end_time_from_id(pos.market_id)
    trader.state.open_positions = [pos]
    trader.executor.positions[pos.token_id] = pos.shares
    SqliteStore(market_db).upsert_market({
        "market_id": pos.market_id,
        "slug": pos.market_id,
        "window_start_iso": "1970-01-01T00:16:40+00:00",
        "window_end_iso": "1970-01-01T00:21:40+00:00",
        "strike": 83000.0,
        "strike_source": "polymarket",
        "settlement_status": "finalized",
        "settlement_price": 83100.0,
        "settlement_source": "polymarket",
    })
    monkeypatch.setattr(
        "scripts.trader.single_trader.get_chainlink_price_at",
        lambda end: pytest.fail("SQLite settlement should bypass Chainlink"),
    )

    trader._retry_pending_settlements()

    assert trader.state.open_positions == []
    assert len(trader.state.closed_positions) == 1
    assert trader.state.closed_positions[0].result == TradeResult.WIN_EXPIRY
    assert trader.state.closed_positions[0].final_btc_price == 83100.0


def test_orphan_detection_bounds_historical_known_token_scan(tmp_path):
    class _RecordingExec(_LiveExec):
        def __init__(self):
            super().__init__()
            self.checked_tokens = []

        def get_position(self, token_id, strict=False):
            self.checked_tokens.append(token_id)
            return 0.0

    executor = _RecordingExec()
    trader = _trader(tmp_path, executor, orphan_historical_token_scan_limit=3)
    trader.state.current_market = {
        "market_id": "btc-updown-15m-1300",
        "title": "current",
        "reference_price": 83000.0,
        "clob_token_ids": {"UP": "current-up", "DOWN": "current-down"},
    }
    trader.state.known_token_ids = {f"hist-{i}" for i in range(10)}

    trader._detect_orphan_positions()

    assert set(executor.checked_tokens) >= {"current-up", "current-down"}
    assert len(executor.checked_tokens) <= 5


def test_orphan_detection_rotates_historical_known_token_scan(tmp_path):
    class _RecordingExec(_LiveExec):
        def __init__(self):
            super().__init__()
            self.checked_tokens = []

        def get_position(self, token_id, strict=False):
            self.checked_tokens.append(token_id)
            return 0.0

    executor = _RecordingExec()
    trader = _trader(tmp_path, executor, orphan_historical_token_scan_limit=2)
    trader.state.current_market = {
        "market_id": "btc-updown-15m-1300",
        "title": "current",
        "reference_price": 83000.0,
        "clob_token_ids": {"UP": "current-up", "DOWN": "current-down"},
    }
    trader.state.known_token_ids = {f"hist-{i}" for i in range(5)}

    trader._detect_orphan_positions()
    first = set(executor.checked_tokens)
    executor.checked_tokens.clear()
    trader._detect_orphan_positions()
    second = set(executor.checked_tokens)
    executor.checked_tokens.clear()
    trader._detect_orphan_positions()
    third = set(executor.checked_tokens)

    assert {"current-up", "current-down"} <= first
    assert {"current-up", "current-down"} <= second
    assert {"current-up", "current-down"} <= third
    assert "hist-3" in second
    assert "hist-4" in third


def test_phantom_entry_blocked_when_executor_returns_success_without_order_id(tmp_path):
    """Regression: live executor returning success=True with empty order_id
    used to slip through and create a position that did NOT exist on
    Polymarket. Recorded in 15m_live_trades.csv with empty entry_order_id,
    silently inflating equity until expiry. Now must be hard-blocked.

    Reproduces the 2026-05-01 phantom-entry on btc-updown-15m-1777642500."""
    executor = _LiveExec(
        OrderResult(
            success=True,
            order_id="",  # ← the bug: empty order_id with success=True
            shares_filled=9.72,
            avg_fill_price=0.71,
            total_cost=6.90,
        )
    )
    trader = _trader(tmp_path, executor)
    trader.state.current_market = {
        "market_id": "btc-updown-15m-1777642500",
        "title": "Bitcoin Up or Down - May 1, 9:35AM-9:40AM ET",
        "reference_price": 78423.29,
        "clob_token_ids": {"UP": "up-token"},
    }

    result = trader.execute_entry({
        "direction": "UP",
        "polymarket_prob": 0.45,
        "model_prob": 0.81,
        "required_prob": 0.55,
        "edge": 1.40,
        "token_id": "up-token",
        "tick": make_tick(market_id="btc-updown-15m-1777642500"),
    })

    # No phantom Position is created.
    assert result.position is None
    assert len(trader.state.open_positions) == 0
    # And no pending_entry — only ambiguous FOK failures create those.
    assert trader.state.pending_entry is None


def test_tp_post_close_unreadable_balance_voids_tentative_close(tmp_path, monkeypatch):
    class _UnreadableBalanceExec(_LiveExec):
        def get_position(self, token_id, strict=False):
            if strict:
                raise RuntimeError("balance unavailable")
            return 0.0

    trader = _trader(tmp_path, _UnreadableBalanceExec())
    pos = _position(token_id="tp-token")
    pos.tp_order_id = "tp-order"
    trader.state.open_positions = [pos]
    events = []
    trader.state.log_risk_event = lambda kind, details=None: events.append((kind, details or {}))
    monkeypatch.setattr("scripts.trader.single_trader.time.sleep", lambda seconds: None)

    trader.close_position_tp(pos, 0.70, proceeds_override=7.0)

    assert len(trader.state.closed_positions) == 0
    assert len(trader.state.open_positions) == 1
    assert trader.state.open_positions[0].pending_settlement is True
    assert trader.state.open_positions[0].pending_settlement_reason == "tp_post_close_balance_unreadable"
    assert events[0][0] == "tp_post_close_balance_unreadable"


def test_tp_post_close_unreadable_balance_keeps_authoritative_close(tmp_path, monkeypatch):
    class _UnreadableAuthoritativeExec(_LiveExec):
        def get_proceeds_from_trades(self, *, order_id, token_id, since_timestamp, target_shares):
            return 7.0

        def get_position(self, token_id, strict=False):
            if strict:
                raise RuntimeError("balance unavailable")
            return 0.0

    trader = _trader(tmp_path, _UnreadableAuthoritativeExec())
    pos = _position(token_id="tp-token")
    pos.tp_order_id = "tp-order"
    trader.state.open_positions = [pos]
    events = []
    trader.state.log_risk_event = lambda kind, details=None: events.append((kind, details or {}))
    monkeypatch.setattr("scripts.trader.single_trader.time.sleep", lambda seconds: None)

    trader.close_position_tp(pos, 0.70)

    assert trader.state.open_positions == []
    assert len(trader.state.closed_positions) == 1
    assert trader.state.closed_positions[0].proceeds_source == "polymarket_history"
    assert events[0][0] == "tp_post_close_balance_unreadable"


def test_live_delayed_recheck_rejects_deteriorated_book(tmp_path):
    trader = _trader(tmp_path, _LiveExec(OrderResult(success=True)), live_recheck_delay_seconds=0.0)
    trader.order_book_client = _BookClient(asks=[(0.96, 1000.0)])

    assert trader._simulate_live_entry_recheck(
        "up-token",
        5.75,
        {"direction": "UP", "model_prob": 0.99},
    ) is None


def test_live_entry_spot_recheck_blocks_side_flip_before_buy(tmp_path, monkeypatch):
    class _CountingExec(_LiveExec):
        def __init__(self):
            super().__init__(
                OrderResult(
                    success=True,
                    order_id="entry-1",
                    shares_filled=6.5,
                    avg_fill_price=0.55,
                    total_cost=3.575,
                )
            )
            self.buy_calls = 0

        def buy_limit(self, *args, **kwargs):
            self.buy_calls += 1
            return super().buy_limit(*args, **kwargs)

    executor = _CountingExec()
    trader = _trader(
        tmp_path,
        executor,
        live_entry_spot_recheck_enabled=True,
        live_entry_spot_recheck_buffer_usd=5.0,
    )
    trader.state.current_market = {
        "market_id": "btc-updown-15m-1777652400",
        "title": "current",
        "reference_price": 78320.89,
        "clob_token_ids": {"UP": "up-token"},
    }
    events = []
    trader.state.log_risk_event = lambda kind, details=None: events.append((kind, details or {}))
    monkeypatch.setattr("scripts.trader.single_trader.get_current_btc_price", lambda: 78266.75)

    result = trader.execute_entry({
        "direction": "UP",
        "polymarket_prob": 0.55,
        "model_prob": 0.99,
        "required_prob": 0.6,
        "edge": 1.8,
        "token_id": "up-token",
        "tick": make_tick(
            market_id="btc-updown-15m-1777652400",
            barrier=78320.89,
            spot_price=78367.30,
        ),
    })

    assert result.position is None
    assert executor.buy_calls == 0
    assert trader.state.open_positions == []
    assert events[0][0] == "entry_rejected_spot_side_flip"


def test_live_entry_spot_recheck_allows_same_side_fresh_spot(tmp_path, monkeypatch):
    executor = _LiveExec(
        OrderResult(
            success=True,
            order_id="entry-1",
            shares_filled=6.5,
            avg_fill_price=0.55,
            total_cost=3.575,
        )
    )
    executor.positions["up-token"] = 6.5
    trader = _trader(
        tmp_path,
        executor,
        live_entry_spot_recheck_enabled=True,
        live_entry_spot_recheck_buffer_usd=5.0,
    )
    trader.state.current_market = {
        "market_id": "btc-updown-15m-1777652400",
        "title": "current",
        "reference_price": 78320.89,
        "clob_token_ids": {"UP": "up-token"},
    }
    monkeypatch.setattr("scripts.trader.single_trader.get_current_btc_price", lambda: 78370.0)

    result = trader.execute_entry({
        "direction": "UP",
        "polymarket_prob": 0.55,
        "model_prob": 0.99,
        "required_prob": 0.6,
        "edge": 1.8,
        "token_id": "up-token",
        "tick": make_tick(
            market_id="btc-updown-15m-1777652400",
            barrier=78320.89,
            spot_price=78367.30,
        ),
    })

    assert result.position is not None
    assert result.position.reference_price == pytest.approx(78320.89)


def test_tp_fill_requires_second_zero_balance_confirmation(tmp_path, monkeypatch):
    class _FlakyBalanceExec(_LiveExec):
        def __init__(self):
            super().__init__()
            self.reads = [0.0, 10.0]
            self.orders["tp-order"] = {
                "status": "matched",
                "size_matched": 10.0,
                "price": 0.72,
            }

        def get_position(self, token_id, strict=False):
            if self.reads:
                return self.reads.pop(0)
            return 10.0

    trader = _trader(tmp_path, _FlakyBalanceExec(), tp_fill_balance_recheck_seconds=2.0)
    pos = _position(token_id="tp-token")
    pos.tp_order_id = "tp-order"
    trader.state.open_positions = [pos]
    close_calls = []
    trader.close_position_tp = lambda *args, **kwargs: close_calls.append((args, kwargs))
    monkeypatch.setattr("scripts.trader.single_trader.time.sleep", lambda seconds: None)

    trader.check_tp_fills()

    assert close_calls == []
    assert trader.state.open_positions == [pos]


def test_tp_fill_closes_after_two_zero_balance_reads(tmp_path, monkeypatch):
    class _ConfirmedBalanceExec(_LiveExec):
        def __init__(self):
            super().__init__()
            self.reads = [0.0, 0.0]
            self.orders["tp-order"] = {
                "status": "matched",
                "size_matched": 10.0,
                "price": 0.72,
            }

        def get_position(self, token_id, strict=False):
            if self.reads:
                return self.reads.pop(0)
            return 0.0

    trader = _trader(tmp_path, _ConfirmedBalanceExec(), tp_fill_balance_recheck_seconds=2.0)
    pos = _position(token_id="tp-token")
    pos.tp_order_id = "tp-order"
    trader.state.open_positions = [pos]
    close_calls = []
    trader.close_position_tp = lambda *args, **kwargs: close_calls.append((args, kwargs))
    monkeypatch.setattr("scripts.trader.single_trader.time.sleep", lambda seconds: None)

    trader.check_tp_fills()

    assert len(close_calls) == 1
