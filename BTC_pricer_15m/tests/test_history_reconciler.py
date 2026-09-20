"""Tests for the order_status proceeds fallback (Phase 1) and the
polymarket_history_reconciler (Phase 2) — together they fix the
e312391d split-fill case where the trade landed at +$0 instead of
the wallet-truth +$1.84.
"""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from decimal import Decimal
import csv

import pytest

from scripts.trader.config import TraderConfig
from scripts.trader.position import Direction, MicroPosition, Trade, TradeResult
from scripts.trader.single_trader import (
    ProceedsUnverifiedError,
    SingleTrader,
)
from scripts.trader.state import StateManager


def _config(tmp_path: Path) -> TraderConfig:
    return TraderConfig(
        paper_mode=False,
        live_trading_enabled=True,
        require_confirmation=False,
        max_slippage_estimate_pct=0.0,
        min_liquidity_depth_usd=0.0,
        reconciliation_enabled=False,
        history_reconcile_interval_seconds=0.0,  # disable auto-throttling in tests
        state_file=tmp_path / "paper_state.json",
        live_state_file=tmp_path / "live_state.json",
        trades_file=tmp_path / "paper_trades.csv",
        live_trades_file=tmp_path / "live_trades.csv",
        equity_file=tmp_path / "paper_equity.csv",
        live_equity_file=tmp_path / "live_equity.csv",
        risk_events_file=tmp_path / "risk.csv",
        friction_realized_file=tmp_path / "friction.csv",
        orderbook_file=tmp_path / "orderbook.csv",
    )


class _StubLiveExec:
    """Minimal live-executor stub for these tests.

    ``proceeds_to_return``: value returned by get_proceeds_from_trades.
        None mimics Polymarket /data/trades indexing lag.
    """

    is_live = True

    def __init__(self, proceeds_to_return=None):
        self.proceeds_to_return = proceeds_to_return
        self.proceeds_calls: list = []

    def get_proceeds_from_trades(self, *, order_id, token_id, since_timestamp, target_shares):
        self.proceeds_calls.append({
            "order_id": order_id,
            "token_id": token_id,
            "since_timestamp": since_timestamp,
            "target_shares": target_shares,
        })
        return self.proceeds_to_return


def _trader(tmp_path: Path, executor: _StubLiveExec) -> SingleTrader:
    cfg = _config(tmp_path)
    trader = SingleTrader.__new__(SingleTrader)
    trader.config = cfg
    trader.verbose = False
    trader.state = StateManager(cfg)
    trader.executor = executor
    trader.log = lambda message: None
    trader._current_market_id = "btc-updown-15m-1000"
    trader._last_tick = None
    trader._last_heartbeat = None
    trader._last_reconciliation = None
    trader._last_history_reconcile = None
    trader._tp_status_failures = {}
    trader._tp_backup_failures = {}
    trader._recently_settled = {}
    trader._start_time = datetime.now(timezone.utc)
    return trader


def _position() -> MicroPosition:
    return MicroPosition(
        direction=Direction.DOWN,
        token_id="tok-down",
        entry_price=0.64,
        shares=10.64,
        cost_basis=6.81,
        tp_price=0.832,
        stop_loss_price=0.0,
        opened_at=datetime.now(timezone.utc),
        market_id="btc-updown-15m-1777617600",
        peak_price=0.64,
        market_title="May 1, 2:40-2:45 ET",
        reference_price=77027.0,
    )


# --------------------------------------------------------------- Phase 1 ---

def test_record_exit_uses_proceeds_override_when_data_trades_empty(tmp_path):
    """e312391d scenario: /data/trades returns None (split-fill indexing lag),
    but order_status had verified size_matched + price → record at fill time
    with proceeds_source='order_status'."""
    executor = _StubLiveExec(proceeds_to_return=None)
    trader = _trader(tmp_path, executor)
    pos = _position()
    trader.state.open_positions = [pos]

    # Verified at fill time: size_matched=10.6389 × price=$0.83 = $8.83
    proceeds_override = 10.6389 * 0.83

    trade = trader._record_exit(
        pos, 0.83, TradeResult.TP_FILLED, proceeds_override=proceeds_override,
    )

    assert trade.proceeds == pytest.approx(8.83, rel=1e-3)
    assert trade.proceeds_source == "order_status"
    assert trade.pnl == pytest.approx(8.83 - 6.81, rel=1e-2)
    assert len(executor.proceeds_calls) == 1  # we did try /data/trades first


def test_record_exit_prefers_data_trades_over_override(tmp_path):
    """When /data/trades returns a real value, that wins; override is ignored.
    Polymarket /data/trades is post-fee final, so it's the authoritative
    source."""
    # Real on-chain delta would be slightly less than size×price after fees.
    executor = _StubLiveExec(proceeds_to_return=8.79)
    trader = _trader(tmp_path, executor)
    pos = _position()
    trader.state.open_positions = [pos]

    # User passes a coarser order_status estimate.
    proceeds_override = 10.6389 * 0.83

    trade = trader._record_exit(
        pos, 0.83, TradeResult.TP_FILLED, proceeds_override=proceeds_override,
    )

    assert trade.proceeds == pytest.approx(8.79, rel=1e-4)
    assert trade.proceeds_source == "polymarket_history"


def test_record_exit_raises_when_neither_chain_nor_override(tmp_path):
    """No /data/trades AND no override → defer (the original phantom-TP
    safety still fires for genuinely ambiguous cases)."""
    executor = _StubLiveExec(proceeds_to_return=None)
    trader = _trader(tmp_path, executor)
    pos = _position()
    trader.state.open_positions = [pos]

    with pytest.raises(ProceedsUnverifiedError):
        trader._record_exit(
            pos, 0.83, TradeResult.TP_FILLED, proceeds_override=None,
        )


# --------------------------------------------------------------- Phase 2 ---

def _seed_csv_with_tentative_row(trader: SingleTrader) -> Trade:
    """Write a header + one closed Trade row tagged proceeds_source=order_status
    so the reconciler has something to upgrade."""
    trades_file = trader.state.trades_file
    trades_file.parent.mkdir(parents=True, exist_ok=True)

    pos = _position()
    trade = Trade(
        id="abc12345",
        direction=pos.direction,
        shares=pos.shares,
        entry_price=pos.entry_price,
        exit_price=0.83,
        cost_basis=pos.cost_basis,
        proceeds=10.6389 * 0.83,  # tentative order_status proceeds
        pnl=(10.6389 * 0.83) - pos.cost_basis,
        pnl_pct=((10.6389 * 0.83) - pos.cost_basis) / pos.cost_basis * 100,
        result=TradeResult.TP_FILLED,
        opened_at=pos.opened_at,
        market_title=pos.market_title,
        reference_price=pos.reference_price,
        entry_order_id="0xbuy",
        exit_order_id="0xsell",
        token_id=pos.token_id,
        proceeds_source="order_status",
    )
    trader.state.closed_positions.append(trade)
    # Persist the CSV row via state.log_trade (writes the header on first call)
    trader.state.log_trade(trade)
    return trade


def test_history_reconciler_upgrades_tentative_row(tmp_path):
    """When /data/trades catches up, the reconciler updates the CSV +
    state to the exact value and tags it polymarket_history."""
    # /data/trades returns a slightly-different exact value (after fees).
    executor = _StubLiveExec(proceeds_to_return=8.7900)
    trader = _trader(tmp_path, executor)
    seeded = _seed_csv_with_tentative_row(trader)
    starting_proceeds = seeded.proceeds

    from scripts.trader.polymarket_history_reconciler import (
        reconcile_pending_proceeds,
    )

    upgraded = reconcile_pending_proceeds(trader, epsilon=0.01)

    assert upgraded == 1
    assert seeded.proceeds_source == "polymarket_history"
    assert seeded.proceeds == pytest.approx(8.79, rel=1e-4)
    assert executor.proceeds_calls[0]["token_id"] == "tok-down"
    # CSV row should also reflect the upgrade
    csv_text = trader.state.trades_file.read_text(encoding="utf-8")
    assert "polymarket_history" in csv_text
    assert "8.79" in csv_text
    # Audit file should record the pre-upgrade row
    audit = trader.state.trades_file.parent / (
        trader.state.trades_file.stem + "_reconciled_audit.csv"
    )
    assert audit.exists()
    audit_text = audit.read_text(encoding="utf-8")
    assert f"{starting_proceeds:.2f}" in audit_text or "8.83" in audit_text


def test_history_reconciler_skips_already_final_rows(tmp_path):
    """proceeds_source=polymarket_history / on-chain rows are never
    overwritten, even if /data/trades returns a different value."""
    executor = _StubLiveExec(proceeds_to_return=99.99)
    trader = _trader(tmp_path, executor)
    seeded = _seed_csv_with_tentative_row(trader)
    seeded.proceeds_source = "polymarket_history"  # already authoritative

    from scripts.trader.polymarket_history_reconciler import (
        reconcile_pending_proceeds,
    )

    upgraded = reconcile_pending_proceeds(trader)

    assert upgraded == 0
    assert seeded.proceeds_source == "polymarket_history"
    assert seeded.proceeds != pytest.approx(99.99)  # unchanged


class _ReceiptExec(_StubLiveExec):
    def __init__(self, *, cost="7.01", fee="0.20", wallet=None, proceeds=None):
        super().__init__(proceeds)
        self.cost, self.fee, self.wallet = cost, fee, wallet
        self.receipt_calls = []

    def get_execution_cashflow(self, **kwargs):
        self.receipt_calls.append(kwargs)
        if self.cost is None:
            return None
        return SimpleNamespace(net_cashflow=-Decimal(self.cost), fee_collateral=Decimal(self.fee))

    def get_usdc_balance(self):
        return self.wallet


def test_new_open_entry_cost_refines_without_charging_wallet_twice(tmp_path):
    from scripts.trader.polymarket_history_reconciler import reconcile_pending_entry_costs
    executor = _ReceiptExec(wallet=90.0)
    trader = _trader(tmp_path, executor)
    pos = _position()
    pos.order_id = "0xbuy"
    pos.entry_context = {"cost_basis_source": "order_status", "entry_fill_shares": pos.shares}
    trader.state.open_positions = [pos]
    trader.state.current_capital = 90.0
    before_pnl = trader.state.total_pnl
    assert reconcile_pending_entry_costs(trader) == 1
    assert pos.cost_basis == pytest.approx(7.01)
    assert pos.entry_context["cost_basis_source"] == "v2_receipt"
    assert trader.state.current_capital == 90.0
    assert trader.state.total_pnl == before_pnl
    assert executor.receipt_calls[0]["target_shares"] == pos.shares
    assert reconcile_pending_entry_costs(trader) == 0
    assert len(executor.receipt_calls) == 1


def test_closed_entry_cost_updates_net_pnl_csv_and_audit(tmp_path):
    from scripts.trader.polymarket_history_reconciler import reconcile_pending_entry_costs
    executor = _ReceiptExec(wallet=100.0)
    trader = _trader(tmp_path, executor)
    trade = _seed_csv_with_tentative_row(trader)
    trade.entry_context = {"cost_basis_source": "order_status", "entry_fill_shares": trade.shares}
    trader.state.total_pnl = trade.pnl
    old_proceeds, old_price = trade.proceeds, trade.entry_price
    assert reconcile_pending_entry_costs(trader) == 1
    assert trade.cost_basis == pytest.approx(7.01)
    assert trade.pnl == pytest.approx(old_proceeds-7.01)
    assert trader.state.total_pnl == pytest.approx(trade.pnl)
    assert trade.entry_price == old_price  # gross fill price stays distinct from fees
    assert trader.state.current_capital == 100.0
    with trader.state.trades_file.open() as handle:
        row = next(csv.DictReader(handle))
    assert row["cost_basis"] == "7.01"
    audit = trader.state.trades_file.with_name(trader.state.trades_file.stem+"_reconciled_audit.csv")
    assert "6.81" in audit.read_text()


def test_partial_entry_cost_uses_original_quantity_and_delta_when_wallet_unavailable(tmp_path):
    from scripts.trader.polymarket_history_reconciler import reconcile_pending_entry_costs
    executor = _ReceiptExec(wallet=None)
    trader = _trader(tmp_path, executor)
    pos = _position()
    original_shares = pos.shares
    pos.shares *= 0.5
    pos.cost_basis *= 0.5
    pos.order_id = "0xbuy"
    pos.entry_context = {"cost_basis_source": "order_status", "entry_fill_shares": original_shares}
    trader.state.open_positions = [pos]
    trader.state.current_capital = 90.0
    assert reconcile_pending_entry_costs(trader) == 1
    assert executor.receipt_calls[0]["target_shares"] == original_shares
    assert pos.cost_basis == pytest.approx(7.01/2)
    assert pos.entry_context["entry_fee_collateral"] == pytest.approx(0.1)
    assert trader.state.current_capital == pytest.approx(89.9)


def test_legacy_or_missing_receipt_entry_costs_stay_unchanged(tmp_path):
    from scripts.trader.polymarket_history_reconciler import reconcile_pending_entry_costs
    executor = _ReceiptExec(cost=None)
    trader = _trader(tmp_path, executor)
    legacy = _seed_csv_with_tentative_row(trader)
    assert reconcile_pending_entry_costs(trader) == 0
    assert executor.receipt_calls == []
    legacy.entry_context = {"cost_basis_source": "order_status"}
    before = legacy.to_dict()
    assert reconcile_pending_entry_costs(trader) == 0
    assert legacy.to_dict() == before


def test_v2_proceeds_preserve_gross_exit_price_and_wallet_cash(tmp_path):
    from scripts.trader.polymarket_history_reconciler import reconcile_pending_proceeds
    executor = _ReceiptExec(wallet=0.0, proceeds=8.79)
    executor.last_proceeds_source = "v2_receipt"
    trader = _trader(tmp_path, executor)
    trade = _seed_csv_with_tentative_row(trader)
    old_exit = trade.exit_price
    trader.state.current_capital = 1.0
    assert reconcile_pending_proceeds(trader) == 1
    assert trade.proceeds_source == "v2_receipt"
    assert trade.exit_price == old_exit
    assert trade.proceeds == 8.79
    assert trader.state.current_capital == 0.0
    with trader.state.trades_file.open() as handle:
        row = next(csv.DictReader(handle))
    assert row["proceeds_source"] == "v2_receipt"
