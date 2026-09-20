"""WS6 near-real-time auto-amend: ack predicate + open-entry void applicator."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from scripts.trader.config import GridConfig, InstanceParams
from scripts.trader.main import void_open_entry
from scripts.trader.mirror_control import ack_is_unplaceable_rejection
from scripts.trader.position import Direction, MicroPosition
from scripts.trader.trader import PaperInstance


NOW = datetime(2026, 6, 6, 0, 0, 0, tzinfo=timezone.utc)


def _make_instance() -> PaperInstance:
    cfg = GridConfig(starting_capital=1000.0)
    params = InstanceParams(
        alpha_up=2.5, alpha_down=2.5, floor_up=0.45, floor_down=0.45,
        tp_percentage=0.30, stop_loss_pct=0.40,
    )
    return PaperInstance(id=1271, params=params, shared_config=cfg)


def _open_position(inst: PaperInstance, market_id: str, *, cost_basis: float = 5.6) -> None:
    inst.position = MicroPosition(
        direction=Direction.DOWN, token_id="tok", entry_price=0.56, shares=10.0,
        cost_basis=cost_basis, tp_price=0.73, stop_loss_price=0.34, opened_at=NOW,
        market_id=market_id, peak_price=0.56, reference_price=83000.0,
    )
    inst.capital = inst.capital - cost_basis  # mirror _commit_entry_plan debit


# ── ack predicate ────────────────────────────────────────────────────────────
def test_ack_unplaceable_rejection_true():
    assert ack_is_unplaceable_rejection({
        "status": "rejected", "event": "ENTRY", "reason": "entry_failed:order_failed",
        "order_id": "", "live_position_id": "",
    })


def test_ack_accepted_is_false():
    assert not ack_is_unplaceable_rejection({
        "status": "accepted", "event": "ENTRY", "reason": "entered",
        "order_id": "0xabc", "live_position_id": "p1",
    })


def test_ack_rejected_with_order_is_false():
    # An order exists → live did place something; not a phantom.
    assert not ack_is_unplaceable_rejection({
        "status": "rejected", "event": "ENTRY", "reason": "order_failed",
        "order_id": "0xabc", "live_position_id": "",
    })


def test_ack_rejected_for_non_order_reason_is_false():
    # Legitimate strategy divergence (price guard) is not a parity violation.
    assert not ack_is_unplaceable_rejection({
        "status": "rejected", "event": "ENTRY", "reason": "price_guard_exceeded",
        "order_id": "", "live_position_id": "",
    })


def test_ack_non_entry_event_is_false():
    assert not ack_is_unplaceable_rejection({
        "status": "rejected", "event": "STOP_LOSS", "reason": "order_failed",
        "order_id": "", "live_position_id": "",
    })


def test_ack_diagnostic_status_is_false():
    assert not ack_is_unplaceable_rejection({
        "status": "diagnostic_only", "event": "ENTRY", "reason": "order_failed",
    })


# ── void applicator ──────────────────────────────────────────────────────────
def test_void_open_entry_refunds_and_clears():
    inst = _make_instance()
    _open_position(inst, "btc-updown-15m-X", cost_basis=5.6)
    assert inst.capital == pytest.approx(994.4)

    ev = void_open_entry(inst, "btc-updown-15m-X", now=NOW)

    assert ev is not None
    assert ev.event == "ENTRY_VOID"
    assert ev.market_id == "btc-updown-15m-X"
    assert ev.direction == "DOWN"
    assert ev.pnl == 0.0
    assert inst.position is None
    assert inst.capital == pytest.approx(1000.0)  # cost basis refunded
    assert ev.capital == pytest.approx(1000.0)


def test_void_open_entry_no_match_returns_none():
    inst = _make_instance()
    _open_position(inst, "btc-updown-15m-Y")
    cap = inst.capital
    assert void_open_entry(inst, "btc-updown-15m-X", now=NOW) is None
    assert inst.position is not None  # untouched
    assert inst.capital == cap


def test_void_open_entry_no_position_returns_none():
    inst = _make_instance()
    assert void_open_entry(inst, "btc-updown-15m-X", now=NOW) is None


def test_void_then_recovery_leaves_no_trace(tmp_path):
    # Integration with WS7: the emitted ENTRY_VOID, replayed, refunds cleanly.
    from scripts.trader.main import recover_grid_instances_from_csv

    inst = _make_instance()
    _open_position(inst, "btc-updown-15m-X", cost_basis=5.6)
    ev = void_open_entry(inst, "btc-updown-15m-X", now=NOW)

    trades = tmp_path / "trades.csv"
    header = (
        "instance_id,timestamp,event,direction,market_id,entry_price,exit_price,"
        "shares,pnl,pnl_pct,capital,model_prob,poly_prob,spot_price,barrier\n"
    )
    entry = "1271,2026-06-06T00:00:00+00:00,ENTRY,DOWN,btc-updown-15m-X,0.5600,,10.0,,,994.40,0.7,0.5,83000,83100\n"
    void = (
        f"1271,{ev.timestamp.isoformat()},ENTRY_VOID,DOWN,btc-updown-15m-X,0.5600,,10.0,0.00,,1000.00,,,,\n"
    )
    trades.write_text(header + entry + void, encoding="utf-8")

    fresh = _make_instance()
    recover_grid_instances_from_csv([fresh], trades, starting_capital=1000.0)
    assert fresh.position is None
    assert fresh.trades_count == 0
    assert fresh.total_pnl == pytest.approx(0.0)
