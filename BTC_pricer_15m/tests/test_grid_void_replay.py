"""ENTRY_VOID replay tests for grid recovery.

A phantom dry-run entry that live could not place is corrected with an
append-only ``ENTRY_VOID`` marker. Recovery must drop the voided entry and any
paired close so the rebuilt leaderboard reflects live-achievable fills.
"""
from __future__ import annotations

import pytest

from scripts.trader.config import DEFAULT_DRY_RUN_FRICTION_PCT, GridConfig, InstanceParams
from scripts.trader.main import recover_grid_instances_from_csv
from scripts.trader.trader import PaperInstance


GRID_CSV_HEADER = (
    "instance_id,timestamp,event,direction,market_id,entry_price,exit_price,"
    "shares,pnl,pnl_pct,capital,model_prob,poly_prob,spot_price,barrier,"
    "liquidity_mode,settlement_source,effective_tp_pct\n"
)


def _row(
    event: str,
    *,
    timestamp: str,
    market_id: str,
    entry_price: str = "0.5000",
    exit_price: str = "",
    shares: str = "10.0000",
    pnl: str = "",
    capital: str = "1000.00",
) -> str:
    return (
        f"0,{timestamp},{event},UP,{market_id},{entry_price},{exit_price},"
        f"{shares},{pnl},,{capital},0.7000,0.5000,83000.00,83100.00,"
        "independent,,0.1500\n"
    )


def _make_instance() -> PaperInstance:
    config = GridConfig(starting_capital=1000.0)
    params = InstanceParams(
        alpha_up=1.3, alpha_down=1.3, floor_up=0.50, floor_down=0.50,
        tp_percentage=0.15, stop_loss_pct=0.0,
    )
    return PaperInstance(id=0, params=params, shared_config=config)


def test_void_open_entry_refunds_capital(tmp_path):
    trades = tmp_path / "trades.csv"
    trades.write_text(
        GRID_CSV_HEADER
        + _row("ENTRY", timestamp="2026-06-03T12:39:28+00:00",
               market_id="btc-updown-15m-1780489800", entry_price="0.5575")
        + _row("ENTRY_VOID", timestamp="2026-06-06T00:00:00+00:00",
               market_id="btc-updown-15m-1780489800", entry_price="0.5575"),
        encoding="utf-8",
    )
    inst = _make_instance()
    recover_grid_instances_from_csv([inst], trades)

    assert inst.position is None
    assert inst.trades_count == 0
    assert inst.total_pnl == pytest.approx(0.0)
    assert inst.capital == pytest.approx(1000.0)  # cost basis never deducted


def test_void_after_exit_reverses_pnl(tmp_path):
    trades = tmp_path / "trades.csv"
    trades.write_text(
        GRID_CSV_HEADER
        + _row("ENTRY", timestamp="2026-06-03T12:39:28+00:00",
               market_id="btc-updown-15m-A", entry_price="0.5575")
        + _row("WIN_EXPIRY", timestamp="2026-06-03T12:54:00+00:00",
               market_id="btc-updown-15m-A", entry_price="0.5575",
               exit_price="1.0000", pnl="70.04", capital="1070.04")
        + _row("ENTRY_VOID", timestamp="2026-06-06T00:00:00+00:00",
               market_id="btc-updown-15m-A", entry_price="0.5575"),
        encoding="utf-8",
    )
    inst = _make_instance()
    recover_grid_instances_from_csv([inst], trades)

    assert inst.trades_count == 0
    assert inst.wins == 0
    assert inst.total_pnl == pytest.approx(0.0)
    assert inst.trade_pnls == []
    assert inst.capital == pytest.approx(1000.0)


def test_duplicate_void_is_idempotent(tmp_path):
    trades = tmp_path / "trades.csv"
    trades.write_text(
        GRID_CSV_HEADER
        + _row("ENTRY", timestamp="2026-06-03T12:39:28+00:00",
               market_id="btc-updown-15m-A", entry_price="0.5575")
        + _row("WIN_EXPIRY", timestamp="2026-06-03T12:54:00+00:00",
               market_id="btc-updown-15m-A", entry_price="0.5575",
               exit_price="1.0000", pnl="70.04")
        + _row("ENTRY_VOID", timestamp="2026-06-06T00:00:00+00:00",
               market_id="btc-updown-15m-A")
        + _row("ENTRY_VOID", timestamp="2026-06-06T00:00:01+00:00",
               market_id="btc-updown-15m-A"),
        encoding="utf-8",
    )
    inst = _make_instance()
    recover_grid_instances_from_csv([inst], trades)

    assert inst.trades_count == 0
    assert inst.wins == 0
    assert inst.total_pnl == pytest.approx(0.0)


def test_void_before_close_ignores_late_settlement(tmp_path):
    # Auto-amend scenario: void written, then a late settlement close for the
    # same market lands before restart. The close must be ignored.
    trades = tmp_path / "trades.csv"
    trades.write_text(
        GRID_CSV_HEADER
        + _row("ENTRY", timestamp="2026-06-03T12:39:28+00:00",
               market_id="btc-updown-15m-A", entry_price="0.5575")
        + _row("ENTRY_VOID", timestamp="2026-06-03T12:39:35+00:00",
               market_id="btc-updown-15m-A")
        + _row("WIN_EXPIRY", timestamp="2026-06-03T12:54:00+00:00",
               market_id="btc-updown-15m-A", entry_price="0.5575",
               exit_price="1.0000", pnl="70.04"),
        encoding="utf-8",
    )
    inst = _make_instance()
    recover_grid_instances_from_csv([inst], trades)

    assert inst.trades_count == 0
    assert inst.total_pnl == pytest.approx(0.0)
    assert inst.position is None


def test_void_one_market_leaves_others_intact(tmp_path):
    trades = tmp_path / "trades.csv"
    trades.write_text(
        GRID_CSV_HEADER
        + _row("ENTRY", timestamp="2026-06-03T12:00:00+00:00",
               market_id="btc-updown-15m-A", entry_price="0.5000")
        + _row("WIN_EXPIRY", timestamp="2026-06-03T12:14:00+00:00",
               market_id="btc-updown-15m-A", entry_price="0.5000",
               exit_price="1.0000", pnl="5.00")
        + _row("ENTRY", timestamp="2026-06-03T12:39:28+00:00",
               market_id="btc-updown-15m-B", entry_price="0.5575")
        + _row("WIN_EXPIRY", timestamp="2026-06-03T12:54:00+00:00",
               market_id="btc-updown-15m-B", entry_price="0.5575",
               exit_price="1.0000", pnl="70.04")
        + _row("ENTRY_VOID", timestamp="2026-06-06T00:00:00+00:00",
               market_id="btc-updown-15m-B"),
        encoding="utf-8",
    )
    inst = _make_instance()
    recover_grid_instances_from_csv([inst], trades)

    # Only market A's winning trade survives.
    assert inst.trades_count == 1
    assert inst.wins == 1
    assert inst.trade_pnls == [5.0]
    assert inst.total_pnl == pytest.approx(5.0)


def test_void_preserves_last_entered_market_id(tmp_path):
    # Parity with the runtime void_open_entry: recovery must NOT clear
    # last_entered_market_id on ENTRY_VOID, so a restart can't let the grid
    # re-enter the window the live trader just failed in.
    trades = tmp_path / "trades.csv"
    trades.write_text(
        GRID_CSV_HEADER
        + _row("ENTRY", timestamp="2026-06-03T12:39:28+00:00",
               market_id="btc-updown-15m-A", entry_price="0.5575")
        + _row("ENTRY_VOID", timestamp="2026-06-03T12:39:35+00:00",
               market_id="btc-updown-15m-A"),
        encoding="utf-8",
    )
    inst = _make_instance()
    recover_grid_instances_from_csv([inst], trades)

    assert inst.trades_count == 0
    assert inst.position is None
    assert inst.last_entered_market_id == "btc-updown-15m-A"
