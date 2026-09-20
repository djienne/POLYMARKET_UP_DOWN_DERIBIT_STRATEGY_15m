from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from scripts.trader.config import TraderConfig
from scripts.trader.position import Direction, MicroPosition, Trade, TradeResult
from scripts.trader.state import StateManager


def _sqlite_config(tmp_path: Path) -> TraderConfig:
    return TraderConfig(
        paper_mode=False,
        persistence_backend="sqlite",
        export_legacy_files=False,
        live_sqlite_db_file=tmp_path / "live.sqlite",
        live_state_file=tmp_path / "live_state.json",
        live_trades_file=tmp_path / "live_trades.csv",
        live_equity_file=tmp_path / "live_equity.csv",
        risk_events_file=tmp_path / "risk.csv",
        risk_events_current_file=tmp_path / "risk.current.csv",
        live_tick_eval_file=tmp_path / "ticks.current.csv",
    )


def test_state_manager_can_round_trip_state_through_sqlite(tmp_path: Path) -> None:
    cfg = _sqlite_config(tmp_path)
    state = StateManager(cfg)
    opened = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
    pos = MicroPosition(
        direction=Direction.UP,
        token_id="token-up",
        entry_price=0.50,
        shares=10.0,
        cost_basis=5.0,
        tp_price=0.65,
        stop_loss_price=0.0,
        opened_at=opened,
        market_id="btc-updown-15m-test",
        reference_price=80000.0,
    )
    state.add_position(pos)
    state.current_market = {
        "market_id": pos.market_id,
        "reference_price": 80000.0,
    }
    state.save()

    restored = StateManager(cfg)
    assert restored.load() is True
    assert len(restored.open_positions) == 1
    assert restored.open_positions[0].id == pos.id
    assert restored.current_market["market_id"] == pos.market_id
    assert not cfg.live_state_file.exists()


def test_trade_and_equity_are_normalized_in_sqlite(tmp_path: Path) -> None:
    cfg = _sqlite_config(tmp_path)
    state = StateManager(cfg)
    opened = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
    closed = datetime(2026, 5, 1, 12, 3, tzinfo=timezone.utc)
    trade = Trade(
        id="trade-1",
        direction=Direction.DOWN,
        shares=8.0,
        entry_price=0.40,
        exit_price=0.70,
        cost_basis=3.2,
        proceeds=5.6,
        pnl=2.4,
        pnl_pct=75.0,
        result=TradeResult.TP_FILLED,
        opened_at=opened,
        closed_at=closed,
        market_title="BTC test",
        reference_price=80000.0,
        token_id="token-down",
        proceeds_source="polymarket_history",
        entry_order_id="buy-1",
        exit_order_id="sell-1",
        entry_context={"market_id": "btc-updown-15m-test"},
    )
    state.log_trade(trade)
    state.log_equity_event(
        "TP_FILLED",
        direction="DOWN",
        entry_price=trade.entry_price,
        exit_price=trade.exit_price,
        shares=trade.shares,
        pnl=trade.pnl,
    )

    conn = sqlite3.connect(cfg.live_sqlite_db_file)
    try:
        trade_row = conn.execute("SELECT * FROM trades WHERE trade_id='trade-1'").fetchone()
        equity_count = conn.execute("SELECT COUNT(*) FROM equity_points").fetchone()[0]
    finally:
        conn.close()

    assert trade_row is not None
    assert equity_count == 1
    assert not cfg.live_trades_file.exists()
    assert not cfg.live_equity_file.exists()


def test_live_sqlite_load_error_records_fail_closed_reason(tmp_path: Path) -> None:
    cfg = _sqlite_config(tmp_path)
    state = StateManager(cfg)

    def _raise(*args, **kwargs):
        raise RuntimeError("sqlite broken")

    state._sqlite_store.load_state_snapshot = _raise

    assert state.load() is False
    assert state.last_load_error.startswith("sqlite_load_failed")


def test_live_current_run_logs_are_rotated_not_deleted(tmp_path: Path) -> None:
    cfg = _sqlite_config(tmp_path)
    first = StateManager(cfg)
    cfg.risk_events_current_file.write_text(
        "timestamp,mode,kind,details\nold,live,test,x\n",
        encoding="utf-8",
    )
    cfg.live_tick_eval_file.write_text("old tick\n", encoding="utf-8")

    StateManager(cfg)

    rotated_risk = list(tmp_path.glob("risk.current.*.csv"))
    rotated_ticks = list(tmp_path.glob("ticks.current.*.csv"))
    assert rotated_risk
    assert rotated_ticks
    assert "old,live,test,x" in rotated_risk[0].read_text(encoding="utf-8")
    assert cfg.risk_events_current_file.read_text(encoding="utf-8").startswith("timestamp,mode")
    assert cfg.live_tick_eval_file.read_text(encoding="utf-8").startswith("timestamp,market_id")
    assert first.mode == "live"
