import csv
from datetime import datetime, timezone

from scripts.trader.config import TraderConfig
from scripts.trader.grid_diagnostics import (
    GridEntryDiagnostic,
    GridEntryDiagnosticsLogger,
)
from scripts.trader.position import Direction, Trade, TradeEvent, TradeResult
from scripts.trader.state import StateManager
from scripts.trader.trade_log import TradeLogger


def test_trade_logger_preserves_legitimate_zero_values(tmp_path):
    path = tmp_path / "trades.csv"
    logger = TradeLogger(path)
    logger.log(
        TradeEvent(
            instance_id=0,
            timestamp=datetime(2026, 5, 1, tzinfo=timezone.utc),
            event="LOSS_EXPIRY",
            direction="UP",
            market_id="m",
            entry_price=0.50,
            exit_price=0.0,
            shares=0.0,
            pnl=0.0,
            pnl_pct=0.0,
            capital=100.0,
            model_prob=0.0,
            poly_prob=0.0,
            spot_price=0.0,
            barrier=0.0,
            effective_tp_pct=0.0,
        )
    )
    logger.flush()

    with path.open(newline="") as f:
        row = next(csv.DictReader(f))
    assert row["exit_price"] == "0.0000"
    assert row["shares"] == "0.0000"
    assert row["pnl"] == "0.00"
    assert row["pnl_pct"] == "0.00"
    assert row["model_prob"] == "0.0000"
    assert row["poly_prob"] == "0.0000"
    assert row["spot_price"] == "0.00"
    assert row["barrier"] == "0.00"
    assert row["effective_tp_pct"] == "0.0000"


def test_grid_entry_diagnostics_logger_writes_rejections_and_entries(tmp_path):
    path = tmp_path / "grid_eval.csv"
    logger = GridEntryDiagnosticsLogger(path)
    timestamp = datetime(2026, 5, 1, tzinfo=timezone.utc)

    def row(**overrides):
        values = {
            "timestamp": timestamp,
            "market_id": "btc-updown-15m-1778445900",
            "instance_id": 773,
            "entered": False,
            "direction": "DOWN",
            "alpha_up": 1.0,
            "alpha_down": 1.0,
            "floor_up": 0.45,
            "floor_down": 0.45,
            "tp_pct": 0.05,
            "sl_pct": 0.0,
            "capital_before": 1454.70,
            "order_size_usd": 72.73,
            "signal_price": 0.95,
            "signal_model_prob": 0.99,
            "signal_required_prob": 0.95,
            "signal_edge_ratio": 1.0421,
            "current_avg_price": 0.95,
            "current_fillable_usd": 72.73,
            "current_shares": 76.5579,
            "delayed_avg_price": 0.97,
            "delayed_fillable_usd": 10.0,
            "delayed_shares": 10.3093,
            "selected_entry_price": 0.97,
            "max_entry_price": 0.95,
            "execution_max_entry_price": 0.97,
            "paper_delayed_book_slippage": 0.02,
            "rejection_reason": "delayed_fill_insufficient",
            "rejection_details": '{"required_usd": "72.73"}',
        }
        values.update(overrides)
        return GridEntryDiagnostic(**values)

    logger.log(row(), liquidity_mode="capacity_aware")
    logger.log(row(rejection_details='{"required_usd": "72.73", "retry": true}'), liquidity_mode="capacity_aware")
    logger.log(
        row(
            instance_id=774,
            entered=True,
            rejection_reason="",
            rejection_details="",
            delayed_fillable_usd=72.73,
        ),
        liquidity_mode="capacity_aware",
    )
    logger.flush()

    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    assert len(rows) == 2
    assert rows[0]["entered"] == "0"
    assert rows[0]["rejection_reason"] == "delayed_fill_insufficient"
    assert rows[0]["order_size_usd"] == "72.73"
    assert rows[0]["execution_max_entry_price"] == "0.9700"
    assert rows[0]["liquidity_mode"] == "capacity_aware"
    assert rows[1]["entered"] == "1"
    assert rows[1]["instance_id"] == "774"
    assert rows[1]["rejection_reason"] == ""


def test_state_trade_csv_migration_preserves_reordered_columns(tmp_path):
    cfg = TraderConfig(
        paper_mode=False,
        live_state_file=tmp_path / "state.json",
        live_trades_file=tmp_path / "trades.csv",
        live_equity_file=tmp_path / "equity.csv",
        risk_events_file=tmp_path / "risk.csv",
        risk_events_current_file=tmp_path / "risk.current.csv",
        live_tick_eval_file=tmp_path / "tick.csv",
    )
    state = StateManager(cfg)
    header = Trade.csv_header().split(",")
    reordered = ["direction", "id", *[col for col in header if col not in {"direction", "id"}]]
    row = {col: "" for col in reordered}
    row.update(
        {
            "id": "old-1",
            "direction": "DOWN",
            "shares": "3.000000",
            "entry_price": "0.4200",
            "exit_price": "0.7000",
            "cost_basis": "1.26",
            "proceeds": "2.10",
            "pnl": "0.84",
            "pnl_pct": "66.67%",
            "result": "TP_FILLED",
            "opened_at": "2026-05-01T00:00:00+00:00",
            "closed_at": "2026-05-01T00:05:00+00:00",
            "market_title": "old market",
            "reference_price": "70000.00",
            "mode": "live",
        }
    )
    with cfg.live_trades_file.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=reordered)
        writer.writeheader()
        writer.writerow(row)

    state.log_trade(
        Trade(
            id="new-1",
            direction=Direction.UP,
            shares=2,
            entry_price=0.5,
            exit_price=0.6,
            cost_basis=1,
            proceeds=1.2,
            pnl=0.2,
            pnl_pct=20,
            result=TradeResult.TP_FILLED,
            opened_at=datetime(2026, 5, 1, tzinfo=timezone.utc),
            closed_at=datetime(2026, 5, 1, 0, 1, tzinfo=timezone.utc),
        )
    )

    with cfg.live_trades_file.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    assert reader.fieldnames == header
    assert rows[0]["id"] == "old-1"
    assert rows[0]["direction"] == "DOWN"
    assert rows[0]["entry_price"] == "0.4200"
    assert rows[1]["id"] == "new-1"


def _trade_event(instance_id: int = 1, market_id: str = "m-new") -> TradeEvent:
    return TradeEvent(
        instance_id=instance_id,
        timestamp=datetime(2026, 5, 2, tzinfo=timezone.utc),
        event="ENTRY",
        direction="UP",
        market_id=market_id,
        entry_price=0.55,
        shares=10.0,
        capital=94.5,
        model_prob=0.7,
        poly_prob=0.55,
        spot_price=83000.0,
        barrier=83100.0,
        effective_tp_pct=0.15,
    )


def test_ensure_trailing_newline_helper(tmp_path):
    from scripts.trader._io import ensure_trailing_newline

    path = tmp_path / "f.csv"
    assert ensure_trailing_newline(path) is False  # missing file
    path.write_bytes(b"")
    assert ensure_trailing_newline(path) is False  # empty file
    path.write_bytes(b"a,b\n")
    assert ensure_trailing_newline(path) is False  # already terminated
    path.write_bytes(b"a,b\nc,d")
    assert ensure_trailing_newline(path) is True  # repaired
    assert path.read_bytes() == b"a,b\nc,d\n"


def test_trade_logger_repairs_partial_last_line_before_appending(tmp_path):
    from scripts.trader.trade_log import HEADER

    path = tmp_path / "trades.csv"
    complete_row = (
        "0,2026-05-01T00:00:00+00:00,ENTRY,UP,m-old,0.5000,,10.0000,,,90.00,"
        "0.7000,0.5000,83000.00,83100.00,independent,,0.1500"
    )
    partial_row = "0,2026-05-01T00:05:00+00:00,TP_FIL"  # crash mid-write
    path.write_text(
        ",".join(HEADER) + "\r\n" + complete_row + "\r\n" + partial_row,
        encoding="utf-8",
        newline="",
    )

    logger = TradeLogger(path)
    logger.log(_trade_event())
    logger.flush()

    with path.open(newline="") as f:
        rows = list(csv.DictReader(f))
    # partial line stays its own (malformed) row; the new event is intact
    assert len(rows) == 3
    assert rows[1]["instance_id"] == "0"
    assert rows[2]["instance_id"] == "1"
    assert rows[2]["event"] == "ENTRY"
    assert rows[2]["market_id"] == "m-new"


def test_trade_logger_warns_on_legacy_header_missing_columns(tmp_path, caplog):
    import logging

    from scripts.trader.trade_log import HEADER

    path = tmp_path / "trades.csv"
    legacy_header = HEADER[:15]  # pre liquidity_mode/settlement_source/effective_tp_pct
    path.write_text(",".join(legacy_header) + "\n", encoding="utf-8")

    with caplog.at_level(logging.WARNING, logger="scripts.trader.trade_log"):
        logger = TradeLogger(path)
    assert any("legacy header" in rec.message for rec in caplog.records)

    # schema-freeze behavior unchanged: new rows keep the 15-column shape
    logger.log(_trade_event())
    logger.flush()
    with path.open(newline="") as f:
        lines = [line for line in f.read().splitlines() if line]
    assert len(lines[1].split(",")) == 15
