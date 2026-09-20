import csv
import json
from pathlib import Path

from scripts.trader.cli.seed_stop_loss_extension import (
    build_stop_loss_clone_map,
    run_migration,
)
from scripts.trader.config import GridConfig


def _write_config(path: Path) -> None:
    path.write_text(
        json.dumps(
            {
                "starting_capital": 1000.0,
                "liquidity_mode": "independent",
                "grid": {
                    "alpha_up": [1.0, 2.5],
                    "alpha_down": [1.0],
                    "floor_up": [0.45],
                    "floor_down": [0.65],
                    "tp_percentage": [0.30],
                    "stop_loss_pct": [0.0, 0.20],
                },
                "append_stop_loss_pct": [0.40],
            }
        ),
        encoding="utf-8",
    )


def _write_snapshot(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "timestamp": "2026-05-15T00:00:00+00:00",
        "n_instances": 4,
        "markets_resolved": 1,
        "instances": {
            "0": {"capital": 1000.0, "total_pnl": 0.0, "trades_count": 0, "position": None},
            "1": {"capital": 1001.0, "total_pnl": 1.0, "trades_count": 1, "position": None},
            "2": {"capital": 1002.0, "total_pnl": 2.0, "trades_count": 1, "position": None},
            "3": {
                "capital": 990.0,
                "total_pnl": -10.0,
                "trades_count": 1,
                "position": {
                    "direction": "UP",
                    "token_id": "token",
                    "entry_price": 0.80,
                    "shares": 10.0,
                    "cost_basis": 8.0,
                    "tp_price": 0.99,
                    "stop_loss_price": 0.64,
                    "opened_at": "2026-05-15T00:01:00+00:00",
                    "market_id": "market",
                    "peak_price": 0.80,
                    "reference_price": 100.0,
                    "barrier_source": "polymarket",
                    "effective_tp_pct": 0.2375,
                },
            },
        },
    }
    path.write_text(json.dumps(payload), encoding="utf-8")


def _write_trades(path: Path) -> None:
    fieldnames = [
        "instance_id",
        "timestamp",
        "event",
        "direction",
        "market_id",
        "entry_price",
        "exit_price",
        "shares",
        "pnl",
        "pnl_pct",
        "capital",
        "model_prob",
        "poly_prob",
        "spot_price",
        "barrier",
        "liquidity_mode",
        "settlement_source",
        "effective_tp_pct",
    ]
    rows = [
        {
            "instance_id": "1",
            "timestamp": "2026-05-15T00:00:00+00:00",
            "event": "TP_FILLED",
            "direction": "UP",
            "market_id": "old",
            "entry_price": "0.5000",
            "exit_price": "0.6500",
            "shares": "10.0000",
            "pnl": "1.00",
            "capital": "1001.00",
            "liquidity_mode": "independent",
        },
        {
            "instance_id": "3",
            "timestamp": "2026-05-15T00:01:00+00:00",
            "event": "ENTRY",
            "direction": "UP",
            "market_id": "market",
            "entry_price": "0.8000",
            "shares": "10.0000",
            "capital": "990.00",
            "liquidity_mode": "independent",
        },
    ]
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def test_stop_loss_clone_map_uses_append_only_target_ids(tmp_path):
    config_path = tmp_path / "grid.json"
    _write_config(config_path)
    config = GridConfig.from_json(config_path)

    mappings = build_stop_loss_clone_map(config, source_sl=0.20, target_sl=0.40)

    assert [(m.source_id, m.target_id) for m in mappings] == [(1, 4), (3, 5)]


def test_run_migration_clones_snapshot_and_trades(tmp_path):
    config_path = tmp_path / "grid.json"
    results_dir = tmp_path / "results"
    _write_config(config_path)
    _write_snapshot(results_dir / "state_snapshot.json")
    _write_trades(results_dir / "trades.csv")

    result = run_migration(
        config_path=config_path,
        results_dir=results_dir,
        source_sl=0.20,
        target_sl=0.40,
        backup=False,
    )

    snapshot = json.loads((results_dir / "state_snapshot.json").read_text(encoding="utf-8"))
    assert result.base_count == 4
    assert result.total_count == 6
    assert result.cloned_snapshot_instances == 2
    assert result.adjusted_open_positions == 1
    assert result.cloned_trade_rows == 2
    assert snapshot["n_instances"] == 6
    assert snapshot["instances"]["5"]["position"]["stop_loss_price"] == 0.48

    with (results_dir / "trades.csv").open(encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))
    assert [r["instance_id"] for r in rows] == ["1", "4", "3", "5"]

    with (results_dir / "leaderboard.csv").open(encoding="utf-8", newline="") as f:
        leaderboard_rows = list(csv.DictReader(f))
    assert {r["instance_id"] for r in leaderboard_rows} >= {"4", "5"}
    assert next(r for r in leaderboard_rows if r["instance_id"] == "5")["sl_pct"] == "0.4"
