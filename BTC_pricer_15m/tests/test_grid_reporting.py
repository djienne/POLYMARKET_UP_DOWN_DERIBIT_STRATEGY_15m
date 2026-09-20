import csv
import json
import math
import threading
from dataclasses import replace
from datetime import datetime, timezone

import pytest

from scripts.trader import main as grid_main, summary
from scripts.trader.config import GridConfig, InstanceParams
from scripts.trader.position import Direction, MicroPosition, TradeEvent
from scripts.trader.trade_log import TradeLogger
from scripts.trader.trader import PaperInstance
from tests.conftest import make_tick


def instance(*, max_history=1):
    return PaperInstance(0, InstanceParams(1, 1, .45, .45, .15, .2),
                         GridConfig(starting_capital=100, max_trade_pnls=max_history))


def record_pnls(inst, values):
    for value in values:
        inst._record_closed_pnl(value)
        inst.total_pnl += value
        inst.trades_count += 1


def test_full_history_moments_and_drawdown_survive_tail_truncation_and_restart(tmp_path):
    inst = instance()
    record_pnls(inst, [-20, 10, 5])
    restored = instance()
    restored.load_snapshot(inst.to_snapshot())
    assert restored.trade_pnls == [5]
    assert summary.realized_drawdown(restored) == (20, 20)
    # Three dollar-PnLs: mean=-5/3; unbiased variance=(525-25/3)/2.
    expected = (-5 / 3) / math.sqrt((525 - 25 / 3) / 2)
    assert summary.pnl_mean_to_sd(restored) == pytest.approx(expected)
    path = tmp_path / "leaderboard.csv"
    summary.write_leaderboard_csv([restored], path)
    with path.open() as handle:
        row = next(csv.DictReader(handle))
    assert "sharpe" not in row
    assert float(row["pnl_mean_to_sd"]) == pytest.approx(expected, abs=5e-5)
    assert float(row["max_drawdown_pct"]) == 20


def test_old_truncated_snapshot_does_not_invent_all_time_statistics():
    inst = instance()
    inst.load_snapshot({"capital": 110, "total_pnl": 10, "trades_count": 50, "trade_pnls": [5]})
    assert summary.realized_drawdown(inst) == (None, None)
    assert summary.pnl_mean_to_sd(inst) is None


def test_async_reports_are_immutable_bounded_and_do_not_scan_trade_history(tmp_path, monkeypatch):
    monkeypatch.setattr(grid_main, "SNAPSHOT_PATH", tmp_path / "state_snapshot.json")
    monkeypatch.setattr(grid_main, "LEADERBOARD_PATH", tmp_path / "leaderboard.csv")
    monkeypatch.setattr(grid_main, "GRID_BACKUP_DIR", tmp_path / "backups")
    monkeypatch.setattr(grid_main, "TRADES_PATH", tmp_path / "trades.csv")
    monkeypatch.setattr(summary, "_last_close_per_instance", lambda path: pytest.fail("periodic full-history scan"))
    started, release = threading.Event(), threading.Event()
    written = []
    original = grid_main._write_snapshot_files

    def slow_snapshot(snapshot):
        if not written:
            started.set()
            assert release.wait(10)
        written.append(snapshot)
        original(snapshot)

    monkeypatch.setattr(grid_main, "_write_snapshot_files", slow_snapshot)
    inst = instance(max_history=3)
    record_pnls(inst, [1])
    try:
        grid_main.save_state([inst], markets_resolved=1, async_io=True)
        assert started.wait(5)
        record_pnls(inst, [2])
        grid_main.save_state([inst], markets_resolved=2, async_io=True)
        record_pnls(inst, [3])
        grid_main.save_state([inst], markets_resolved=3, async_io=True)
    finally:
        release.set()
        grid_main._state_io_future.result(timeout=10)
    assert [row["markets_resolved"] for row in written] == [1, 3]
    assert written[0]["instances"]["0"]["trade_pnls"] == [1]
    final = json.loads((tmp_path / "state_snapshot.json").read_text())
    assert final["instances"]["0"]["trade_pnls"] == [1, 2, 3]
    assert final["written_at"] >= final["timestamp"]
    with (tmp_path / "leaderboard.csv").open() as handle:
        row = next(csv.DictReader(handle))
    assert row["as_of"] == final["timestamp"]
    assert row["written_at"] >= row["as_of"]


def test_fee_metadata_and_execution_time_survive_real_csv_replay(tmp_path):
    now = datetime(2026, 9, 5, tzinfo=timezone.utc)
    fee = {"enabled": True, "rate": .1, "exponent": 1, "takerOnly": True}
    metadata = {"condition_id": "condition-old", "payoff_kind": "chainlink_twap_60s", "averaging_window_seconds": 60,
                "fee_schedule": fee}
    path = tmp_path / "trades.csv"
    logger = TradeLogger(path)
    logger.log(TradeEvent(0, now, "ENTRY", "UP", "old-market", entry_price=.5, shares=10,
                         cost_basis=5.25, fee_schedule=fee, contract_metadata=metadata, decision_at=now))
    logger.close()
    inst = instance()
    grid_main.recover_grid_instances_from_csv([inst], path, starting_capital=100)
    assert inst.capital == pytest.approx(94.75)
    assert inst.position.cost_basis == 5.25
    assert inst.position.fee_schedule == fee
    assert inst.position.condition_id == "condition-old"
    assert inst.position.payoff_kind == "chainlink_twap_60s"
    assert inst.position.decision_at == now


def test_old_market_position_resolves_from_own_record_after_restart(tmp_path):
    inst = instance()
    inst.capital = 95
    inst.position = MicroPosition(Direction.UP, "old-up-token", .5, 10, 5, .6, 0,
                                  datetime.now(timezone.utc), "old-market", .5,
                                  condition_id="condition-old", payoff_kind="chainlink_twap_60s")
    current = replace(make_tick(market_id="new-market"), payoff_kind="chainlink_twap_60s")
    assert grid_main.settle_recovered_grid_positions([inst], current, tmp_path) == []
    assert inst.position is not None and inst.capital == 95
    (tmp_path / "old-market.json").write_text(json.dumps({
        "market_id": "old-market", "condition_id": "condition-old", "settlement_status": "finalized",
        "settlement_payouts": {"old-up-token": 1.0}, "settlement_source": "polymarket_resolved_outcome",
    }))
    grid_main._GRID_PENDING_SETTLEMENT_CACHE.clear()
    events = grid_main.settle_recovered_grid_positions([inst], current, tmp_path)
    assert len(events) == 1 and events[0].market_id == "old-market"
    assert events[0].pnl == 5 and inst.capital == 105
    assert inst.position is None
    assert grid_main.settle_recovered_grid_positions([inst], current, tmp_path) == []


def test_modern_trade_refuses_to_lose_metadata_in_legacy_header(tmp_path):
    path = tmp_path / "trades.csv"
    original = "instance_id,timestamp,event,direction,market_id,entry_price,exit_price,shares,pnl,pnl_pct,capital,model_prob,poly_prob,spot_price,barrier\n"
    path.write_text(original)
    logger = TradeLogger(path)
    logger.log(TradeEvent(0, datetime.now(timezone.utc), "ENTRY", "UP", "m", cost_basis=2.5))
    with pytest.raises(RuntimeError, match="migrate_trades_header"):
        logger.flush()
    assert path.read_text() == original


@pytest.mark.parametrize("matching_snapshot", [False, True])
def test_new_experiment_refuses_legacy_csv_even_with_a_matching_snapshot(tmp_path, monkeypatch, matching_snapshot):
    monkeypatch.chdir(tmp_path)
    results = tmp_path / "results"
    results.mkdir()
    monkeypatch.setattr(grid_main, "SNAPSHOT_PATH", results / "state_snapshot.json")
    inst = PaperInstance(0, InstanceParams(1, 1, .45, .45, .15, .2),
                         GridConfig(starting_capital=100, experiment_id="new-experiment"))
    (results / "trades.csv").write_text(
        "instance_id,timestamp,event,direction,market_id,entry_price,shares,pnl\n"
        "0,2026-09-01T00:00:00+00:00,WIN_EXPIRY,UP,old-market,.5,10,5\n"
    )
    if matching_snapshot:
        (results / "state_snapshot.json").write_text(json.dumps({
            "experiment_id": "new-experiment", "n_instances": 1, "instances": {"0": inst.to_snapshot()},
        }))
    with pytest.raises(RuntimeError, match="experiment.*does not match"):
        grid_main.try_recover([inst])
    assert inst.capital == 100 and inst.total_pnl == 0 and inst.position is None


def test_same_experiment_csv_only_recovery_and_mixed_epoch_rejection(tmp_path):
    cfg = GridConfig(starting_capital=100, experiment_id="same-experiment")
    inst = PaperInstance(0, InstanceParams(1, 1, .45, .45, .15, .2), cfg)
    path = tmp_path / "trades.csv"
    logger = TradeLogger(path)
    logger.log(TradeEvent(0, datetime.now(timezone.utc), "ENTRY", "UP", "m", entry_price=.5,
                         shares=10, cost_basis=5, experiment_id="same-experiment"))
    logger.close()
    assert grid_main.recover_grid_instances_from_csv([inst], path, starting_capital=100)[0] == 1
    assert inst.capital == 95 and inst.position.shares == 10
    logger.log(TradeEvent(0, datetime.now(timezone.utc), "WIN_EXPIRY", "UP", "m", entry_price=.5,
                         shares=10, pnl=5, experiment_id="another-experiment"))
    logger.close()
    untouched = PaperInstance(0, inst.params, cfg)
    with pytest.raises(RuntimeError, match="does not match"):
        grid_main.recover_grid_instances_from_csv([untouched], path, starting_capital=100)
    assert untouched.capital == 100 and untouched.position is None


def test_cleanup_void_keeps_experiment_identity_and_replays_without_cash_invention(tmp_path):
    from scripts.trader.cli.clean_unavailable_dry_trades import DryEntry, Verdict, append_voids

    cfg = GridConfig(starting_capital=100, experiment_id="same-experiment")
    inst = PaperInstance(0, InstanceParams(1, 1, .45, .45, .15, .2), cfg)
    path = tmp_path / "trades.csv"
    now = datetime.now(timezone.utc)
    logger = TradeLogger(path)
    logger.log(TradeEvent(0, now, "ENTRY", "UP", "m", entry_price=.5, shares=10,
                         cost_basis=5, experiment_id="same-experiment"))
    logger.close()
    with path.open() as handle:
        reader = csv.DictReader(handle)
        row = next(reader)
        header = reader.fieldnames
    entry = DryEntry(0, "m", "UP", .5, now, raw=row)
    assert append_voids(path, header, [Verdict(entry, "A", "confirmed unplaceable order")], now) == 1
    grid_main.recover_grid_instances_from_csv([inst], path, starting_capital=100)
    assert inst.capital == 100 and inst.position is None and inst.total_pnl == 0
