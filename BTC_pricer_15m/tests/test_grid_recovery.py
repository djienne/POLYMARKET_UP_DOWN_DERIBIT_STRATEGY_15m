"""Regression tests for grid snapshot recovery and barrier settlement."""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest
from dataclasses import replace

from btc_pricer.edge import required_model_prob
from scripts.trader.config import DEFAULT_DRY_RUN_FRICTION_PCT, GridConfig, InstanceParams
from scripts.trader.main import recover_grid_instances_from_csv, save_state, try_recover
from scripts.trader import main as grid_main
from scripts.trader.position import Direction, MicroPosition, TradeResult
from scripts.trader.trader import PaperInstance
from tests.conftest import make_tick


def _entry_sig(instance_id, direction, model_prob, market_prob, alpha=1.3, floor=0.50):
    return {
        "instance_id": instance_id,
        "direction": direction,
        "edge_ratio": round(model_prob / market_prob, 4) if market_prob > 0 else 0,
        "executable_price": market_prob,
        "required_prob": round(required_model_prob(market_prob, alpha, floor), 4),
        "model_prob": round(model_prob, 4),
    }


def _make_instance(
    *,
    instance_id: int = 0,
    params: InstanceParams | None = None,
    config: GridConfig | None = None,
) -> PaperInstance:
    config = config or GridConfig(starting_capital=1000.0)
    params = params or InstanceParams(
        alpha_up=1.3,
        alpha_down=1.3,
        floor_up=0.50,
        floor_down=0.50,
        tp_percentage=0.15,
        stop_loss_pct=0.0,
    )
    return PaperInstance(id=instance_id, params=params, shared_config=config)


def _snapshot_without_params(instance: PaperInstance) -> dict:
    snap = instance.to_snapshot()
    snap.pop("params", None)
    return snap


GRID_CSV_HEADER = (
    "instance_id,timestamp,event,direction,market_id,entry_price,exit_price,"
    "shares,pnl,pnl_pct,capital,model_prob,poly_prob,spot_price,barrier,"
    "liquidity_mode,settlement_source,effective_tp_pct\n"
)


def _grid_row(
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


def test_grid_recovery_rebuilds_closed_stats_and_open_position_from_csv(tmp_path):
    trades_path = tmp_path / "trades.csv"
    trades_path.write_text(
        GRID_CSV_HEADER
        + _grid_row(
            "ENTRY",
            timestamp="2026-04-29T14:00:00+00:00",
            market_id="btc-updown-15m-A",
        )
        + _grid_row(
            "TP_FILLED",
            timestamp="2026-04-29T14:01:00+00:00",
            market_id="btc-updown-15m-A",
            exit_price="0.7000",
            pnl="1.23",
            capital="1001.23",
        )
        + _grid_row(
            "ENTRY",
            timestamp="2026-04-29T14:05:00+00:00",
            market_id="btc-updown-15m-B",
            entry_price="0.6000",
            shares="20.0000",
            capital="989.05",
        ),
        encoding="utf-8",
    )

    inst = _make_instance()
    inst.trades_count = 0

    applied, markets = recover_grid_instances_from_csv(
        [inst],
        trades_path,
        starting_capital=1000.0,
        friction_pct=DEFAULT_DRY_RUN_FRICTION_PCT,
    )

    assert applied == 1
    assert markets == 1
    assert inst.trades_count == 1
    assert inst.wins == 1
    assert inst.losses == 0
    assert inst.trade_pnls == [1.23]
    assert inst.position is not None
    assert inst.position.market_id == "btc-updown-15m-B"
    assert inst.last_entered_market_id == "btc-updown-15m-B"
    assert inst.capital == pytest.approx(
        1000.0 + 1.23 - (0.6 * 20.0 * (1.0 + DEFAULT_DRY_RUN_FRICTION_PCT))
    )


def test_grid_recovery_ignores_duplicate_entry_and_close_for_same_market(tmp_path):
    trades_path = tmp_path / "trades.csv"
    trades_path.write_text(
        GRID_CSV_HEADER
        + _grid_row(
            "ENTRY",
            timestamp="2026-04-29T14:00:00+00:00",
            market_id="btc-updown-15m-A",
            entry_price="0.5000",
        )
        + _grid_row(
            "ENTRY",
            timestamp="2026-04-29T14:00:10+00:00",
            market_id="btc-updown-15m-A",
            entry_price="0.6000",
        )
        + _grid_row(
            "TP_FILLED",
            timestamp="2026-04-29T14:01:00+00:00",
            market_id="btc-updown-15m-A",
            entry_price="0.6000",
            exit_price="0.8000",
            pnl="2.00",
        )
        + _grid_row(
            "TP_FILLED",
            timestamp="2026-04-29T14:01:10+00:00",
            market_id="btc-updown-15m-A",
            entry_price="0.5000",
            exit_price="0.7000",
            pnl="1.00",
        ),
        encoding="utf-8",
    )

    inst = _make_instance()
    recover_grid_instances_from_csv([inst], trades_path)

    assert inst.trades_count == 1
    assert inst.trade_pnls == [1.0]
    assert inst.total_pnl == 1.0
    assert inst.position is None


def test_save_state_refuses_to_overwrite_richer_grid_history(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    results = tmp_path / "results"
    results.mkdir()
    (results / "trades.csv").write_text(GRID_CSV_HEADER, encoding="utf-8")
    (results / "state_snapshot.json").write_text(
        '{"instances":{"0":{"trades_count":10}}}',
        encoding="utf-8",
    )

    inst = _make_instance()
    inst.trades_count = 1

    with pytest.raises(RuntimeError, match="Refusing to overwrite grid history"):
        save_state([inst])


def test_save_state_allows_csv_recovery_to_repair_stale_derived_history(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    results = tmp_path / "results"
    results.mkdir()
    (results / "trades.csv").write_text(
        GRID_CSV_HEADER
        + _grid_row(
            "ENTRY",
            timestamp="2026-04-29T14:00:00+00:00",
            market_id="btc-updown-15m-A",
        )
        + _grid_row(
            "TP_FILLED",
            timestamp="2026-04-29T14:01:00+00:00",
            market_id="btc-updown-15m-A",
            exit_price="0.7000",
            pnl="1.23",
            capital="1001.23",
        ),
        encoding="utf-8",
    )
    (results / "state_snapshot.json").write_text(
        '{"instances":{"0":{"trades_count":10}}}',
        encoding="utf-8",
    )
    (results / "leaderboard.csv").write_text(
        "instance_id,trades\n0,10\n",
        encoding="utf-8",
    )

    inst = _make_instance()
    applied, _ = recover_grid_instances_from_csv([inst])

    assert applied == 1
    assert inst.trades_count == 1
    save_state([inst])

    repaired = json.loads((results / "state_snapshot.json").read_text(encoding="utf-8"))
    assert repaired["instances"]["0"]["trades_count"] == 1


def test_save_state_writes_params_to_snapshot(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    results = tmp_path / "results"
    results.mkdir()

    inst = _make_instance()

    save_state([inst])

    saved = json.loads((results / "state_snapshot.json").read_text(encoding="utf-8"))
    params = saved["instances"]["0"]["params"]
    assert params == {
        "alpha_up": 1.3,
        "alpha_down": 1.3,
        "floor_up": 0.5,
        "floor_down": 0.5,
        "tp_percentage": 0.15,
        "stop_loss_pct": 0.0,
    }


def test_save_state_continues_when_snapshot_replace_is_locked(tmp_path, monkeypatch, caplog):
    monkeypatch.chdir(tmp_path)
    results = tmp_path / "results"
    results.mkdir()

    def locked_snapshot_write(path, payload, **kwargs):
        if path.name == "state_snapshot.json":
            raise PermissionError("simulated locked state snapshot")
        grid_main.atomic_write_json(path, payload, **kwargs)

    monkeypatch.setattr(grid_main, "atomic_write_json", locked_snapshot_write)

    # The leaderboard only lists traded instances, so give it one trade.
    inst = _make_instance()
    inst.restore_pnl_history([1.0], total_pnl=1.0, pnl_count=1)
    inst.trades_count = 1
    inst.wins = 1
    save_state([inst])

    assert not (results / "state_snapshot.json").exists()
    leaderboard = (results / "leaderboard.csv").read_text(encoding="utf-8")
    assert "instance_id" in leaderboard
    assert "\n1,0," in leaderboard  # rank 1, instance_id 0
    assert "state snapshot write failed" in caplog.text


def test_try_recover_loads_snapshot_when_params_match(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    results = tmp_path / "results"
    results.mkdir()

    saved = _make_instance()
    saved.restore_pnl_history([4.0], total_pnl=4.0, pnl_count=1)
    saved.total_pnl = 4.0
    saved.capital = 1004.0
    saved.wins = 1
    saved.trades_count = 1
    (results / "state_snapshot.json").write_text(
        json.dumps({
            "n_instances": 1,
            "markets_resolved": 3,
            "instances": {"0": saved.to_snapshot()},
        }),
        encoding="utf-8",
    )

    recovered = _make_instance()

    markets = try_recover([recovered])

    assert markets == 3
    assert recovered.trades_count == 1
    assert recovered.total_pnl == pytest.approx(4.0)
    assert recovered.capital == pytest.approx(1004.0)


def test_try_recover_skips_snapshot_when_params_mismatch(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    results = tmp_path / "results"
    results.mkdir()

    saved = _make_instance(
        params=InstanceParams(
            alpha_up=9.9,
            alpha_down=1.3,
            floor_up=0.50,
            floor_down=0.50,
            tp_percentage=0.15,
            stop_loss_pct=0.0,
        )
    )
    saved.restore_pnl_history([99.0], total_pnl=99.0, pnl_count=1)
    saved.total_pnl = 99.0
    saved.capital = 1099.0
    saved.trades_count = 1
    (results / "state_snapshot.json").write_text(
        json.dumps({
            "n_instances": 1,
            "markets_resolved": 9,
            "instances": {"0": saved.to_snapshot()},
        }),
        encoding="utf-8",
    )

    recovered = _make_instance()

    markets = try_recover([recovered])

    assert markets == 0
    assert recovered.trades_count == 0
    assert recovered.total_pnl == pytest.approx(0.0)
    assert recovered.capital == pytest.approx(recovered.shared_config.starting_capital)


def test_try_recover_accepts_appended_grid_instances_from_legacy_snapshot(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    results = tmp_path / "results"
    results.mkdir()

    saved = _make_instance()
    saved.restore_pnl_history([5.0, 7.0], total_pnl=12.0, pnl_count=2)
    saved.total_pnl = 12.0
    saved.capital = 1012.0
    saved.wins = 2
    saved.trades_count = 2
    (results / "state_snapshot.json").write_text(
        json.dumps({
            "n_instances": 1,
            "markets_resolved": 7,
            "instances": {"0": _snapshot_without_params(saved)},
        }),
        encoding="utf-8",
    )

    recovered = _make_instance()
    appended = _make_instance(
        instance_id=1,
        params=InstanceParams(
            alpha_up=1.3,
            alpha_down=1.3,
            floor_up=0.50,
            floor_down=0.50,
            tp_percentage=0.15,
            stop_loss_pct=0.40,
        ),
        config=recovered.shared_config,
    )

    markets = try_recover([recovered, appended])

    assert markets == 7
    assert recovered.trades_count == 2
    assert recovered.total_pnl == pytest.approx(12.0)
    assert recovered.trade_pnls == [5.0, 7.0]
    assert appended.trades_count == 0
    assert appended.capital == pytest.approx(appended.shared_config.starting_capital)


def test_try_recover_accepts_appended_grid_instances_with_param_snapshot(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    results = tmp_path / "results"
    results.mkdir()

    saved = _make_instance()
    saved.restore_pnl_history([5.0, 7.0], total_pnl=12.0, pnl_count=2)
    saved.total_pnl = 12.0
    saved.capital = 1012.0
    saved.wins = 2
    saved.trades_count = 2
    (results / "state_snapshot.json").write_text(
        json.dumps({
            "n_instances": 1,
            "markets_resolved": 7,
            "instances": {"0": saved.to_snapshot()},
        }),
        encoding="utf-8",
    )

    recovered = _make_instance()
    appended = _make_instance(
        instance_id=1,
        params=InstanceParams(
            alpha_up=1.3,
            alpha_down=1.3,
            floor_up=0.50,
            floor_down=0.50,
            tp_percentage=0.15,
            stop_loss_pct=0.40,
        ),
        config=recovered.shared_config,
    )

    markets = try_recover([recovered, appended])

    assert markets == 7
    assert recovered.trades_count == 2
    assert recovered.total_pnl == pytest.approx(12.0)
    assert appended.trades_count == 0
    assert appended.capital == pytest.approx(appended.shared_config.starting_capital)


def test_try_recover_skips_appended_grid_old_id_when_params_mismatch(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    results = tmp_path / "results"
    results.mkdir()

    saved = _make_instance(
        params=InstanceParams(
            alpha_up=9.9,
            alpha_down=1.3,
            floor_up=0.50,
            floor_down=0.50,
            tp_percentage=0.15,
            stop_loss_pct=0.0,
        )
    )
    saved.restore_pnl_history([99.0], total_pnl=99.0, pnl_count=1)
    saved.total_pnl = 99.0
    saved.capital = 1099.0
    saved.trades_count = 1
    (results / "state_snapshot.json").write_text(
        json.dumps({
            "n_instances": 1,
            "markets_resolved": 9,
            "instances": {"0": saved.to_snapshot()},
        }),
        encoding="utf-8",
    )

    recovered = _make_instance()
    appended = _make_instance(
        instance_id=1,
        params=InstanceParams(
            alpha_up=1.3,
            alpha_down=1.3,
            floor_up=0.50,
            floor_down=0.50,
            tp_percentage=0.15,
            stop_loss_pct=0.40,
        ),
        config=recovered.shared_config,
    )

    markets = try_recover([recovered, appended])

    assert markets == 0
    assert recovered.trades_count == 0
    assert recovered.capital == pytest.approx(recovered.shared_config.starting_capital)
    assert appended.trades_count == 0
    assert appended.capital == pytest.approx(appended.shared_config.starting_capital)


def test_save_state_writes_gzipped_backups(tmp_path, monkeypatch):
    """save_state archives a gzipped copy of each small derived file
    before overwriting it. Detailed retention behaviour is tested in
    tests/test_grid_backups.py; here we just confirm the integration
    point produces .bak.gz files that round-trip cleanly.
    """
    import gzip

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(grid_main, "GRID_BACKUP_INTERVAL_SECONDS", 0)
    grid_main._MIGRATED_BACKUP_DIRS.clear()
    results = tmp_path / "results"
    results.mkdir()
    (results / "state_snapshot.json").write_text(
        '{"instances":{"0":{"trades_count":0}}}',
        encoding="utf-8",
    )
    (results / "leaderboard.csv").write_text("old csv", encoding="utf-8")
    (results / "leaderboard.txt").write_text("old txt", encoding="utf-8")

    inst = _make_instance()
    save_state([inst])

    backups = tmp_path / "results" / "backups" / "grid_state"
    state_baks = list(backups.glob("state_snapshot.json.*.bak.gz"))
    csv_baks = list(backups.glob("leaderboard.csv.*.bak.gz"))
    txt_baks = list(backups.glob("leaderboard.txt.*.bak.gz"))

    assert len(state_baks) >= 1
    assert len(csv_baks) >= 1
    assert len(txt_baks) >= 1
    # No plain .bak files — only gzipped.
    assert not list(backups.glob("*.bak"))

    # Gzip round-trip works on the most recent state snapshot.
    latest = max(state_baks, key=lambda p: p.stat().st_mtime)
    with gzip.open(latest, "rb") as f:
        assert b'"instances"' in f.read()


def test_snapshot_round_trips_reference_price():
    inst = _make_instance()
    sig = _entry_sig(0, "UP", 0.80, 0.50)
    tick = replace(
        make_tick(
            elapsed_seconds=400,
            time_remaining_seconds=500,
            prob_above=0.80,
            poly_prob_up=0.50,
            barrier=83000.0,
        ),
        entry_signals=(sig,),
    )

    inst.on_tick(tick)
    snap = inst.to_snapshot()

    assert snap["position"]["reference_price"] == 83000.0

    restored = _make_instance()
    restored.load_snapshot(snap)

    assert restored.position is not None
    assert restored.position.reference_price == 83000.0


def test_recovered_expiry_uses_position_reference_price():
    inst = _make_instance()
    inst.position = MicroPosition(
        direction=Direction.UP,
        token_id="test_up_token",
        entry_price=0.50,
        shares=10.0,
        cost_basis=5.0,
        tp_price=0.60,
        stop_loss_price=0.0,
        opened_at=datetime.now(timezone.utc),
        market_id="btc-updown-15m-test",
        peak_price=0.50,
        reference_price=83000.0,
    )

    tick = make_tick(
        market_id=inst.position.market_id,
        is_expired=True,
        barrier=82000.0,
        final_spot=82500.0,
        spot_price=82500.0,
    )

    events = inst.on_tick(tick)

    assert len(events) == 1
    assert events[0].event == "LOSS_EXPIRY"
    assert events[0].barrier == 83000.0


def test_old_snapshot_without_reference_price_falls_back_to_tick_barrier():
    inst = _make_instance()
    sig = _entry_sig(0, "UP", 0.80, 0.50)
    tick = replace(
        make_tick(
            elapsed_seconds=400,
            time_remaining_seconds=500,
            prob_above=0.80,
            poly_prob_up=0.50,
            barrier=83000.0,
        ),
        entry_signals=(sig,),
    )
    inst.on_tick(tick)
    snap = inst.to_snapshot()
    snap["position"].pop("reference_price", None)

    restored = _make_instance()
    restored.load_snapshot(snap)

    expiry_tick = make_tick(
        is_expired=True,
        barrier=82000.0,
        final_spot=82500.0,
        spot_price=82500.0,
    )

    events = restored.on_tick(expiry_tick)

    assert len(events) == 1
    assert events[0].event == "WIN_EXPIRY"
    assert events[0].barrier == 82000.0


# ---------------------------------------------------------------
# Fix #1 — settled_market_ids makes restart-after-expiry idempotent
# ---------------------------------------------------------------

def test_settle_expiry_records_settled_market_id():
    """After an expiry tick settles the position, the market_id must be in
    settled_market_ids so a restart can detect it."""
    inst = _make_instance()
    inst.position = MicroPosition(
        direction=Direction.UP,
        token_id="test_up_token",
        entry_price=0.50,
        shares=10.0,
        cost_basis=5.0,
        tp_price=0.60,
        stop_loss_price=0.0,
        opened_at=datetime.now(timezone.utc),
        market_id="btc-updown-15m-A",
        peak_price=0.50,
        reference_price=83000.0,
    )
    tick = make_tick(
        market_id="btc-updown-15m-A",
        is_expired=True,
        barrier=83000.0,
        final_spot=83500.0,
        spot_price=83500.0,
    )

    events = inst.on_tick(tick)

    assert len(events) == 1
    assert events[0].event == "WIN_EXPIRY"
    assert "btc-updown-15m-A" in inst.settled_market_ids


def test_already_settled_market_is_cleared_silently():
    """Fix #1: if we crashed between CSV flush and state save, on restart the
    snapshot shows the position open for market X but settled_market_ids
    contains X. handle_market_transition must clear the position silently."""
    inst = _make_instance()
    inst.position = MicroPosition(
        direction=Direction.UP,
        token_id="test_up_token",
        entry_price=0.50,
        shares=10.0,
        cost_basis=5.0,
        tp_price=0.60,
        stop_loss_price=0.0,
        opened_at=datetime.now(timezone.utc),
        market_id="btc-updown-15m-A",
        peak_price=0.50,
        reference_price=83000.0,
    )
    inst.settled_market_ids.add("btc-updown-15m-A")

    wins_before = inst.wins
    losses_before = inst.losses
    trades_before = inst.trades_count

    # New market tick, different market_id, is_new_market=True
    tick = make_tick(
        market_id="btc-updown-15m-B",
        elapsed_seconds=100,
        time_remaining_seconds=800,
        is_new_market=True,
    )

    events = inst.on_tick(tick)

    # No event emitted (idempotent clear). Counters untouched.
    assert events == []
    assert inst.position is None
    assert inst.wins == wins_before
    assert inst.losses == losses_before
    assert inst.trades_count == trades_before


def test_settled_market_ids_round_trips_through_snapshot():
    inst = _make_instance()
    inst.settled_market_ids.update(
        ["btc-updown-15m-A", "btc-updown-15m-B", "btc-updown-15m-C"]
    )
    snap = inst.to_snapshot()
    # JSON-friendly (sorted list, not set)
    assert isinstance(snap["settled_market_ids"], list)

    restored = _make_instance()
    restored.load_snapshot(snap)
    assert restored.settled_market_ids == inst.settled_market_ids


def test_snapshot_pnl_history_is_bounded_but_running_stats_survive():
    config = GridConfig(starting_capital=1000.0, max_trade_pnls=2)
    inst = PaperInstance(
        id=0,
        params=InstanceParams(
            alpha_up=1.3,
            alpha_down=1.3,
            floor_up=0.50,
            floor_down=0.50,
            tp_percentage=0.15,
            stop_loss_pct=0.0,
        ),
        shared_config=config,
    )
    for pnl in [1.0, -2.0, 3.0]:
        inst._record_closed_pnl(pnl)

    assert inst.trade_pnls == [-2.0, 3.0]
    assert inst.pnl_count == 3
    assert inst.pnl_sum == pytest.approx(2.0)

    restored = PaperInstance(id=0, params=inst.params, shared_config=config)
    restored.load_snapshot(inst.to_snapshot())

    assert restored.trade_pnls == [-2.0, 3.0]
    assert restored.pnl_count == 3
    assert restored.pnl_sum == pytest.approx(2.0)


def test_settled_market_ids_are_pruned_to_config_limit():
    config = GridConfig(starting_capital=1000.0, max_settled_market_ids=2)
    inst = PaperInstance(
        id=0,
        params=InstanceParams(
            alpha_up=1.3,
            alpha_down=1.3,
            floor_up=0.50,
            floor_down=0.50,
            tp_percentage=0.15,
            stop_loss_pct=0.0,
        ),
        shared_config=config,
    )

    for suffix in ["001", "002", "003"]:
        inst._remember_settled_market_id(f"btc-updown-15m-{suffix}")

    assert inst.settled_market_ids == {"btc-updown-15m-002", "btc-updown-15m-003"}


# ---------------------------------------------------------------
# Fix #3 — cross-market restart emits UNRESOLVED_RESTART, not LOSS_EXPIRY
# ---------------------------------------------------------------

def test_cross_market_restart_retains_exposure_until_own_market_resolves():
    """A new market provides no evidence of the old position's payout."""
    inst = _make_instance()
    entry_price = 0.60
    cost_basis = 30.0  # $30 sunk into the position
    capital_before_tick = inst.capital
    inst.position = MicroPosition(
        direction=Direction.UP,
        token_id="test_up_token",
        entry_price=entry_price,
        shares=50.0,
        cost_basis=cost_basis,
        tp_price=0.72,
        stop_loss_price=0.0,
        opened_at=datetime.now(timezone.utc),
        market_id="btc-updown-15m-OLD",
        peak_price=entry_price,
        reference_price=83000.0,
    )

    tick = make_tick(
        market_id="btc-updown-15m-NEW",
        elapsed_seconds=100,
        time_remaining_seconds=800,
        is_new_market=True,
    )

    wins_before = inst.wins
    losses_before = inst.losses

    events = inst.on_tick(tick)

    assert events == []
    assert inst.capital == capital_before_tick
    # Wins/losses untouched so leaderboard isn't biased
    assert inst.wins == wins_before
    assert inst.losses == losses_before
    assert inst.trades_count == 0
    assert inst.position is not None
    assert inst.position.market_id == "btc-updown-15m-OLD"


# ---------------------------------------------------------------
# Fix #2 — settlement_source tagged on expiry events
# ---------------------------------------------------------------

def test_settlement_source_tagged_on_win_expiry():
    inst = _make_instance()
    inst.position = MicroPosition(
        direction=Direction.UP,
        token_id="test_up_token",
        entry_price=0.50,
        shares=10.0,
        cost_basis=5.0,
        tp_price=0.60,
        stop_loss_price=0.0,
        opened_at=datetime.now(timezone.utc),
        market_id="btc-updown-15m-A",
        peak_price=0.50,
        reference_price=83000.0,
    )
    tick = make_tick(
        market_id=inst.position.market_id,
        is_expired=True,
        barrier=83000.0,
        final_spot=83500.0,
        spot_price=83500.0,
        final_spot_source="chainlink_end",
    )

    events = inst.on_tick(tick)

    assert len(events) == 1
    assert events[0].event == "WIN_EXPIRY"
    assert events[0].settlement_source == "chainlink_end"


def test_settlement_source_tagged_on_loss_expiry_with_binance_fallback():
    inst = _make_instance()
    inst.position = MicroPosition(
        direction=Direction.UP,
        token_id="test_up_token",
        entry_price=0.50,
        shares=10.0,
        cost_basis=5.0,
        tp_price=0.60,
        stop_loss_price=0.0,
        opened_at=datetime.now(timezone.utc),
        market_id="btc-updown-15m-A",
        peak_price=0.50,
        reference_price=83000.0,
    )
    tick = make_tick(
        market_id=inst.position.market_id,
        is_expired=True,
        barrier=83000.0,
        final_spot=82500.0,
        spot_price=82500.0,
        final_spot_source="binance_end",
    )

    events = inst.on_tick(tick)

    assert len(events) == 1
    assert events[0].event == "LOSS_EXPIRY"
    assert events[0].settlement_source == "binance_end"


# ---------------------------------------------------------------
# Fix #5 — effective_tp_pct logged on entry and exit
# ---------------------------------------------------------------

def test_effective_tp_pct_logged_on_entry_and_matches_on_exit():
    """At an entry near the ceiling, the configured tp_percentage clips to
    0.99. The event must expose the actual (effective) TP for both the
    entry and the matching exit row."""
    config = GridConfig(starting_capital=1000.0, max_entry_price=0.95)
    params = InstanceParams(
        # alpha=1.0 keeps required_model_prob = market, so a small model edge
        # against a ~0.91 ask is enough to enter.
        alpha_up=1.0,
        alpha_down=1.0,
        floor_up=0.50,
        floor_down=0.50,
        tp_percentage=0.30,  # Will clip at high entry
        stop_loss_pct=0.0,
    )
    inst = PaperInstance(id=0, params=params, shared_config=config)

    # Entry against a book with mid ~0.90 → best_ask ~0.91, which clips TP.
    sig = _entry_sig(0, "UP", 0.95, 0.90, alpha=1.0, floor=0.50)
    tick = replace(
        make_tick(
            elapsed_seconds=400,
            time_remaining_seconds=500,
            prob_above=0.95,
            poly_prob_up=0.90,
            barrier=83000.0,
            spot_price=83100.0,
        ),
        entry_signals=(sig,),
    )
    events = inst.on_tick(tick)

    assert len(events) == 1
    entry_event = events[0]
    assert entry_event.event == "ENTRY"

    # Effective TP = (tp_price - entry_price) / entry_price, with tp_price
    # clipped at 0.99.
    expected = (0.99 - entry_event.entry_price) / entry_event.entry_price
    assert abs(entry_event.effective_tp_pct - expected) < 1e-6
    # And the clipping really happened for this high-entry case.
    assert entry_event.effective_tp_pct < params.tp_percentage

    # Force expiry — the exit row must carry the same effective_tp_pct.
    expiry = make_tick(
        is_expired=True,
        barrier=83000.0,
        final_spot=83200.0,
        spot_price=83200.0,
        final_spot_source="chainlink_end",
    )
    exit_events = inst.on_tick(expiry)
    assert len(exit_events) == 1
    assert abs(exit_events[0].effective_tp_pct - entry_event.effective_tp_pct) < 1e-6


def test_grid_recovery_matches_close_by_market_direction_fallback(tmp_path, caplog):
    """entry_price drift > 1e-9 between ENTRY and close must not drop the close."""
    import logging

    trades_path = tmp_path / "trades.csv"
    trades_path.write_text(
        GRID_CSV_HEADER
        + _grid_row(
            "ENTRY",
            timestamp="2026-04-29T14:00:00+00:00",
            market_id="btc-updown-15m-A",
            entry_price="0.6543",
        )
        + _grid_row(
            "TP_FILLED",
            timestamp="2026-04-29T14:01:00+00:00",
            market_id="btc-updown-15m-A",
            entry_price="0.6544",  # precision drift vs the ENTRY row
            exit_price="0.8000",
            pnl="1.50",
        ),
        encoding="utf-8",
    )

    inst = _make_instance()
    with caplog.at_level(logging.WARNING, logger="grid_trader"):
        applied, markets = recover_grid_instances_from_csv(
            [inst], trades_path, starting_capital=1000.0
        )

    assert applied == 1
    assert markets == 1
    assert inst.trades_count == 1
    assert inst.wins == 1
    assert inst.position is None
    assert inst.capital == pytest.approx(1001.50)
    assert any("fallback" in rec.message for rec in caplog.records)


def test_grid_recovery_warns_and_drops_close_without_matching_entry(tmp_path, caplog):
    """A close that matches no open entry is still dropped, but now loudly."""
    import logging

    trades_path = tmp_path / "trades.csv"
    trades_path.write_text(
        GRID_CSV_HEADER
        + _grid_row(
            "ENTRY",
            timestamp="2026-04-29T14:00:00+00:00",
            market_id="btc-updown-15m-A",
        )
        + _grid_row(
            "TP_FILLED",
            timestamp="2026-04-29T14:01:00+00:00",
            market_id="btc-updown-15m-B",  # no ENTRY for this market
            exit_price="0.8000",
            pnl="9.99",
        ),
        encoding="utf-8",
    )

    inst = _make_instance()
    with caplog.at_level(logging.WARNING, logger="grid_trader"):
        recover_grid_instances_from_csv([inst], trades_path, starting_capital=1000.0)

    assert inst.trades_count == 0
    assert inst.position is not None
    assert inst.position.market_id == "btc-updown-15m-A"
    assert any("dropping close row" in rec.message for rec in caplog.records)


def test_grid_recovery_matches_close_with_direction_case_mismatch(tmp_path):
    """Direction string case drift recovers via the (market_id, direction) fallback."""
    trades_path = tmp_path / "trades.csv"
    close_row = (
        "0,2026-04-29T14:01:00+00:00,TP_FILLED,up,btc-updown-15m-A,0.5000,0.8000,"
        "10.0000,3.00,,1003.00,0.7000,0.5000,83000.00,83100.00,independent,,0.1500\n"
    )
    trades_path.write_text(
        GRID_CSV_HEADER
        + _grid_row(
            "ENTRY",
            timestamp="2026-04-29T14:00:00+00:00",
            market_id="btc-updown-15m-A",
        )
        + close_row,
        encoding="utf-8",
    )

    inst = _make_instance()
    recover_grid_instances_from_csv([inst], trades_path, starting_capital=1000.0)

    assert inst.trades_count == 1
    assert inst.wins == 1
    assert inst.position is None
