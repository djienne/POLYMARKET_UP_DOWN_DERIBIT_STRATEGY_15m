from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from scripts.trader.calc_daemon import CalcConfig
from scripts.trader.config import GridConfig, TraderConfig
from scripts.trader.hybrid_calibration import CalibrationEvent, MarketContext
from scripts.trader.ipc import (
    CompositeCalibrationPublisher,
    FileCalibrationPublisher,
    FileCalibrationSubscriber,
    RedisStreamCalibrationSubscriber,
)
from scripts.trader.leaderboard_ipc import (
    DEFAULT_CHANNEL as LEADERBOARD_DEFAULT_CHANNEL,
    DEFAULT_LATEST_KEY as LEADERBOARD_DEFAULT_LATEST_KEY,
    LeaderboardIPCConfig,
    _rows_from_csv,
)
from scripts.trader.market_context_ipc import MarketContextTick
from scripts.trader.market_orchestrator import MarketOrchestrator
from scripts.trader.persistence import SqliteStore


def _event() -> CalibrationEvent:
    now = datetime.now(timezone.utc)
    context = MarketContext(
        market_id="btc-updown-15m-ipc",
        barrier=80000.0,
        market_end_time_iso=(now + timedelta(minutes=3)).isoformat(),
        barrier_source="polymarket",
        barrier_offset_s=0.0,
    )
    return CalibrationEvent.from_parts(
        source="test",
        context=context,
        ready_at=now,
        model_data={
            "prob_above": 0.62,
            "prob_below": 0.38,
            "model_used": "ssvi_surface",
            "calibration_seconds": 1.2,
        },
        terminal_payload={"timing": {"surface_fit_s": 1.0, "mc_s": 0.2}},
    )


def test_file_ipc_adapter_round_trips_and_deletes(tmp_path: Path) -> None:
    inbox = tmp_path / "inbox"
    event = _event()

    FileCalibrationPublisher((inbox,)).publish(event)
    assert len(list(inbox.glob("*.json"))) == 1

    events = FileCalibrationSubscriber(inbox).poll()

    assert [ev.event_id for ev in events] == [event.event_id]
    assert list(inbox.glob("*.json")) == []


def test_composite_ipc_keeps_file_fallback_when_primary_fails(tmp_path: Path) -> None:
    class FailingPublisher:
        def publish(self, event: CalibrationEvent) -> None:
            raise RuntimeError("redis down")

    inbox = tmp_path / "inbox"
    event = _event()
    publisher = CompositeCalibrationPublisher(
        [FailingPublisher(), FileCalibrationPublisher((inbox,))]
    )

    publisher.publish(event)

    assert len(FileCalibrationSubscriber(inbox).poll()) == 1


def test_redis_subscriber_poll_is_nonblocking_by_default() -> None:
    class FakeRedis:
        def __init__(self) -> None:
            self.kwargs = None

        def xreadgroup(self, group, consumer, streams, **kwargs):
            self.kwargs = kwargs
            return []

    fake = FakeRedis()
    subscriber = RedisStreamCalibrationSubscriber.__new__(RedisStreamCalibrationSubscriber)
    subscriber.group = "live"
    subscriber.consumer = "live"
    subscriber.stream = "market.calibration"
    subscriber.count = 50
    subscriber.block_ms = None
    subscriber._client = fake

    assert subscriber.poll() == []
    assert fake.kwargs == {"count": 50}


def test_redis_subscriber_drops_and_acks_invalid_probability_event() -> None:
    """A NaN-probability event must be dropped AND acked, never retried."""
    from dataclasses import asdict

    bad = asdict(_event())
    bad["prob_above"] = float("nan")  # json.dumps emits NaN; json.loads accepts it
    payload = json.dumps(bad).encode("utf-8")

    class FakeRedis:
        def __init__(self) -> None:
            self.acked: list = []
            self.reads = 0

        def xreadgroup(self, group, consumer, streams, **kwargs):
            self.reads += 1
            if self.reads > 1:
                return []
            return [(b"market.calibration", [(b"1-1", {b"payload": payload})])]

        def xack(self, stream, group, *ids):
            self.acked.extend(ids)

    fake = FakeRedis()
    subscriber = RedisStreamCalibrationSubscriber.__new__(RedisStreamCalibrationSubscriber)
    subscriber.group = "live"
    subscriber.consumer = "live"
    subscriber.stream = "market.calibration"
    subscriber.count = 50
    subscriber.block_ms = None
    subscriber._client = fake

    assert subscriber.poll() == []
    assert fake.acked == [b"1-1"]


def test_market_orchestrator_persists_json_and_sqlite(tmp_path: Path) -> None:
    db = SqliteStore(tmp_path / "markets.sqlite")
    orch = MarketOrchestrator(settlements_dir=tmp_path / "settlements", sqlite_store=db)
    start = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
    end = start + timedelta(minutes=15)

    assert orch.write_strike(
        market_id="btc-updown-15m-market",
        window_start=start,
        window_end=end,
        strike=80000.0,
        strike_offset_s=0.0,
        strike_source="polymarket",
    ) is True
    orch.finalize(
        market_id="btc-updown-15m-market",
        window_start=start,
        window_end=end,
        strike=80000.0,
        strike_offset_s=0.0,
        settlement_price=80010.0,
        settlement_offset_s=0.0,
        settlement_source="polymarket",
    )

    json_payload = json.loads(
        (tmp_path / "settlements" / "btc-updown-15m-market.json").read_text()
    )
    sqlite_payload = db.get_market("btc-updown-15m-market")

    assert json_payload["settlement_status"] == "finalized"
    assert sqlite_payload is not None
    assert sqlite_payload["strike_source"] == "polymarket"
    assert sqlite_payload["settlement_source"] == "polymarket"


def test_market_orchestrator_restores_from_sqlite_without_json_exports(tmp_path: Path) -> None:
    db = SqliteStore(tmp_path / "markets.sqlite")
    market_id = "btc-updown-15m-sqlite-only"
    start = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
    end = start + timedelta(minutes=15)
    db.upsert_market(
        {
            "market_id": market_id,
            "slug": market_id,
            "window_start_iso": start.isoformat(),
            "window_end_iso": end.isoformat(),
            "strike": 80000.0,
            "strike_offset_s": 0.0,
            "strike_source": "chain_broken",
            "strike_captured_at_iso": start.isoformat(),
            "settlement_status": "pending",
            "settlement_price": None,
            "settlement_offset_s": None,
            "settlement_source": None,
            "settlement_captured_at_iso": None,
        }
    )
    orch = MarketOrchestrator(settlements_dir=tmp_path / "empty_settlements", sqlite_store=db)

    assert orch.write_strike(
        market_id=market_id,
        window_start=start,
        window_end=end,
        strike=79900.0,
        strike_offset_s=1.0,
        strike_source="polymarket",
    ) is False

    barriers, pending, chain_broken = orch.restore_state()

    assert barriers[market_id] == (80000.0, "chain_broken", 0.0)
    assert pending[market_id].window_end == end
    assert market_id in chain_broken


def test_single_yaml_can_load_calc_grid_and_live_sections(tmp_path: Path) -> None:
    cfg_path = tmp_path / "config.yaml"
    cfg_path.write_text(
        """
calc:
  inbox_targets: [results/calibration_inbox_grid, results/calibration_inbox_live]
  snapshot_path: results/terminal_data.shared.json
  delay_min_seconds: 0
  delay_max_seconds: 0
  inbox_ttl_seconds: 300
  source_label: shared_calc
  market_scan_interval_seconds: 30
  ipc_backend: redis
  redis_url: redis://redis:6379/0
  market_db_path: results/db/markets.sqlite
grid:
  starting_capital: 1000
  no_trade_first_seconds: 100
  no_trade_last_seconds: 90
  hybrid_calibration_enabled: true
  ipc_backend: redis
  grid:
    alpha_up: [2.5]
    alpha_down: [2.5]
    floor_up: [0.65]
    floor_down: [0.45]
    tp_percentage: [0.3]
    stop_loss_pct: [0.0]
live:
  paper_mode: false
  persistence_backend: sqlite
  live_sqlite_db_file: results/db/live.sqlite
  ipc_backend: redis
  edge_alpha_up: 2.5
  edge_alpha_down: 2.5
  edge_floor_up: 0.65
  edge_floor_down: 0.45
  tp_percentage: 0.3
""",
        encoding="utf-8",
    )

    calc = CalcConfig.from_json(cfg_path)
    grid = GridConfig.from_json(cfg_path)
    live = TraderConfig.from_json(cfg_path)

    assert calc.ipc_backend == "redis"
    assert calc.market_db_path == Path("results/db/markets.sqlite")
    assert len(grid.generate_params()) == 1
    assert grid.ipc_backend == "redis"
    assert live.sqlite_enabled()
    assert live.market_db_path == (tmp_path / "results/db/markets.sqlite").resolve()
    assert live.get_sqlite_db_file() == (tmp_path / "results/db/live.sqlite").resolve()


def test_market_context_tick_round_trips() -> None:
    start = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
    end = start + timedelta(minutes=15)

    tick = MarketContextTick.create(
        source="calc_daemon",
        market_id="btc-updown-15m-1777636800",
        slug="btc-updown-15m-1777636800",
        title="Bitcoin Up or Down - May 1, 12:00PM",
        window_start_iso=start.isoformat(),
        window_end_iso=end.isoformat(),
        barrier=80000.25,
        barrier_source="polymarket",
        barrier_offset_s=0.2,
        clob_token_ids={"UP": "up-token", "DOWN": "down-token"},
        sequence=7,
    )

    restored = MarketContextTick.from_json(tick.to_json())

    assert restored.sequence == 7
    assert restored.window_start_unix == int(start.timestamp())
    assert restored.window_end_unix == int(end.timestamp())
    assert restored.barrier == 80000.25
    assert restored.barrier_confirmed is True
    assert restored.clob_token_ids["UP"] == "up-token"


def test_single_yaml_loads_market_context_and_leaderboard_ipc_sections(tmp_path: Path) -> None:
    cfg_path = tmp_path / "config.yaml"
    cfg_path.write_text(
        """
calc:
  inbox_targets: [results/calibration_inbox_grid]
  snapshot_path: results/terminal_data.shared.json
  market_scan_interval_seconds: 30
  ipc_backend: redis
  redis_url: redis://redis:6379/0
market_context:
  enabled: true
  latest_key: market:context:test
  channel: market.context.test
  stream: market.context.test
leaderboard_ipc:
  enabled: true
  latest_key: grid:leaderboard:test
  channel: grid.leaderboard.test
  stream: grid.leaderboard.test
""",
        encoding="utf-8",
    )

    calc = CalcConfig.from_json(cfg_path)
    leaderboard = LeaderboardIPCConfig.from_file(cfg_path, fallback_redis_url=calc.redis_url)

    assert calc.market_context_latest_key == "market:context:test"
    assert calc.market_context_channel == "market.context.test"
    assert leaderboard.enabled is True
    assert leaderboard.redis_url == "redis://redis:6379/0"
    assert leaderboard.latest_key == "grid:leaderboard:test"
    assert leaderboard.channel == "grid.leaderboard.test"
    assert LEADERBOARD_DEFAULT_LATEST_KEY == "grid:leaderboard:15m:latest"
    assert LEADERBOARD_DEFAULT_CHANNEL == "grid.leaderboard.15m"


def test_leaderboard_ipc_builds_dashboard_payload_rows(tmp_path: Path) -> None:
    path = tmp_path / "leaderboard.csv"
    path.write_text(
        "rank,instance_id,total_pnl,pnl_mean_to_sd,max_drawdown,max_drawdown_pct,"
        "wins,losses,win_rate,trades,alpha_up,alpha_down,floor_up,"
        "floor_down,tp_pct,sl_pct,liquidity_mode\n"
        "1,1250,12.50,1.2000,3.40,5.60,7,1,87.5,8,"
        "2.5,1.8,0.45,0.65,0.30,0.40,independent\n",
        encoding="utf-8",
    )

    rows = _rows_from_csv(path)

    assert rows == [
        {
            "rank": 1,
            "instance_id": 1250,
            "total_pnl": 12.5,
            "pnl_mean_to_sd": 1.2,
            "max_drawdown": 3.4,
            "max_drawdown_pct": 5.6,
            "wins": 7,
            "losses": 1,
            "win_rate": 87.5,
            "trades": 8,
            "params": {
                "alpha_up": 2.5,
                "alpha_down": 1.8,
                "floor_up": 0.45,
                "floor_down": 0.65,
                "tp_pct": 0.3,
                "sl_pct": 0.4,
            },
            "liquidity_mode": "independent",
            "experiment_id": "legacy",
            "as_of": None,
            "written_at": None,
        }
    ]
