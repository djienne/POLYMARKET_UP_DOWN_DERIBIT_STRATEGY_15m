from datetime import datetime, timezone
import json

from app.api.state_hub import Hub
from app.config import settings
from app.models import (
    InstanceParams,
    LeaderboardRow,
    MarketContextInfo,
    MarketInfo,
    OpenPosition,
    PolymarketPrices,
    PositionState,
    ProbabilityBundle,
    SharedConfig,
    TerminalSnapshot,
    TradeEvent,
)


def test_grace_remaining_counts_down():
    exited = datetime(2026, 4, 22, 10, 0, 0, tzinfo=timezone.utc).timestamp()
    rem = Hub._grace_remaining(
        "2026-04-22T10:00:00+00:00",
        grace_period_s=60,
        now_unix=exited + 30,
    )
    assert rem == 30


def test_grace_remaining_clamps_to_zero():
    exited = datetime(2026, 4, 22, 10, 0, 0, tzinfo=timezone.utc).timestamp()
    rem = Hub._grace_remaining(
        "2026-04-22T10:00:00+00:00",
        grace_period_s=60,
        now_unix=exited + 100,
    )
    assert rem == 0


def test_edges_keep_zero_probability_values():
    hub = object.__new__(Hub)
    hub.shared_config = lambda: SharedConfig(
        alpha_up=2.5,
        alpha_down=2.5,
        floor_up=0.45,
        floor_down=0.55,
    )
    terminal = TerminalSnapshot(
        probabilities=ProbabilityBundle(
            avg_above=0.0,
            avg_below=1.0,
            mc_above=0.42,
            mc_below=0.58,
        ),
        polymarket=PolymarketPrices(prob_up=0.2, prob_down=0.8),
    )

    edge_up, edge_down = hub._edges_from(terminal, None)

    assert edge_up.model_prob == 0.0
    assert edge_down.model_prob == 1.0


def test_live_mode_does_not_fall_back_to_dry_run(monkeypatch):
    monkeypatch.setattr(settings, "mode", "live")
    hub = object.__new__(Hub)
    hub.live_mirror_instance_id = lambda: settings.default_instance_id

    assert hub.effective_mode == "live"
    assert hub.recommended_instance_id() == settings.default_instance_id


class _FakeLeaderboard:
    def __init__(self, rows):
        self.rows = rows

    def read_if_changed(self):
        return False


class _FakeMarketContext:
    def __init__(self, ctx):
        self._ctx = ctx

    def current(self):
        return self._ctx


def _shared_config_hub() -> Hub:
    hub = object.__new__(Hub)
    hub._shared_cfg_cache = None
    hub._shared_cfg_mtime = None
    hub._shared_cfg_path = None
    hub._shared_cfg_cache_by_path = {}
    return hub


def _write_canonical_config(tmp_path) -> None:
    (tmp_path / "config.yaml").write_text(
        """
grid:
  starting_capital: 1000.0
  order_size_pct: 0.05
  friction_pct: 0.01
  max_entry_price: 0.95
  grid:
    tp_percentage: [0.08, 0.15, 0.30]
    stop_loss_pct: [0.0, 0.20]
live:
  starting_capital: 1000.0
  order_size_pct: 0.05
  friction_pct: 0.0
  max_entry_price: 0.95
""".lstrip(),
        encoding="utf-8",
    )
    (tmp_path / "config_grid.json").write_text(
        json.dumps({"friction_pct": 0.015, "starting_capital": 500.0}),
        encoding="utf-8",
    )
    (tmp_path / "config_trader_live.json").write_text(
        json.dumps({"friction_pct": 0.02, "starting_capital": 500.0}),
        encoding="utf-8",
    )


def test_shared_config_uses_yaml_grid_section_over_stale_legacy_json(monkeypatch, tmp_path):
    monkeypatch.setattr(settings, "config_dir", tmp_path)
    _write_canonical_config(tmp_path)

    cfg = _shared_config_hub().shared_config("dry_run")

    assert cfg.friction_pct == 0.01
    assert cfg.starting_capital == 1000.0


def test_shared_config_yaml_cache_keeps_dry_run_and_live_separate(monkeypatch, tmp_path):
    monkeypatch.setattr(settings, "config_dir", tmp_path)
    _write_canonical_config(tmp_path)

    hub = _shared_config_hub()
    dry_first = hub.shared_config("dry_run")
    live_second = hub.shared_config("live")
    assert dry_first.friction_pct == 0.01
    assert live_second.friction_pct == 0.0

    hub = _shared_config_hub()
    live_first = hub.shared_config("live")
    dry_second = hub.shared_config("dry_run")
    assert live_first.friction_pct == 0.0
    assert dry_second.friction_pct == 0.01


def test_shared_config_falls_back_to_legacy_json_without_config_yaml(monkeypatch, tmp_path):
    monkeypatch.setattr(settings, "config_dir", tmp_path)
    (tmp_path / "config_grid.json").write_text(
        json.dumps({"friction_pct": 0.015}),
        encoding="utf-8",
    )
    (tmp_path / "config_trader_live.json").write_text(
        json.dumps({"friction_pct": 0.0}),
        encoding="utf-8",
    )

    hub = _shared_config_hub()

    assert hub.shared_config("dry_run").friction_pct == 0.015
    assert hub.shared_config("live").friction_pct == 0.0


def _lb_row(rank: int, instance_id: int, params: InstanceParams) -> LeaderboardRow:
    return LeaderboardRow(
        rank=rank,
        instance_id=instance_id,
        total_pnl=0.0,
        pnl_mean_to_sd=0.0,
        max_drawdown=0.0,
        max_drawdown_pct=0.0,
        wins=0,
        losses=0,
        win_rate=0.0,
        trades=0,
        params=params,
        liquidity_mode="independent",
    )


def test_live_mode_dry_run_recommendation_uses_live_param_mirror(monkeypatch):
    monkeypatch.setattr(settings, "mode", "live")
    live_params = InstanceParams(
        alpha_up=2.5,
        alpha_down=2.5,
        floor_up=0.45,
        floor_down=0.45,
        tp_pct=0.30,
        sl_pct=0.40,
    )
    better_rank_params = InstanceParams(
        alpha_up=2.5,
        alpha_down=1.8,
        floor_up=0.45,
        floor_down=0.65,
        tp_pct=0.30,
        sl_pct=0.40,
    )
    hub = object.__new__(Hub)
    hub.shared_config = lambda source=None: SharedConfig(
        alpha_up=live_params.alpha_up,
        alpha_down=live_params.alpha_down,
        floor_up=live_params.floor_up,
        floor_down=live_params.floor_down,
        tp_pct=live_params.tp_pct,
        sl_pct=live_params.sl_pct,
    )
    hub.leaderboard = _FakeLeaderboard(
        [
            _lb_row(1, 815, better_rank_params),
            _lb_row(4, 1271, live_params),
        ]
    )

    assert hub.recommended_instance_id("dry_run") == 1271
    assert hub.recommended_instance_id("live") == 1271


def test_source_helpers_keep_dry_and_live_readers_separate(monkeypatch):
    monkeypatch.setattr(settings, "mode", "live")
    hub = object.__new__(Hub)
    hub.state_dry = object()
    hub.state_live = object()
    hub.trades_dry = object()
    hub.trades_live = object()

    assert hub.normalize_source(None) == "live"
    assert hub.normalize_source("dry_run") == "dry_run"
    assert hub.state_for("dry_run") is hub.state_dry
    assert hub.state_for("live") is hub.state_live
    assert hub.trades_for("dry_run") is hub.trades_dry
    assert hub.trades_for("live") is hub.trades_live


def test_current_slug_prefers_redis_market_context():
    hub = object.__new__(Hub)
    hub.market_context = _FakeMarketContext(
        MarketContextInfo(
            slug="btc-updown-15m-1778780700",
            title="Bitcoin Up or Down - May 20, 12:45PM-1:00PM ET",
            window_start_unix=1778780700,
            window_end_unix=1778781600,
            barrier=81468.65,
        )
    )

    assert hub._current_slug() == "btc-updown-15m-1778780700"


def test_terminal_for_slug_keeps_bootstrap_market_fields_consistent():
    terminal = TerminalSnapshot(
        market=MarketInfo(
            slug="btc-updown-15m-1778779800",
            window_start_unix=1778779800,
            window_end_unix=1778780700,
            barrier=81468.65,
        )
    )

    normalized = Hub._terminal_for_slug(terminal, "btc-updown-15m-1778780700")

    assert normalized.market.slug == "btc-updown-15m-1778780700"
    assert normalized.market.window_start_unix == 1778780700
    assert normalized.market.window_end_unix == 1778781600
    assert normalized.market.barrier == 81468.65
    assert terminal.market.slug == "btc-updown-15m-1778779800"


class _FakeTrades:
    def __init__(self, events=None):
        self.events = events or []

    def recent(self, instance_id: int, n: int = 200):
        return self.events[:n]


def test_markers_include_current_open_position_without_csv_entry():
    hub = object.__new__(Hub)
    hub.trades = _FakeTrades()
    position = PositionState(
        open=OpenPosition(
            direction="UP",
            entry_price=0.42,
            shares=10.0,
            entered_at="2026-05-02T20:00:10+00:00",
            market_id="btc-updown-15m-1777752000",
        )
    )

    markers = hub._markers_for(773, "btc-updown-15m-1777752000", position)

    assert len(markers) == 1
    assert markers[0].kind == "ENTRY"
    assert markers[0].side == "UP"
    assert markers[0].price == 0.42


def test_markers_do_not_duplicate_open_position_entry_from_trades():
    opened_at = "2026-05-02T20:00:10+00:00"
    hub = object.__new__(Hub)
    hub.trades = _FakeTrades(
        [
            TradeEvent(
                instance_id=773,
                timestamp=opened_at,
                event="ENTRY",
                direction="DOWN",
                market_id="btc-updown-15m-1777752000",
                entry_price=0.58,
            )
        ]
    )
    position = PositionState(
        open=OpenPosition(
            direction="DOWN",
            entry_price=0.58,
            shares=10.0,
            entered_at=opened_at,
            market_id="btc-updown-15m-1777752000",
        )
    )

    markers = hub._markers_for(773, "btc-updown-15m-1777752000", position)

    assert len(markers) == 1
    assert markers[0].kind == "ENTRY"
