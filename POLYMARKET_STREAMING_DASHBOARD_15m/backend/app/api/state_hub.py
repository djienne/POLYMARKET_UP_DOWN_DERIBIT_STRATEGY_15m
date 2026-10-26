"""Central registry of collector instances and derived snapshots.

Exposed to routes so both HTTP + WS can build the same payloads.
"""
from __future__ import annotations

import time
from typing import Optional

from ..collector.calibration_watcher import CalibrationWatcher
from ..collector.docker_log_tail import DockerLogTail
from ..collector.equity_reader import LiveEquityReader
from ..collector.event_loop_monitor import loop_lag_monitor
from ..collector.leaderboard_reader import LeaderboardReader
from ..collector.leaderboard_redis import build_default_redis_leaderboard_reader
from ..collector.liveness import current_liveness
from ..collector.market_context_reader import build_default_market_context_reader
from ..collector.orderbook_tail import OrderbookTail
from ..collector.polymarket_client import PolymarketClient
from ..collector.polymarket_status import PolymarketStatusWatcher
from ..collector.spot_reader import build_default_spot_reader
from ..collector.state_reader import (
    STARTING_CAPITAL,
    StateReader,
    _is_meaningful_live_close_values,
)
from ..collector.terminal_reader import TerminalReader
from ..collector.trades_tail import TradesTail
from ..config import settings
from ..derive.edge import compute_edge
from ..derive.equity import equity_curve, equity_timeseries
from ..derive.window import compute_window
from datetime import datetime, timezone

from ..models import (
    BootstrapPayload,
    ChartMarker,
    EdgeRatio,
    InstanceParams,
    InstanceStats,
    LivenessInfo,
    MarketInfo,
    PositionState,
    SharedConfig,
    TerminalSnapshot,
    TodaySummary,
    ViewSource,
)
from ..time_utils import iso_to_unix, paris_date_key, paris_day_start_utc


def _normalize_source(source: Optional[str] = None) -> ViewSource:
    return "live" if source == "live" else "dry_run"


def _model_series_for_chart(
    hub: "Hub",
    side: str,
    source: Optional[str] = None,
) -> list[dict]:
    if _normalize_source(source or hub.effective_mode) == "live":
        return hub.terminal.model_series(side)
    return hub.terminal.model_series(side) or hub.docker_log.model_series(side)


def _first_not_none(*values):
    for value in values:
        if value is not None:
            return value
    return None


def _parse_simple_yaml_scalar(raw: str):
    text = raw.strip()
    if not text:
        return None
    if " #" in text:
        text = text.split(" #", 1)[0].strip()
    if text in {"true", "True"}:
        return True
    if text in {"false", "False"}:
        return False
    if (text.startswith('"') and text.endswith('"')) or (
        text.startswith("'") and text.endswith("'")
    ):
        return text[1:-1]
    try:
        if any(ch in text for ch in (".", "e", "E")):
            return float(text)
        return int(text)
    except ValueError:
        return text


def _read_simple_yaml_section(path, section: str) -> dict:
    """Read scalar keys from a top-level YAML section without adding PyYAML."""
    out: dict = {}
    in_section = False
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        if not raw_line.strip() or raw_line.lstrip().startswith("#"):
            continue
        is_top_level = raw_line == raw_line.lstrip()
        stripped = raw_line.strip()
        if is_top_level:
            if stripped == f"{section}:":
                in_section = True
                continue
            if in_section:
                break
        if not in_section or not raw_line.startswith("  ") or raw_line.startswith("    "):
            continue
        line = raw_line.strip()
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        if value.strip() == "":
            continue
        out[key.strip()] = _parse_simple_yaml_scalar(value)
    return out


class Hub:
    def __init__(self) -> None:
        self.terminal = TerminalReader(settings.terminal_paths())
        self.state_dry = StateReader(
            path_fn=lambda: settings.state_snapshot_path(),
            run_source="dry_run",
        )
        self.state_live = StateReader(
            path_fn=lambda: settings.live_state_path(),
            run_source="live",
            single_trader_instance_id_fn=lambda: self.live_mirror_instance_id(),
        )
        self.state = self.state_live if settings.mode == "live" else self.state_dry
        self.equity_live = LiveEquityReader(settings.live_equity_path)
        self.trades_dry = TradesTail(
            path_fn=lambda: settings.resolved_results_dir / "trades.csv",
            run_source="dry_run",
        )
        # Secondary tail of the LIVE trader's CSV (15m_live_trades.csv).
        # When mode=dry_run, self.trades watches the grid's trades.csv —
        # without this second tail the live trader's closes (TP_FILLED /
        # LOSS_EXPIRY / STOP_LOSS) would be invisible in the trade feed.
        # Both readers publish to the same `trade.append` bus topic;
        # entry_registry dedupes ENTRY events between the live state_reader
        # and this CSV path using the opened_at timestamp.
        self.trades_live = TradesTail(
            path_fn=lambda: settings.live_trades_path(),
            run_source="live",
            single_trader_instance_id_fn=lambda: self.live_mirror_instance_id(),
        )
        self.trades = self.trades_live if settings.mode == "live" else self.trades_dry
        self.leaderboard = LeaderboardReader(path_fn=lambda: settings.leaderboard_path())
        self.redis_leaderboard = build_default_redis_leaderboard_reader(self.leaderboard)
        self.orderbook = OrderbookTail(path_fn=lambda: settings.orderbook_path())
        self.docker_log = DockerLogTail(
            container=settings.docker_container,
            slug_fn=lambda: self._current_slug(),
        )
        self.polymarket = PolymarketClient(slug_fn=lambda: self._current_slug())
        self.polymarket_status = PolymarketStatusWatcher(
            health_fn=lambda: self.polymarket.health,
        )
        self.spot = build_default_spot_reader()
        self.market_context = build_default_market_context_reader()
        self.calibration = CalibrationWatcher(
            log_paths=settings.trader_log_paths(),
            terminal_reader=self.terminal,
        )
        self._shared_cfg_cache: Optional[SharedConfig] = None
        self._shared_cfg_mtime: Optional[float] = None
        self._shared_cfg_path: Optional[str] = None
        self._shared_cfg_cache_by_path: dict[tuple[str, str], tuple[float, SharedConfig]] = {}
        self._auto_instance_id: Optional[int] = None
        self._auto_instance_ts: float = 0.0

    @staticmethod
    def canonical_instance_id(instance_id: Optional[int] = None) -> int:
        if instance_id is None:
            return int(settings.default_instance_id)
        return int(instance_id)

    @property
    def effective_mode(self) -> str:
        """Return the configured dashboard mode.

        Liveness is reported separately. Falling back from live to dry-run when
        a health probe is stale makes the dashboard show the grid leaderboard
        exactly when the operator needs to inspect live state.
        """
        return settings.mode

    _AUTO_SELECT_COOLDOWN_S = 86400  # re-evaluate best grid instance once per day

    @staticmethod
    def normalize_source(source: Optional[str] = None) -> ViewSource:
        if source is None:
            return _normalize_source(settings.mode)
        return _normalize_source(source)

    def state_for(self, source: Optional[str] = None) -> StateReader:
        return self.state_live if self.normalize_source(source) == "live" else self.state_dry

    def trades_for(self, source: Optional[str] = None) -> TradesTail:
        return self.trades_live if self.normalize_source(source) == "live" else self.trades_dry

    def trade_tail_statuses(self) -> list[dict]:
        seen: set[str] = set()
        statuses: list[dict] = []
        for tail in (self.trades_dry, self.trades_live):
            key = str(tail.path)
            if key in seen:
                continue
            seen.add(key)
            statuses.append(tail.status())
        return statuses

    def dashboard_health(self) -> dict:
        trade_statuses = self.trade_tail_statuses()
        trade_busy = any(bool(item.get("busy") or item.get("seed_requested")) for item in trade_statuses)
        last_poll_ms = max(
            (float(item["last_poll_ms"]) for item in trade_statuses if item.get("last_poll_ms") is not None),
            default=None,
        )
        last_seed_ms = max(
            (float(item["last_seed_ms"]) for item in trade_statuses if item.get("last_seed_ms") is not None),
            default=None,
        )
        largest_file = max(
            (int(item["file_size_bytes"]) for item in trade_statuses if item.get("file_size_bytes") is not None),
            default=0,
        )
        spot_health = self.spot.health()
        loop_health = loop_lag_monitor.snapshot()
        return {
            **spot_health,
            **loop_health,
            "trade_tail_busy": trade_busy,
            "trade_tail_last_poll_ms": last_poll_ms,
            "trade_tail_last_seed_ms": last_seed_ms,
            "trade_tail_file_size_mb": largest_file / (1024 * 1024),
            "trade_tails": trade_statuses,
        }

    def liveness_info(self) -> LivenessInfo:
        info = current_liveness()
        health = self.dashboard_health()
        for key, value in health.items():
            if hasattr(info, key):
                setattr(info, key, value)
        return info

    @staticmethod
    def _params_match(a: InstanceParams, b: InstanceParams) -> bool:
        return all(
            abs(float(getattr(a, field)) - float(getattr(b, field))) < 1e-9
            for field in (
                "alpha_up",
                "alpha_down",
                "floor_up",
                "floor_down",
                "tp_pct",
                "sl_pct",
            )
        )

    def live_params(self) -> Optional[InstanceParams]:
        cfg = self.shared_config("live")
        if (
            cfg.alpha_up is None
            or cfg.alpha_down is None
            or cfg.floor_up is None
            or cfg.floor_down is None
            or cfg.tp_pct is None
            or cfg.sl_pct is None
        ):
            return None
        return InstanceParams(
            alpha_up=float(cfg.alpha_up),
            alpha_down=float(cfg.alpha_down),
            floor_up=float(cfg.floor_up),
            floor_down=float(cfg.floor_down),
            tp_pct=float(cfg.tp_pct),
            sl_pct=float(cfg.sl_pct),
        )

    def live_mirror_instance_id(self) -> int:
        """Resolve the grid instance whose params match the configured live trader."""
        target = self.live_params()
        if target is None:
            return int(settings.default_instance_id)
        for row in self.leaderboard.rows:
            if self._params_match(row.params, target):
                return row.instance_id
        return int(settings.default_instance_id)

    def recommended_instance_id(self, source: Optional[str] = None) -> int:
        """Return the live/mirror instance for live, else leaderboard #1."""
        view_source = self.normalize_source(source)
        if view_source == "live":
            self._auto_instance_id = None
            self._auto_instance_ts = 0.0
            return self.live_mirror_instance_id()
        if settings.mode == "live":
            return self.live_mirror_instance_id()
        now = time.time()
        if (
            self._auto_instance_id is not None
            and now - self._auto_instance_ts < self._AUTO_SELECT_COOLDOWN_S
        ):
            return self._auto_instance_id
        if self.leaderboard.rows:
            self._auto_instance_id = self.leaderboard.rows[0].instance_id
            self._auto_instance_ts = now
            return self._auto_instance_id
        return int(settings.default_instance_id)

    def _current_slug(self, source: Optional[str] = None) -> Optional[str]:
        """Resolve the current 15-min market slug from the freshest available signal."""
        ctx = self.current_market_context()
        if ctx is not None and ctx.slug:
            return ctx.slug

        # Collectors own change detection and publication. Consumers must not
        # advance a reader's mtime and steal a transition from its event loop.
        state = self.state_for(source)

        # 1. Terminal JSON (rare but accurate)
        term = self.terminal.latest
        slug = term.market.slug if term and term.market else None

        # 2. Any instance with an open position is by definition trading the active market
        if not slug:
            for inst in (state.raw.get("instances") or {}).values():
                pos = inst.get("position")
                if pos and pos.get("market_id"):
                    slug = pos["market_id"]
                    break

        # 3. Fallback: current quarter-hour boundary.
        import re
        import time as _t
        now = int(_t.time())
        m = re.search(r"btc-updown-15m-(\d+)", slug or "")
        if not slug or not m or (int(m.group(1)) + 900) <= now:
            slug = f"btc-updown-15m-{(now // 900) * 900}"
        return slug

    def current_market_context(self):
        reader = getattr(self, "market_context", None)
        if reader is None:
            return None
        ctx = reader.current()
        if ctx is not None:
            return ctx
        return None

    @classmethod
    def _terminal_for_slug(
        cls,
        terminal: Optional[TerminalSnapshot],
        slug: Optional[str],
    ) -> TerminalSnapshot:
        out = terminal.model_copy(deep=True) if terminal is not None else TerminalSnapshot()
        if not slug:
            return out
        bounds = cls._window_bounds(slug)
        market = out.market.model_copy(deep=True) if out.market is not None else MarketInfo()
        market.slug = slug
        if bounds is not None:
            market.window_start_unix = bounds[0]
            market.window_end_unix = bounds[1]
        out.market = market
        return out

    def shared_config(self, source: Optional[str] = None) -> SharedConfig:
        import json
        view_source = self.normalize_source(source)
        section = "grid" if view_source == "dry_run" else "live"
        if settings.config_yaml_path().exists():
            path = settings.config_yaml_path()
        else:
            path = settings.grid_config_path() if view_source == "dry_run" else settings.live_config_path()
            section = ""
        path_str = str(path)
        cache_key = (path_str, section)
        try:
            mtime = path.stat().st_mtime
        except FileNotFoundError:
            return self._shared_cfg_cache or SharedConfig()
        cached = getattr(self, "_shared_cfg_cache_by_path", {}).get(cache_key)
        if cached is not None and cached[0] == mtime:
            return cached[1]
        try:
            if section:
                raw = _read_simple_yaml_section(path, section)
            else:
                raw = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return SharedConfig()
        cfg = SharedConfig(
            starting_capital=raw.get("starting_capital"),
            order_size_pct=raw.get("order_size_pct"),
            friction_pct=raw.get("friction_pct"),
            max_entry_price=raw.get("max_entry_price"),
            no_trade_first_s=raw.get("no_trade_first_seconds"),
            no_trade_last_s=raw.get("no_trade_last_seconds"),
            grace_period_s=raw.get("grace_period_seconds"),
            liquidity_mode=raw.get("liquidity_mode"),
            alpha_up=raw.get("edge_alpha_up"),
            alpha_down=raw.get("edge_alpha_down"),
            floor_up=raw.get("edge_floor_up"),
            floor_down=raw.get("edge_floor_down"),
            tp_pct=raw.get("tp_percentage"),
            sl_pct=raw.get("stop_loss_pct"),
        )
        self._shared_cfg_cache = cfg
        self._shared_cfg_mtime = mtime
        self._shared_cfg_path = f"{path_str}#{section}" if section else path_str
        if not hasattr(self, "_shared_cfg_cache_by_path"):
            self._shared_cfg_cache_by_path = {}
        self._shared_cfg_cache_by_path[cache_key] = (mtime, cfg)
        return cfg

    def starting_capital(self, source: Optional[str] = None) -> float:
        state = self.state_for(source)
        capital = state.raw.get("capital")
        if isinstance(capital, dict):
            live_starting = capital.get("starting")
            try:
                return float(live_starting)
            except (TypeError, ValueError):
                pass
        configured = self.shared_config(source).starting_capital
        if configured is None:
            return STARTING_CAPITAL
        try:
            return float(configured)
        except (TypeError, ValueError):
            return STARTING_CAPITAL

    @staticmethod
    def _apply_leaderboard_context(
        instance: Optional[InstanceStats],
        lb_row,
    ) -> Optional[InstanceStats]:
        if instance is None or lb_row is None:
            return instance
        instance.rank = lb_row.rank
        instance.params = lb_row.params
        instance.pnl_mean_to_sd = lb_row.pnl_mean_to_sd
        instance.max_drawdown = lb_row.max_drawdown
        instance.max_drawdown_pct = lb_row.max_drawdown_pct
        instance.wins = lb_row.wins
        instance.losses = lb_row.losses
        instance.win_rate = lb_row.win_rate
        instance.trades_count = lb_row.trades
        return instance

    def instance_snapshot(self, instance_id: int, source: Optional[str] = None):
        instance_id = self.canonical_instance_id(instance_id)
        view_source = self.normalize_source(source)
        state = self.state_for(view_source)
        trades_tail = self.trades_for(view_source)
        shared_cfg = self.shared_config(view_source)
        starting_capital = self.starting_capital(view_source)
        lb_row = self.leaderboard.row(instance_id) if view_source == "dry_run" else None
        instance = state.instance(instance_id, starting_capital=starting_capital)
        instance = self._apply_leaderboard_context(instance, lb_row)
        position = self._position_with_grace(
            state.position(instance_id),
            shared_cfg.grace_period_s,
        )
        pnls = state.trade_pnls(instance_id)
        equity = equity_curve(pnls, starting_capital=starting_capital)
        equity_series = (
            self.equity_live.series()
            if view_source == "live"
            else equity_timeseries(
                trades_tail.realized_history(instance_id),
                starting_capital=starting_capital,
                prefer_event_capital=False,
            )
        )
        today_summary = self._today_summary(
            instance_id,
            starting_capital,
            instance.capital if instance is not None else starting_capital,
            view_source,
        )
        return instance, position, equity, equity_series, today_summary

    def build_bootstrap(self, instance_id: int, source: Optional[str] = None) -> BootstrapPayload:
        instance_id = self.canonical_instance_id(instance_id)
        view_source = self.normalize_source(source)
        trades_tail = self.trades_for(view_source)
        # Use the collector snapshot; reads here used to suppress WS updates
        # for every other client by consuming their change detectors.
        slug = self._current_slug(view_source)
        shared_cfg = self.shared_config(view_source)

        effective_mode = view_source
        instance, position, equity, equity_series, today_summary = self.instance_snapshot(instance_id, view_source)
        lb_row = self.leaderboard.row(instance_id) if view_source == "dry_run" else None

        terminal = self._terminal_for_slug(self.terminal.latest, slug)
        # Polymarket prices: direct CLOB API (primary) ? orderbook CSV fallback for paper mode.
        price_source = self.polymarket.latest or self.orderbook.latest
        if price_source is not None:
            terminal.polymarket = price_source
        market_context = self.current_market_context()
        if market_context is not None:
            terminal.market.slug = market_context.slug or terminal.market.slug
            terminal.market.title = market_context.title or terminal.market.title
            terminal.market.window_start_unix = (
                market_context.window_start_unix
                if market_context.window_start_unix is not None
                else terminal.market.window_start_unix
            )
            terminal.market.window_end_unix = (
                market_context.window_end_unix
                if market_context.window_end_unix is not None
                else terminal.market.window_end_unix
            )
            terminal.market.barrier = (
                market_context.barrier
                if market_context.barrier is not None
                else terminal.market.barrier
            )
        spot = self.spot.latest
        if spot is not None and spot.price is not None:
            terminal.market.spot_price = spot.price
            terminal.market.spot_source = spot.source
            terminal.market.spot_source_label = spot.source_label
            terminal.market.spot_sequence = spot.sequence
            terminal.market.spot_published_at_ms = spot.published_at_ms
            terminal.market.spot_age_seconds = spot.age_seconds

        window = compute_window(
            slug,
            no_trade_first_s=(
                shared_cfg.no_trade_first_s
                if shared_cfg.no_trade_first_s is not None
                else 300.0
            ),
            no_trade_last_s=(
                shared_cfg.no_trade_last_s
                if shared_cfg.no_trade_last_s is not None
                else 180.0
            ),
        )

        trades = trades_tail.recent(instance_id, n=50)

        dashboard_health = self.dashboard_health()
        liveness = self.liveness_info()

        edge_up, edge_down = self._edges_from(terminal, lb_row, view_source)

        return BootstrapPayload(
            mode=effective_mode,
            instance=instance,
            position=position,
            terminal=terminal,
            spot=spot,
            market_context=market_context,
            window=window,
            trades=trades,
            equity=equity,
            leaderboard_top=self.leaderboard.top(15),
            liveness=liveness,
            calibration=self.calibration.status,
            edge_up=edge_up,
            edge_down=edge_down,
            shared_config=shared_cfg,
            today_summary=today_summary,
            series_up=self._scope_series(
                self.polymarket.series("UP") or self.orderbook.series("UP"), slug
            ),
            series_down=self._scope_series(
                self.polymarket.series("DOWN") or self.orderbook.series("DOWN"), slug
            ),
            series_up_bid=self._scope_series(
                self.polymarket.series("UP", "bid") or self.orderbook.series("UP", "bid"),
                slug,
            ),
            series_up_ask=self._scope_series(
                self.polymarket.series("UP", "ask") or self.orderbook.series("UP", "ask"),
                slug,
            ),
            series_down_bid=self._scope_series(
                self.polymarket.series("DOWN", "bid") or self.orderbook.series("DOWN", "bid"),
                slug,
            ),
            series_down_ask=self._scope_series(
                self.polymarket.series("DOWN", "ask") or self.orderbook.series("DOWN", "ask"),
                slug,
            ),
            model_up=self._scope_series(
                _model_series_for_chart(self, "UP", view_source), slug
            ),
            model_down=self._scope_series(
                _model_series_for_chart(self, "DOWN", view_source), slug
            ),
            markers=self._markers_for(instance_id, slug, position, view_source),
            window_start_iso=(
                market_context.window_start_iso
                if market_context is not None and market_context.window_start_iso
                else _window_iso(slug, 0)
            ),
            window_end_iso=(
                market_context.window_end_iso
                if market_context is not None and market_context.window_end_iso
                else _window_iso(slug, 900)
            ),
            equity_series=equity_series,
            polymarket_status=self.polymarket_status.status,
            dashboard_health=dashboard_health,
        )

    def _edges_from(
        self,
        terminal: TerminalSnapshot,
        lb_row,
        source: Optional[str] = None,
    ) -> tuple[Optional["EdgeRatio"], Optional["EdgeRatio"]]:
        if not terminal.probabilities:
            return None, None
        if lb_row is not None:
            params = lb_row.params
        else:
            try:
                cfg = self.shared_config(source)
            except TypeError:
                cfg = self.shared_config()
            if (
                cfg.alpha_up is None or
                cfg.alpha_down is None or
                cfg.floor_up is None or
                cfg.floor_down is None
            ):
                return None, None
            class _Params:
                alpha_up = cfg.alpha_up
                alpha_down = cfg.alpha_down
                floor_up = cfg.floor_up
                floor_down = cfg.floor_down
            params = _Params()
        model_up = _first_not_none(
            terminal.probabilities.avg_above,
            terminal.probabilities.mc_above,
            terminal.probabilities.ssvi_surface_above,
        )
        model_down = _first_not_none(
            terminal.probabilities.avg_below,
            terminal.probabilities.mc_below,
            terminal.probabilities.ssvi_surface_below,
        )
        market_up = terminal.polymarket.prob_up if terminal.polymarket else None
        market_down = terminal.polymarket.prob_down if terminal.polymarket else None
        return (
            compute_edge("UP", model_up, market_up, params.alpha_up, params.floor_up),
            compute_edge("DOWN", model_down, market_down, params.alpha_down, params.floor_down),
        )

    def current_edges(
        self,
        instance_id: int,
        source: Optional[str] = None,
    ) -> tuple[Optional["EdgeRatio"], Optional["EdgeRatio"]]:
        """Compute live edges from current terminal/polymarket state for the given instance."""
        instance_id = self.canonical_instance_id(instance_id)
        view_source = self.normalize_source(source)
        terminal = self.terminal.latest or TerminalSnapshot()
        price_source = self.polymarket.latest or self.orderbook.latest
        if price_source is not None:
            terminal.polymarket = price_source
        lb_row = self.leaderboard.row(instance_id) if view_source == "dry_run" else None
        return self._edges_from(terminal, lb_row, view_source)

    @staticmethod
    def _grace_remaining(
        last_exit_at: Optional[str],
        grace_period_s: Optional[float],
        now_unix: Optional[float] = None,
    ) -> Optional[float]:
        if last_exit_at is None or grace_period_s is None or grace_period_s <= 0:
            return None
        exited_at = iso_to_unix(last_exit_at)
        if exited_at is None:
            return None
        if now_unix is None:
            now_unix = time.time()
        remaining = grace_period_s - (now_unix - exited_at)
        return max(0.0, remaining)

    @classmethod
    def _position_with_grace(
        cls,
        position: PositionState,
        grace_period_s: Optional[float],
    ) -> PositionState:
        if position.open is not None:
            position.grace_remaining_s = None
            return position
        position.grace_remaining_s = cls._grace_remaining(
            position.last_exit_at,
            grace_period_s,
        )
        return position

    @staticmethod
    def _today_pnl_pct(
        summary: TodaySummary,
        history,
        starting_capital: float,
        current_capital: float,
        day_key: str,
        source: Optional[str] = None,
    ) -> Optional[float]:
        if summary.closed <= 0:
            return None
        day_start = paris_day_start_utc(day_key)
        base_capital: Optional[float] = None
        if day_start is not None:
            running_capital = starting_capital
            for ev in reversed(history):
                ts = iso_to_unix(getattr(ev, "timestamp", None))
                if ts is None or ts >= day_start.timestamp():
                    continue
                if _normalize_source(source or settings.mode) == "live":
                    capital = getattr(ev, "capital", None)
                    if capital is not None:
                        try:
                            base_capital = float(capital)
                        except (TypeError, ValueError):
                            base_capital = None
                        break
                pnl = getattr(ev, "pnl", None)
                if pnl is not None:
                    try:
                        running_capital += float(pnl)
                    except (TypeError, ValueError):
                        pass
            if _normalize_source(source or settings.mode) != "live":
                base_capital = running_capital
        if base_capital is None:
            fallback = current_capital - summary.pnl
            base_capital = fallback if fallback > 0 else starting_capital
        if base_capital <= 0:
            return None
        return (summary.pnl / base_capital) * 100.0

    def _live_today_summary(
        self,
        day_key: str,
        starting_capital: float,
        current_capital: float,
    ) -> TodaySummary:
        summary = TodaySummary()
        day_start = paris_day_start_utc(day_key)
        # State retains full precision; the trade CSV rounds each P&L to cents.
        for row in self.state_live.raw.get("closed_positions") or []:
            if not isinstance(row, dict) or not _is_meaningful_live_close_values(
                row.get("pnl"), row.get("cost_basis"), row.get("proceeds")
            ):
                continue
            if paris_date_key(row.get("opened_at")) == day_key:
                summary.entries += 1
            if paris_date_key(row.get("closed_at")) != day_key:
                continue
            summary.closed += 1
            pnl = float(row.get("pnl") or 0.0)
            summary.pnl += pnl
            summary.wins += int(pnl > 1e-6)
            summary.losses += int(pnl < -1e-6)
        for pos in self.state_live.raw.get("open_positions") or []:
            if not isinstance(pos, dict):
                continue
            if paris_date_key(pos.get("opened_at")) == day_key:
                summary.entries += 1
        base_capital = self.equity_live.before(day_start.timestamp()) if day_start else None
        summary.pnl_pct = (
            (summary.pnl / base_capital) * 100.0
            if base_capital is not None and base_capital > 0 and summary.closed > 0
            else None
        )
        return summary

    def _today_summary(
        self,
        instance_id: int,
        starting_capital: float,
        current_capital: float,
        source: Optional[str] = None,
    ) -> TodaySummary:
        day_key = paris_date_key(dt=datetime.now(timezone.utc))
        view_source = self.normalize_source(source)
        trades_tail = self.trades_for(view_source)
        if view_source == "live":
            return self._live_today_summary(day_key, starting_capital, current_capital)
        summary = trades_tail.today_summary(instance_id, day_key)
        summary.pnl_pct = self._today_pnl_pct(
            summary,
            trades_tail.realized_history(instance_id),
            starting_capital,
            current_capital,
            day_key,
            view_source,
        )
        return summary

    @staticmethod
    def _window_bounds(slug: Optional[str]) -> Optional[tuple[int, int]]:
        if not slug:
            return None
        import re
        m = re.search(r"btc-updown-15m-(\d+)", slug)
        if not m:
            return None
        start = int(m.group(1))
        return start, start + 900

    @classmethod
    def _scope_series(cls, rows: list, slug: Optional[str]) -> list:
        bounds = cls._window_bounds(slug)
        if not bounds:
            return rows
        start, end = bounds
        out = []
        for p in rows:
            t = p["t"] if isinstance(p, dict) else p.t
            ts = iso_to_unix(t)
            if ts is None:
                continue
            if start <= ts <= end:
                out.append(p)
        return out

    def _markers_for(
        self,
        instance_id: int,
        slug: Optional[str],
        position: Optional[PositionState] = None,
        source: Optional[str] = None,
    ) -> list[ChartMarker]:
        bounds = self._window_bounds(slug)
        if not bounds:
            return []
        view_source = self.normalize_source(source)
        try:
            trades_tail = self.trades_for(view_source)
        except AttributeError:
            trades_tail = self.trades
        try:
            state = self.state_for(view_source)
        except AttributeError:
            state = getattr(self, "state", None)
        start, end = bounds
        out: list[ChartMarker] = []
        for ev in trades_tail.recent(instance_id, n=200):
            if ev.market_id and ev.market_id != slug:
                continue
            ts = iso_to_unix(ev.timestamp)
            if ts is None:
                continue
            if not (start <= ts <= end):
                continue
            kind = None
            if ev.event == "ENTRY":
                kind = "ENTRY"
            elif ev.event in ("TP_FILLED", "WIN_EXPIRY"):
                kind = "WIN"
            elif ev.event in ("STOP_LOSS", "LOSS_EXPIRY"):
                kind = "LOSS"
            if kind is None:
                continue
            price = ev.entry_price if kind == "ENTRY" else ev.exit_price
            out.append(
                ChartMarker(
                    t=ev.timestamp,
                    kind=kind,  # type: ignore[arg-type]
                    side=(ev.direction if ev.direction in ("UP", "DOWN") else None),
                    price=price,
                    pnl=ev.pnl,
                )
            )
        fallback_position = state.position(instance_id) if state is not None else PositionState()
        open_pos = (position or fallback_position).open
        if open_pos is not None and open_pos.entered_at:
            if open_pos.market_id is None or open_pos.market_id == slug:
                ts = iso_to_unix(open_pos.entered_at)
                duplicate = any(
                    marker.kind == "ENTRY"
                    and marker.t == open_pos.entered_at
                    and marker.side == open_pos.direction
                    for marker in out
                )
                if ts is not None and start <= ts <= end and not duplicate:
                    out.append(
                        ChartMarker(
                            t=open_pos.entered_at,
                            kind="ENTRY",
                            side=open_pos.direction,
                            price=open_pos.entry_price,
                            pnl=None,
                        )
                    )
        return out


def _window_iso(slug: Optional[str], offset: int) -> Optional[str]:
    if not slug:
        return None
    import re
    m = re.search(r"btc-updown-15m-(\d+)", slug)
    if not m:
        return None
    ts = int(m.group(1)) + offset
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


hub: Optional[Hub] = None


def get_hub() -> Hub:
    global hub
    if hub is None:
        hub = Hub()
    return hub
