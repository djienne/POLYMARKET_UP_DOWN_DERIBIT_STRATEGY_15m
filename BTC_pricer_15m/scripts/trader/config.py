"""Configuration for the 15-minute grid search and single trader."""

import json
import itertools
import warnings
from dataclasses import dataclass, field, fields
from pathlib import Path
from typing import Any, Optional

import yaml


DEFAULT_POLYGON_RPC_URLS: tuple[str, ...] = (
    "https://polygon-bor-rpc.publicnode.com",
    "https://polygon-rpc.com",
)

DEFAULT_DRY_RUN_FRICTION_PCT = 0.01
GRID_PARAM_KEYS: tuple[str, ...] = (
    "alpha_up",
    "alpha_down",
    "floor_up",
    "floor_down",
    "tp_percentage",
    "stop_loss_pct",
)
GRID_NON_STOP_LOSS_KEYS: tuple[str, ...] = GRID_PARAM_KEYS[:-1]
GRID_PARAM_DEFAULTS: dict[str, list[float]] = {
    "alpha_up": [1.5],
    "alpha_down": [1.5],
    "floor_up": [0.55],
    "floor_down": [0.55],
    "tp_percentage": [0.15],
    "stop_loss_pct": [0.0],
}


def _load_config_mapping(path: Path, *, section: str | None = None) -> dict:
    """Load JSON or YAML and optionally select a top-level section.

    The migration target is one canonical YAML file, but the current runtime
    still has JSON files mounted into containers.  This helper lets both formats
    coexist while callers move over one at a time.
    """
    with open(path, "r", encoding="utf-8") as f:
        if path.suffix.lower() in {".yaml", ".yml"}:
            raw = yaml.safe_load(f) or {}
        else:
            raw = json.load(f)
    if (
        section
        and path.suffix.lower() in {".yaml", ".yml"}
        and isinstance(raw, dict)
        and section in raw
    ):
        section_data = raw.get(section) or {}
        if not isinstance(section_data, dict):
            raise ValueError(f"{path}: section {section!r} must be a mapping")
        return dict(section_data)
    if not isinstance(raw, dict):
        raise ValueError(f"{path}: config root must be a mapping")
    return dict(raw)


def _validate_number_range(
    name: str,
    value: float | int,
    *,
    min_value: float | None = None,
    max_value: float | None = None,
    min_inclusive: bool = True,
    max_inclusive: bool = True,
) -> None:
    if min_value is not None:
        if (min_inclusive and value < min_value) or (not min_inclusive and value <= min_value):
            op = ">=" if min_inclusive else ">"
            raise ValueError(f"{name} must be {op} {min_value}, got {value!r}")
    if max_value is not None:
        if (max_inclusive and value > max_value) or (not max_inclusive and value >= max_value):
            op = "<=" if max_inclusive else "<"
            raise ValueError(f"{name} must be {op} {max_value}, got {value!r}")


@dataclass(frozen=True)
class InstanceParams:
    """Frozen parameter set for one paper trading instance."""
    alpha_up: float
    alpha_down: float
    floor_up: float
    floor_down: float
    tp_percentage: float
    stop_loss_pct: float = 0.0

    def label(self) -> str:
        parts = [
            f"aU={self.alpha_up}",
            f"aD={self.alpha_down}",
            f"fU={self.floor_up}",
            f"fD={self.floor_down}",
            f"tp={self.tp_percentage}",
        ]
        if self.stop_loss_pct > 0:
            parts.append(f"sl={self.stop_loss_pct}")
        return " ".join(parts)


@dataclass
class GridConfig:
    """Shared configuration loaded from config_grid.json."""

    experiment_id: str = "legacy"

    # Capital
    starting_capital: float = 1000.0
    order_size_pct: float = 0.10
    friction_pct: float = DEFAULT_DRY_RUN_FRICTION_PCT
    max_entry_price: float = 0.95

    # Timing guards (seconds)
    no_trade_first_seconds: int = 300   # First 5 min blocked
    no_trade_last_seconds: int = 180    # Last 3 min blocked

    # Timing
    tick_interval_seconds: int = 0      # 0 = as fast as possible (sequential)
    market_scan_interval_seconds: int = 30  # How often to scan for new markets
    grace_period_seconds: int = 60      # Cooldown after TP/SL before re-entry
    paper_recheck_delay_seconds: float = 1.0
    paper_delayed_book_slippage: float = 0.02
    paper_max_execution_entry_price: Optional[float] = None
    settlement_wait_seconds: int = 20

    # Wall-clock hold between a grid instance deciding to enter and committing
    # the paper fill. The live trader needs ~2-3s from the mirror signal to its
    # real POST /order; only that POST reveals a 425 / post-only taker outage.
    # Holding the paper fill this long lets the live trader's reactive CLOB-health
    # flag land first, so the grid drops entries live could not place (dry-run /
    # live parity). 0 disables (legacy instant-commit behaviour). The dataclass
    # default is 0 to preserve direct-construction callers/tests; production opts
    # in via config.yaml (grid.grid_entry_confirm_seconds: 4.0).
    grid_entry_confirm_seconds: float = 0.0

    # Execution model
    liquidity_mode: str = "independent"
    capacity_seed: int = 17
    max_trade_pnls: int = 1000
    max_settled_market_ids: int = 1000

    # Hybrid calibration knobs (mirror TraderConfig). Default OFF for the grid
    # to preserve historical behaviour; set hybrid_calibration_enabled=true in
    # config_grid.json to delegate calibration to the shared calc daemon.
    hybrid_calibration_enabled: bool = False
    hybrid_remote_inbox_enabled: bool = True
    hybrid_local_worker_enabled: bool = False
    hybrid_local_worker_delay_min_seconds: float = 0.0
    hybrid_local_worker_delay_max_seconds: float = 0.0
    hybrid_inbox_dir: str = ""
    ipc_backend: str = "file"
    redis_url: str = "redis://redis:6379/0"
    redis_stream: str = "market.calibration"
    redis_consumer_group: str = "grid"
    redis_consumer_name: str = "grid"
    ipc_file_fallback_enabled: bool = True
    # Blocking XREADGROUP listener thread applying calibration events the
    # moment they land (vs the once-per-tick poll). Fail-open to polling.
    hybrid_push_consumer_enabled: bool = True
    # Legacy terminal-price re-evaluation. Canonical YAML enables this flag,
    # but current TWAP contracts always use producer-owned v5 scalars with
    # two-second pricing/source-age entry guards, never terminal quantiles.
    local_prob_eval_enabled: bool = False
    local_eval_event_max_age_s: float = 30.0
    local_eval_spot_max_age_s: float = 2.0
    # With local eval on: the staged-entry finalize re-check uses the
    # finalize tick's then-current probability (what live's real order
    # executes against) instead of the stage-time snapshot. Inert while
    # local_prob_eval is off.
    staged_finalize_fresh_prob: bool = True
    polygon_rpc_urls: tuple[str, ...] = DEFAULT_POLYGON_RPC_URLS

    # Mirror signal stream. When enabled, the grid publishes trade lifecycle
    # events for the live-parameter mirror instance only; live consumes these
    # as timing authority for entry/SL while keeping its own real TP order.
    mirror_signal_publish_enabled: bool = False
    mirror_signal_instance_id: str = "auto_from_live"
    mirror_signal_stream: str = "trade.mirror.signals"
    mirror_signal_max_age_seconds: float = 30.0
    mirror_heartbeat_key: str = "trade.mirror.heartbeat"
    mirror_heartbeat_ttl_seconds: float = 120.0

    # Near-real-time auto-amend. When enabled, the grid reads
    # the live trader's mirror rejection acks and voids the mirror instance's
    # just-committed paper entry (append-only ENTRY_VOID, refunds capital, clears
    # the position) whenever live could not place the order. Default off — the
    # offline cleaner (scripts.trader.cli.clean_unavailable_dry_trades) is the
    # robust fallback and recovery replay understands ENTRY_VOID either way.
    grid_auto_amend_enabled: bool = False
    mirror_ack_stream: str = "trade.mirror.acks"

    # CLOB taker-health gate. When enabled, every grid instance skips an entry
    # while the shared flag file (written by the live trader when the CLOB
    # rejects a real taker order) marks a taker outage. Keeps the dry-run grid
    # from "winning" trades the live trader could never actually place. The grid
    # only *reads* this file — no network, no orders.
    clob_health_gate_enabled: bool = True
    clob_health_shared_path: str = "results/15m_clob_health.json"

    # Canonical availability ledger (historical interval truth of when live
    # trading was impossible — see availability.py). The grid
    # only READS the normalized interval ledger for an OPTIONAL second forward
    # gate (the live trader is the writer). Default off so behaviour is unchanged
    # until the ledger is reviewed; fail-open on a missing/corrupt ledger.
    availability_gate_enabled: bool = False
    availability_gate_min_confidence: str = "confirmed"
    availability_ledger_path: str = "results/availability_ledger.csv"

    # Independent Binance-only sanity check on calc's published barrier. When
    # calc says barrier_source="chainlink", live/grid pull the Binance kline
    # at window-start and reject the barrier if divergence > tolerance.
    # Mirrors calc's own >1% Chainlink/Binance cross-validation, but on the
    # consumer side with no Chainlink dependency. Default-on; turn off only
    # for tests that don't have Binance kline access.
    binance_sanity_check_enabled: bool = True
    binance_sanity_tolerance_pct: float = 1.0

    # Grid definition (raw lists from JSON)
    grid: dict = field(default_factory=dict)
    append_stop_loss_pct: list[float] = field(default_factory=list)

    def __post_init__(self):
        if self.liquidity_mode not in {"independent", "capacity_aware"}:
            raise ValueError(
                f"liquidity_mode must be 'independent' or 'capacity_aware', got {self.liquidity_mode!r}"
            )
        if self.ipc_backend not in {"file", "redis"}:
            raise ValueError(f"ipc_backend must be 'file' or 'redis', got {self.ipc_backend!r}")
        _validate_number_range("starting_capital", self.starting_capital, min_value=0, min_inclusive=False)
        _validate_number_range("order_size_pct", self.order_size_pct, min_value=0, max_value=1, min_inclusive=False)
        _validate_number_range("friction_pct", self.friction_pct, min_value=0, max_value=1)
        _validate_number_range("max_entry_price", self.max_entry_price, min_value=0, max_value=1, min_inclusive=False)
        for name in (
            "no_trade_first_seconds",
            "no_trade_last_seconds",
            "tick_interval_seconds",
            "market_scan_interval_seconds",
            "grace_period_seconds",
            "settlement_wait_seconds",
            "capacity_seed",
            "max_trade_pnls",
            "max_settled_market_ids",
        ):
            min_value = 1 if name in {"market_scan_interval_seconds", "max_trade_pnls", "max_settled_market_ids"} else 0
            _validate_number_range(name, getattr(self, name), min_value=min_value)
        _validate_number_range("paper_recheck_delay_seconds", self.paper_recheck_delay_seconds, min_value=0)
        _validate_number_range("grid_entry_confirm_seconds", self.grid_entry_confirm_seconds, min_value=0)
        _validate_number_range("paper_delayed_book_slippage", self.paper_delayed_book_slippage, min_value=0)
        if self.paper_max_execution_entry_price is not None:
            _validate_number_range(
                "paper_max_execution_entry_price",
                self.paper_max_execution_entry_price,
                min_value=0,
                max_value=1,
                min_inclusive=False,
            )
            if self.paper_max_execution_entry_price < self.max_entry_price:
                raise ValueError(
                    "paper_max_execution_entry_price must be >= max_entry_price"
                )
        _validate_number_range("hybrid_local_worker_delay_min_seconds", self.hybrid_local_worker_delay_min_seconds, min_value=0)
        _validate_number_range("hybrid_local_worker_delay_max_seconds", self.hybrid_local_worker_delay_max_seconds, min_value=0)
        if self.hybrid_local_worker_delay_max_seconds < self.hybrid_local_worker_delay_min_seconds:
            raise ValueError("hybrid_local_worker_delay_max_seconds must be >= hybrid_local_worker_delay_min_seconds")
        _validate_number_range("binance_sanity_tolerance_pct", self.binance_sanity_tolerance_pct, min_value=0)
        _validate_number_range("mirror_signal_max_age_seconds", self.mirror_signal_max_age_seconds, min_value=0)
        _validate_number_range("mirror_heartbeat_ttl_seconds", self.mirror_heartbeat_ttl_seconds, min_value=1)
        self._validate_append_stop_loss_pct()
        self._validate_grid_values()

    def _validate_stop_loss_value(self, name: str, value: float | int) -> None:
        _validate_number_range(name, value, min_value=0, max_value=1, max_inclusive=False)

    def _validate_append_stop_loss_pct(self) -> None:
        if self.append_stop_loss_pct is None:
            self.append_stop_loss_pct = []
        if not isinstance(self.append_stop_loss_pct, list):
            raise ValueError("append_stop_loss_pct must be a list")
        base_values = list(self.grid.get("stop_loss_pct") or GRID_PARAM_DEFAULTS["stop_loss_pct"])
        seen: list[float] = []
        for idx, value in enumerate(self.append_stop_loss_pct):
            self._validate_stop_loss_value(f"append_stop_loss_pct[{idx}]", value)
            f_value = float(value)
            if any(abs(f_value - float(existing)) < 1e-9 for existing in base_values):
                raise ValueError(
                    "append_stop_loss_pct must not duplicate grid.stop_loss_pct "
                    f"value {value!r}"
                )
            if any(abs(f_value - existing) < 1e-9 for existing in seen):
                raise ValueError(
                    f"append_stop_loss_pct contains duplicate value {value!r}"
                )
            seen.append(f_value)

    def _validate_grid_values(self) -> None:
        allowed = set(GRID_PARAM_KEYS)
        unknown = sorted(k for k in self.grid if not str(k).startswith("_") and k not in allowed)
        if unknown:
            raise ValueError(f"grid contains unknown key(s): {', '.join(unknown)}")
        for key, values in self.grid.items():
            if str(key).startswith("_") or values in (None, []):
                continue
            if not isinstance(values, list):
                raise ValueError(f"grid.{key} must be a list")
            for value in values:
                if key in {"alpha_up", "alpha_down"}:
                    _validate_number_range(f"grid.{key}", value, min_value=0, min_inclusive=False)
                elif key in {"floor_up", "floor_down", "tp_percentage"}:
                    _validate_number_range(f"grid.{key}", value, min_value=0, max_value=1, min_inclusive=False)
                elif key == "stop_loss_pct":
                    self._validate_stop_loss_value(f"grid.{key}", value)

    @classmethod
    def from_json(cls, path: Path) -> "GridConfig":
        data = _load_config_mapping(path, section="grid")

        grid = data.pop("grid", {})
        # Strip underscore-prefixed keys (JSON/YAML comments), but reject
        # real unknown keys so config typos do not silently change behavior.
        data = {k: v for k, v in data.items() if not str(k).startswith("_")}
        known = {f.name for f in cls.__dataclass_fields__.values()}
        unknown = sorted(k for k in data if k not in known)
        if unknown:
            raise ValueError(f"{path}: unknown GridConfig key(s): {', '.join(unknown)}")
        filtered = {k: v for k, v in data.items() if k in known}
        cfg = cls(**filtered)
        cfg.grid = grid
        cfg._validate_append_stop_loss_pct()
        cfg._validate_grid_values()
        return cfg

    @classmethod
    def from_file(cls, path: Path) -> "GridConfig":
        return cls.from_json(path)

    def generate_params(self) -> list[InstanceParams]:
        """Generate all parameter combinations from the grid."""
        return self.generate_base_params() + self.generate_appended_stop_loss_params()

    def _grid_values(self, key: str) -> list[float]:
        vals = self.grid.get(key, [])
        if not vals:
            vals = GRID_PARAM_DEFAULTS[key]
        return list(vals)

    def generate_base_params(self) -> list[InstanceParams]:
        """Generate the configured grid without append-only stop-loss extensions."""
        lists = []
        for k in GRID_PARAM_KEYS:
            lists.append(self._grid_values(k))

        params = []
        for combo in itertools.product(*lists):
            params.append(InstanceParams(
                alpha_up=combo[0],
                alpha_down=combo[1],
                floor_up=combo[2],
                floor_down=combo[3],
                tp_percentage=combo[4],
                stop_loss_pct=combo[5],
            ))
        return params

    def generate_appended_stop_loss_params(self) -> list[InstanceParams]:
        """Generate append-only stop-loss variants after all existing instances."""
        if not self.append_stop_loss_pct:
            return []
        lists = [self._grid_values(k) for k in GRID_NON_STOP_LOSS_KEYS]
        params = []
        for stop_loss_pct in self.append_stop_loss_pct:
            for combo in itertools.product(*lists):
                params.append(InstanceParams(
                    alpha_up=combo[0],
                    alpha_down=combo[1],
                    floor_up=combo[2],
                    floor_down=combo[3],
                    tp_percentage=combo[4],
                    stop_loss_pct=stop_loss_pct,
                ))
        return params

    def execution_max_entry_price(self) -> float:
        """Max paper execution VWAP after delayed-book slippage allowance."""
        if self.paper_max_execution_entry_price is not None:
            return float(self.paper_max_execution_entry_price)
        return min(1.0, self.max_entry_price + self.paper_delayed_book_slippage)


# ---------------------------------------------------------------------------
# Single-trader configuration (for --mode trader, paper + live)
# ---------------------------------------------------------------------------


@dataclass
class TraderConfig:
    """Configuration for single-instance paper/live trading."""

    # Trading mode
    paper_mode: bool = True
    live_trading_enabled: bool = True
    entries_enabled: bool = True  # Disables new exposure; exits/reconciliation continue.
    live_shadow_mode: bool = False
    signature_type: int = 1  # 0=EOA, 1=Magic wallet, 2=Browser wallet
    max_slippage_pct: float = 0.02
    require_confirmation: bool = True

    # Capital management
    starting_capital: float = 100.0   # Live mode fallback (overwritten from USDC balance)
    paper_starting_capital: float = 1000.0
    order_size_pct: float = 0.10
    min_order_size_usd: float = 2.0
    min_position_shares: float = 5.10  # Polymarket $5 notional floor (≤$1/share ⇒ ≥5 shares)

    # Edge curve parameters
    edge_alpha_up: float = 1.5
    edge_alpha_down: float = 1.5
    edge_floor_up: float = 0.55
    edge_floor_down: float = 0.55

    # Take profit
    tp_percentage: float = 0.15
    max_entry_price: float = 0.95
    friction_pct: float = DEFAULT_DRY_RUN_FRICTION_PCT

    # Trailing stop
    trail_activation: float = 0.0  # 0 = disabled
    trail_distance: float = 0.0    # 0 = disabled

    # Stop-loss
    stop_loss_pct: float = 0.0     # 0 = disabled
    exit_order_timeout_seconds: int = 60
    exit_order_refresh_seconds: int = 5

    # Timing (15-minute market specific)
    no_trade_first_seconds: int = 300   # First 5 min blocked (barrier settling)
    no_trade_last_seconds: int = 180    # Last 3 min blocked (avoid late-window whipsaws)
    grace_period_seconds: int = 60      # Cooldown after TP/SL
    check_interval_seconds: int = 10    # TP fill polling interval

    # Reconciliation
    reconciliation_enabled: bool = True
    reconciliation_interval_seconds: int = 300
    settlement_wait_seconds: int = 20
    orphan_historical_token_scan_limit: int = 20
    # Cadence (seconds) for the polymarket_history_reconciler — re-queries
    # /data/trades for trades tagged proceeds_source="order_status"/"planned"
    # and upgrades them to "polymarket_history" when indexing catches up.
    # Set to 0 to disable.
    history_reconcile_interval_seconds: float = 60.0

    # Pre-trade risk gates (live mode)
    max_concurrent_positions: int = 1
    require_tp_confirmed_before_new_entry: bool = True
    max_slippage_estimate_pct: float = 0.02
    min_liquidity_depth_usd: float = 50.0
    live_recheck_delay_seconds: float = 1.0
    live_entry_spot_recheck_enabled: bool = True
    live_entry_spot_recheck_buffer_usd: float = 5.0
    tp_fill_balance_recheck_seconds: float = 2.0
    # Entry order is GTC + IOC-like: poll for fill up to this many seconds
    # then cancel any unfilled residual. Replaces the prior FOK semantics,
    # which rejected the whole order on partial liquidity. 2 s is enough
    # for a CLOB match on a tight book; longer values risk an adverse
    # price move on the residual before we cancel.
    entry_cancel_timeout_s: float = 2.0
    # After a GTC entry cancel returns success, keep reconciling briefly before
    # treating the order as cleanly unfilled. The CLOB/indexer can lag a cancel
    # response while a fill is still becoming visible.
    post_cancel_reconcile_seconds: float = 3.0
    post_cancel_reconcile_poll_seconds: float = 0.5
    post_cancel_ambiguous_on_unreadable_balance: bool = True
    # Live entry placement retry for transient CLOB errors such as
    # Only a definite pre-submit failure or explicit rejection permits retry;
    # missing order IDs and transport timeouts do not establish non-acceptance.
    live_entry_order_retries: int = 5
    live_entry_retry_window_seconds: float = 30.0
    live_entry_retry_delay_seconds: float = 5.0
    live_entry_retry_price_guard: float = 0.02

    # CLOB taker-health gate. The live trader writes a short, self-expiring
    # "taker outage" flag whenever a real order POST is refused by the CLOB
    # (post_only_mode / 425 service-not-ready / transport failure), and reads it
    # (plus the read-only clob_reads_healthy() probe) before each entry. Both the
    # flag and the probe are also consulted by the grid so dry-run and live skip
    # the same windows. unhealthy_until = now + clamp(retry_after, default, max).
    clob_health_gate_enabled: bool = True
    clob_health_shared_path: str = "results/15m_clob_health.json"
    clob_health_default_cooldown_seconds: float = 90.0
    clob_health_max_cooldown_seconds: float = 120.0
    clob_reads_probe_ttl_seconds: float = 8.0

    # Canonical availability ledger. The live trader appends scoped evidence
    # (availability_events.csv) on every CLOB/order observation and rebuilds the
    # normalized interval ledger (availability_ledger.csv); the optional forward
    # gate consults it, and the offline cleaner / 5m bot read it. Writing is
    # best-effort and never blocks the order path. shared_availability_ledger_path
    # (when set) receives platform-scope evidence for the dry-run-only 5m bot.
    availability_ledger_enabled: bool = True
    availability_gate_enabled: bool = False
    availability_gate_min_confidence: str = "confirmed"
    availability_coalesce_gap_seconds: float = 120.0
    availability_ledger_path: str = "results/availability_ledger.csv"
    availability_events_path: str = "results/availability_events.csv"
    shared_availability_ledger_path: str = ""

    # Delayed-order handling; pending_entry_max_age may need re-tuning if the
    # 15m live fill profile changes.
    delayed_order_timeout_seconds: int = 10
    pending_entry_max_age_seconds: int = 120

    # Hybrid calibration: accept both VPS-local and remotely offloaded
    # calibrations through a single broker on the VPS.
    hybrid_calibration_enabled: bool = True
    hybrid_remote_inbox_enabled: bool = True
    hybrid_local_worker_enabled: bool = True
    # Default 10-20s leaves enough CPU headroom on a 2-vCPU VPS while still
    # producing ~3 backup calibrations/minute. Offload typically delivers
    # faster, so most worker events get superseded; override to 0.0/0.0 in
    # config_trader_live.json if you want maximum redundancy.
    hybrid_local_worker_delay_min_seconds: float = 10.0
    hybrid_local_worker_delay_max_seconds: float = 20.0
    # Per-consumer inbox directory (relative to project root). When the shared
    # calc daemon fans out to multiple inboxes, each consumer points at its
    # own subdir to avoid the destructive poll_inbox race. Empty string keeps
    # the broker default (results/calibration_inbox).
    hybrid_inbox_dir: str = ""
    ipc_backend: str = "file"
    redis_url: str = "redis://redis:6379/0"
    redis_stream: str = "market.calibration"
    redis_consumer_group: str = "live"
    redis_consumer_name: str = "live"
    redis_lock_enabled: bool = True
    redis_lock_key: str = "locks:polymarket_updown_15m:live"
    redis_lock_ttl_ms: int = 45000
    redis_lock_renew_ms: int = 15000
    redis_lock_fail_closed: bool = True
    ipc_file_fallback_enabled: bool = True
    # Blocking XREADGROUP listener thread applying calibration events the
    # moment they land (vs the once-per-tick poll). Fail-open to polling.
    hybrid_push_consumer_enabled: bool = True
    # Legacy terminal-price evaluation, matching GridConfig. Current TWAP
    # entries use v5 scalars with independent freshness checks in each loop.
    local_prob_eval_enabled: bool = False
    local_eval_event_max_age_s: float = 30.0
    local_eval_spot_max_age_s: float = 2.0
    staged_finalize_fresh_prob: bool = True
    polygon_rpc_urls: tuple[str, ...] = DEFAULT_POLYGON_RPC_URLS

    # Mirror-signal control. The live trader can let the matching dry-run
    # grid instance decide ENTRY/STOP timing while live keeps the real TP
    # limit order on the venue.
    mirror_signal_control_enabled: bool = False
    mirror_instance_id: str = "auto"
    mirror_controls_entry: bool = True
    mirror_controls_stop_loss: bool = True
    allow_local_entry_with_mirror: bool = False
    local_entry_enabled: bool = True
    local_stop_loss_enabled: bool = True
    tp_limit_order_enabled: bool = True
    dry_tp_signal_action: str = "grace_then_market_exit"
    dry_tp_live_fill_grace_seconds: float = 120.0
    dry_tp_market_exit_enabled: bool = True
    dry_tp_market_exit_skip_if_expired: bool = True
    mirror_signal_stream: str = "trade.mirror.signals"
    mirror_ack_stream: str = "trade.mirror.acks"
    mirror_ack_stream_maxlen: int = 100_000
    mirror_signal_max_age_seconds: float = 30.0
    mirror_signal_async_enabled: bool = False
    mirror_signal_async_block_ms: int = 250
    mirror_signal_entry_max_consumption_age_seconds: float = 15.0
    mirror_signal_entry_tick_max_age_seconds: float = 5.0
    mirror_signal_entry_retry_delay_seconds: float = 0.5
    mirror_signal_latency_warn_seconds: float = 1.0
    mirror_signal_ack_duplicate_throttle_seconds: float = 30.0
    mirror_signal_lock_warn_seconds: float = 1.0
    mirror_signal_pending_reclaim_min_idle_ms: int = 1000
    mirror_signal_pending_recover_interval_seconds: float = 1.0
    mirror_heartbeat_key: str = "trade.mirror.heartbeat"
    mirror_heartbeat_stale_seconds: float = 60.0
    mirror_heartbeat_check_interval_seconds: float = 10.0

    # Persistence migration. "files" preserves the historical JSON/CSV source
    # of truth. "sqlite" stores the snapshot + normalized trades/equity rows in
    # SQLite and can still export the legacy files for dashboard/backward compat.
    persistence_backend: str = "files"
    export_legacy_files: bool = True
    market_db_path: Optional[Path] = None
    sqlite_db_file: Path = field(default_factory=lambda: Path("results/15m_paper.sqlite"))
    live_sqlite_db_file: Path = field(default_factory=lambda: Path("results/15m_live.sqlite"))

    # Paths (separate for paper vs live)
    state_file: Path = field(default_factory=lambda: Path("results/15m_paper_state.json"))
    live_state_file: Path = field(default_factory=lambda: Path("results/15m_live_state.json"))
    trades_file: Path = field(default_factory=lambda: Path("results/15m_paper_trades.csv"))
    live_trades_file: Path = field(default_factory=lambda: Path("results/15m_live_trades.csv"))
    equity_file: Path = field(default_factory=lambda: Path("results/15m_paper_equity.csv"))
    live_equity_file: Path = field(default_factory=lambda: Path("results/15m_live_equity.csv"))

    # Logging
    log_orderbook: bool = True
    orderbook_file: Path = field(default_factory=lambda: Path("results/15m_orderbook.csv"))
    risk_events_file: Path = field(default_factory=lambda: Path("results/15m_risk_events.csv"))
    risk_events_current_file: Path = field(default_factory=lambda: Path("results/15m_risk_events.current.csv"))
    live_tick_eval_file: Path = field(default_factory=lambda: Path("results/15m_live_tick_eval.current.csv"))
    friction_realized_file: Path = field(default_factory=lambda: Path("results/15m_friction_realized.csv"))
    mirror_signal_audit_file: Path = field(default_factory=lambda: Path("results/15m_mirror_signal_audit.csv"))
    mirror_signal_audit_current_file: Path = field(default_factory=lambda: Path("results/15m_mirror_signal_audit.current.csv"))

    def __post_init__(self):
        if self.trail_activation > 0 or self.trail_distance > 0:
            if self.trail_activation <= 0 or self.trail_distance <= 0:
                raise ValueError(
                    "Both trail_activation and trail_distance must be > 0 if either is set"
                )
            if self.trail_activation >= self.tp_percentage:
                raise ValueError(
                    f"trail_activation ({self.trail_activation}) must be < tp_percentage ({self.tp_percentage})"
                )
            if self.trail_distance >= self.trail_activation:
                raise ValueError(
                    f"trail_distance ({self.trail_distance}) must be < trail_activation ({self.trail_activation})"
                )
        self._validate_ranges()

    def _validate_ranges(self) -> None:
        if self.signature_type not in {0, 1, 2}:
            raise ValueError(f"signature_type must be 0, 1, or 2, got {self.signature_type!r}")
        if self.ipc_backend not in {"file", "redis"}:
            raise ValueError(f"ipc_backend must be 'file' or 'redis', got {self.ipc_backend!r}")
        if self.persistence_backend not in {"files", "sqlite"}:
            raise ValueError(
                f"persistence_backend must be 'files' or 'sqlite', got {self.persistence_backend!r}"
            )
        if self.dry_tp_signal_action not in {"ignore", "grace_then_market_exit", "immediate_market_exit"}:
            raise ValueError(
                "dry_tp_signal_action must be 'ignore', 'grace_then_market_exit', "
                "'immediate_market_exit', "
                f"got {self.dry_tp_signal_action!r}"
            )
        if (
            self.mirror_signal_control_enabled
            and self.mirror_controls_entry
            and self.local_entry_enabled
            and not self.allow_local_entry_with_mirror
        ):
            raise ValueError(
                "local_entry_enabled must be false when mirror signal entry control is enabled "
                "unless allow_local_entry_with_mirror=true"
            )

        _validate_number_range("starting_capital", self.starting_capital, min_value=0)
        _validate_number_range("paper_starting_capital", self.paper_starting_capital, min_value=0)
        _validate_number_range("order_size_pct", self.order_size_pct, min_value=0, max_value=1, min_inclusive=False)
        _validate_number_range("min_order_size_usd", self.min_order_size_usd, min_value=0, min_inclusive=False)
        _validate_number_range("min_position_shares", self.min_position_shares, min_value=0, min_inclusive=False)
        _validate_number_range("tp_percentage", self.tp_percentage, min_value=0, max_value=1, min_inclusive=False)
        _validate_number_range("max_entry_price", self.max_entry_price, min_value=0, max_value=1, min_inclusive=False)
        _validate_number_range("friction_pct", self.friction_pct, min_value=0, max_value=1)
        _validate_number_range("max_slippage_pct", self.max_slippage_pct, min_value=0, max_value=1)
        _validate_number_range("stop_loss_pct", self.stop_loss_pct, min_value=0, max_value=1, max_inclusive=False)
        _validate_number_range("trail_activation", self.trail_activation, min_value=0, max_value=1)
        _validate_number_range("trail_distance", self.trail_distance, min_value=0, max_value=1)

        for name in (
            "no_trade_first_seconds",
            "no_trade_last_seconds",
            "grace_period_seconds",
            "check_interval_seconds",
            "reconciliation_interval_seconds",
            "settlement_wait_seconds",
            "orphan_historical_token_scan_limit",
            "delayed_order_timeout_seconds",
            "pending_entry_max_age_seconds",
        ):
            _validate_number_range(name, getattr(self, name), min_value=0)
        _validate_number_range("exit_order_timeout_seconds", self.exit_order_timeout_seconds, min_value=0, min_inclusive=False)
        _validate_number_range("exit_order_refresh_seconds", self.exit_order_refresh_seconds, min_value=0, min_inclusive=False)
        _validate_number_range("history_reconcile_interval_seconds", self.history_reconcile_interval_seconds, min_value=0)
        _validate_number_range("max_concurrent_positions", self.max_concurrent_positions, min_value=1)
        _validate_number_range("max_slippage_estimate_pct", self.max_slippage_estimate_pct, min_value=0, max_value=1)
        _validate_number_range("min_liquidity_depth_usd", self.min_liquidity_depth_usd, min_value=0)
        _validate_number_range("live_recheck_delay_seconds", self.live_recheck_delay_seconds, min_value=0)
        _validate_number_range("live_entry_spot_recheck_buffer_usd", self.live_entry_spot_recheck_buffer_usd, min_value=0)
        _validate_number_range("tp_fill_balance_recheck_seconds", self.tp_fill_balance_recheck_seconds, min_value=0)
        _validate_number_range("entry_cancel_timeout_s", self.entry_cancel_timeout_s, min_value=0, min_inclusive=False)
        _validate_number_range("post_cancel_reconcile_seconds", self.post_cancel_reconcile_seconds, min_value=0)
        _validate_number_range(
            "post_cancel_reconcile_poll_seconds",
            self.post_cancel_reconcile_poll_seconds,
            min_value=0,
            min_inclusive=False,
        )
        _validate_number_range("live_entry_order_retries", self.live_entry_order_retries, min_value=0)
        _validate_number_range("live_entry_retry_window_seconds", self.live_entry_retry_window_seconds, min_value=0)
        _validate_number_range("live_entry_retry_delay_seconds", self.live_entry_retry_delay_seconds, min_value=0)
        _validate_number_range("live_entry_retry_price_guard", self.live_entry_retry_price_guard, min_value=0, max_value=1)
        _validate_number_range("clob_health_default_cooldown_seconds", self.clob_health_default_cooldown_seconds, min_value=0)
        _validate_number_range("clob_health_max_cooldown_seconds", self.clob_health_max_cooldown_seconds, min_value=0)
        _validate_number_range("clob_reads_probe_ttl_seconds", self.clob_reads_probe_ttl_seconds, min_value=0)
        _validate_number_range("hybrid_local_worker_delay_min_seconds", self.hybrid_local_worker_delay_min_seconds, min_value=0)
        _validate_number_range("hybrid_local_worker_delay_max_seconds", self.hybrid_local_worker_delay_max_seconds, min_value=0)
        _validate_number_range("dry_tp_live_fill_grace_seconds", self.dry_tp_live_fill_grace_seconds, min_value=0)
        _validate_number_range("mirror_ack_stream_maxlen", self.mirror_ack_stream_maxlen, min_value=1)
        _validate_number_range("mirror_signal_max_age_seconds", self.mirror_signal_max_age_seconds, min_value=0)
        _validate_number_range("mirror_signal_async_block_ms", self.mirror_signal_async_block_ms, min_value=0)
        _validate_number_range(
            "mirror_signal_entry_max_consumption_age_seconds",
            self.mirror_signal_entry_max_consumption_age_seconds,
            min_value=0,
        )
        _validate_number_range(
            "mirror_signal_entry_tick_max_age_seconds",
            self.mirror_signal_entry_tick_max_age_seconds,
            min_value=0,
        )
        _validate_number_range(
            "mirror_signal_entry_retry_delay_seconds",
            self.mirror_signal_entry_retry_delay_seconds,
            min_value=0,
        )
        _validate_number_range("mirror_signal_latency_warn_seconds", self.mirror_signal_latency_warn_seconds, min_value=0)
        _validate_number_range(
            "mirror_signal_ack_duplicate_throttle_seconds",
            self.mirror_signal_ack_duplicate_throttle_seconds,
            min_value=0,
        )
        _validate_number_range("mirror_signal_lock_warn_seconds", self.mirror_signal_lock_warn_seconds, min_value=0)
        _validate_number_range("mirror_signal_pending_reclaim_min_idle_ms", self.mirror_signal_pending_reclaim_min_idle_ms, min_value=0)
        _validate_number_range("mirror_signal_pending_recover_interval_seconds", self.mirror_signal_pending_recover_interval_seconds, min_value=0)
        _validate_number_range("mirror_heartbeat_stale_seconds", self.mirror_heartbeat_stale_seconds, min_value=0)
        _validate_number_range("mirror_heartbeat_check_interval_seconds", self.mirror_heartbeat_check_interval_seconds, min_value=0)
        _validate_number_range("redis_lock_ttl_ms", self.redis_lock_ttl_ms, min_value=1000)
        _validate_number_range("redis_lock_renew_ms", self.redis_lock_renew_ms, min_value=100)
        if self.redis_lock_enabled and self.redis_lock_renew_ms >= self.redis_lock_ttl_ms:
            raise ValueError("redis_lock_renew_ms must be < redis_lock_ttl_ms")
        if self.hybrid_local_worker_delay_max_seconds < self.hybrid_local_worker_delay_min_seconds:
            raise ValueError("hybrid_local_worker_delay_max_seconds must be >= hybrid_local_worker_delay_min_seconds")

    def get_state_file(self) -> Path:
        return self.state_file if self.paper_mode else self.live_state_file

    def get_trades_file(self) -> Path:
        return self.trades_file if self.paper_mode else self.live_trades_file

    def get_equity_file(self) -> Path:
        return self.equity_file if self.paper_mode else self.live_equity_file

    def get_starting_capital(self) -> float:
        return self.paper_starting_capital if self.paper_mode else self.starting_capital

    def get_sqlite_db_file(self) -> Path:
        return self.sqlite_db_file if self.paper_mode else self.live_sqlite_db_file

    def sqlite_enabled(self) -> bool:
        return self.persistence_backend.lower() == "sqlite"

    @classmethod
    def from_json(cls, path: Path) -> "TraderConfig":
        """Load config from JSON/YAML file."""
        data = _load_config_mapping(path, section="live")
        if path.name == "config_trader.json":
            warnings.warn(
                "config_trader.json is deprecated; config.yaml live: is the authoritative trader config.",
                DeprecationWarning,
                stacklevel=2,
            )
        if path.suffix.lower() in {".yaml", ".yml"}:
            raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
            persistence = raw.get("persistence") if isinstance(raw, dict) else None
            if isinstance(persistence, dict):
                if "market_db_path" not in data and persistence.get("market_db_path"):
                    data["market_db_path"] = persistence["market_db_path"]
            calc = raw.get("calc") if isinstance(raw, dict) else None
            if (
                "market_db_path" not in data
                and isinstance(calc, dict)
                and calc.get("market_db_path")
            ):
                data["market_db_path"] = calc["market_db_path"]
            if (
                "polygon_rpc_urls" not in data
                and isinstance(calc, dict)
                and calc.get("polygon_rpc_urls")
            ):
                data["polygon_rpc_urls"] = calc["polygon_rpc_urls"]

        if "polygon_rpc_urls" in data:
            data["polygon_rpc_urls"] = tuple(data.get("polygon_rpc_urls") or ())

        # Convert paths if present
        path_fields = ["state_file", "live_state_file", "trades_file",
                       "live_trades_file", "equity_file", "live_equity_file",
                       "orderbook_file", "risk_events_file",
                       "risk_events_current_file", "live_tick_eval_file",
                       "friction_realized_file", "market_db_path", "sqlite_db_file",
                       "live_sqlite_db_file", "mirror_signal_audit_file",
                       "mirror_signal_audit_current_file"]
        for pf in path_fields:
            if pf in data:
                configured_path = Path(data[pf])
                if not configured_path.is_absolute():
                    configured_path = (path.parent / configured_path).resolve()
                data[pf] = configured_path

        # Strip underscore-prefixed keys (JSON comments)
        data = {k: v for k, v in data.items() if not k.startswith("_")}

        known = {f.name for f in cls.__dataclass_fields__.values()}
        unknown = sorted(k for k in data if k not in known)
        if unknown:
            raise ValueError(f"{path}: unknown TraderConfig key(s): {', '.join(unknown)}")
        filtered = {k: v for k, v in data.items() if k in known}

        return cls(**filtered)

    @classmethod
    def from_file(cls, path: Path) -> "TraderConfig":
        return cls.from_json(path)

    def to_json(self, path: Path) -> None:
        """Save config to JSON file."""
        data: dict[str, Any] = {}
        for item in fields(self):
            value = getattr(self, item.name)
            if isinstance(value, Path):
                data[item.name] = str(value)
            elif isinstance(value, tuple):
                data[item.name] = list(value)
            else:
                data[item.name] = value
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4)
