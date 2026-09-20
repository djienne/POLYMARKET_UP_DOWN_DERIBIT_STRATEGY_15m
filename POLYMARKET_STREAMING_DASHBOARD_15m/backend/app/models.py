from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel, Field


ViewSource = Literal["dry_run", "live"]


class InstanceParams(BaseModel):
    alpha_up: float
    alpha_down: float
    floor_up: float
    floor_down: float
    tp_pct: float
    sl_pct: float


class LeaderboardRow(BaseModel):
    rank: int
    instance_id: int
    total_pnl: float
    pnl_mean_to_sd: Optional[float] = None
    max_drawdown: Optional[float] = None
    max_drawdown_pct: Optional[float] = None
    wins: int
    losses: int
    win_rate: float
    trades: int
    params: InstanceParams
    liquidity_mode: str
    as_of: Optional[str] = None
    written_at: Optional[str] = None


class InstanceStats(BaseModel):
    instance_id: int
    rank: Optional[int] = None
    capital: float
    starting_capital: float
    total_pnl: float
    total_pnl_pct: float
    pnl_mean_to_sd: Optional[float] = None
    max_drawdown: Optional[float] = None
    max_drawdown_pct: Optional[float] = None
    wins: int
    losses: int
    win_rate: float
    trades_count: int
    params: Optional[InstanceParams] = None


class OpenPosition(BaseModel):
    direction: Literal["UP", "DOWN"]
    entry_price: float
    shares: float
    tp_target: Optional[float] = None
    sl_target: Optional[float] = None
    entered_at: Optional[str] = None
    market_id: Optional[str] = None
    notional: Optional[float] = None
    # Edge ratio the trader captured at the moment of fill (model_prob vs
    # executable market price). Sourced from Position.entry_context["edge_ratio"]
    # in the live trader, which is also persisted to 15m_live_trades.csv.
    entry_edge_ratio: Optional[float] = None


class PositionState(BaseModel):
    open: Optional[OpenPosition] = None
    last_exit_at: Optional[str] = None
    grace_remaining_s: Optional[float] = None


class ProbabilityBundle(BaseModel):
    # All optional because some models may not run
    ssvi_surface_above: Optional[float] = None
    ssvi_surface_below: Optional[float] = None
    mc_above: Optional[float] = None
    mc_below: Optional[float] = None
    heston_above: Optional[float] = None
    heston_below: Optional[float] = None
    bl_above: Optional[float] = None
    bl_below: Optional[float] = None
    avg_above: Optional[float] = None
    avg_below: Optional[float] = None
    selected_above: Optional[float] = None
    selected_below: Optional[float] = None
    probability_mode: Optional[str] = None
    probability_source_label: Optional[str] = None
    payoff_kind: Optional[str] = None
    averaging_window_seconds: Optional[float] = None
    probability_evaluated_at: Optional[str] = None
    model_assumption: Optional[str] = None
    bl_mc_divergence: Optional[float] = None
    preferred_model: Optional[str] = None


class MarketInfo(BaseModel):
    slug: Optional[str] = None
    title: Optional[str] = None
    window_start_unix: Optional[int] = None
    window_end_unix: Optional[int] = None
    spot_price: Optional[float] = None
    spot_source: Optional[str] = None
    spot_source_label: Optional[str] = None
    spot_sequence: Optional[int] = None
    spot_published_at_ms: Optional[int] = None
    spot_age_seconds: Optional[float] = None
    barrier: Optional[float] = None
    direction: Optional[str] = None
    ttm_days: Optional[float] = None
    ttm_seconds: Optional[float] = None


class PolymarketPrices(BaseModel):
    best_bid: Optional[float] = None
    best_ask: Optional[float] = None
    mid: Optional[float] = None
    prob_up: Optional[float] = None
    prob_down: Optional[float] = None


class PolymarketStatus(BaseModel):
    # Instatus page-level status: "UP" | "UNDERMAINTENANCE" | "HASISSUES" |
    # "DEGRADEDPERFORMANCE" | "PARTIALOUTAGE" | "MINOROUTAGE" | "MAJOROUTAGE".
    status: str = "UP"
    is_operational: bool = True
    active_maintenance: Optional[str] = None
    fetched_at: Optional[str] = None
    page_status: str = "UP"
    clob_status: Optional[str] = None
    effective_status: str = "UP"
    empirical_clob_healthy: Optional[bool] = None
    empirical_clob_consecutive_ok: int = 0
    status_reason: Optional[str] = None


class TimingInfo(BaseModel):
    calibration_s: Optional[float] = None
    surface_fit_s: Optional[float] = None
    mc_s: Optional[float] = None
    bl_s: Optional[float] = None
    surface_bl_s: Optional[float] = None
    used_gap_s: Optional[float] = None
    used_source: Optional[str] = None


class TerminalSnapshot(BaseModel):
    timestamp: Optional[str] = None
    market: MarketInfo = MarketInfo()
    probabilities: ProbabilityBundle = ProbabilityBundle()
    polymarket: PolymarketPrices = PolymarketPrices()
    timing: TimingInfo = TimingInfo()
    age_seconds: Optional[float] = None


class SpotInfo(BaseModel):
    schema_version: int = 1
    symbol: str = "BTC/USD"
    price: Optional[float] = None
    source: Optional[str] = None
    source_label: Optional[str] = None
    source_timestamp_ms: Optional[int] = None
    received_at_ms: Optional[int] = None
    published_at_ms: Optional[int] = None
    source_age_seconds: Optional[float] = None
    receive_age_seconds: Optional[float] = None
    sequence: Optional[int] = None
    stale_after_seconds: Optional[float] = None
    age_seconds: Optional[float] = None
    details: dict = {}


class MarketContextInfo(BaseModel):
    schema_version: int = 1
    contract_metadata: dict = Field(default_factory=dict)
    source: Optional[str] = None
    market_id: Optional[str] = None
    slug: Optional[str] = None
    title: Optional[str] = None
    window_start_iso: Optional[str] = None
    window_end_iso: Optional[str] = None
    window_start_unix: Optional[int] = None
    window_end_unix: Optional[int] = None
    barrier: Optional[float] = None
    barrier_source: Optional[str] = None
    barrier_confirmed: bool = False
    barrier_offset_s: Optional[float] = None
    clob_token_ids: dict = {}
    sequence: Optional[int] = None
    published_at_ms: Optional[int] = None
    age_seconds: Optional[float] = None


class WindowState(BaseModel):
    elapsed_s: float
    total_s: float = 900.0
    no_trade_first_s: float = 300.0
    no_trade_last_s: float = 180.0
    zone: Literal["blocked_first", "tradeable", "blocked_last", "expired", "unknown"]
    slug: Optional[str] = None


class TradeEvent(BaseModel):
    instance_id: int
    timestamp: str
    event: str
    run_source: Optional[ViewSource] = None
    direction: Optional[str] = None
    market_id: Optional[str] = None
    entry_price: Optional[float] = None
    exit_price: Optional[float] = None
    shares: Optional[float] = None
    pnl: Optional[float] = None
    pnl_pct: Optional[float] = None
    capital: Optional[float] = None
    model_prob: Optional[float] = None
    poly_prob: Optional[float] = None
    spot_price: Optional[float] = None
    barrier: Optional[float] = None


class EdgeRatio(BaseModel):
    side: Literal["UP", "DOWN"]
    market_prob: Optional[float] = None
    model_prob: Optional[float] = None
    required_prob: Optional[float] = None
    current_ratio: Optional[float] = None
    required_ratio: Optional[float] = None
    margin: Optional[float] = None
    has_edge: Optional[bool] = None


class CalibrationStatus(BaseModel):
    active: bool = False
    started_at: Optional[str] = None
    elapsed_s: Optional[float] = None
    last_timing: Optional[TimingInfo] = None


class LivenessInfo(BaseModel):
    bot_live: bool
    lock_exists: bool
    terminal_age_s: Optional[float] = None
    last_terminal_ts: Optional[str] = None
    cpu_pct: Optional[float] = None
    live_position_open: bool = False
    live_position_direction: Optional[Literal["UP", "DOWN"]] = None
    live_position_market_id: Optional[str] = None
    # Local live-trader execution state + dashboard/trader Polymarket latency.
    # execution_location is "local" | "stopped" | null (unknown).
    execution_location: Optional[str] = None
    execution_label: Optional[str] = None
    polymarket_ping_ms: Optional[float] = None
    polymarket_ping_age_s: Optional[float] = None
    spot_pubsub_connected: Optional[bool] = None
    spot_age_seconds: Optional[float] = None
    spot_sequence: Optional[int] = None
    spot_source: Optional[str] = None
    spot_reader_reconnects: Optional[int] = None
    event_loop_lag_ms: Optional[float] = None
    trade_tail_busy: Optional[bool] = None
    trade_tail_last_poll_ms: Optional[float] = None
    trade_tail_last_seed_ms: Optional[float] = None
    trade_tail_file_size_mb: Optional[float] = None


class SharedConfig(BaseModel):
    starting_capital: Optional[float] = None
    order_size_pct: Optional[float] = None
    friction_pct: Optional[float] = None
    max_entry_price: Optional[float] = None
    no_trade_first_s: Optional[float] = None
    no_trade_last_s: Optional[float] = None
    grace_period_s: Optional[float] = None
    liquidity_mode: Optional[str] = None
    alpha_up: Optional[float] = None
    alpha_down: Optional[float] = None
    floor_up: Optional[float] = None
    floor_down: Optional[float] = None
    tp_pct: Optional[float] = None
    sl_pct: Optional[float] = None


class TodaySummary(BaseModel):
    pnl: float = 0.0
    pnl_pct: Optional[float] = None
    entries: int = 0
    wins: int = 0
    losses: int = 0
    closed: int = 0


class PricePoint(BaseModel):
    t: str
    v: float


class ChartMarker(BaseModel):
    t: str
    kind: Literal["ENTRY", "WIN", "LOSS"]
    side: Optional[Literal["UP", "DOWN"]] = None
    price: Optional[float] = None
    pnl: Optional[float] = None


class BootstrapPayload(BaseModel):
    mode: ViewSource
    instance: Optional[InstanceStats] = None
    position: PositionState = PositionState()
    terminal: TerminalSnapshot = TerminalSnapshot()
    spot: Optional[SpotInfo] = None
    market_context: Optional[MarketContextInfo] = None
    window: Optional[WindowState] = None
    trades: list[TradeEvent] = []
    equity: list[float] = []
    leaderboard_top: list[LeaderboardRow] = []
    liveness: LivenessInfo
    calibration: CalibrationStatus = CalibrationStatus()
    edge_up: Optional[EdgeRatio] = None
    edge_down: Optional[EdgeRatio] = None
    shared_config: SharedConfig = SharedConfig()
    today_summary: TodaySummary = TodaySummary()
    series_up: list[PricePoint] = []
    series_down: list[PricePoint] = []
    # Bid/ask bands per side. Lets the chart render an Area between bid
    # and ask so trade markers (plotted at the actual fill price — the
    # ask paid for entries, the bid received for TP exits) lie on the
    # right band edge instead of floating above the mid line.
    series_up_bid: list[PricePoint] = []
    series_up_ask: list[PricePoint] = []
    series_down_bid: list[PricePoint] = []
    series_down_ask: list[PricePoint] = []
    model_up: list[PricePoint] = []
    model_down: list[PricePoint] = []
    markers: list[ChartMarker] = []
    window_start_iso: Optional[str] = None
    window_end_iso: Optional[str] = None
    equity_series: list[PricePoint] = []
    polymarket_status: PolymarketStatus = PolymarketStatus()
    dashboard_health: dict = {}


class WsEnvelope(BaseModel):
    type: str
    id: str
    server_time: str
    data: dict
