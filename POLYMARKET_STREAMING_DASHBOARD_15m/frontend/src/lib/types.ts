export type ViewSource = "dry_run" | "live";

export interface InstanceParams {
  alpha_up: number;
  alpha_down: number;
  floor_up: number;
  floor_down: number;
  tp_pct: number;
  sl_pct: number;
}

export interface LeaderboardRow {
  rank: number;
  instance_id: number;
  total_pnl: number;
  pnl_mean_to_sd: number | null;
  max_drawdown: number | null;
  max_drawdown_pct: number | null;
  wins: number;
  losses: number;
  win_rate: number;
  trades: number;
  params: InstanceParams;
  liquidity_mode: string;
}

export interface InstanceStats {
  instance_id: number;
  rank?: number | null;
  capital: number;
  starting_capital: number;
  total_pnl: number;
  total_pnl_pct: number;
  pnl_mean_to_sd: number | null;
  max_drawdown: number | null;
  max_drawdown_pct: number | null;
  wins: number;
  losses: number;
  win_rate: number;
  trades_count: number;
  params?: InstanceParams | null;
}

export interface OpenPosition {
  direction: "UP" | "DOWN";
  entry_price: number;
  shares: number;
  tp_target?: number | null;
  sl_target?: number | null;
  entered_at?: string | null;
  market_id?: string | null;
  notional?: number | null;
  entry_edge_ratio?: number | null;
}

export interface PositionState {
  open: OpenPosition | null;
  last_exit_at?: string | null;
  grace_remaining_s?: number | null;
}

export interface ProbabilityBundle {
  ssvi_surface_above: number | null;
  ssvi_surface_below: number | null;
  mc_above: number | null;
  mc_below: number | null;
  heston_above: number | null;
  heston_below: number | null;
  bl_above: number | null;
  bl_below: number | null;
  avg_above: number | null;
  avg_below: number | null;
  selected_above: number | null;
  selected_below: number | null;
  probability_mode: string | null;
  probability_source_label: string | null;
  payoff_kind?: string | null;
  averaging_window_seconds?: number | null;
  probability_evaluated_at?: string | null;
  model_assumption?: string | null;
  bl_mc_divergence: number | null;
  preferred_model: string | null;
}

export interface MarketInfo {
  slug: string | null;
  title: string | null;
  window_start_unix: number | null;
  window_end_unix: number | null;
  spot_price: number | null;
  spot_source?: string | null;
  spot_source_label?: string | null;
  spot_sequence?: number | null;
  spot_published_at_ms?: number | null;
  spot_age_seconds?: number | null;
  barrier: number | null;
  direction: string | null;
  ttm_days: number | null;
  ttm_seconds: number | null;
}

export interface PolymarketPrices {
  best_bid: number | null;
  best_ask: number | null;
  mid: number | null;
  prob_up: number | null;
  prob_down: number | null;
}

export interface PolymarketStatus {
  status: string;
  is_operational: boolean;
  active_maintenance: string | null;
  fetched_at: string | null;
  page_status: string;
  clob_status: string | null;
  effective_status: string;
  empirical_clob_healthy: boolean | null;
  empirical_clob_consecutive_ok: number;
  status_reason: string | null;
}

export interface TimingInfo {
  calibration_s: number | null;
  surface_fit_s: number | null;
  mc_s: number | null;
  bl_s: number | null;
  surface_bl_s: number | null;
  used_gap_s?: number | null;
  used_source?: string | null;
}

export interface TerminalSnapshot {
  timestamp: string | null;
  market: MarketInfo;
  probabilities: ProbabilityBundle;
  polymarket: PolymarketPrices;
  timing: TimingInfo;
  age_seconds: number | null;
}

export interface TerminalUpdateData extends TerminalSnapshot {
  model_up?: PricePoint[] | null;
  model_down?: PricePoint[] | null;
}

export interface SpotInfo {
  schema_version: number;
  symbol: string;
  price: number | null;
  source: string | null;
  source_label: string | null;
  source_timestamp_ms: number | null;
  received_at_ms: number | null;
  published_at_ms: number | null;
  source_age_seconds: number | null;
  receive_age_seconds: number | null;
  sequence: number | null;
  stale_after_seconds: number | null;
  age_seconds: number | null;
  details?: Record<string, unknown>;
}

export interface MarketContextInfo {
  schema_version: number;
  source?: string | null;
  market_id?: string | null;
  slug?: string | null;
  title?: string | null;
  window_start_iso?: string | null;
  window_end_iso?: string | null;
  window_start_unix?: number | null;
  window_end_unix?: number | null;
  barrier?: number | null;
  barrier_source?: string | null;
  barrier_confirmed: boolean;
  barrier_offset_s?: number | null;
  clob_token_ids?: Record<string, unknown>;
  sequence?: number | null;
  published_at_ms?: number | null;
  age_seconds?: number | null;
}

export type WindowZone =
  | "blocked_first"
  | "tradeable"
  | "blocked_last"
  | "expired"
  | "unknown";

export interface WindowState {
  elapsed_s: number;
  total_s: number;
  no_trade_first_s: number;
  no_trade_last_s: number;
  zone: WindowZone;
  slug: string | null;
}

export interface TradeEvent {
  instance_id: number;
  timestamp: string;
  event: string;
  run_source?: ViewSource | null;
  direction?: string | null;
  market_id?: string | null;
  entry_price?: number | null;
  exit_price?: number | null;
  shares?: number | null;
  pnl?: number | null;
  pnl_pct?: number | null;
  capital?: number | null;
  model_prob?: number | null;
  poly_prob?: number | null;
  spot_price?: number | null;
  barrier?: number | null;
}

export interface EdgeRatio {
  side: "UP" | "DOWN";
  market_prob: number | null;
  model_prob: number | null;
  required_prob: number | null;
  current_ratio: number | null;
  required_ratio: number | null;
  margin: number | null;
  has_edge: boolean | null;
}

export interface CalibrationStatus {
  active: boolean;
  started_at: string | null;
  elapsed_s: number | null;
  last_timing: TimingInfo | null;
}

export interface LivenessInfo {
  bot_live: boolean;
  lock_exists: boolean;
  terminal_age_s: number | null;
  last_terminal_ts: string | null;
  cpu_pct?: number | null;
  live_position_open: boolean;
  live_position_direction?: "UP" | "DOWN" | null;
  live_position_market_id?: string | null;
  execution_location?: "local" | "stopped" | "unknown" | null;
  execution_label?: string | null;
  polymarket_ping_ms?: number | null;
  polymarket_ping_age_s?: number | null;
  spot_pubsub_connected?: boolean | null;
  spot_age_seconds?: number | null;
  spot_sequence?: number | null;
  spot_source?: string | null;
  spot_reader_reconnects?: number | null;
  event_loop_lag_ms?: number | null;
  trade_tail_busy?: boolean | null;
  trade_tail_last_poll_ms?: number | null;
  trade_tail_last_seed_ms?: number | null;
  trade_tail_file_size_mb?: number | null;
}

export interface DashboardHealth {
  spot_pubsub_connected?: boolean | null;
  spot_age_seconds?: number | null;
  spot_sequence?: number | null;
  spot_source?: string | null;
  spot_reader_reconnects?: number | null;
  event_loop_lag_ms?: number | null;
  event_loop_max_lag_ms?: number | null;
  trade_tail_busy?: boolean | null;
  trade_tail_last_poll_ms?: number | null;
  trade_tail_last_seed_ms?: number | null;
  trade_tail_file_size_mb?: number | null;
  trade_tails?: Record<string, unknown>[];
}

export interface SharedConfig {
  starting_capital: number | null;
  order_size_pct: number | null;
  friction_pct: number | null;
  max_entry_price: number | null;
  no_trade_first_s: number | null;
  no_trade_last_s: number | null;
  grace_period_s: number | null;
  liquidity_mode: string | null;
  alpha_up?: number | null;
  alpha_down?: number | null;
  floor_up?: number | null;
  floor_down?: number | null;
  tp_pct?: number | null;
  sl_pct?: number | null;
}

export interface TodaySummary {
  pnl: number;
  pnl_pct: number | null;
  entries: number;
  wins: number;
  losses: number;
  closed: number;
}

export interface PricePoint {
  t: string; // ISO timestamp
  v: number; // 0..1 mid probability
}

export interface ChartMarker {
  t: string;
  kind: "ENTRY" | "WIN" | "LOSS";
  side?: "UP" | "DOWN" | null;
  price?: number | null;
  pnl?: number | null;
}

export interface BootstrapPayload {
  mode: ViewSource;
  instance: InstanceStats | null;
  position: PositionState;
  terminal: TerminalSnapshot;
  spot?: SpotInfo | null;
  market_context?: MarketContextInfo | null;
  window: WindowState | null;
  trades: TradeEvent[];
  equity: number[];
  leaderboard_top: LeaderboardRow[];
  liveness: LivenessInfo;
  calibration: CalibrationStatus;
  edge_up: EdgeRatio | null;
  edge_down: EdgeRatio | null;
  shared_config: SharedConfig;
  today_summary: TodaySummary;
  series_up: PricePoint[];
  series_down: PricePoint[];
  series_up_bid: PricePoint[];
  series_up_ask: PricePoint[];
  series_down_bid: PricePoint[];
  series_down_ask: PricePoint[];
  model_up: PricePoint[];
  model_down: PricePoint[];
  markers: ChartMarker[];
  window_start_iso: string | null;
  window_end_iso: string | null;
  equity_series: PricePoint[];
  polymarket_status: PolymarketStatus;
  dashboard_health?: DashboardHealth;
}

export interface WsEnvelopeBase<TType extends string, TData> {
  type: TType;
  id: string;
  server_time: string;
  data: TData;
}

export interface OrderbookUpdateData {
  prices: PolymarketPrices | null;
  series_up?: PricePoint[] | null;
  series_down?: PricePoint[] | null;
  series_up_bid?: PricePoint[] | null;
  series_up_ask?: PricePoint[] | null;
  series_down_bid?: PricePoint[] | null;
  series_down_ask?: PricePoint[] | null;
}

export interface ModelUpdateData {
  series_up?: PricePoint[] | null;
  series_down?: PricePoint[] | null;
}

export interface InstanceUpdateData {
  source?: ViewSource;
  instance: InstanceStats | null;
  position: PositionState;
  equity: number[];
  equity_series?: PricePoint[];
  today_summary?: TodaySummary;
}

export interface LeaderboardUpdateData {
  top: LeaderboardRow[];
}

export interface EdgeUpdateData {
  edge_up: EdgeRatio | null;
  edge_down: EdgeRatio | null;
}

export interface PongData {
  t: number;
}

export type WsEnvelope =
  | WsEnvelopeBase<"bootstrap", BootstrapPayload>
  | WsEnvelopeBase<"terminal.update", TerminalUpdateData>
  | WsEnvelopeBase<"spot.update", SpotInfo>
  | WsEnvelopeBase<"market_context.update", MarketContextInfo>
  | WsEnvelopeBase<"orderbook.update", OrderbookUpdateData>
  | WsEnvelopeBase<"model.update", ModelUpdateData>
  | WsEnvelopeBase<"instance.update", InstanceUpdateData>
  | WsEnvelopeBase<"window.tick", WindowState>
  | WsEnvelopeBase<"liveness.update", LivenessInfo>
  | WsEnvelopeBase<"liveness.tick", LivenessInfo>
  | WsEnvelopeBase<"leaderboard.update", LeaderboardUpdateData>
  | WsEnvelopeBase<"edge.update", EdgeUpdateData>
  | WsEnvelopeBase<"calibration.start", CalibrationStatus>
  | WsEnvelopeBase<"calibration.end", CalibrationStatus>
  | WsEnvelopeBase<"trade.append", TradeEvent>
  | WsEnvelopeBase<"trades.reset", TradesResetData>
  | WsEnvelopeBase<"polymarket_status.update", PolymarketStatus>
  | WsEnvelopeBase<"pong", PongData>;

export interface TradesResetData {
  events: TradeEvent[];
  source?: ViewSource;
  snapshot?: InstanceUpdateData;
}

export interface LegacyWsEnvelope<T = unknown> {
  type: string;
  id: string;
  server_time: string;
  data: T;
}
