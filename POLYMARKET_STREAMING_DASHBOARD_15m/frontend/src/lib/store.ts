import { create } from "zustand";
import type {
  BootstrapPayload,
  CalibrationStatus,
  ChartMarker,
  EdgeRatio,
  InstanceStats,
  LeaderboardRow,
  LivenessInfo,
  MarketInfo,
  MarketContextInfo,
  PolymarketStatus,
  PositionState,
  PricePoint,
  SharedConfig,
  SpotInfo,
  TimingInfo,
  TerminalSnapshot,
  TodaySummary,
  TradeEvent,
  TradesResetData,
  ViewSource,
  WindowState,
  WsEnvelope,
} from "./types";
import {
  computeWindowStateFromBounds,
  nextEntryChartContext,
  nextFlashQueue,
  nextLiveEquity,
  nextMarkers,
  nextMarketContext,
  nextPositionState,
  windowBoundsFromSlug,
  type FlashEvent,
} from "./trade-derive";
import { tradeEventKey } from "./trade-key";

type WsStatus = "connecting" | "open" | "closed";

interface DashState {
  mode: ViewSource;
  viewSource: ViewSource | null;
  alternateModeEnabled: boolean;
  alternateLastSwitchAt: number | null;
  selectedInstanceId: number;
  instance: InstanceStats | null;
  position: PositionState;
  terminal: TerminalSnapshot | null;
  spot: SpotInfo | null;
  window: WindowState | null;
  trades: TradeEvent[];
  equity: number[];
  leaderboard: LeaderboardRow[];
  allInstances: LeaderboardRow[];
  liveness: LivenessInfo | null;
  calibration: CalibrationStatus;
  edgeUp: EdgeRatio | null;
  edgeDown: EdgeRatio | null;
  sharedConfig: SharedConfig;
  todaySummary: TodaySummary;
  seriesUp: PricePoint[];
  seriesDown: PricePoint[];
  seriesUpBid: PricePoint[];
  seriesUpAsk: PricePoint[];
  seriesDownBid: PricePoint[];
  seriesDownAsk: PricePoint[];
  modelUp: PricePoint[];
  modelDown: PricePoint[];
  markers: ChartMarker[];
  windowStartIso: string | null;
  windowEndIso: string | null;
  equitySeries: PricePoint[];
  polymarketStatus: PolymarketStatus;
  wsStatus: WsStatus;
  flashQueue: FlashEvent[];

  setSelected: (id: number) => void;
  setAllInstances: (rows: LeaderboardRow[]) => void;
  setWsStatus: (s: WsStatus) => void;
  setViewSource: (source: ViewSource | null) => void;
  setAlternateModeEnabled: (enabled: boolean) => void;
  setAlternateLastSwitchAt: (ts: number | null) => void;
  applyBootstrap: (p: BootstrapPayload) => void;
  applyEnvelope: (env: WsEnvelope) => void;
  tickWindow: (nowMs?: number) => void;
  consumeFlash: (id: string) => void;
}

const initialCalibration: CalibrationStatus = {
  active: false,
  started_at: null,
  elapsed_s: null,
  last_timing: null,
};

const initialPolymarketStatus: PolymarketStatus = {
  status: "UP",
  is_operational: true,
  active_maintenance: null,
  fetched_at: null,
  page_status: "UP",
  clob_status: null,
  effective_status: "UP",
  empirical_clob_healthy: null,
  empirical_clob_consecutive_ok: 0,
  status_reason: null,
};

function mergeLivenessInfo(
  prev: LivenessInfo | null | undefined,
  incoming: LivenessInfo | null | undefined,
): LivenessInfo | null {
  if (!incoming) return prev ?? null;
  return {
    ...incoming,
    polymarket_ping_ms:
      incoming.polymarket_ping_ms != null
        ? incoming.polymarket_ping_ms
        : (prev?.polymarket_ping_ms ?? null),
    polymarket_ping_age_s:
      incoming.polymarket_ping_ms != null
        ? incoming.polymarket_ping_age_s
        : (prev?.polymarket_ping_age_s ?? null),
    execution_label:
      incoming.execution_label != null
        ? incoming.execution_label
        : (prev?.execution_label ?? null),
    execution_location:
      incoming.execution_location != null
        ? incoming.execution_location
        : (prev?.execution_location ?? null),
    spot_pubsub_connected:
      incoming.spot_pubsub_connected ?? prev?.spot_pubsub_connected ?? null,
    spot_age_seconds: incoming.spot_age_seconds ?? prev?.spot_age_seconds ?? null,
    spot_sequence: incoming.spot_sequence ?? prev?.spot_sequence ?? null,
    spot_source: incoming.spot_source ?? prev?.spot_source ?? null,
    spot_reader_reconnects:
      incoming.spot_reader_reconnects ?? prev?.spot_reader_reconnects ?? null,
    event_loop_lag_ms:
      incoming.event_loop_lag_ms ?? prev?.event_loop_lag_ms ?? null,
    trade_tail_busy: incoming.trade_tail_busy ?? prev?.trade_tail_busy ?? null,
    trade_tail_last_poll_ms:
      incoming.trade_tail_last_poll_ms ?? prev?.trade_tail_last_poll_ms ?? null,
    trade_tail_last_seed_ms:
      incoming.trade_tail_last_seed_ms ?? prev?.trade_tail_last_seed_ms ?? null,
    trade_tail_file_size_mb:
      incoming.trade_tail_file_size_mb ?? prev?.trade_tail_file_size_mb ?? null,
  };
}

function mergeTimingInfo(
  prev: TimingInfo | null | undefined,
  incoming: TimingInfo | null | undefined,
): TimingInfo | null {
  if (!incoming) return prev ?? null;
  return {
    ...incoming,
    // Do not carry source/cadence labels across timing payloads. A local
    // calibration may omit them; preserving old values would keep showing a
    // stale source or cadence after a trader restart.
    used_gap_s: incoming.used_gap_s ?? null,
    used_source: incoming.used_source ?? null,
  };
}

function toPointTs(t: string): number | null {
  const ts = Date.parse(t);
  return Number.isFinite(ts) ? ts : null;
}

function mergePriceSeries(
  prev: PricePoint[],
  incoming: PricePoint[] | null | undefined,
): PricePoint[] {
  if (!incoming || incoming.length === 0) return prev;
  const byTs = new Map<string, number>();
  for (const p of prev) byTs.set(p.t, p.v);
  for (const p of incoming) byTs.set(p.t, p.v);
  return Array.from(byTs.entries())
    .map(([t, v]) => ({ t, v }))
    .sort((a, b) => {
      const aTs = toPointTs(a.t);
      const bTs = toPointTs(b.t);
      if (aTs != null && bTs != null) return aTs - bTs;
      if (aTs != null) return -1;
      if (bTs != null) return 1;
      return a.t.localeCompare(b.t);
    })
    .slice(-1000);
}

function isoFromUnixSeconds(value?: number | null): string | null {
  if (value == null || !Number.isFinite(value)) return null;
  return new Date(value * 1000).toISOString();
}

function initialAlternateModeEnabled(): boolean {
  if (typeof window === "undefined") return false;
  const stored = window.localStorage.getItem("dashboard.alternateModeEnabled");
  return stored === "true";
}

function marketPatchFromContext(
  ctx: MarketContextInfo,
  terminal: TerminalSnapshot | null,
): MarketInfo {
  const current = terminal?.market;
  return {
    slug: ctx.slug ?? ctx.market_id ?? current?.slug ?? null,
    title: ctx.title ?? current?.title ?? null,
    window_start_unix: ctx.window_start_unix ?? current?.window_start_unix ?? null,
    window_end_unix: ctx.window_end_unix ?? current?.window_end_unix ?? null,
    spot_price: current?.spot_price ?? null,
    spot_source: current?.spot_source ?? null,
    spot_source_label: current?.spot_source_label ?? null,
    spot_sequence: current?.spot_sequence ?? null,
    spot_published_at_ms: current?.spot_published_at_ms ?? null,
    spot_age_seconds: current?.spot_age_seconds ?? null,
    barrier: ctx.barrier ?? current?.barrier ?? null,
    direction: current?.direction ?? null,
    ttm_days: current?.ttm_days ?? null,
    ttm_seconds: current?.ttm_seconds ?? null,
  };
}

export const useDash = create<DashState>((set, get) => ({
  mode: "dry_run",
  viewSource: null,
  alternateModeEnabled: initialAlternateModeEnabled(),
  alternateLastSwitchAt: null,
  selectedInstanceId: 1271,
  instance: null,
  position: { open: null },
  terminal: null,
  spot: null,
  window: null,
  trades: [],
  equity: [],
  leaderboard: [],
  allInstances: [],
  liveness: null,
  calibration: initialCalibration,
  edgeUp: null,
  edgeDown: null,
  seriesUp: [],
  seriesDown: [],
  seriesUpBid: [],
  seriesUpAsk: [],
  seriesDownBid: [],
  seriesDownAsk: [],
  modelUp: [],
  modelDown: [],
  markers: [],
  windowStartIso: null,
  windowEndIso: null,
  equitySeries: [],
  polymarketStatus: initialPolymarketStatus,
  sharedConfig: {
    starting_capital: null,
    order_size_pct: null,
    friction_pct: null,
    max_entry_price: null,
    no_trade_first_s: null,
    no_trade_last_s: null,
    grace_period_s: null,
    liquidity_mode: null,
  },
  todaySummary: {
    pnl: 0,
    pnl_pct: null,
    entries: 0,
    wins: 0,
    losses: 0,
    closed: 0,
  },
  wsStatus: "connecting",
  flashQueue: [],

  setSelected: (id) => set({ selectedInstanceId: id }),
  setAllInstances: (rows) => set({ allInstances: rows }),
  setWsStatus: (s) => set({ wsStatus: s }),
  setViewSource: (source) => set({ viewSource: source }),
  setAlternateModeEnabled: (enabled) => {
    if (typeof window !== "undefined") {
      window.localStorage.setItem(
        "dashboard.alternateModeEnabled",
        enabled ? "true" : "false",
      );
    }
    set({ alternateModeEnabled: enabled });
  },
  setAlternateLastSwitchAt: (ts) => set({ alternateLastSwitchAt: ts }),

  applyBootstrap: (p) =>
    set((st) => ({
      ...(st.terminal?.market?.slug !== p.terminal?.market?.slug
        ? {
            calibration: p.calibration ?? initialCalibration,
          }
        : {}),
      mode: p.mode,
      viewSource: p.mode,
      selectedInstanceId:
        p.instance?.instance_id != null
          ? p.instance.instance_id
          : st.selectedInstanceId,
      instance: p.instance,
      position: p.position,
      terminal:
        p.terminal != null
          ? {
              ...p.terminal,
              timing:
                mergeTimingInfo(
                  st.terminal?.market?.slug === p.terminal?.market?.slug
                    ? st.terminal?.timing
                    : null,
                  p.terminal.timing,
                ) ?? p.terminal.timing,
            }
          : p.terminal,
      window: computeWindowStateFromBounds(
        p.window,
        p.window_start_iso ?? null,
        p.window_end_iso ?? null,
      ),
      trades: p.trades,
      equity: p.equity,
      leaderboard: p.leaderboard_top,
      liveness: mergeLivenessInfo(st.liveness, p.liveness),
      calibration:
        p.terminal?.timing != null
          ? {
              ...((st.terminal?.market?.slug !== p.terminal?.market?.slug
                ? initialCalibration
                : st.calibration) ?? initialCalibration),
              ...(p.calibration ?? initialCalibration),
              last_timing: mergeTimingInfo(
                st.terminal?.market?.slug === p.terminal?.market?.slug
                  ? st.calibration.last_timing
                  : null,
                p.terminal.timing,
              ),
            }
          : (p.calibration ?? initialCalibration),
      edgeUp: p.edge_up,
      edgeDown: p.edge_down,
      sharedConfig: p.shared_config ?? get().sharedConfig,
      todaySummary: p.today_summary ?? get().todaySummary,
      seriesUp: p.series_up ?? [],
      seriesDown: p.series_down ?? [],
      seriesUpBid: p.series_up_bid ?? [],
      seriesUpAsk: p.series_up_ask ?? [],
      seriesDownBid: p.series_down_bid ?? [],
      seriesDownAsk: p.series_down_ask ?? [],
      modelUp: p.model_up ?? [],
      modelDown: p.model_down ?? [],
      markers: p.markers ?? [],
      windowStartIso: p.window_start_iso ?? null,
      windowEndIso: p.window_end_iso ?? null,
      equitySeries: p.equity_series ?? [],
      polymarketStatus: p.polymarket_status ?? initialPolymarketStatus,
      spot: p.spot ?? st.spot,
    })),

  applyEnvelope: (env) => {
    switch (env.type) {
      case "bootstrap": {
        get().applyBootstrap(env.data);
        break;
      }
      case "terminal.update": {
        const incoming = env.data;
        const prev = get().terminal;
        const polymarket =
          incoming.polymarket?.prob_up != null
            ? incoming.polymarket
            : (prev?.polymarket ?? incoming.polymarket);
        const market =
          incoming.market?.slug
            ? incoming.market
            : { ...incoming.market, slug: prev?.market?.slug ?? incoming.market?.slug };
        // Also extract model probabilities from the terminal snapshot and append
        // to modelUp/modelDown. terminal.update fires on every SSVI calibration
        // (every 2–11s), whereas `model.update` (from docker log tailing) only
        // publishes on grid log ticks (~60s). Appending here keeps the dashed
        // chart lines in sync with the trader's actual calibration cadence.
        const probs = incoming.probabilities || {};
        const incomingModelUp = incoming.model_up ?? null;
        const incomingModelDown = incoming.model_down ?? null;
        const up =
          probs.selected_above ??
          probs.avg_above ??
          probs.mc_above ??
          probs.ssvi_surface_above ??
          null;
        const down =
          probs.selected_below ??
          probs.avg_below ??
          probs.mc_below ??
          probs.ssvi_surface_below ??
          null;
        const ts = incoming.timestamp as string | undefined;
        set((st) => {
          const patch = ensureSlug(st, market?.slug ?? null);
          let modelUp = mergePriceSeries(st.modelUp, incomingModelUp);
          let modelDown = mergePriceSeries(st.modelDown, incomingModelDown);
          if (ts && (up != null || down != null)) {
            const lastUpTs = modelUp[modelUp.length - 1]?.t;
            const lastDownTs = modelDown[modelDown.length - 1]?.t;
            if (up != null) {
              modelUp =
                ts === lastUpTs
                  ? [...modelUp.slice(0, -1), { t: ts, v: up }]
                  : [...modelUp, { t: ts, v: up }].slice(-1000);
            }
            if (down != null) {
              modelDown =
                ts === lastDownTs
                  ? [...modelDown.slice(0, -1), { t: ts, v: down }]
                  : [...modelDown, { t: ts, v: down }].slice(-1000);
            }
          }
          const sameMarket = st.terminal?.market?.slug === (market?.slug ?? null);
          const calibration =
            incoming.timing != null
              ? {
                  ...(sameMarket ? st.calibration : initialCalibration),
                  last_timing: mergeTimingInfo(
                    sameMarket ? st.calibration.last_timing : null,
                    incoming.timing,
                  ),
                }
              : (sameMarket ? st.calibration : initialCalibration);
          const timing = mergeTimingInfo(
            sameMarket ? st.terminal?.timing : null,
            incoming.timing,
          );
          return {
            ...patch,
            terminal: {
              ...incoming,
              polymarket,
              market,
              timing: timing ?? incoming.timing,
            },
            calibration,
            modelUp,
            modelDown,
          };
        });
        break;
      }
      case "spot.update": {
        const spot = env.data;
        set((st) => ({
          spot,
          terminal:
            st.terminal && st.terminal.market
              ? {
                  ...st.terminal,
                  market: {
                    ...st.terminal.market,
                    spot_price: spot.price,
                    spot_source: spot.source,
                    spot_source_label: spot.source_label,
                    spot_sequence: spot.sequence,
                    spot_published_at_ms: spot.published_at_ms,
                    spot_age_seconds: spot.age_seconds ?? spot.source_age_seconds,
                  },
                }
              : st.terminal,
        }));
        break;
      }
      case "market_context.update": {
        const ctx = env.data;
        const startIso = ctx.window_start_iso ?? isoFromUnixSeconds(ctx.window_start_unix);
        const endIso = ctx.window_end_iso ?? isoFromUnixSeconds(ctx.window_end_unix);
        set((st) => {
          const market = marketPatchFromContext(ctx, st.terminal);
          const patch = ensureSlug(st, market.slug ?? null);
          return {
            ...patch,
            terminal: st.terminal
              ? { ...st.terminal, market }
              : st.terminal,
            window: computeWindowStateFromBounds(st.window, startIso, endIso),
            windowStartIso: startIso ?? st.windowStartIso,
            windowEndIso: endIso ?? st.windowEndIso,
          };
        });
        break;
      }
      case "orderbook.update": {
        const prices = env.data.prices;
        const incomingUp = env.data.series_up ?? null;
        const incomingDown = env.data.series_down ?? null;
        const incomingUpBid = env.data.series_up_bid ?? null;
        const incomingUpAsk = env.data.series_up_ask ?? null;
        const incomingDownBid = env.data.series_down_bid ?? null;
        const incomingDownAsk = env.data.series_down_ask ?? null;
        set((st) => ({
          terminal:
            st.terminal && prices
              ? { ...st.terminal, polymarket: prices }
              : st.terminal,
          // Both the direct CLOB poller and the CSV fallback can publish this
          // topic. Merge by timestamp so a shorter/staler update never makes
          // the chart "snap back" to a smoother-looking sparse series.
          seriesUp: mergePriceSeries(st.seriesUp, incomingUp),
          seriesDown: mergePriceSeries(st.seriesDown, incomingDown),
          seriesUpBid: mergePriceSeries(st.seriesUpBid, incomingUpBid),
          seriesUpAsk: mergePriceSeries(st.seriesUpAsk, incomingUpAsk),
          seriesDownBid: mergePriceSeries(st.seriesDownBid, incomingDownBid),
          seriesDownAsk: mergePriceSeries(st.seriesDownAsk, incomingDownAsk),
        }));
        break;
      }
      case "edge.update":
        set({ edgeUp: env.data.edge_up, edgeDown: env.data.edge_down });
        break;
      case "model.update": {
        if (get().mode === "live") break;
        const incomingUp = env.data.series_up ?? null;
        const incomingDown = env.data.series_down ?? null;
        set((st) => ({
          modelUp: mergePriceSeries(st.modelUp, incomingUp),
          modelDown: mergePriceSeries(st.modelDown, incomingDown),
        }));
        break;
      }
      case "instance.update":
        if (env.data.source != null && env.data.source !== get().mode) break;
        if (env.data.instance != null && env.data.instance.instance_id !== get().selectedInstanceId) break;
        set({
          instance: env.data.instance,
          position: env.data.position,
          equity: env.data.equity,
          equitySeries: env.data.equity_series ?? get().equitySeries,
          todaySummary: env.data.today_summary ?? get().todaySummary,
        });
        break;
      case "window.tick": {
        const win = env.data;
        const bounds = windowBoundsFromSlug(win.slug);
        set((st) => {
          const patch = ensureSlug(st, win.slug ?? null);
          return {
            ...patch,
            window: computeWindowStateFromBounds(
              win,
              bounds?.startIso ?? st.windowStartIso,
              bounds?.endIso ?? st.windowEndIso,
            ),
            windowStartIso: bounds?.startIso ?? st.windowStartIso,
            windowEndIso: bounds?.endIso ?? st.windowEndIso,
          };
        });
        break;
      }
      case "liveness.update":
      case "liveness.tick":
        set((st) => ({ liveness: mergeLivenessInfo(st.liveness, env.data) }));
        break;
      case "leaderboard.update":
        set({ leaderboard: env.data.top });
        break;
      case "calibration.start":
      case "calibration.end":
        set((st) => ({
          calibration: {
            ...env.data,
            last_timing: mergeTimingInfo(
              st.calibration.last_timing,
              env.data.last_timing,
            ),
          },
        }));
        break;
      case "trade.append":
        handleTrade(env.data, get, set);
        break;
      case "trades.reset":
        // Backend detected an in-place rewrite of the trades CSV — e.g.
        // the live trader voided a phantom TP_FILLED row and rewrote it
        // as VOIDED_PHANTOM_TP at the same byte offset. Clear cached
        // events and re-apply the freshly-parsed list so the trade feed
        // reflects the corrected state without F5.
        handleTradesReset(env.data, get, set);
        break;
      case "polymarket_status.update":
        set({ polymarketStatus: env.data ?? initialPolymarketStatus });
        break;
      default:
        break;
    }
  },

  tickWindow: (nowMs = Date.now()) =>
    set((st) => ({
      window: computeWindowStateFromBounds(
        st.window,
        st.windowStartIso,
        st.windowEndIso,
        nowMs,
      ),
    })),

  consumeFlash: (id) =>
    set((st) => ({ flashQueue: st.flashQueue.filter((f) => f.id !== id) })),
}));

// PriceChart filters by window bounds at render time and backend collectors
// reset their own deques on slug boundaries, so we don't clear stored series
// here. Kept as a hook point for future per-slug logic.
function ensureSlug(
  st: DashState,
  newSlug: string | null,
): Partial<DashState> {
  if (newSlug && st.terminal?.market?.slug && newSlug !== st.terminal.market.slug) {
    return { modelUp: [], modelDown: [] };
  }
  return {};
}

function handleTradesReset(
  payload: TradesResetData | null | undefined,
  get: () => DashState,
  set: (partial: Partial<DashState>) => void,
) {
  const all = Array.isArray(payload?.events) ? payload!.events : [];
  const st = get();
  const selected = st.selectedInstanceId;
  if (payload?.source != null && payload.source !== st.mode) return;
  const ordered = all.filter(
    (e) => e.instance_id === selected && (e.run_source ?? st.mode) === st.mode,
  ).sort((a, b) => Date.parse(a.timestamp) - Date.parse(b.timestamp));

  // Recent feed rows cannot reconstruct a full history or the current open
  // position. The backend supplies both in the same correction envelope.
  const snapshot = payload?.snapshot;
  let markers: typeof st.markers = [];
  const activeSlug = st.terminal?.market?.slug ?? null;
  for (const ev of ordered) {
    markers = nextMarkers(markers, ev, activeSlug);
  }
  const trades = ordered.slice().reverse().slice(0, 200);
  set({
    trades,
    markers,
    flashQueue: [],
    ...(snapshot ? {
      instance: snapshot.instance,
      position: snapshot.position,
      equity: snapshot.equity,
      equitySeries: snapshot.equity_series ?? st.equitySeries,
      todaySummary: snapshot.today_summary ?? st.todaySummary,
    } : {}),
  });
}

function handleTrade(
  ev: TradeEvent,
  get: () => DashState,
  set: (partial: Partial<DashState>) => void,
) {
  if (ev.instance_id !== get().selectedInstanceId) return;
  const st = get();
  if ((ev.run_source ?? st.mode) !== st.mode) return;
  const key = tradeEventKey(ev);
  if (st.trades.some((trade) => tradeEventKey(trade) === key)) return;
  const ctx = {
    position: st.position,
    terminal: st.terminal,
    window: st.window,
    windowStartIso: st.windowStartIso,
    windowEndIso: st.windowEndIso,
    modelUp: st.modelUp,
    modelDown: st.modelDown,
    equity: st.equity,
    equitySeries: st.equitySeries,
    mode: st.mode,
    markers: st.markers,
    flashQueue: st.flashQueue,
    startingCapital:
      st.instance?.starting_capital ?? st.sharedConfig.starting_capital ?? 1000,
    tpPct: st.instance?.params?.tp_pct ?? null,
    slPct: st.instance?.params?.sl_pct ?? null,
  };

  const marketContext = nextMarketContext(ctx, ev);
  const chartContext = nextEntryChartContext(ctx, ev);
  const liveEquity = nextLiveEquity(ctx, ev);
  const activeSlug =
    marketContext.terminal?.market?.slug ?? st.terminal?.market?.slug ?? null;

  set({
    trades: [ev, ...st.trades].slice(0, 200),
    flashQueue: nextFlashQueue(st.flashQueue, ev),
    markers: nextMarkers(st.markers, ev, activeSlug),
    position: nextPositionState(ctx, ev),
    ...marketContext,
    ...(chartContext ?? {}),
    ...(liveEquity ?? {}),
  });
}
