import type {
  ChartMarker,
  PositionState,
  PricePoint,
  TerminalSnapshot,
  TradeEvent,
  WindowState,
} from "./types";

export interface FlashEvent {
  id: string;
  kind: "entry" | "win" | "loss";
  direction?: "UP" | "DOWN";
  amount?: number | null;
  at: number;
}

export const CLOSE_EVENTS: ReadonlySet<string> = new Set([
  "TP_FILLED",
  "TP_SIGNAL_MARKET_EXIT",
  "WIN_EXPIRY",
  "LOSS_EXPIRY",
  "STOP_LOSS",
  "UNRESOLVED_RESTART",
]);

export interface TradeDeriveContext {
  position: PositionState;
  terminal: TerminalSnapshot | null;
  window: WindowState | null;
  windowStartIso: string | null;
  windowEndIso: string | null;
  modelUp: PricePoint[];
  modelDown: PricePoint[];
  equity: number[];
  equitySeries: PricePoint[];
  markers: ChartMarker[];
  flashQueue: FlashEvent[];
  startingCapital: number;
  tpPct: number | null;
  slPct: number | null;
}

export function classifyEvent(
  eventName: string,
): "entry" | "win" | "loss" | null {
  if (eventName === "ENTRY") return "entry";
  if (eventName === "TP_SIGNAL_MARKET_EXIT") return null;
  if (eventName === "TP_FILLED" || eventName === "WIN_EXPIRY") return "win";
  if (eventName === "STOP_LOSS" || eventName === "LOSS_EXPIRY") return "loss";
  return null;
}

function markerKindFor(eventName: string): ChartMarker["kind"] | null {
  if (eventName === "ENTRY") return "ENTRY";
  if (eventName === "TP_SIGNAL_MARKET_EXIT") return null;
  if (eventName === "TP_FILLED" || eventName === "WIN_EXPIRY") return "WIN";
  if (eventName === "STOP_LOSS" || eventName === "LOSS_EXPIRY") return "LOSS";
  return null;
}

export function nextFlashQueue(
  prev: FlashEvent[],
  ev: TradeEvent,
): FlashEvent[] {
  const kind = classifyEvent(ev.event);
  if (!kind) return prev;
  const baseQueue =
    kind === "win" || kind === "loss"
      ? prev.filter((f) => f.kind !== "entry")
      : prev;
  return [
    ...baseQueue,
    {
      id: `${ev.timestamp}-${ev.event}-${ev.instance_id}`,
      kind,
      direction: (ev.direction as "UP" | "DOWN" | undefined) ?? undefined,
      amount: ev.pnl ?? null,
      at: Date.now(),
    },
  ];
}

export function nextMarkers(
  prev: ChartMarker[],
  ev: TradeEvent,
  activeSlug: string | null | undefined,
): ChartMarker[] {
  const kind = markerKindFor(ev.event);
  if (!kind) return prev;
  if (activeSlug && ev.market_id && ev.market_id !== activeSlug) return prev;
  const price = kind === "ENTRY" ? ev.entry_price ?? null : ev.exit_price ?? null;
  return [
    ...prev,
    {
      t: ev.timestamp,
      kind,
      side: (ev.direction as "UP" | "DOWN" | null | undefined) ?? null,
      price,
      pnl: ev.pnl ?? null,
    },
  ].slice(-60);
}

export function nextLiveEquity(
  ctx: Pick<TradeDeriveContext, "equity" | "equitySeries" | "startingCapital"> & {
    mode?: string | null;
  },
  ev: TradeEvent,
): { equity: number[]; equitySeries: PricePoint[] } | null {
  if (!CLOSE_EVENTS.has(ev.event)) return null;
  // Live equity is an account observation, delivered by the backend's equity
  // reader. Trade P&L cannot reconstruct fees, cash flows or reconciliation.
  if (ctx.mode === "live") return null;
  const lastEquity =
    ctx.equity.length > 0 ? ctx.equity[ctx.equity.length - 1] : ctx.startingCapital;
  const nextEquity =
    ctx.mode === "dry_run"
      ? ev.pnl != null
        ? lastEquity + ev.pnl
        : null
      : ev.capital ?? (ev.pnl != null ? lastEquity + ev.pnl : null);
  if (nextEquity == null) return null;
  const equity = [...ctx.equity, nextEquity];
  const equitySeries =
    ctx.equitySeries.length === 0
      ? [
          { t: ev.timestamp, v: ctx.startingCapital },
          { t: ev.timestamp, v: nextEquity },
        ]
      : [...ctx.equitySeries, { t: ev.timestamp, v: nextEquity }];
  return { equity, equitySeries };
}

export function nextPositionState(
  ctx: Pick<TradeDeriveContext, "position" | "tpPct" | "slPct">,
  ev: TradeEvent,
): PositionState {
  if (
    ev.event === "ENTRY" &&
    ev.entry_price != null &&
    ev.shares != null &&
    (ev.direction === "UP" || ev.direction === "DOWN")
  ) {
    const tpTarget =
      ctx.tpPct != null ? clampProb(ev.entry_price * (1 + ctx.tpPct)) : null;
    const slTarget =
      ctx.slPct != null && ctx.slPct > 0
        ? clampProb(ev.entry_price * (1 - ctx.slPct))
        : null;
    return {
      ...ctx.position,
      open: {
        direction: ev.direction,
        entry_price: ev.entry_price,
        shares: ev.shares,
        tp_target: tpTarget,
        sl_target: slTarget,
        entered_at: ev.timestamp,
        market_id: ev.market_id ?? null,
        notional: ev.entry_price * ev.shares,
      },
    };
  }
  if (CLOSE_EVENTS.has(ev.event)) {
    return { ...ctx.position, open: null, last_exit_at: ev.timestamp };
  }
  return ctx.position;
}

export function nextMarketContext(
  ctx: Pick<TradeDeriveContext, "terminal" | "window" | "windowStartIso" | "windowEndIso">,
  ev: TradeEvent,
): {
  terminal?: TerminalSnapshot | null;
  window?: WindowState | null;
  windowStartIso?: string | null;
  windowEndIso?: string | null;
} {
  if (ev.event !== "ENTRY" || !ev.market_id) return {};
  const currentSlug = ctx.terminal?.market?.slug ?? ctx.window?.slug ?? null;
  if (currentSlug === ev.market_id) return {};
  const bounds = windowBoundsFromSlug(ev.market_id);
  return {
    terminal: ctx.terminal
      ? {
          ...ctx.terminal,
          market: { ...ctx.terminal.market, slug: ev.market_id },
        }
      : ctx.terminal,
    window: ctx.window ? { ...ctx.window, slug: ev.market_id } : ctx.window,
    windowStartIso: bounds?.startIso ?? ctx.windowStartIso,
    windowEndIso: bounds?.endIso ?? ctx.windowEndIso,
  };
}

export function nextEntryChartContext(
  ctx: Pick<TradeDeriveContext, "modelUp" | "modelDown">,
  ev: TradeEvent,
): { modelUp: PricePoint[]; modelDown: PricePoint[] } | null {
  if (
    ev.event !== "ENTRY" ||
    ev.timestamp == null ||
    ev.model_prob == null ||
    (ev.direction !== "UP" && ev.direction !== "DOWN")
  ) {
    return null;
  }
  const modelProb = clampProb(ev.model_prob);
  if (ev.direction === "UP") {
    return {
      modelUp: appendPoint(ctx.modelUp, ev.timestamp, modelProb),
      modelDown: appendPoint(ctx.modelDown, ev.timestamp, clampProb(1 - modelProb)),
    };
  }
  return {
    modelUp: appendPoint(ctx.modelUp, ev.timestamp, clampProb(1 - modelProb)),
    modelDown: appendPoint(ctx.modelDown, ev.timestamp, modelProb),
  };
}

export function clampProb(v: number): number {
  return Math.max(0, Math.min(1, v));
}

export function appendPoint(
  series: PricePoint[],
  t: string,
  v: number,
): PricePoint[] {
  const next = [...series];
  const last = next[next.length - 1];
  if (last && last.t === t) {
    next[next.length - 1] = { t, v };
    return next;
  }
  next.push({ t, v });
  return next;
}

export function windowBoundsFromSlug(slug: string | null | undefined): {
  startIso: string;
  endIso: string;
} | null {
  if (!slug) return null;
  const m = /btc-updown-15m-(\d+)/.exec(slug);
  if (!m) return null;
  const start = Number(m[1]) * 1000;
  const end = start + 900_000;
  return {
    startIso: new Date(start).toISOString(),
    endIso: new Date(end).toISOString(),
  };
}

export function computeWindowStateFromBounds(
  window: WindowState | null,
  startIso: string | null | undefined,
  endIso: string | null | undefined,
  nowMs: number = Date.now(),
): WindowState | null {
  if (!window) return null;
  const startMs = startIso != null ? Date.parse(startIso) : NaN;
  const endMs = endIso != null ? Date.parse(endIso) : NaN;
  const totalFromBounds =
    Number.isFinite(startMs) && Number.isFinite(endMs) && endMs > startMs
      ? (endMs - startMs) / 1000
      : null;
  const total_s = totalFromBounds ?? window.total_s ?? 900;
  const no_trade_first_s = window.no_trade_first_s ?? 300;
  const no_trade_last_s = window.no_trade_last_s ?? 180;

  if (!Number.isFinite(startMs)) {
    return {
      ...window,
      total_s,
      no_trade_first_s,
      no_trade_last_s,
      zone: "unknown",
    };
  }

  const elapsed_s = Math.max(0, nowMs / 1000 - startMs / 1000);
  const zone =
    elapsed_s >= total_s
      ? "expired"
      : elapsed_s < no_trade_first_s
        ? "blocked_first"
        : elapsed_s >= total_s - no_trade_last_s
          ? "blocked_last"
          : "tradeable";

  return {
    ...window,
    elapsed_s,
    total_s,
    no_trade_first_s,
    no_trade_last_s,
    zone,
  };
}
