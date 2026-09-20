import { useDash } from "../lib/store";

export default function MarketInfoCard() {
  const terminal = useDash((s) => s.terminal);
  const spot = useDash((s) => s.spot);
  const window = useDash((s) => s.window);
  const windowStartIso = useDash((s) => s.windowStartIso);
  const m = terminal?.market;
  const poly = terminal?.polymarket;
  const liveTimeLeft =
    window != null ? Math.max(0, window.total_s - window.elapsed_s) : null;

  // Suppress the PTB display when terminal_data.json is still showing
  // the *previous* market's barrier — this happens for the first ~30 s
  // of a new 15-min window because calc waits ``freshness_minimum`` seconds
  // before fetching the new Polymarket openPrice. During that gap,
  // terminal_data.json carries the prior market_id and showing its
  // barrier is misleading. Detect by comparing terminal's slug-derived
  // window start to the active window from store.windowStartIso.
  const activeWindowStartUnix = windowStartIso
    ? Math.floor(new Date(windowStartIso).getTime() / 1000)
    : null;
  const terminalIsStale =
    activeWindowStartUnix != null &&
    m?.window_start_unix != null &&
    m.window_start_unix !== activeWindowStartUnix;
  const displayBarrier = terminalIsStale ? null : m?.barrier;
  const displaySpot = spot?.price ?? m?.spot_price ?? null;
  const spotSourceLabel =
    spot?.source_label ?? m?.spot_source_label ?? sourceLabel(m?.spot_source);
  const spotAge = spot?.age_seconds ?? m?.spot_age_seconds ?? null;
  const spotIsStale = spotAge != null && spotAge > 5;

  const barrierDelta =
    displaySpot != null && displayBarrier != null
      ? displaySpot - displayBarrier
      : null;
  const barrierPct =
    barrierDelta != null && displayBarrier
      ? (barrierDelta / displayBarrier) * 100
      : null;

  return (
    <div className="card p-4 h-full flex flex-col">
      <div className="flex items-baseline justify-between mb-1">
        <h2 className="card-header">Current market</h2>
        {m?.direction && (
          <span
            className={`chip font-mono ${
              m.direction.toLowerCase() === "up" ? "chip-up" : "chip-down"
            }`}
          >
            {m.direction.toUpperCase()}
          </span>
        )}
      </div>

      <div className="text-[13px] text-slate-200 font-mono truncate mb-3 tracking-tight">
        {m?.slug ?? "—"}
      </div>

      <div className="grid grid-cols-2 gap-x-4 gap-y-2 flex-1 min-h-0">
        <Kv
          label="Spot"
          sublabel={spotIsStale ? `${spotSourceLabel ?? "Spot"} · stale` : spotSourceLabel}
          tooltip="Shared BTC/USD spot propagated through Redis. Primary source is Polymarket RTDS Chainlink; fallback labels appear here only when the shared spot daemon has switched source."
        >
          <span className="font-mono text-slate-100 text-sm tabular-nums inline-block min-w-[11ch]">
            {displaySpot != null
              ? `$${displaySpot.toLocaleString(undefined, {
                  minimumFractionDigits: 2,
                  maximumFractionDigits: 2,
                })}`
              : "—"}
          </span>
          {spotAge != null && (
            <span
              className={`text-[10px] font-mono ${
                spotIsStale ? "text-amber-300" : "text-slate-400"
              }`}
            >
              {spotAge.toFixed(1)}s
            </span>
          )}
        </Kv>
        <Kv
          label="Price to Beat"
          sublabel={
            terminalIsStale
              ? "awaiting Polymarket"
              : "fiveminute openPrice @ window-start"
          }
          tooltip="Resolution-source strike: Polymarket's /api/crypto/crypto-price openPrice with variant=fiveminute and eventStartTime set to the 15-minute window start. Same value Polymarket shows users as 'Price to Beat' on the market page. UP wins iff the close >= this. Calc accepts a new PTB only after 30 s and before 5 min, so the cell shows '—' during the brief wait rather than the previous market's value."
        >
          <span className="font-mono text-slate-100 text-sm tabular-nums inline-block min-w-[11ch]">
            {displayBarrier != null
              ? `$${displayBarrier.toLocaleString(undefined, {
                  minimumFractionDigits: 2,
                  maximumFractionDigits: 2,
                })}`
              : "—"}
          </span>
          {barrierDelta != null && (
            <span
              className={`ml-1 text-[10px] font-mono ${
                barrierDelta >= 0 ? "text-emerald-300" : "text-rose-300"
              }`}
            >
              {(barrierDelta >= 0 ? "+" : "−") +
                "$" +
                Math.abs(barrierDelta).toFixed(2)}
              {barrierPct != null && (
                <span className="opacity-70">
                  {" "}
                  {(barrierPct >= 0 ? "+" : "−") +
                    Math.abs(barrierPct).toFixed(3) +
                    "%"}
                </span>
              )}
            </span>
          )}
        </Kv>
        <Kv label="Poly bid / ask">
          <span className="font-mono text-slate-100 text-sm tabular-nums inline-block min-w-[13ch]">
            {poly?.best_bid != null ? poly.best_bid.toFixed(3) : "—"}
            <span className="text-slate-500"> / </span>
            {poly?.best_ask != null ? poly.best_ask.toFixed(3) : "—"}
          </span>
        </Kv>
        <Kv label="Time left">
          <span className="font-mono text-slate-100 text-sm tabular-nums inline-block min-w-[4ch]">
            {liveTimeLeft != null
              ? formatSeconds(liveTimeLeft)
              : m?.ttm_seconds != null
                ? formatSeconds(m.ttm_seconds)
                : "—"}
          </span>
        </Kv>
      </div>
    </div>
  );
}

function Kv({
  label,
  sublabel,
  tooltip,
  children,
}: {
  label: string;
  sublabel?: string;
  tooltip?: string;
  children: React.ReactNode;
}) {
  return (
    <div className="min-w-0" title={tooltip}>
      <div className="stat-label mb-0.5">
        {label}
        {sublabel && (
          <span className="ml-1 text-[9px] opacity-60 normal-case font-normal tracking-normal">
            {sublabel}
          </span>
        )}
      </div>
      <div className="flex items-baseline gap-1 truncate">{children}</div>
    </div>
  );
}

function formatSeconds(s: number): string {
  const m = Math.max(0, Math.floor(s / 60));
  const r = Math.max(0, Math.floor(s % 60));
  return `${m}:${r.toString().padStart(2, "0")}`;
}

function sourceLabel(source?: string | null): string {
  if (!source) return "Shared spot";
  if (source === "polymarket_rtds") return "Polymarket RTDS Chainlink";
  if (source.startsWith("ws_median")) return "Multi-exchange websocket fallback";
  if (source === "binance_rest_last_resort") return "REST fallback";
  return source;
}
