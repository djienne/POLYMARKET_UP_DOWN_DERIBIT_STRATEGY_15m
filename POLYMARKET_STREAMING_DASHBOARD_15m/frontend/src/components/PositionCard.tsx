import { useMemo } from "react";
import { AnimatePresence, motion } from "framer-motion";
import { useDash } from "../lib/store";
import {
  deriveEntryCapStatus,
  entryCapHint,
  type EntryCapStatus,
} from "../lib/entry-cap";
import { fmtLocalHMS, fmtMoney } from "../lib/format";
import type { TradeEvent } from "../lib/types";

// Real outcome events. UNRESOLVED_RESTART is intentionally excluded — it's a
// neutral close emitted when a grid restart crosses a market boundary; it's
// not a meaningful "last result" to surface in the empty-state.
const CLOSE_EVENTS = new Set([
  "TP_FILLED",
  "TP_SIGNAL_MARKET_EXIT",
  "WIN_EXPIRY",
  "LOSS_EXPIRY",
  "STOP_LOSS",
]);

export default function PositionCard() {
  const pos = useDash((s) => s.position);
  const mode = useDash((s) => s.mode);
  const terminal = useDash((s) => s.terminal);
  const trades = useDash((s) => s.trades);
  const window = useDash((s) => s.window);
  const edgeUp = useDash((s) => s.edgeUp);
  const edgeDown = useDash((s) => s.edgeDown);
  const maxEntryPrice = useDash((s) => s.sharedConfig.max_entry_price);
  const polymarketDown = useDash((s) => !s.polymarketStatus.is_operational);
  const polymarketWarning = useDash(
    (s) =>
      s.polymarketStatus.is_operational &&
      s.polymarketStatus.effective_status === "WARNING",
  );
  const polymarketReason = useDash((s) => s.polymarketStatus.status_reason);
  const flashes = useDash((s) => s.flashQueue);
  const hasFlash = flashes.length > 0;
  const windowZone = window?.zone ?? "unknown";
  const upEntryCap = deriveEntryCapStatus(
    "UP",
    edgeUp,
    maxEntryPrice,
    windowZone,
  );
  const downEntryCap = deriveEntryCapStatus(
    "DOWN",
    edgeDown,
    maxEntryPrice,
    windowZone,
  );

  const open = pos.open;
  const dirUp = open?.direction === "UP";
  const dirColor = dirUp ? "text-emerald-300" : "text-rose-300";
  const dirRing = dirUp ? "ring-emerald-400/40" : "ring-rose-400/40";

  let livePnL: number | null = null;
  if (open && terminal?.polymarket) {
    const market =
      open.direction === "UP"
        ? terminal.polymarket.prob_up
        : terminal.polymarket.prob_down;
    if (market != null && open.shares) {
      livePnL = (market - open.entry_price) * open.shares;
    }
  }

  return (
    <div
      className={`card p-3 h-full flex flex-col relative overflow-hidden transition-shadow ${
        open ? `ring-1 ${dirRing}` : ""
      }`}
    >
      <AnimatePresence>
        {hasFlash && open && (
          <motion.div
            key="ring"
            initial={{ opacity: 0.6, scale: 1 }}
            animate={{ opacity: 0, scale: 1.3 }}
            exit={{ opacity: 0 }}
            transition={{ duration: 1.2, ease: "easeOut" }}
            className={`absolute inset-2 rounded-xl ${
              dirUp ? "bg-emerald-500/10" : "bg-rose-500/10"
            } pointer-events-none`}
          />
        )}
      </AnimatePresence>

      <div className="flex items-baseline justify-between mb-2">
        <h2 className="card-header">Position</h2>
        {mode === "live" ? (
          <GracePill />
        ) : (
          <span className="chip chip-mute text-[10px]">grid - n/a grace</span>
        )}
      </div>

      {open ? (
        <div className="space-y-2">
          <div className="flex items-baseline gap-2 flex-wrap">
            <span className={`text-lg font-semibold ${dirColor}`}>
              {open.direction}
            </span>
            <span className="text-slate-500 text-[11px] font-mono">
              opened {fmtLocalHMS(open.entered_at)}
            </span>
            {open.entry_edge_ratio != null && (
              <span
                title="Edge ratio the trader captured at fill (model_prob / executable market price). Higher = better-quality entry."
                className={`chip font-mono text-[10px] ${
                  open.entry_edge_ratio >= 1.2
                    ? "bg-emerald-500/10 text-emerald-300 border-emerald-500/30"
                    : open.entry_edge_ratio >= 1.05
                      ? "bg-cyan-500/10 text-cyan-300 border-cyan-500/30"
                      : "bg-amber-500/10 text-amber-300 border-amber-500/30"
                }`}
              >
                <span className="text-slate-400/80">edge</span>
                <span className="tabular-nums">
                  {open.entry_edge_ratio.toFixed(2)}x
                </span>
              </span>
            )}
          </div>
          <div className="grid grid-cols-3 gap-x-3 gap-y-1.5">
            <Stat label="entry" value={open.entry_price.toFixed(4)} />
            <Stat label="shares" value={open.shares.toFixed(3)} />
            <Stat
              label="notional"
              value={
                open.notional != null ? `$${open.notional.toFixed(2)}` : "--"
              }
            />
            <Stat
              label="tp"
              value={open.tp_target != null ? open.tp_target.toFixed(4) : "--"}
            />
            <Stat
              label="sl"
              value={
                open.sl_target != null && open.sl_target > 0
                  ? open.sl_target.toFixed(4)
                  : "off"
              }
            />
            <Stat
              label="unrealized"
              value={fmtMoney(livePnL)}
              color={
                livePnL == null
                  ? "text-slate-400"
                  : livePnL >= 0
                    ? "text-emerald-300"
                    : "text-rose-300"
              }
            />
          </div>
        </div>
      ) : (
        <EmptyState
          mode={mode}
          trades={trades}
          windowZone={windowZone}
          windowElapsedS={window?.elapsed_s ?? 0}
          hasUpEdge={edgeUp?.has_edge === true}
          hasDownEdge={edgeDown?.has_edge === true}
          graceRemainingS={pos.grace_remaining_s ?? 0}
          polymarketDown={polymarketDown}
          polymarketWarning={polymarketWarning}
          polymarketReason={polymarketReason}
          entryCapStatuses={[upEntryCap, downEntryCap]}
        />
      )}
    </div>
  );
}

function Stat({
  label,
  value,
  color,
}: {
  label: string;
  value: string;
  color?: string;
}) {
  return (
    <div>
      <div className="stat-label">{label}</div>
      <div className={`font-mono text-sm ${color ?? "text-slate-100"}`}>
        {value}
      </div>
    </div>
  );
}

function GracePill() {
  const pos = useDash((s) => s.position);
  const rem = pos.grace_remaining_s;
  if (rem == null || rem <= 0) {
    return <span className="chip chip-ok text-[10px]">grace clear</span>;
  }
  return (
    <span className="chip chip-warn text-[10px] font-mono">
      grace {rem.toFixed(0)}s
    </span>
  );
}

type EmptyStateProps = {
  mode: string;
  trades: TradeEvent[];
  windowZone: string;
  windowElapsedS: number;
  hasUpEdge: boolean;
  hasDownEdge: boolean;
  graceRemainingS: number;
  polymarketDown: boolean;
  polymarketWarning: boolean;
  polymarketReason: string | null;
  entryCapStatuses: EntryCapStatus[];
};

function EmptyState({
  mode,
  trades,
  windowZone,
  windowElapsedS,
  hasUpEdge,
  hasDownEdge,
  graceRemainingS,
  polymarketDown,
  polymarketWarning,
  polymarketReason,
  entryCapStatuses,
}: EmptyStateProps) {
  // Most recent CLOSE event across the trade history (max-timestamp scan, robust
  // to whatever order trades happen to be in the store).
  const lastClose = useMemo<TradeEvent | null>(() => {
    let best: TradeEvent | null = null;
    for (const t of trades) {
      if (!CLOSE_EVENTS.has(t.event)) continue;
      if (best == null || (t.timestamp || "") > (best.timestamp || "")) {
        best = t;
      }
    }
    return best;
  }, [trades]);

  // Re-renders with the WindowTimer tick (windowElapsedS) so the "X ago" stays fresh.
  void windowElapsedS;
  const elapsedMs =
    lastClose != null ? Date.now() - new Date(lastClose.timestamp).getTime() : null;

  const tradeable = windowZone === "tradeable";
  const blocked = windowZone === "blocked_first" || windowZone === "blocked_last";
  const hasAnyEdge = hasUpEdge || hasDownEdge;
  const priceCapHint = entryCapHint(entryCapStatuses);

  let statusLabel: string;
  let statusClass: string;
  let hintText: string;
  if (polymarketDown) {
    statusLabel = "polymarket down";
    statusClass = "chip-warn";
    hintText = polymarketReason ?? "Polymarket CLOB unavailable";
  } else if (polymarketWarning) {
    statusLabel = "clob warning";
    statusClass = "chip-warn";
    hintText = polymarketReason ?? "official CLOB maintenance flag; books reachable";
  } else if (mode === "live" && graceRemainingS > 0) {
    statusLabel = `grace ${graceRemainingS.toFixed(0)}s`;
    statusClass = "chip-warn";
    hintText = "cooling down after the last fill";
  } else if (priceCapHint) {
    statusLabel = "price cap";
    statusClass = "chip-warn";
    hintText = priceCapHint;
  } else if (tradeable && hasAnyEdge) {
    statusLabel = "edge ready";
    statusClass = "chip-up";
    hintText = "next entry imminent";
  } else if (blocked && hasAnyEdge) {
    statusLabel = "blocked window";
    statusClass = "chip-warn";
    hintText = "edge present — waiting for entry window";
  } else if (blocked) {
    statusLabel = "blackout";
    statusClass = "chip-mute";
    hintText =
      windowZone === "blocked_first"
        ? "entry blackout (first 5 min)"
        : "TP/expiry only (last 3 min)";
  } else {
    statusLabel = "watching";
    statusClass = "chip-mute";
    hintText = "scanning for next valid edge";
  }

  return (
    <div className="flex-1 min-h-0 flex items-center">
      <div className="w-full rounded-xl border border-ink-800/80 bg-[linear-gradient(180deg,rgba(15,23,42,0.42),rgba(15,23,42,0.16))] px-3 py-3">
        <div className="flex items-center justify-between gap-2">
          <div className="text-sm font-semibold text-slate-200">
            No open position
          </div>
          <span
            className={`chip ${statusClass} text-[10px] font-mono`}
            title={hintText}
          >
            {statusLabel}
          </span>
        </div>

        {lastClose ? <LastCloseLine ev={lastClose} elapsedMs={elapsedMs ?? 0} /> : (
          <div className="mt-1.5 text-xs leading-relaxed text-slate-500">
            no closes yet — first opportunity pending
          </div>
        )}

        <div className="mt-0.5 text-xs leading-relaxed text-slate-500">
          {hintText}
        </div>
      </div>
    </div>
  );
}

function LastCloseLine({ ev, elapsedMs }: { ev: TradeEvent; elapsedMs: number }) {
  const isWin = ev.event === "TP_FILLED" || ev.event === "WIN_EXPIRY";
  const isLoss = ev.event === "STOP_LOSS" || ev.event === "LOSS_EXPIRY";
  const resultColor = isWin
    ? "text-emerald-300"
    : isLoss
      ? "text-rose-300"
      : "text-slate-300";
  const resultLabel = isWin ? "WIN" : isLoss ? "LOSS" : "RESTART";
  return (
    <div className="mt-1.5 text-xs leading-relaxed text-slate-400 font-mono truncate">
      <span className="text-slate-500">last close </span>
      <span className={resultColor}>
        {resultLabel} {fmtMoney(ev.pnl ?? 0)}
      </span>
      <span className="text-slate-500"> · {fmtElapsed(elapsedMs)}</span>
    </div>
  );
}

function fmtElapsed(ms: number): string {
  if (ms < 0 || !Number.isFinite(ms)) return "--";
  const s = Math.floor(ms / 1000);
  if (s < 60) return `${s}s ago`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m ${(s % 60).toString().padStart(2, "0")}s ago`;
  const h = Math.floor(m / 60);
  if (h < 24) return `${h}h ${(m % 60).toString().padStart(2, "0")}m ago`;
  const d = Math.floor(h / 24);
  return `${d}d ${(h % 24).toString().padStart(2, "0")}h ago`;
}
