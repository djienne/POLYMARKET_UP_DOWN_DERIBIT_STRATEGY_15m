import { useDash } from "../lib/store";
import { fmtRatio } from "../lib/format";
import type { EdgeRatio, InstanceParams, WindowZone } from "../lib/types";
import {
  deriveEntryCapStatus,
  entryCapTooltip,
  type EntryCapStatus,
} from "../lib/entry-cap";

export default function EdgeRatioPanel() {
  const inst = useDash((s) => s.instance);
  const shared = useDash((s) => s.sharedConfig);
  const edgeUp = useDash((s) => s.edgeUp);
  const edgeDown = useDash((s) => s.edgeDown);
  const window = useDash((s) => s.window);
  const polymarketDown = useDash((s) => !s.polymarketStatus.is_operational);
  const polymarketWarning = useDash(
    (s) =>
      s.polymarketStatus.is_operational &&
      s.polymarketStatus.effective_status === "WARNING",
  );
  const polymarketReason = useDash((s) => s.polymarketStatus.status_reason);
  const zone: WindowZone = window?.zone ?? "unknown";
  const blocked = zone === "blocked_first" || zone === "blocked_last";
  const upEntryCap = deriveEntryCapStatus(
    "UP",
    edgeUp,
    shared.max_entry_price,
    zone,
  );
  const downEntryCap = deriveEntryCapStatus(
    "DOWN",
    edgeDown,
    shared.max_entry_price,
    zone,
  );
  const capBlocked = upEntryCap.blocked || downEntryCap.blocked;
  const params =
    inst?.params ??
    (shared.alpha_up != null &&
    shared.alpha_down != null &&
    shared.floor_up != null &&
    shared.floor_down != null &&
    shared.tp_pct != null
      ? {
          alpha_up: shared.alpha_up,
          alpha_down: shared.alpha_down,
          floor_up: shared.floor_up,
          floor_down: shared.floor_down,
          tp_pct: shared.tp_pct,
          sl_pct: shared.sl_pct ?? 0,
        }
      : null);

  return (
    <div className="card px-3 pt-2 pb-1.5 self-start flex flex-col justify-start overflow-hidden relative">
      <div className="flex items-baseline justify-between mb-1">
        <h2 className="card-header">Edge Ratio</h2>
        {polymarketDown ? (
          <span className="chip chip-warn text-[8px] px-1.5 py-0 font-mono leading-tight">
            unavailable
          </span>
        ) : polymarketWarning ? (
          <span
            className="chip chip-warn text-[8px] px-1.5 py-0 font-mono leading-tight"
            title={polymarketReason ?? undefined}
          >
            warning
          </span>
        ) : blocked ? (
          <span className="chip chip-warn text-[8px] px-1.5 py-0 font-mono leading-tight">
            blocked
          </span>
        ) : capBlocked ? (
          <span className="chip chip-warn text-[8px] px-1.5 py-0 font-mono leading-tight">
            price cap
          </span>
        ) : null}
      </div>
      <div
        className={
          polymarketDown
            ? "grayscale opacity-40 transition-opacity pointer-events-none"
            : ""
        }
      >
        <div className="grid grid-cols-1 gap-1 content-start auto-rows-min">
          <EdgeRow
            edge={edgeUp}
            blocked={blocked}
            floor={params?.floor_up ?? null}
            entryCap={upEntryCap}
          />
          <EdgeRow
            edge={edgeDown}
            blocked={blocked}
            floor={params?.floor_down ?? null}
            entryCap={downEntryCap}
          />
        </div>
        {params && <StrategyParams params={params} />}
      </div>
    </div>
  );
}

function StrategyParams({ params }: { params: InstanceParams }) {
  const tp = `${(params.tp_pct * 100).toFixed(0)}%`;
  const sl = params.sl_pct > 0 ? `${(params.sl_pct * 100).toFixed(0)}%` : "off";
  const pairs: { k: string; v: string; title: string }[] = [
    { k: "α", v: `${params.alpha_up}/${params.alpha_down}`, title: "alpha up / down" },
    {
      k: "floor",
      v: `${params.floor_up.toFixed(2)}/${params.floor_down.toFixed(2)}`,
      title: "floor up / down",
    },
    { k: "TP", v: tp, title: "take profit" },
    { k: "SL", v: sl, title: "stop loss" },
  ];
  return (
    <div className="mt-2 pt-1.5 border-t border-ink-800/70 flex flex-wrap gap-x-3 gap-y-0.5 text-[11px] font-mono leading-tight">
      {pairs.map((p) => (
        <span key={p.k} title={p.title} className="text-cyan-200/80">
          {p.k}{" "}
          <span className="text-slate-100">{p.v}</span>
        </span>
      ))}
    </div>
  );
}

function EdgeRow({
  edge,
  blocked,
  floor,
  entryCap,
}: {
  edge: EdgeRatio | null;
  blocked: boolean;
  floor: number | null;
  entryCap: EntryCapStatus;
}) {
  if (!edge) {
    return (
      <div className="border border-ink-800 rounded-lg px-2 py-1.5 text-slate-500 text-xs">
        Waiting for probabilities...
      </div>
    );
  }

  const hasEdge = edge.has_edge === true;
  const blockedByEntryCap = entryCap.blocked;
  const isUp = edge.side === "UP";
  const sideColor = isUp ? "text-emerald-300" : "text-rose-300";

  // Trader skip-reason gates, in the order btc_pricer/edge.py:has_edge applies
  // them:
  //   1. market_prob < floor      → side is too unlikely per market consensus
  //   2. model_prob <= market     → no informational advantage
  //   3. model_prob < required    → close miss on the edge curve
  const marketBelowFloor =
    !hasEdge &&
    floor != null &&
    edge.market_prob != null &&
    edge.market_prob < floor;
  const modelBelowMarket =
    !hasEdge &&
    !marketBelowFloor &&
    edge.model_prob != null &&
    edge.market_prob != null &&
    edge.model_prob <= edge.market_prob;

  const max = Math.max(edge.current_ratio ?? 0, edge.required_ratio ?? 0, 1.5);
  const curW =
    edge.current_ratio != null ? (edge.current_ratio / max) * 100 : 0;
  const reqW =
    edge.required_ratio != null ? (edge.required_ratio / max) * 100 : 0;

  const fmtPctSmall = (p: number | null | undefined) =>
    p == null || Number.isNaN(p) ? "--" : `${(p * 100).toFixed(1)}%`;

  let chipClass: string;
  let chipText: string;
  let chipTitle: string | undefined;
  if (blocked) {
    chipClass = "chip-warn";
    chipText = hasEdge ? "edge+hold" : "hold";
  } else if (edge.has_edge == null) {
    chipClass = "chip-mute";
    chipText = "--";
  } else if (blockedByEntryCap) {
    chipClass = "chip-warn";
    chipText = "price > cap";
    chipTitle = entryCapTooltip(entryCap);
  } else if (marketBelowFloor) {
    chipClass = "chip-warn";
    chipText = "market < floor";
    chipTitle = `market ${fmtPctSmall(edge.market_prob)} < floor ${fmtPctSmall(floor)} — trader refuses this side regardless of model edge`;
  } else if (modelBelowMarket) {
    chipClass = "chip-down";
    chipText = "no model edge";
    chipTitle = `model ${fmtPctSmall(edge.model_prob)} ≤ market ${fmtPctSmall(edge.market_prob)} — no informational advantage`;
  } else if (hasEdge) {
    chipClass = "chip-up";
    chipText = "edge";
  } else {
    chipClass = "chip-down";
    chipText = "below req";
    chipTitle = `model ${fmtPctSmall(edge.model_prob)} < required ${fmtPctSmall(edge.required_prob)} — close, but not enough edge`;
  }

  const barColor = blocked
    ? "bg-amber-500/60"
    : blockedByEntryCap
      ? "bg-amber-500/70"
    : marketBelowFloor
      ? "bg-amber-500/35"
    : hasEdge
      ? isUp
        ? "bg-emerald-500/70"
        : "bg-rose-500/65"
      : "bg-slate-500/40";

  // Loudness ranks by actionability: edge active > blocked > everything else.
  // Skip-reason states are conveyed by the chip + softened bar only — no
  // container tint or glow, since they're steady-state and informational.
  const containerClass = hasEdge
    ? blockedByEntryCap
      ? "border-amber-400/40 bg-amber-500/10 shadow-[0_0_18px_-8px_rgba(245,158,11,0.55)]"
      : isUp
      ? "border-emerald-400/40 bg-emerald-500/10 shadow-[0_0_18px_-8px_rgba(52,211,153,0.55)]"
      : "border-rose-400/40 bg-rose-500/10 shadow-[0_0_18px_-8px_rgba(251,113,133,0.55)]"
    : blocked
      ? "border-amber-500/30 bg-amber-500/[0.06]"
      : "border-ink-800 bg-ink-900/40";

  const ratioColor = hasEdge
    ? blockedByEntryCap
      ? "text-amber-100"
      : isUp
      ? "text-emerald-200"
      : "text-rose-200"
    : "text-slate-100";
  const marginColor =
    blockedByEntryCap
      ? "text-amber-200 bg-amber-500/10 border-amber-500/30"
      : edge.margin == null
      ? "text-slate-400 bg-slate-500/10 border-slate-500/30"
      : edge.margin >= 0
        ? "text-emerald-200 bg-emerald-500/10 border-emerald-500/30"
        : "text-rose-200 bg-rose-500/10 border-rose-500/30";
  const marginText =
    edge.margin != null
      ? `${edge.margin >= 0 ? "+" : "−"}${Math.abs(edge.margin).toFixed(3)}`
      : "—";

  return (
    <div className={`rounded-lg border px-2 py-1.5 transition-colors ${containerClass}`}>
      {/* header: side label + status chip */}
      <div className="flex items-center justify-between mb-1">
        <span className={`font-semibold tracking-wide ${sideColor}`}>
          {edge.side}
        </span>
        <span
          title={chipTitle}
          className={`chip ${chipClass} text-[9px] px-1.5 py-0 font-mono leading-tight`}
        >
          {chipText}
        </span>
      </div>

      {/* hero ratio: cur → req, with margin pill on the right */}
      <div className="flex items-baseline justify-between gap-2 mb-1.5">
        <div className="flex items-baseline gap-1.5 min-w-0">
          <span className={`font-mono font-semibold text-base leading-none ${ratioColor}`}>
            {fmtRatio(edge.current_ratio)}
          </span>
          <span className="text-xs text-slate-500 font-mono leading-none">→</span>
          <span className="text-xs font-mono leading-none truncate">
            <span className="text-slate-300">required</span>{" "}
            <span className="text-slate-200 font-medium">{fmtRatio(edge.required_ratio)}</span>
          </span>
        </div>
        <span
          title="margin = current_ratio − required_ratio"
          className={`shrink-0 inline-flex items-center rounded-full border px-1.5 text-[11px] font-mono leading-tight ${marginColor}`}
        >
          {marginText}
        </span>
      </div>

      {/* bar with required-ratio target marker */}
      <div className="relative h-2.5 bg-ink-800 rounded">
        <div
          className={`absolute top-0 left-0 h-full rounded ${barColor} transition-[width] duration-500`}
          style={{ width: `${Math.min(100, curW)}%` }}
        />
        <div
          className="absolute -top-0.5 bottom-[-2px] w-[3px] bg-amber-300/90 rounded-sm"
          style={{ left: `calc(${Math.min(100, reqW)}% - 1.5px)` }}
          title={`required ${fmtRatio(edge.required_ratio)}`}
        />
      </div>

      {/* model vs market probabilities (single, legible line) */}
      <div className="mt-1 text-[10px] text-slate-400 font-mono leading-tight truncate">
        model{" "}
        <span className="text-slate-200">{fmtPctSmall(edge.model_prob)}</span>
        <span className="text-slate-600 mx-1.5">·</span>
        market{" "}
        <span className="text-slate-200">{fmtPctSmall(edge.market_prob)}</span>
      </div>
    </div>
  );
}
