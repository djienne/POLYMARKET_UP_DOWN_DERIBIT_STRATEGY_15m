import { useMemo } from "react";
import { useDash } from "../lib/store";
import { fmtProb } from "../lib/format";
import { fmtCapPrice } from "../lib/entry-cap";

type Row = {
  label: string;
  above: number | null;
  below: number | null;
  preferred?: boolean;
  available: boolean;
};

export default function ProbabilityPanel() {
  const terminal = useDash((s) => s.terminal);
  const calibration = useDash((s) => s.calibration);
  const maxEntryPrice = useDash((s) => s.sharedConfig.max_entry_price);

  const probs = terminal?.probabilities;
  const poly = terminal?.polymarket;
  const pref = probs?.preferred_model ?? "";
  const showCalibrationOverlay =
    calibration.active && (calibration.elapsed_s ?? 0) >= 5;

  const rows = useMemo<Row[]>(() => {
    const selectedUsed =
      probs?.selected_above != null ||
      probs?.selected_below != null ||
      probs?.avg_above != null ||
      probs?.avg_below != null;
    const selectedLabel =
      probs?.probability_source_label != null
        ? `${probs.probability_source_label} used by trader`
        : "Selected probability used by trader";
    const ssviUsed = !selectedUsed && pref === "ssvi_surface";
    const hestonUsed = !selectedUsed && pref === "heston";
    return [
      {
        label: "SSVI Surface B-L",
        above: probs?.ssvi_surface_above ?? null,
        below: probs?.ssvi_surface_below ?? null,
        preferred: ssviUsed,
        available:
          probs?.ssvi_surface_above != null || probs?.ssvi_surface_below != null,
      },
      {
        label: "SSVI Surface MC",
        above: probs?.mc_above ?? null,
        below: probs?.mc_below ?? null,
        available: probs?.mc_above != null || probs?.mc_below != null,
      },
      {
        label: "Heston (skipped by default)",
        above: probs?.heston_above ?? null,
        below: probs?.heston_below ?? null,
        preferred: hestonUsed,
        available: probs?.heston_above != null || probs?.heston_below != null,
      },
      {
        label: selectedLabel,
        above: probs?.selected_above ?? probs?.avg_above ?? null,
        below: probs?.selected_below ?? probs?.avg_below ?? null,
        preferred: selectedUsed,
        available: selectedUsed,
      },
    ];
  }, [probs, pref]);

  return (
    <div className={`card p-4 relative overflow-hidden h-full flex flex-col ${showCalibrationOverlay ? "opacity-90" : ""}`}>
      {showCalibrationOverlay && (
        <div className="absolute inset-0 pointer-events-none shimmer-bar animate-shimmer" />
      )}
      <div className="flex items-baseline justify-between mb-2 gap-3">
        <div className="flex items-baseline gap-2 flex-wrap min-w-0">
          <h2 className="card-header">Model probabilities vs market</h2>
          <span className="text-[11px] leading-tight text-slate-400 inline-flex items-center gap-1.5">
            <span>model fit from</span>
            <img
              src="/deribit.svg"
              alt="Deribit"
              className="h-6 w-auto"
            />
            <span>BTC options</span>
          </span>
        </div>
        <div className="flex items-center gap-2 shrink-0">
          {probs?.bl_mc_divergence != null &&
            Math.abs(probs.bl_mc_divergence) > 0.1 && (
              <span className="chip chip-warn font-mono text-[10px] px-2 py-0">
                divergence {probs.bl_mc_divergence.toFixed(3)}
              </span>
            )}
          <img src="/polymarket.svg" alt="Polymarket" className="h-4 w-auto opacity-90" />
        </div>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-4 flex-1 min-h-0">
        <SideColumn
          side="UP"
          rows={rows.map((r) => ({ ...r, value: r.above }))}
          marketProb={poly?.prob_up ?? null}
          maxEntryPrice={maxEntryPrice}
        />
        <SideColumn
          side="DOWN"
          rows={rows.map((r) => ({ ...r, value: r.below }))}
          marketProb={poly?.prob_down ?? null}
          maxEntryPrice={maxEntryPrice}
        />
      </div>
    </div>
  );
}

function SideColumn({
  side,
  rows,
  marketProb,
  maxEntryPrice,
}: {
  side: "UP" | "DOWN";
  rows: (Row & { value: number | null })[];
  marketProb: number | null;
  maxEntryPrice: number | null;
}) {
  const sideColor =
    side === "UP" ? "text-emerald-300" : "text-rose-300";
  const sideBg =
    side === "UP" ? "bg-emerald-400/70" : "bg-rose-400/70";
  const overEntryCap =
    marketProb != null && maxEntryPrice != null && marketProb > maxEntryPrice;

  return (
    <div className="min-h-0">
      <div className="flex items-baseline justify-between mb-2">
        <div className="flex items-center gap-2">
          <span className={`font-semibold ${sideColor} tracking-wide`}>
            {side}
          </span>
          <span className="text-[11px] text-slate-500">
            Polymarket implied
          </span>
        </div>
        <div className="flex items-center gap-1.5">
          {overEntryCap && (
            <span
              className="chip chip-warn font-mono text-[9px] px-1.5 py-0"
              title={`${side} price ${fmtCapPrice(marketProb)} > max entry ${fmtCapPrice(maxEntryPrice)}`}
            >
              &gt; cap
            </span>
          )}
          <span className={`font-mono text-lg ${sideColor}`}>
            {fmtProb(marketProb)}
          </span>
        </div>
      </div>

      <div className="space-y-1.5">
        {rows.map((r) => (
          <ProbRow
            key={r.label}
            label={r.label}
            value={r.value}
            marketProb={marketProb}
            sideBg={sideBg}
            preferred={r.preferred}
            available={r.available}
          />
        ))}
      </div>
    </div>
  );
}

function ProbRow({
  label,
  value,
  marketProb,
  sideBg,
  preferred,
  available,
}: {
  label: string;
  value: number | null;
  marketProb: number | null;
  sideBg: string;
  preferred?: boolean;
  available: boolean;
}) {
  const w = value != null ? Math.max(0, Math.min(1, value)) * 100 : 0;
  const mw = marketProb != null ? Math.max(0, Math.min(1, marketProb)) * 100 : null;
  const containerClass = preferred
    ? "rounded-md border border-cyan-400/40 bg-cyan-500/10 px-2 py-1.5 shadow-[0_0_18px_-8px_rgba(34,211,238,0.55)]"
    : "";
  const labelClass = preferred
    ? "min-w-0 text-cyan-100 font-semibold uppercase tracking-wide"
    : "min-w-0 text-slate-400";
  const valueClass = preferred
    ? "font-mono text-cyan-100 text-base font-semibold shrink-0"
    : "font-mono text-slate-200 shrink-0";
  const barHeight = preferred ? "h-2" : "h-1.5";
  const barFill = preferred
    ? `${sideBg} brightness-125 saturate-150`
    : sideBg;
  return (
    <div className={containerClass}>
      <div className="flex items-center justify-between text-[11px] mb-0.5 leading-tight gap-2">
        <div className="flex items-center gap-2 min-w-0">
          <span className={labelClass}>{label}</span>
          {!available && (
            <span className="text-slate-600 text-[10px]">n/a</span>
          )}
        </div>
        <span className={valueClass}>{fmtProb(value)}</span>
      </div>
      <div className={`relative ${barHeight} bg-ink-800 rounded`}>
        <div
          className={`absolute top-0 left-0 h-full ${barFill} rounded transition-[width] duration-500`}
          style={{ width: `${w}%` }}
        />
        {mw != null && (
          <div
            className="absolute -top-0.5 bottom-[-2px] w-[2px] bg-white/80"
            style={{ left: `calc(${mw}% - 1px)` }}
            title={`Polymarket ${mw.toFixed(1)}%`}
          />
        )}
      </div>
    </div>
  );
}
