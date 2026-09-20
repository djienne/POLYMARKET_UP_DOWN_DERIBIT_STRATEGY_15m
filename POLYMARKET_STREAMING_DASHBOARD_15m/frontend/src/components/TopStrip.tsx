import { useDash } from "../lib/store";
import ModeBadge from "./ModeBadge";
import CalibrationStatus from "./CalibrationStatus";
import CpuStatus from "./CpuStatus";
import BotStatusBanner from "./BotStatusBanner";
import LocationChip from "./LocationChip";

export default function TopStrip() {
  const mode = useDash((s) => s.mode);
  const selected = useDash((s) => s.selectedInstanceId);
  const rank = useDash((s) => s.instance?.rank ?? null);
  const alternateModeEnabled = useDash((s) => s.alternateModeEnabled);
  const setAlternateModeEnabled = useDash((s) => s.setAlternateModeEnabled);
  const isLiveView = mode === "live";
  const modePanelClass = isLiveView
    ? "border-red-400/70 bg-red-500/20 text-red-50 shadow-[0_0_28px_rgba(248,113,113,0.20)]"
    : "border-cyan-400/60 bg-cyan-500/15 text-cyan-50 shadow-[0_0_24px_rgba(34,211,238,0.15)]";
  const modeLabel = isLiveView ? "REAL MONEY MODE" : "DRY RUN SIMULATION";
  const detail =
    mode === "dry_run" && rank != null
      ? `rank #${rank}`
      : mode === "live"
        ? "https://polymarket.com/@freqtradefr"
        : `instance ${selected}`;
  const detailClass = isLiveView
    ? "mt-0.5 truncate font-mono text-[9px] normal-case tracking-normal opacity-85"
    : "mt-0.5 truncate font-mono text-[10px] uppercase tracking-wide opacity-85";

  return (
    <header className="sticky top-0 z-20 border-b border-ink-800/90 bg-ink-950/72 backdrop-blur-xl shadow-[0_12px_36px_rgba(2,6,23,0.34)]">
      <div className="mx-auto flex max-w-[1680px] items-center gap-3 px-4 py-3 lg:px-6">
        <button
          type="button"
          onClick={() => setAlternateModeEnabled(!alternateModeEnabled)}
          aria-pressed={alternateModeEnabled}
          className="relative flex flex-none items-center gap-3 rounded-xl border border-ink-800/80 bg-ink-900/55 px-3 py-1.5 text-left shadow-[inset_0_1px_0_rgba(255,255,255,0.02)] transition-colors hover:border-ink-700/90 focus:outline-none focus:ring-1 focus:ring-cyan-400/40"
          title="Toggle dry/live alternation"
        >
          <img src="/polymarket.svg" alt="Polymarket" className="h-11 w-auto flex-none" />
          <img src="/bitcoin.svg" alt="BTC" className="h-9 w-9 flex-none" />
          <div className="leading-tight">
            <span className="whitespace-nowrap text-sm font-semibold text-slate-100">
              BTC 15-min UP / DOWN
            </span>
            <div className="mt-0.5 whitespace-nowrap text-[10px] uppercase tracking-widest text-slate-500">
              Streaming Dashboard
            </div>
          </div>
        </button>

        <div className="flex-none">
          <ModeBadge mode={mode} />
        </div>

        <div
          className={`w-[250px] flex-none rounded-xl border px-3 py-2 leading-tight ${modePanelClass}`}
        >
          <div className="text-[14px] font-black uppercase tracking-[0.12em]">
            {modeLabel}
          </div>
          <div className={detailClass}>
            {detail}
          </div>
        </div>

        <div className="min-w-[320px] max-w-[620px] flex-[1_1_460px]">
          <BotStatusBanner compact />
        </div>
        <div className="ml-auto flex min-w-max flex-none items-center justify-end gap-2.5">
          <div className="hidden min-[1700px]:block">
            <CalibrationStatus />
          </div>
          <CpuStatus />
          <LocationChip />
        </div>
      </div>
    </header>
  );
}
