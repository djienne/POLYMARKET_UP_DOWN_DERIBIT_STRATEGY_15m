import { AnimatePresence, motion } from "framer-motion";
import { useDash } from "../lib/store";

function sourceLabel(
  source: string | null | undefined,
  executionLocation: string | null | undefined,
): string | null {
  if (executionLocation === "local") return "LOCAL";
  if (source === "local_offload" || source === "shared_calc") return "LOCAL";
  if (source) return source.replace(/_/g, " ").toUpperCase();
  return null;
}

export default function CalibrationStatus() {
  const cal = useDash((s) => s.calibration);
  const executionLocation = useDash((s) => s.liveness?.execution_location ?? null);
  const terminalTiming = useDash((s) => s.terminal?.timing ?? null);
  const timing =
    terminalTiming?.surface_fit_s != null ||
    terminalTiming?.mc_s != null ||
    terminalTiming?.calibration_s != null
      ? terminalTiming
      : cal.last_timing;
  const hasTiming = Boolean(
    timing &&
      (timing.used_gap_s != null ||
        timing.used_source != null ||
        timing.surface_fit_s ||
        timing.mc_s),
  );
  const usedSource = sourceLabel(timing?.used_source, executionLocation);
  const activeElapsed = cal.elapsed_s ?? 0;
  const showUpdating = cal.active && (!hasTiming || activeElapsed >= 5);

  const tone = showUpdating
    ? "border-amber-500/35 bg-amber-500/10 text-amber-200"
    : hasTiming
      ? "border-cyan-500/30 bg-cyan-500/10 text-cyan-100"
      : "border-slate-700 bg-slate-900/70 text-slate-200";

  const title = showUpdating
    ? "MODEL UPDATING"
    : hasTiming
      ? "MODEL READY"
      : "MODEL WAITING";

  const detail = showUpdating
    ? `Refreshing probabilities${cal.elapsed_s != null ? ` | ${cal.elapsed_s.toFixed(0)}s` : ""}`
    : hasTiming
      ? timing?.used_gap_s != null
        ? `${usedSource ? `${usedSource} | ` : ""}cadence ${timing.used_gap_s.toFixed(1)}s`
        : usedSource
          ? `${usedSource} | live updates`
          : "Live updates"
      : "Waiting for first used value";

  return (
    <AnimatePresence mode="wait">
      <motion.div
        key={showUpdating ? "active" : hasTiming ? "ready" : "waiting"}
        initial={{ opacity: 0, y: -4 }}
        animate={{ opacity: 1, y: 0 }}
        exit={{ opacity: 0, y: -4 }}
        className={`w-[clamp(190px,16vw,280px)] min-w-[190px] rounded-lg border px-3 py-1.5 leading-tight ${tone}`}
      >
        <div className="flex items-center gap-2 whitespace-nowrap">
          <span className="relative flex h-2.5 w-2.5 shrink-0">
            {showUpdating && (
              <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-amber-400 opacity-75"></span>
            )}
            <span
              className={`relative inline-flex h-2.5 w-2.5 rounded-full ${
                showUpdating
                  ? "bg-amber-400"
                  : hasTiming
                    ? "bg-cyan-300"
                    : "bg-slate-500"
              }`}
            ></span>
          </span>
          <span className="truncate text-[9px] font-semibold tracking-[0.18em]">
            {title}
          </span>
        </div>
        <div className="truncate pl-[18px] font-mono text-[11px] text-slate-200/90">
          {detail}
        </div>
      </motion.div>
    </AnimatePresence>
  );
}
