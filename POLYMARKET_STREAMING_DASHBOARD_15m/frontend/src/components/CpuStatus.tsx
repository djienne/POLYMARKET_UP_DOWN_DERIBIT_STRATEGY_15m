import { useDash } from "../lib/store";

export default function CpuStatus() {
  const cpuPct = useDash((s) => s.liveness?.cpu_pct ?? null);

  if (cpuPct == null) {
    return (
      <span className="chip chip-mute w-[84px] min-w-[84px] shrink-0 justify-between font-mono">
        <span className="text-slate-500">cpu</span>
        <span className="font-semibold text-slate-200">--</span>
      </span>
    );
  }

  const rounded = Math.round(cpuPct);
  const chipClass =
    rounded < 45 ? "chip-up" : rounded < 75 ? "chip-warn" : "chip-down";

  return (
    <span
      className={`chip ${chipClass} w-[84px] min-w-[84px] shrink-0 justify-between font-mono`}
      title={`CPU usage: ${cpuPct.toFixed(1)}%`}
    >
      <span className="uppercase opacity-85">cpu</span>
      <span className="font-semibold text-slate-50">{rounded}%</span>
    </span>
  );
}
