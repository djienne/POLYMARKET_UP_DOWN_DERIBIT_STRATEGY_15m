import { useDash } from "../lib/store";

export default function ModeBadge({ mode }: { mode: string }) {
  const live = useDash((s) => s.liveness);
  const ws = useDash((s) => s.wsStatus);
  const botLive = live?.bot_live ?? false;
  const color = botLive
    ? "bg-emerald-400 shadow-[0_0_10px_rgba(52,211,153,0.8)]"
    : "bg-rose-400";
  const label = mode === "live" ? "LIVE" : "DRY-RUN";
  const labelClass =
    mode === "live"
      ? "text-amber-300 border-amber-500/30 bg-amber-500/10"
      : "text-cyan-300 border-cyan-500/30 bg-cyan-500/10";
  return (
    <div className="flex items-center gap-2">
      <span
        className={`chip ${labelClass} font-mono text-[11px] tracking-widest`}
      >
        {label}
      </span>
      <span
        title={`bot ${botLive ? "live" : "stale"} · ws ${ws}`}
        className={`dot ${color}`}
      />
      {!botLive && (
        <span className="text-[11px] text-rose-300/80 font-medium">
          STALE
        </span>
      )}
    </div>
  );
}
