import { useDash } from "../lib/store";

export default function WindowTimer() {
  const w = useDash((s) => s.window);
  if (!w) {
    return (
      <div className="card p-5 text-slate-500 text-sm">No window data.</div>
    );
  }
  const elapsedPct = Math.min(100, (w.elapsed_s / w.total_s) * 100);
  const firstPct = (w.no_trade_first_s / w.total_s) * 100;
  const lastPct = 100 - (w.no_trade_last_s / w.total_s) * 100;
  const tradeableStart = formatSec(w.no_trade_first_s);
  const tradeableEnd = formatSec(Math.max(0, w.total_s - w.no_trade_last_s));

  const zoneChipClass =
    w.zone === "tradeable"
      ? "chip-up"
      : w.zone === "expired"
        ? "chip-mute"
        : "chip-warn";
  const zoneLabel =
    w.zone === "blocked_first"
      ? `settling (blocked first ${formatSec(w.no_trade_first_s)})`
      : w.zone === "tradeable"
        ? "tradeable"
        : w.zone === "blocked_last"
          ? `closing (TP/expiry only ${formatSec(w.no_trade_last_s)})`
          : w.zone === "expired"
            ? "expired"
            : "unknown";

  return (
    <div className="card p-4 h-full flex flex-col">
      <div className="flex items-baseline justify-between mb-3">
        <h2 className="card-header">15-min window</h2>
        <span className={`chip ${zoneChipClass} font-mono`}>{zoneLabel}</span>
      </div>

      <div className="mb-2 flex justify-between text-[11px] font-mono text-slate-500">
        <span>
          {formatSec(w.elapsed_s)} /{" "}
          <span className="text-slate-300">{formatSec(w.total_s)}</span>
        </span>
        <span>
          {formatSec(Math.max(0, w.total_s - w.elapsed_s))} left
        </span>
      </div>

      <div className="relative h-3 bg-ink-800 rounded overflow-hidden">
        <div
          className="absolute top-0 bottom-0 bg-amber-500/15"
          style={{ left: 0, width: `${firstPct}%` }}
        />
        <div
          className="absolute top-0 bottom-0 bg-amber-500/15"
          style={{ left: `${lastPct}%`, width: `${100 - lastPct}%` }}
        />
        <div
          className="absolute top-0 bottom-0 bg-emerald-500/10"
          style={{ left: `${firstPct}%`, width: `${lastPct - firstPct}%` }}
        />
        <div
          className="absolute top-0 bottom-0 w-[2px] bg-cyan-300 shadow-[0_0_8px_rgba(34,211,238,0.9)]"
          style={{ left: `calc(${elapsedPct}% - 1px)` }}
        />
        <div
          className="absolute top-0 bottom-0 w-px bg-amber-400/40"
          style={{ left: `${firstPct}%` }}
        />
        <div
          className="absolute top-0 bottom-0 w-px bg-amber-400/40"
          style={{ left: `${lastPct}%` }}
        />
      </div>

      <div className="flex justify-between text-[10px] text-slate-500 mt-2 font-mono uppercase tracking-wider">
        <span>blocked {formatSec(w.no_trade_first_s)}</span>
        <span className="text-emerald-300/70">tradeable {tradeableStart}-{tradeableEnd}</span>
        <span>TP/expiry only {formatSec(w.no_trade_last_s)}</span>
      </div>
    </div>
  );
}

function formatSec(s: number): string {
  const m = Math.floor(Math.max(0, s) / 60);
  const r = Math.floor(Math.max(0, s) % 60);
  return `${m}:${r.toString().padStart(2, "0")}`;
}
