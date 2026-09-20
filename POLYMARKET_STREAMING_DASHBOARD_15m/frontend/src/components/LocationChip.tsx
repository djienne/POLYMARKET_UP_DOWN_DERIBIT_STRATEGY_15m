import { useDash } from "../lib/store";

// Small chip showing the Polymarket CLOB latency. The trader runs locally
// from now on, so we just label this "latency" rather than surfacing a
// location label.
export default function LocationChip() {
  const liveness = useDash((s) => s.liveness);
  const pingMs = liveness?.polymarket_ping_ms ?? null;

  if (pingMs == null) {
    return (
      <span className="chip chip-mute font-mono items-center gap-1.5 px-2">
        <span className="text-slate-500">latency</span>
        <span className="text-slate-300">--</span>
      </span>
    );
  }

  // Ping color thresholds: <100ms great (emerald), <200ms ok (slate), ≥200ms warn (amber)
  const pingClass =
    pingMs < 100
      ? "text-emerald-300"
      : pingMs < 200
        ? "text-slate-100"
        : "text-amber-300";

  return (
    <span
      className="chip chip-mute font-mono items-center gap-2 px-2"
      title={`Polymarket CLOB latency: ${pingMs.toFixed(0)} ms`}
    >
      <span className="text-slate-300 whitespace-nowrap">latency</span>
      <span className={`${pingClass} inline-flex shrink-0 tabular-nums`}>
        {pingMs.toFixed(0)}ms
      </span>
    </span>
  );
}
