import { useMemo, useState } from "react";
import { useDash } from "../lib/store";

function fmtParams(p: {
  alpha_up: number;
  alpha_down: number;
  floor_up: number;
  floor_down: number;
  tp_pct: number;
  sl_pct: number;
}) {
  const sl = p.sl_pct > 0 ? ` sl=${p.sl_pct}` : "";
  return `aU=${p.alpha_up} aD=${p.alpha_down} fU=${p.floor_up} fD=${p.floor_down} tp=${p.tp_pct}${sl}`;
}

export default function InstanceSelector() {
  const all = useDash((s) => s.allInstances);
  const selected = useDash((s) => s.selectedInstanceId);
  const setSelected = useDash((s) => s.setSelected);
  const [query, setQuery] = useState("");
  const [open, setOpen] = useState(false);

  const currentRow = useMemo(
    () => all.find((r) => r.instance_id === selected),
    [all, selected],
  );

  const filtered = useMemo(() => {
    const q = query.trim().toLowerCase();
    const rows = all.slice(0, 864);
    if (!q) return rows.slice(0, 60);
    return rows
      .filter(
        (r) =>
          String(r.instance_id).includes(q) ||
          String(r.rank).includes(q) ||
          fmtParams(r.params).toLowerCase().includes(q),
      )
      .slice(0, 60);
  }, [all, query]);

  return (
    <div className="relative">
      <button
        onClick={() => setOpen((o) => !o)}
        className="card px-3 py-1.5 flex items-center gap-3 text-left hover:border-ink-700 w-full max-w-[560px]"
      >
        <span className="stat-label">Strategy</span>
        {currentRow ? (
          <>
            <span className="font-mono text-sm text-slate-100">
              #{currentRow.rank}
            </span>
            <span className="text-xs text-slate-400 truncate font-mono">
              {fmtParams(currentRow.params)}
            </span>
          </>
        ) : (
          <span className="text-xs text-slate-500 font-mono">—</span>
        )}
        <span className="ml-auto text-slate-500 text-xs">▾</span>
      </button>

      {open && (
        <div
          className="absolute z-30 mt-2 w-full max-w-[560px] bg-ink-900 border border-ink-800 rounded-xl shadow-2xl p-2"
          onMouseLeave={() => setOpen(false)}
        >
          <input
            autoFocus
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            placeholder="Search by rank, id, or params…"
            className="w-full bg-ink-850 border border-ink-800 rounded-md px-2 py-1.5 text-sm text-slate-100 placeholder-slate-500 outline-none focus:border-cyan-500/40"
          />
          <div className="max-h-80 overflow-auto mt-2">
            {filtered.map((r) => {
              const isSel = r.instance_id === selected;
              return (
                <button
                  key={r.instance_id}
                  onClick={() => {
                    setSelected(r.instance_id);
                    setOpen(false);
                  }}
                  className={`w-full text-left px-2 py-1.5 rounded grid grid-cols-[2.5rem_3.5rem_1fr_auto] items-center gap-2 text-xs hover:bg-ink-800/70 ${
                    isSel ? "bg-cyan-500/10 text-cyan-100" : "text-slate-300"
                  }`}
                >
                  <span className="text-slate-400">#{r.rank}</span>
                  <span className="font-mono">id {r.instance_id}</span>
                  <span className="font-mono truncate">
                    {fmtParams(r.params)}
                  </span>
                  <span
                    className={`font-mono ${
                      r.total_pnl >= 0 ? "text-emerald-300" : "text-rose-300"
                    }`}
                  >
                    {(r.total_pnl >= 0 ? "+" : "−") +
                      "$" +
                      Math.abs(r.total_pnl).toFixed(2)}
                  </span>
                </button>
              );
            })}
            {filtered.length === 0 && (
              <div className="px-2 py-6 text-center text-slate-500 text-sm">
                No matches
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
