import { useDash } from "../lib/store";

export default function LeaderboardMini() {
  const rows = useDash((s) => s.leaderboard);
  const selected = useDash((s) => s.selectedInstanceId);
  const setSelected = useDash((s) => s.setSelected);

  return (
    <div className="card p-4 h-full flex flex-col">
      <div className="flex items-baseline justify-between mb-2">
        <h2 className="card-header">Leaderboard · top 15</h2>
        <span className="text-[10px] text-slate-500 font-mono">
          click to focus
        </span>
      </div>

      <div className="overflow-auto flex-1 min-h-0">
        <table className="w-full text-[11px] font-mono">
          <thead className="text-slate-500 text-[10px] uppercase tracking-widest">
            <tr>
              <th className="text-left py-1">#</th>
              <th className="text-right">pnl</th>
              <th className="text-right" title="Trade P&L mean / sample SD">mean/SD</th>
              <th className="text-right">mdd</th>
              <th className="text-right">w/l</th>
              <th className="text-left pl-2">params</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((r) => {
              const isSel = r.instance_id === selected;
              return (
                <tr
                  key={r.instance_id}
                  onClick={() => setSelected(r.instance_id)}
                  className={`border-t border-ink-800/60 cursor-pointer hover:bg-ink-800/60 transition-colors ${
                    isSel ? "bg-cyan-500/10 text-cyan-100" : ""
                  }`}
                >
                  <td className="py-1 text-slate-400">{r.rank}</td>
                  <td
                    className={`text-right ${
                      r.total_pnl >= 0 ? "text-emerald-300" : "text-rose-300"
                    }`}
                  >
                    {(r.total_pnl >= 0 ? "+" : "−") +
                      "$" +
                      Math.abs(r.total_pnl).toFixed(0)}
                  </td>
                  <td className="text-right text-slate-300">
                    {r.trades > 0 && r.pnl_mean_to_sd != null ? r.pnl_mean_to_sd.toFixed(2) : "—"}
                  </td>
                  <td className="text-right text-amber-300/80">
                    {r.max_drawdown_pct != null ? `${r.max_drawdown_pct.toFixed(1)}%` : "—"}
                  </td>
                  <td className="text-right text-slate-300">
                    {r.wins}/{r.losses}
                  </td>
                  <td className="pl-2 text-slate-400 truncate">
                    aU={r.params.alpha_up} aD={r.params.alpha_down} fU=
                    {r.params.floor_up} fD={r.params.floor_down} tp=
                    {r.params.tp_pct}
                    {r.params.sl_pct > 0 ? ` sl=${r.params.sl_pct}` : ""}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}
