import { useMemo } from "react";
import {
  Area,
  AreaChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { useDash } from "../lib/store";
import { fmtLocalDate, fmtLocalFull, fmtLocalHM } from "../lib/format";

export default function EquitySparkline() {
  const series = useDash((s) => s.equitySeries);
  const windowStartIso = useDash((s) => s.windowStartIso);
  const mode = useDash((s) => s.mode);
  const closedTrades = useDash((s) => s.instance?.trades_count ?? 0);
  const cash = useDash((s) => s.instance?.capital ?? null);
  const positionOpen = useDash((s) => s.position.open != null);
  // Prefer the live instance's starting_capital; fall back to the trader's
  // configured starting_capital (live: 100, paper grid: 1000) so a fresh run
  // with no trades still renders on a sensible scale.
  const start = useDash(
    (s) => s.instance?.starting_capital ?? s.sharedConfig.starting_capital ?? 100,
  );

  const data = useMemo(() => {
    const full = series
      .map((p) => {
        const ts = new Date(p.t).getTime();
        return Number.isFinite(ts) ? { ts, v: p.v } : null;
      })
      .filter((x): x is { ts: number; v: number } => x != null)
      .sort((a, b) => a.ts - b.ts);

    if (full.length === 0) {
      if (mode === "live") return [];
      const now = Date.now();
      const seedStart = windowStartIso != null ? Date.parse(windowStartIso) : NaN;
      const startTs =
        Number.isFinite(seedStart) && seedStart < now
          ? seedStart
          : now - 15 * 60 * 1000;
      return [
        { ts: startTs, v: start },
        { ts: Math.max(startTs + 1000, now), v: start },
      ];
    }
    return full;
  }, [series, start, windowStartIso, mode]);

  const last = data[data.length - 1]?.v ?? start;
  const flatAccountValue = mode === "live" && !positionOpen ? cash : null;
  const headlineValue = flatAccountValue ?? (data.length ? last : null);
  const up = last >= start;
  const color = mode === "live" ? "#38bdf8" : up ? "#34d399" : "#fb7185";
  const yDomain = useMemo<[number, number]>(() => {
    const values = data.map((point) => point.v).filter(Number.isFinite);
    const anchor = Number.isFinite(start) && start > 0 ? start : 100;
    const basePad = Math.max(20, anchor * 0.15);
    const baseMin = Math.max(0, anchor - basePad);
    const baseMax = anchor + basePad;
    const dataMin = values.length > 0 ? Math.min(...values, anchor) : anchor;
    const dataMax = values.length > 0 ? Math.max(...values, anchor) : anchor;
    const lower =
      dataMin < baseMin ? Math.max(0, Math.floor((dataMin - 5) / 10) * 10) : baseMin;
    const upper =
      dataMax > baseMax ? Math.ceil((dataMax + 5) / 10) * 10 : baseMax;
    return [lower, upper];
  }, [data, start]);
  const yTicks = useMemo(() => {
    const [lo, hi] = yDomain;
    const n = 5;
    return Array.from({ length: n }, (_, i) =>
      lo + ((hi - lo) * i) / (n - 1),
    );
  }, [yDomain]);

  // Build 5 evenly spaced ticks so recharts doesn't pick dense auto-ticks
  const explicitTicks = useMemo(() => {
    if (data.length < 2) return undefined;
    const min = data[0].ts;
    const max = data[data.length - 1].ts;
    const n = 5;
    return Array.from({ length: n }, (_, i) =>
      Math.round(min + ((max - min) * i) / (n - 1)),
    );
  }, [data]);
  const spansMultipleDays =
    data.length >= 2 && data[data.length - 1].ts - data[0].ts > 86_400_000;

  return (
    <div className="card p-4 h-full flex flex-col">
      <div className="flex items-baseline justify-between mb-1">
        <h2 className="card-header">{mode === "live" ? "Account equity" : "Realized equity"}</h2>
        <div className="text-right">
          <div
            className={`font-mono text-lg ${
              mode === "live" ? "text-slate-100" : up ? "text-emerald-300" : "text-rose-300"
            }`}
          >
            {headlineValue != null ? `$${headlineValue.toFixed(2)}` : "—"}
          </div>
          <div className="text-[10px] text-slate-500 font-mono">
            {flatAccountValue != null ? "cash · no open position" : `${closedTrades} closed trades`}
          </div>
          {mode === "live" && data.length > 0 && (
            <div className="text-[9px] text-slate-500" title={fmtLocalFull(data[data.length - 1].ts)}>
              history ${last.toFixed(2)} · {fmtLocalDate(data[data.length - 1].ts)} {fmtLocalHM(data[data.length - 1].ts)}
            </div>
          )}
        </div>
      </div>
      <div className="flex-1 min-h-0">
        {data.length === 0 ? <p className="text-xs text-slate-500">No recorded account equity yet</p> : <ResponsiveContainer width="100%" height="100%">
          <AreaChart data={data} margin={{ top: 4, right: 8, bottom: 0, left: 0 }}>
            <defs>
              <linearGradient id="equity" x1="0" y1="0" x2="0" y2="1">
                <stop offset="0%" stopColor={color} stopOpacity={0.35} />
                <stop offset="100%" stopColor={color} stopOpacity={0} />
              </linearGradient>
            </defs>
            <XAxis
              dataKey="ts"
              type="number"
              domain={["dataMin", "dataMax"]}
              scale="time"
              ticks={explicitTicks}
              tickFormatter={(v) => formatTick(v, spansMultipleDays)}
              stroke="#475569"
              tick={{ fill: "#64748b", fontSize: 10 }}
              minTickGap={80}
            />
            <YAxis
              domain={yDomain}
              ticks={yTicks}
              allowDataOverflow
              tick={{ fill: "#64748b", fontSize: 10 }}
              stroke="#475569"
              width={48}
              tickFormatter={(v) => `$${v.toFixed(0)}`}
            />
            <Tooltip
              contentStyle={{
                background: "#0f172a",
                border: "1px solid #334155",
                borderRadius: 8,
                fontSize: 11,
              }}
              labelStyle={{ color: "#94a3b8" }}
              labelFormatter={(v: number) => fmtLocalFull(v)}
              formatter={(v: number) => [`$${v.toFixed(2)}`, "equity"]}
            />
            <Area
              type="monotone"
              dataKey="v"
              stroke={color}
              strokeWidth={2}
              fill="url(#equity)"
              isAnimationActive={false}
            />
          </AreaChart>
        </ResponsiveContainer>}
      </div>
    </div>
  );
}

function formatTick(ts: number, multiDay: boolean): string {
  return multiDay ? fmtLocalDate(ts) : fmtLocalHM(ts);
}
