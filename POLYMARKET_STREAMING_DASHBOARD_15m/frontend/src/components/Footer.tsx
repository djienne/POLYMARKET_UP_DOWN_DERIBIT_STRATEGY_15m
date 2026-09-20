import { useEffect, useState } from "react";
import { useDash } from "../lib/store";
import {
  fmtLocalDateTimeSeconds,
  parisTzAbbrev,
  parisUtcOffset,
} from "../lib/format";

function timingSourceLabel(
  source: string | null | undefined,
  executionLocation: string | null | undefined,
): string | null {
  if (executionLocation === "local") return "local";
  if (source === "local_offload" || source === "shared_calc") return "local";
  if (source) return source.replace(/_/g, " ").toLowerCase();
  return null;
}

export default function Footer() {
  const ws = useDash((s) => s.wsStatus);
  const live = useDash((s) => s.liveness);
  const cal = useDash((s) => s.calibration);
  const terminalTiming = useDash((s) => s.terminal?.timing ?? null);
  const t =
    terminalTiming?.surface_fit_s != null ||
    terminalTiming?.mc_s != null ||
    terminalTiming?.calibration_s != null
      ? terminalTiming
      : cal.last_timing;
  const usedSource = timingSourceLabel(t?.used_source, live?.execution_location);
  const [now, setNow] = useState(() => new Date());

  useEffect(() => {
    const id = setInterval(() => {
      setNow(new Date());
    }, 1000);
    return () => clearInterval(id);
  }, []);

  const tzOffset = parisUtcOffset(now);
  const tzAbbrev = parisTzAbbrev(now);
  const nowLabel = fmtLocalDateTimeSeconds(now);
  const parisHour = Number(
    new Intl.DateTimeFormat("en-GB", {
      timeZone: "Europe/Paris",
      hour: "2-digit",
      hour12: false,
    }).format(now),
  );
  const isHighActivityPeriod = parisHour >= 15 || parisHour < 5;

  return (
    <footer className="border-t border-ink-800 bg-ink-950/90 backdrop-blur">
      <div className="max-w-[1680px] mx-auto px-4 lg:px-6 py-2 flex flex-wrap gap-4 items-center text-[11px] font-mono">
        <Dot label="backend" ok={true} />
        <Dot label="ws" ok={ws === "open"} info={ws} />
        <Dot label="bot" ok={!!live?.bot_live} />
        <span className="text-slate-500">
          last tick{" "}
          <span className="inline-flex w-[5ch] justify-end text-slate-300">
            {live?.terminal_age_s != null
              ? `${live.terminal_age_s.toFixed(1)}s`
              : "--"}
          </span>
        </span>
        <span
          className="text-slate-500 uppercase tracking-[0.18em]"
          title="All timestamps shown in Europe/Paris (same as Amsterdam - CET in winter, CEST in summer)"
        >
          tz{" "}
          <span className="text-slate-300">
            Europe/Paris - {tzOffset}
            {tzAbbrev && ` (${tzAbbrev})`}
          </span>
          <span className="text-slate-500"> {" "}/{" "}</span>
          <span className="text-slate-300 normal-case tracking-normal">
            {nowLabel}
          </span>
          <span className="text-slate-500"> {" "}/{" "}</span>
          <span className="text-slate-400 normal-case tracking-normal">
            most trading activity occurs between 15:00 and 05:00 CEST the next day
          </span>
          <span className="text-slate-500"> {" "}/{" "}</span>
          <span
            className={`normal-case tracking-normal ${
              isHighActivityPeriod ? "text-emerald-300" : "text-amber-300"
            }`}
          >
            now is {isHighActivityPeriod ? "high period" : "low period"}
          </span>
        </span>
        <span className="text-slate-500 ml-auto">
          {t?.used_gap_s != null ? (
            <>
              cadence <span className="text-slate-300">{t.used_gap_s.toFixed(1)}s</span>
              {usedSource && (
                <>
                  {" "}
                  <span className="text-slate-500">from</span>{" "}
                  <span className="text-slate-300">{usedSource}</span>
                </>
              )}
            </>
          ) : usedSource ? (
            <>
              <span className="text-slate-500">first value from</span>{" "}
              <span className="text-slate-300">{usedSource}</span>
            </>
          ) : null}
          {t?.used_gap_s != null || usedSource ? null : t?.surface_fit_s != null && (
            <>
              fit <span className="text-slate-300">{t.surface_fit_s.toFixed(2)}s</span>
            </>
          )}
        </span>
        <DevEntryTrigger />
        <DevWinTrigger />
        <DevLossTrigger />
      </div>
    </footer>
  );
}

function DevWinTrigger() {
  return (
    <button
      type="button"
      title="trigger win"
      aria-label="trigger win"
      onClick={() => {
        useDash.setState((st) => ({
          flashQueue: [
            ...st.flashQueue,
            {
              id: `dev-${Date.now()}`,
              kind: "win",
              direction: "UP",
              amount: 4.2,
              at: Date.now(),
            },
          ],
        }));
      }}
      className="w-1.5 h-1.5 rounded-full bg-slate-500 opacity-10 hover:opacity-70 transition-opacity"
    />
  );
}

function DevLossTrigger() {
  return (
    <button
      type="button"
      title="trigger loss"
      aria-label="trigger loss"
      onClick={() => {
        useDash.setState((st) => ({
          flashQueue: [
            ...st.flashQueue,
            {
              id: `dev-loss-${Date.now()}`,
              kind: "loss",
              direction: "DOWN",
              amount: -3.7,
              at: Date.now(),
            },
          ],
        }));
      }}
      className="w-1.5 h-1.5 rounded-full bg-slate-500 opacity-10 hover:opacity-70 transition-opacity"
    />
  );
}

function DevEntryTrigger() {
  return (
    <button
      type="button"
      title="trigger entry"
      aria-label="trigger entry"
      onClick={() => {
        const direction = Math.random() < 0.5 ? "UP" : "DOWN";
        useDash.setState((st) => ({
          flashQueue: [
            ...st.flashQueue,
            {
              id: `dev-entry-${Date.now()}`,
              kind: "entry",
              direction,
              amount: null,
              at: Date.now(),
            },
          ],
        }));
      }}
      className="w-1.5 h-1.5 rounded-full bg-slate-500 opacity-10 hover:opacity-70 transition-opacity"
    />
  );
}

function Dot({
  label,
  ok,
  info,
}: {
  label: string;
  ok: boolean;
  info?: string;
}) {
  return (
    <span className="inline-flex items-center gap-1.5">
      <span
        className={`dot ${
          ok
            ? "bg-emerald-400 shadow-[0_0_6px_rgba(52,211,153,0.9)]"
            : "bg-rose-400"
        }`}
      />
      <span className="text-slate-400">{label}</span>
      {info && <span className="text-slate-500">{info}</span>}
    </span>
  );
}
