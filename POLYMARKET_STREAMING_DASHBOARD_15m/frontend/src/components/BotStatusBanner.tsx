import { useMemo } from "react";
import { useDash } from "../lib/store";
import { fmtDuration, fmtProb } from "../lib/format";

type StatusTone = "slate" | "amber" | "emerald" | "rose" | "cyan";

type BannerState = {
  tone: StatusTone;
  title: string;
  detail: string;
  chips: string[];
};

export default function BotStatusBanner({
  compact = false,
}: {
  compact?: boolean;
}) {
  const wsStatus = useDash((s) => s.wsStatus);
  const liveness = useDash((s) => s.liveness);
  const calibration = useDash((s) => s.calibration);
  const position = useDash((s) => s.position.open);
  const window = useDash((s) => s.window);
  const edgeUp = useDash((s) => s.edgeUp);
  const edgeDown = useDash((s) => s.edgeDown);
  const instance = useDash((s) => s.instance);

  const state = useMemo<BannerState>(() => {
    const tradeCount = instance?.trades_count ?? 0;
    const blocked =
      window?.zone === "blocked_first" || window?.zone === "blocked_last";
    const tradeable = window?.zone === "tradeable";
    const calibrationUpdating =
      calibration.active && (calibration.elapsed_s ?? 0) >= 5;
    const readySides = [
      edgeUp?.has_edge ? "UP" : null,
      edgeDown?.has_edge ? "DOWN" : null,
    ].filter((v): v is "UP" | "DOWN" => v != null);
    const timeLeft =
      window != null ? fmtDuration(Math.max(0, window.total_s - window.elapsed_s)) : "—";
    const chips = [
      tradeCount === 0 ? "no trades yet" : `${tradeCount} trades`,
      window != null ? `window ${window.zone.replace("_", " ")}` : "window unknown",
      readySides.length > 0 ? `edge ${readySides.join(" / ")}` : "no edge",
      timeLeft !== "—" ? `${timeLeft} left` : null,
    ].filter((v): v is string => v != null);

    if (wsStatus !== "open") {
      return {
        tone: "rose",
        title: "Dashboard offline",
        detail: "Waiting for the backend websocket connection to come back.",
        chips: [`ws ${wsStatus}`],
      };
    }

    if (!liveness?.bot_live) {
      return {
        tone: "amber",
        title: "Bot offline",
        detail: "Waiting for fresh bot output before the dashboard can track opportunities.",
        chips,
      };
    }

    if (position != null) {
      const tone = position.direction === "UP" ? "emerald" : "rose";
      return {
        tone,
        title: `In a ${position.direction} position`,
        detail: "Waiting for TP hit or market expiration.",
        chips: [
          `entry ${fmtProb(position.entry_price)}`,
          position.tp_target != null ? `tp ${fmtProb(position.tp_target)}` : "tp off",
          timeLeft !== "—" ? `${timeLeft} to expiry` : "expiry unknown",
        ],
      };
    }

    if (tradeable && readySides.length > 0) {
      return {
        tone: "emerald",
        title:
          tradeCount === 0
            ? "First trade opportunity ready"
            : "Next trade opportunity ready",
        detail: `${readySides.join(" / ")} edge is live and the trading window is open.`,
        chips,
      };
    }

    if (blocked && readySides.length > 0) {
      return {
        tone: "amber",
        title: "Edge found, waiting for entry window",
        detail: `${readySides.join(" / ")} setup is there, but entries are currently blocked by the time window.`,
        chips,
      };
    }

    if (calibrationUpdating) {
      return {
        tone: "cyan",
        title:
          tradeCount === 0
            ? "Calibrating for first trade opportunity"
            : "Recomputing model probabilities",
        detail: "Refreshing the model and edge estimates for the current market.",
        chips,
      };
    }

    if (tradeCount === 0) {
      return {
        tone: "slate",
        title: "Waiting for first trade opportunity",
        detail:
          tradeable
            ? "The trading window is open, but there is no valid edge yet."
            : "Watching the market and waiting for the first valid edge inside the allowed trading time window.",
        chips,
      };
    }

    return {
      tone: "slate",
      title: "Waiting for next trade opportunity",
      detail:
        tradeable
          ? "The bot is flat and scanning for the next valid edge."
          : "The bot is flat and waiting for the next tradeable part of the window.",
      chips,
    };
  }, [
    calibration.active,
    calibration.elapsed_s,
    edgeDown?.has_edge,
    edgeUp?.has_edge,
    instance?.trades_count,
    liveness?.bot_live,
    position,
    window,
    wsStatus,
  ]);

  const toneClasses: Record<StatusTone, string> = {
    slate: "border-slate-700/80 bg-gradient-to-r from-slate-900/90 via-ink-900 to-ink-900 text-slate-100",
    amber: "border-amber-500/30 bg-gradient-to-r from-amber-500/10 via-ink-900 to-ink-900 text-amber-100",
    emerald: "border-emerald-500/30 bg-gradient-to-r from-emerald-500/10 via-ink-900 to-ink-900 text-emerald-100",
    rose: "border-rose-500/30 bg-gradient-to-r from-rose-500/10 via-ink-900 to-ink-900 text-rose-100",
    cyan: "border-cyan-500/30 bg-gradient-to-r from-cyan-500/10 via-ink-900 to-ink-900 text-cyan-100",
  };
  const dotClasses: Record<StatusTone, string> = {
    slate: "bg-slate-300",
    amber: "bg-amber-300 shadow-[0_0_10px_rgba(252,211,77,0.7)]",
    emerald: "bg-emerald-300 shadow-[0_0_10px_rgba(110,231,183,0.7)]",
    rose: "bg-rose-300 shadow-[0_0_10px_rgba(253,164,175,0.7)]",
    cyan: "bg-cyan-300 shadow-[0_0_10px_rgba(103,232,249,0.7)]",
  };

  return (
    <section
      className={`card min-w-0 ${compact ? "h-16 w-full px-3 py-2" : "px-4 py-3"} flex items-center ${compact ? "gap-3" : "gap-4"} ${toneClasses[state.tone]}`}
    >
      <div className="flex items-center gap-3 min-w-0 flex-1">
        <span className={`inline-block flex-none ${compact ? "h-2.5 w-2.5" : "h-3 w-3"} rounded-full ${dotClasses[state.tone]}`} />
        <div className="min-w-0 flex-1 overflow-hidden">
          <div className={`${compact ? "text-[9px] whitespace-nowrap" : "text-[11px]"} uppercase tracking-[0.18em] text-rose-300`}>
            Bot Status
          </div>
          <div className={`${compact ? "text-[13px] truncate whitespace-nowrap" : "text-sm truncate"} font-semibold text-slate-50`}>
            {state.title}
          </div>
          <div className={`${compact ? "text-[10px] leading-tight truncate whitespace-nowrap" : "text-[11px] truncate"} text-slate-400`}>
            {state.detail}
          </div>
        </div>
      </div>

      {!compact && (
        <div className="flex flex-wrap justify-end gap-2">
          {state.chips.map((chip) => (
          <span
            key={chip}
            className={`inline-flex items-center rounded-full border border-white/10 bg-white/5 px-2 py-0.5 ${compact ? "text-[9px]" : "text-[10px]"} font-mono uppercase tracking-wide text-slate-300`}
          >
            {chip}
          </span>
          ))}
        </div>
      )}
    </section>
  );
}
