import { useMemo } from "react";
import { AnimatePresence, motion } from "framer-motion";
import { useDash } from "../lib/store";
import { fmtLocalDate, fmtLocalHMS, fmtMoney, fmtPct } from "../lib/format";
import { tradeEventKey } from "../lib/trade-key";
import type { TradeEvent } from "../lib/types";

// UNRESOLVED_RESTART rows are emitted when a grid restart crosses a market
// boundary while a position was still open: the trader can't determine the
// real outcome (Chainlink wasn't sampled at expiry), so it neutrally refunds
// cost_basis and tags the row. They're useful in trades.csv for audit, but
// noisy in the live feed — hide them, and also hide their paired ENTRY row
// (the one that never got a real WIN/LOSS exit) so the feed only shows
// fully-resolved entry/close pairs.
const REAL_CLOSE_EVENTS = new Set([
  "TP_FILLED",
  "TP_SIGNAL_MARKET_EXIT",
  "TRAILING_STOP",
  "WIN_EXPIRY",
  "LOSS_EXPIRY",
  "STOP_LOSS",
]);

type FeedTrade = TradeEvent & {
  feedKey: string;
  displayEvent: string;
  detailEvent?: string;
  splitCount?: number;
};

export default function TradeFeed() {
  const allTrades = useDash((s) => s.trades);
  const trades = useMemo<FeedTrade[]>(() => {
    // Walk events chronologically, pairing each ENTRY with the next close in
    // its market. If the paired close is UNRESOLVED_RESTART, hide both rows.
    const sorted = [...allTrades].sort((a, b) =>
      (a.timestamp || "").localeCompare(b.timestamp || ""),
    );
    const hide = new Set<string>();
    const openEntryByMarket = new Map<string, TradeEvent>();
    for (const ev of sorted) {
      if (!ev.market_id) continue;
      if (ev.event === "ENTRY") {
        openEntryByMarket.set(ev.market_id, ev);
      } else if (
        ev.event === "UNRESOLVED_RESTART" ||
        REAL_CLOSE_EVENTS.has(ev.event)
      ) {
        const pairedEntry = openEntryByMarket.get(ev.market_id);
        if (ev.event === "UNRESOLVED_RESTART") {
          hide.add(tradeEventKey(ev));
          if (pairedEntry) hide.add(tradeEventKey(pairedEntry));
        }
        openEntryByMarket.delete(ev.market_id);
      }
    }
    const visible = allTrades.filter((t) => !hide.has(tradeEventKey(t)));
    const grouped: FeedTrade[] = [];
    const entryIndex = new Map<string, number>();

    for (const trade of visible) {
      const feedTrade: FeedTrade = {
        ...trade,
        feedKey: tradeEventKey(trade),
        displayEvent: displayEventName(trade.event),
        detailEvent: trade.event,
      };

      if (trade.event === "ENTRY") {
        const key = entryGroupKey(trade);
        const idx = entryIndex.get(key);
        if (idx != null) {
          const prior = grouped[idx];
          grouped[idx] = {
            ...prior,
            feedKey: `entry-group|${key}`,
            shares: addOptional(prior.shares, trade.shares),
            splitCount: (prior.splitCount ?? 1) + 1,
          };
          continue;
        }
        entryIndex.set(key, grouped.length);
      }

      grouped.push(feedTrade);
    }

    return grouped;
  }, [allTrades]);

  return (
    <div className="card p-3.5 h-full flex flex-col">
      <div className="flex items-baseline justify-between mb-2.5">
        <h2 className="card-header">Trade feed</h2>
        <span className="text-[11px] text-slate-500 font-mono">
          last {trades.length}
        </span>
      </div>

      {trades.length === 0 ? (
        <div className="flex-1 min-h-0 flex items-center justify-center">
          <div className="w-full rounded-xl border border-dashed border-ink-700/90 bg-[linear-gradient(180deg,rgba(15,23,42,0.52),rgba(15,23,42,0.22))] px-4 py-6 text-center">
            <div className="mx-auto mb-3 flex h-10 w-10 items-center justify-center rounded-full border border-cyan-500/25 bg-cyan-500/10 shadow-[0_0_20px_rgba(34,211,238,0.08)]">
              <span className="h-2.5 w-2.5 rounded-full bg-cyan-300 shadow-[0_0_10px_rgba(103,232,249,0.8)]" />
            </div>
            <div className="text-sm font-semibold text-slate-200">
              No trade activity yet
            </div>
            <div className="mt-1 text-xs leading-relaxed text-slate-500">
              Entries, take profits, and stop losses will appear here as soon as the bot starts trading.
            </div>
          </div>
        </div>
      ) : (
        <div className="overflow-auto flex-1 min-h-0 pr-1 -mr-1">
          <ul className="flex flex-col gap-1.5">
            <AnimatePresence initial={false}>
              {trades.map((t, idx) => (
                <TradeRow key={t.feedKey} trade={t} newest={idx === 0} />
              ))}
            </AnimatePresence>
          </ul>
        </div>
      )}
    </div>
  );
}

function TradeRow({ trade: t, newest }: { trade: FeedTrade; newest: boolean }) {
  const pnlColor =
    t.pnl == null
      ? "text-slate-400"
      : t.pnl > 0
        ? "text-emerald-300"
        : t.pnl < 0
          ? "text-rose-300"
          : "text-slate-300";
  const dirCls = dirColor(t.direction);
  const priceText = tradePriceText(t);
  const eventTone = eventToneClass(t.event);

  return (
    <motion.li
      initial={newest ? { opacity: 0, y: -8, backgroundColor: bgFor(t.event) } : false}
      animate={{ opacity: 1, y: 0, backgroundColor: "rgba(0,0,0,0)" }}
      transition={{ duration: 1.6 }}
      className="rounded-md border border-ink-800/70 bg-slate-950/20 px-2 py-1 font-mono"
    >
      <div className="flex items-center justify-between gap-2 text-[11px]">
        <span className="text-slate-400">
          {fmtLocalDate(t.timestamp)} {fmtLocalHMS(t.timestamp)}
        </span>
        <span
          className={`rounded-sm border px-1.5 py-0.5 text-[10px] uppercase tracking-wider ${eventTone}`}
          title={t.detailEvent}
        >
          {t.displayEvent}
          {t.splitCount && t.splitCount > 1 ? ` x${t.splitCount}` : ""}
        </span>
        <span className={`${dirCls} text-[10px] uppercase`}>
          {t.direction ?? "--"}
        </span>
      </div>
      <div className="flex items-center justify-between gap-2 text-[11px] mt-0.5">
        <span className="min-w-0 truncate text-slate-500">
          {priceText}
        </span>
        <span className={pnlColor}>
          {fmtMoney(t.pnl)}{" "}
          <span className="text-[10px] opacity-80">{fmtPct(t.pnl_pct)}</span>
        </span>
      </div>
    </motion.li>
  );
}

function addOptional(a: number | null | undefined, b: number | null | undefined) {
  if (a == null) return b;
  if (b == null) return a;
  return a + b;
}

function keyPart(value: unknown): string {
  return value == null ? "" : String(value);
}

function entryGroupKey(t: TradeEvent): string {
  return [
    t.instance_id,
    t.run_source,
    t.timestamp,
    t.direction,
    t.market_id,
    t.entry_price,
  ]
    .map(keyPart)
    .join("|");
}

function displayEventName(event: string): string {
  switch (event) {
    case "ENTRY":
      return "BUY";
    case "MANUAL_CLOSE":
      return "SELL";
    case "WIN_EXPIRY":
      return "REDEEM";
    case "LOSS_EXPIRY":
      return "EXPIRED";
    case "TRAILING_STOP":
      return "TRAIL SELL";
    case "STOP_LOSS":
      return "STOP SELL";
    case "TP_FILLED":
      return "TAKE PROFIT";
    case "TP_SIGNAL_MARKET_EXIT":
      return "MARKET EXIT";
    default:
      return event;
  }
}

function tradePriceText(t: FeedTrade): string {
  const shares = t.shares != null ? `${fmtShares(t.shares)} sh` : null;
  if (t.event === "ENTRY") {
    return [shares, "@", fmtPrice(t.entry_price)].filter(Boolean).join(" ");
  }
  if (t.event === "WIN_EXPIRY") {
    return [shares, "redeem @", fmtPrice(t.exit_price)].filter(Boolean).join(" ");
  }
  if (t.event === "LOSS_EXPIRY") {
    return [shares, "expired @", fmtPrice(t.exit_price)].filter(Boolean).join(" ");
  }
  if (t.entry_price != null || t.exit_price != null) {
    const path = `${fmtPrice(t.entry_price)} -> ${fmtPrice(t.exit_price)}`;
    return shares ? `${shares} ${path}` : path;
  }
  return "--";
}

function fmtShares(v: number): string {
  return v >= 10 ? v.toFixed(2) : v.toFixed(4);
}

function fmtPrice(v: number | null | undefined): string {
  return v != null ? v.toFixed(4) : "--";
}

function eventToneClass(event: string): string {
  if (event === "ENTRY") return "border-cyan-400/25 bg-cyan-400/10 text-cyan-200";
  if (event === "TP_FILLED" || event === "TRAILING_STOP" || event === "WIN_EXPIRY")
    return "border-emerald-400/25 bg-emerald-400/10 text-emerald-200";
  if (event === "MANUAL_CLOSE" || event === "TP_SIGNAL_MARKET_EXIT")
    return "border-slate-400/25 bg-slate-400/10 text-slate-200";
  if (event === "STOP_LOSS" || event === "LOSS_EXPIRY")
    return "border-rose-400/25 bg-rose-400/10 text-rose-200";
  return "border-slate-500/25 bg-slate-500/10 text-slate-200";
}

function bgFor(event: string): string {
  if (event === "ENTRY") return "rgba(34,211,238,0.18)";
  if (event === "TP_FILLED" || event === "TRAILING_STOP" || event === "WIN_EXPIRY")
    return "rgba(52,211,153,0.22)";
  if (event === "TP_SIGNAL_MARKET_EXIT")
    return "rgba(148,163,184,0.18)";
  if (event === "STOP_LOSS" || event === "LOSS_EXPIRY")
    return "rgba(251,113,133,0.22)";
  return "rgba(148,163,184,0.12)";
}

function dirColor(d?: string | null): string {
  if (d === "UP") return "text-emerald-300";
  if (d === "DOWN") return "text-rose-300";
  return "text-slate-400";
}
