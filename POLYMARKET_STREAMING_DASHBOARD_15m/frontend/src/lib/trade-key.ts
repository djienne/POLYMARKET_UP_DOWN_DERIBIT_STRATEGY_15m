import type { TradeEvent } from "./types";

function keyPart(value: unknown): string {
  return value == null ? "" : String(value);
}

export function tradeEventKey(t: TradeEvent): string {
  return [
    t.instance_id,
    t.run_source,
    t.timestamp,
    t.event,
    t.direction,
    t.market_id,
    t.entry_price,
    t.exit_price,
    t.shares,
    t.pnl,
  ]
    .map(keyPart)
    .join("|");
}
