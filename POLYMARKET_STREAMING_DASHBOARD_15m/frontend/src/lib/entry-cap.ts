import type { EdgeRatio, WindowZone } from "./types";

export type EntryCapStatus = {
  side: "UP" | "DOWN";
  blocked: boolean;
  marketProb: number | null;
  maxEntryPrice: number | null;
};

export function deriveEntryCapStatus(
  side: "UP" | "DOWN",
  edge: EdgeRatio | null,
  maxEntryPrice: number | null | undefined,
  zone: WindowZone | string,
): EntryCapStatus {
  const marketProb = edge?.market_prob ?? null;
  const cap = maxEntryPrice ?? null;
  return {
    side,
    blocked:
      zone === "tradeable" &&
      edge?.has_edge === true &&
      marketProb != null &&
      cap != null &&
      marketProb > cap,
    marketProb,
    maxEntryPrice: cap,
  };
}

export function fmtCapPrice(value: number | null | undefined): string {
  return value == null || Number.isNaN(value) ? "--" : value.toFixed(3);
}

export function entryCapTooltip(status: EntryCapStatus): string | undefined {
  if (!status.blocked) return undefined;
  return `${status.side} price ${fmtCapPrice(status.marketProb)} > max entry ${fmtCapPrice(
    status.maxEntryPrice,
  )} - edge is valid but trader will not enter`;
}

export function entryCapHint(statuses: EntryCapStatus[]): string | null {
  const blocked = statuses.filter((status) => status.blocked);
  if (blocked.length === 0) return null;
  if (blocked.length > 1) {
    return `UP/DOWN edges valid, but both prices are above max entry ${fmtCapPrice(
      blocked[0].maxEntryPrice,
    )}`;
  }
  const status = blocked[0];
  return `${status.side} edge valid, but ${status.side} price ${fmtCapPrice(
    status.marketProb,
  )} is above max entry ${fmtCapPrice(status.maxEntryPrice)}`;
}
