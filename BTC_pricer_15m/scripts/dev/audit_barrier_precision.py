"""Audit barrier precision against settlement truth.

Reads ``15m_live_trades.csv`` (the single-trader live history) and computes
the distribution of ``reference_price - final_btc_price`` per closed trade.
``reference_price`` is the barrier captured by the trader at window start;
``final_btc_price`` is the settlement BTC price recorded at expiry. The
difference is the barrier-precision error in dollars; expressed as a
percentage of ``final_btc_price`` it tells us how off the barrier was
relative to the actual resolution price.

Run before AND after a precision-related code change to size and verify the
improvement. Read-only — does not touch any state files.

Example:

    python scripts/dev/audit_barrier_precision.py
    python scripts/dev/audit_barrier_precision.py --live-trades results/15m_live_trades.csv
"""

from __future__ import annotations

import argparse
import csv
import math
import statistics
import sys
from collections import Counter
from pathlib import Path


DEFAULT_LIVE_TRADES = Path("results/15m_live_trades.csv")


def _to_float(s: str) -> float | None:
    if s is None or s == "":
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _percentile(sorted_values: list[float], p: float) -> float:
    if not sorted_values:
        return float("nan")
    if len(sorted_values) == 1:
        return sorted_values[0]
    k = (len(sorted_values) - 1) * p / 100.0
    lo = math.floor(k)
    hi = math.ceil(k)
    if lo == hi:
        return sorted_values[int(k)]
    return sorted_values[lo] + (sorted_values[hi] - sorted_values[lo]) * (k - lo)


def _summarize(label: str, vals: list[float]) -> None:
    n = len(vals)
    print(f"\n=== {label} (n={n}) ===")
    if n == 0:
        print("  (no rows)")
        return
    abs_vals = [abs(v) for v in vals]
    sorted_vals = sorted(vals)
    sorted_abs = sorted(abs_vals)
    print(f"  mean         {statistics.mean(vals):+.4f}%")
    print(f"  stdev        {(statistics.stdev(vals) if n > 1 else 0.0):.4f}%")
    print(f"  mean |Δ|     {statistics.mean(abs_vals):.4f}%")
    print(f"  min          {min(vals):+.4f}%")
    print(f"  max          {max(vals):+.4f}%")
    print(f"  median       {_percentile(sorted_vals, 50):+.4f}%")
    print(f"  p5  / p95    {_percentile(sorted_vals, 5):+.4f}% / {_percentile(sorted_vals, 95):+.4f}%")
    print(f"  p25 / p75    {_percentile(sorted_vals, 25):+.4f}% / {_percentile(sorted_vals, 75):+.4f}%")
    print(f"  median |Δ|   {_percentile(sorted_abs, 50):.4f}%")
    print(f"  p95 |Δ|      {_percentile(sorted_abs, 95):.4f}%")


def _histogram(label: str, vals: list[float], bins: int = 10) -> None:
    if not vals:
        return
    lo, hi = min(vals), max(vals)
    if lo == hi:
        print(f"\n--- {label}: all values = {lo:+.4f}%")
        return
    width = (hi - lo) / bins
    counts = Counter()
    for v in vals:
        idx = min(bins - 1, int((v - lo) / width))
        counts[idx] += 1
    max_count = max(counts.values())
    print(f"\n--- {label} histogram ({bins} bins, {min(vals):+.4f}% → {max(vals):+.4f}%) ---")
    for i in range(bins):
        edge_lo = lo + i * width
        edge_hi = edge_lo + width
        n = counts.get(i, 0)
        bar = "#" * int(40 * n / max_count) if max_count else ""
        print(f"  [{edge_lo:+7.4f}%, {edge_hi:+7.4f}%)  {n:4d}  {bar}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--live-trades",
        type=Path,
        default=DEFAULT_LIVE_TRADES,
        help=f"Path to live trades CSV (default: {DEFAULT_LIVE_TRADES})",
    )
    args = parser.parse_args(argv)

    if not args.live_trades.exists():
        print(f"ERROR: {args.live_trades} does not exist", file=sys.stderr)
        return 2

    rows: list[dict[str, str]] = []
    with args.live_trades.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    deltas_pct: list[float] = []
    deltas_pct_up: list[float] = []
    deltas_pct_down: list[float] = []
    deltas_pct_tp: list[float] = []
    deltas_pct_expiry: list[float] = []

    for row in rows:
        ref = _to_float(row.get("reference_price", ""))
        final = _to_float(row.get("final_btc_price", ""))
        if ref is None or final is None or final == 0.0:
            continue
        delta_pct = (ref - final) / final * 100.0
        deltas_pct.append(delta_pct)
        if (row.get("direction") or "").upper() == "UP":
            deltas_pct_up.append(delta_pct)
        elif (row.get("direction") or "").upper() == "DOWN":
            deltas_pct_down.append(delta_pct)
        result = (row.get("result") or "").upper()
        if result == "TP_FILLED":
            deltas_pct_tp.append(delta_pct)
        elif result in ("WIN_EXPIRY", "LOSS_EXPIRY"):
            deltas_pct_expiry.append(delta_pct)

    print(f"Audit source: {args.live_trades}  (rows scanned: {len(rows)})")
    if not deltas_pct:
        print("No closed trades with both reference_price and final_btc_price.")
        return 1

    _summarize("ALL closed trades", deltas_pct)
    _summarize("UP entries", deltas_pct_up)
    _summarize("DOWN entries", deltas_pct_down)
    _summarize("TP_FILLED only", deltas_pct_tp)
    _summarize("WIN/LOSS at expiry only", deltas_pct_expiry)
    _histogram("Δ% distribution (all)", deltas_pct)
    return 0


if __name__ == "__main__":
    sys.exit(main())
