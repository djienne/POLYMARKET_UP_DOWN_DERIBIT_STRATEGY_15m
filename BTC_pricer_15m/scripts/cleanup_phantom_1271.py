"""One-off cleanup: remove the 2026-06-03 phantom dry-run trades for the live
mirror instance (id 1271) that the live trader could NOT place (CLOB returned
HTTP 425), and backfill those two CLOB outage windows into the audit log.

Why: the grid recorded a paper fill instantly at signal time, ~3s before the
live trader's real POST /order hit a 425. The fix (grid_entry_confirm_seconds)
prevents this going forward; this script repairs the two already-recorded
phantom entries (and their settled exits, +95.77 of fake PnL).

Recovery (recover_grid_instances_from_csv) rebuilds capital/PnL/leaderboard
purely by replaying trades.csv, so removing the rows + restarting the grid is
sufficient — no capital-ladder rewrite needed.

SAFETY:
  * Dry-run by default. Pass --apply to actually modify trades.csv.
  * Refuses to touch trades.csv unless EXACTLY the 4 expected phantom rows match.
  * Aborts if trades.csv was written in the last 30s (grid likely running) unless
    --force is also given. ALWAYS stop the grid first: `docker compose stop grid`.
  * Backs up trades.csv to trades.csv.bak.<UTC-timestamp> before writing.

Usage (from BTC_pricer_15m/, with the grid STOPPED):
    python -m scripts.cleanup_phantom_1271            # dry run, shows what it would do
    python -m scripts.cleanup_phantom_1271 --apply    # perform the cleanup
"""
from __future__ import annotations

import argparse
import csv
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from scripts.trader import clob_health

TRADES_CSV = Path("results/trades.csv")
CLOB_HEALTH_FLAG = Path("results/15m_clob_health.json")

INSTANCE_ID = "1271"
TARGET_MARKETS = {
    "btc-updown-15m-1780489800",
    "btc-updown-15m-1780490700",
}
TARGET_DATE_PREFIX = "2026-06-03"

# Exactly the rows we expect to remove (instance, market, event). Anything else
# -> abort: we never guess which rows to delete.
EXPECTED = {
    (INSTANCE_ID, "btc-updown-15m-1780489800", "ENTRY"),
    (INSTANCE_ID, "btc-updown-15m-1780489800", "WIN_EXPIRY"),
    (INSTANCE_ID, "btc-updown-15m-1780490700", "ENTRY"),
    (INSTANCE_ID, "btc-updown-15m-1780490700", "TP_FILLED"),
}

# The two CLOB taker-outage windows the live trader hit (from 15m_trading.log).
OUTAGE_BACKFILL = [
    {"since": "2026-06-03T12:39:31.554792+00:00", "until": "2026-06-03T12:41:01.554792+00:00"},
    {"since": "2026-06-03T12:52:05.612695+00:00", "until": "2026-06-03T12:53:35.612695+00:00"},
]


def _is_phantom(row: dict) -> bool:
    return (
        row.get("instance_id") == INSTANCE_ID
        and row.get("market_id") in TARGET_MARKETS
        and str(row.get("timestamp", "")).startswith(TARGET_DATE_PREFIX)
    )


def _scan(path: Path) -> tuple[list[str], list[dict]]:
    """Return (header_line_fieldnames, matched_phantom_rows)."""
    with path.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        fieldnames = reader.fieldnames or []
        matched = [row for row in reader if _is_phantom(row)]
    return fieldnames, matched


def _rewrite_without_phantoms(path: Path, fieldnames: list[str]) -> int:
    """Stream-copy trades.csv minus the phantom rows; return rows removed."""
    tmp = path.with_suffix(path.suffix + ".cleantmp")
    removed = 0
    with path.open("r", encoding="utf-8", newline="") as src, \
            tmp.open("w", encoding="utf-8", newline="") as dst:
        reader = csv.DictReader(src)
        writer = csv.DictWriter(dst, fieldnames=reader.fieldnames or fieldnames)
        writer.writeheader()
        for row in reader:
            if _is_phantom(row):
                removed += 1
                continue
            writer.writerow(row)
    tmp.replace(path)
    return removed


def _backfill_audit() -> int:
    """Append the two phantom-window outages to the audit CSV (idempotent)."""
    log_path = clob_health.health_log_path(CLOB_HEALTH_FLAG)
    existing = set()
    if log_path.exists():
        with log_path.open("r", encoding="utf-8", newline="") as fh:
            for row in csv.DictReader(fh):
                existing.add((row.get("event"), row.get("timestamp")))
    added = 0
    for w in OUTAGE_BACKFILL:
        key = ("outage_start", w["since"])
        if key in existing:
            continue
        clob_health._append_health_log(
            CLOB_HEALTH_FLAG,
            {
                "event": "outage_start",
                "timestamp": w["since"],
                "reason": "order_post_refused",
                "source": "live_order_post",
                "status_code": 425,
                "cooldown_seconds": 90.0,
                "unhealthy_until": w["until"],
            },
        )
        added += 1
    return added


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--apply", action="store_true", help="actually modify files (default: dry run)")
    ap.add_argument("--force", action="store_true", help="proceed even if trades.csv was just written")
    args = ap.parse_args(argv)

    if not TRADES_CSV.exists():
        print(f"ERROR: {TRADES_CSV} not found (run from BTC_pricer_15m/).", file=sys.stderr)
        return 2

    fieldnames, matched = _scan(TRADES_CSV)
    print(f"Matched {len(matched)} phantom row(s) for instance {INSTANCE_ID}:")
    for row in matched:
        print(f"  {row['timestamp']}  {row['event']:<11} {row['direction']:<4} "
              f"{row['market_id']}  pnl={row.get('pnl', '')}")

    matched_keys = {(r["instance_id"], r["market_id"], r["event"]) for r in matched}
    if matched_keys != EXPECTED or len(matched) != len(EXPECTED):
        print("\nABORT: matched rows differ from the expected 4 phantom rows; "
              "refusing to modify trades.csv. Inspect manually.", file=sys.stderr)
        return 3

    if not args.apply:
        print("\nDRY RUN — no files changed. Re-run with --apply (grid STOPPED) to perform cleanup.")
        print("Would also backfill 2 outage_start rows into "
              f"{clob_health.health_log_path(CLOB_HEALTH_FLAG)}")
        return 0

    age = time.time() - TRADES_CSV.stat().st_mtime
    if age < 30 and not args.force:
        print(f"\nABORT: trades.csv was modified {age:.0f}s ago — the grid may be running. "
              "Stop it (docker compose stop grid) then re-run, or pass --force.", file=sys.stderr)
        return 4

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    backup = TRADES_CSV.with_suffix(f".csv.bak.{ts}")
    backup.write_bytes(TRADES_CSV.read_bytes())
    print(f"\nBacked up trades.csv -> {backup}")

    removed = _rewrite_without_phantoms(TRADES_CSV, fieldnames)
    if removed != len(EXPECTED):
        print(f"ERROR: removed {removed} rows (expected {len(EXPECTED)}). "
              f"Restore from {backup} and investigate.", file=sys.stderr)
        return 5
    print(f"Removed {removed} phantom rows from trades.csv.")

    added = _backfill_audit()
    print(f"Backfilled {added} outage_start row(s) into the audit log.")
    print("\nDone. Restart the grid; recovery rebuilds instance 1271's capital, "
          "PnL, and the leaderboard from the remaining rows.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
