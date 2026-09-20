"""One-time migration: rewrite a legacy trades.csv with the full 18-column header.

TradeLogger freezes its output schema to whatever header the existing file has
(``trade_log.py`` header inference), so a file created before the
``liquidity_mode`` / ``settlement_source`` / ``effective_tp_pct`` columns were
added keeps silently dropping those fields on every new row. This CLI rewrites
the file atomically with the current full ``HEADER``, padding old rows with
empty cells (all consumers read by column name, so ``""`` is equivalent to the
column never having existed).

Must run with the grid stopped — it refuses while ``trader.lock`` shows a live
heartbeat. Report-only by default; ``--apply`` performs the rewrite after a
gzip backup. Idempotent: a file that already has the full header is a no-op.

Usage
-----
    python -m scripts.trader.cli.migrate_trades_header --trades results/trades.csv
    python -m scripts.trader.cli.migrate_trades_header --trades results/trades.csv --apply
"""
from __future__ import annotations

import argparse
import csv
import shutil
import sys
import time
from pathlib import Path
from typing import Iterator, Optional

from scripts.trader._io import atomic_write_csv_rows
from scripts.trader.cli._shared import (
    backup_trades,
    count_data_rows,
    grid_lock_is_live,
    read_csv_header,
)
from scripts.trader.trade_log import HEADER

DEFAULT_MIN_QUIET_SECONDS = 30.0
# The rewrite needs the original + same-dir temp copy + gzip backup at once.
DISK_HEADROOM_FACTOR = 2.5

# DictReader restkey for cells beyond the file's header (crash garbage from
# pre-newline-repair partial writes). Deliberately not a HEADER column name.
_EXTRA = "__extra_cells__"


def _scan(trades_path: Path, current: list[str]) -> dict:
    """Streaming pre-scan: row count plus short/extra-cell row counts."""
    stats = {"rows": 0, "short_rows": 0, "extra_cell_rows": 0}
    with trades_path.open("r", newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh, restkey=_EXTRA)
        for row in reader:
            stats["rows"] += 1
            if row.get(_EXTRA):
                stats["extra_cell_rows"] += 1
            if row.get(current[-1]) is None:
                stats["short_rows"] += 1
    return stats


def _padded_rows(trades_path: Path, stats: dict) -> Iterator[list[str]]:
    """Yield every data row mapped by name onto the full HEADER.

    Short rows are padded with "" (kept, never dropped); cells beyond the old
    header are unnamed garbage and are dropped (counted; the gzip backup keeps
    the originals).
    """
    stats["rows"] = 0
    with trades_path.open("r", newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh, restkey=_EXTRA)
        for row in reader:
            stats["rows"] += 1
            yield [(row.get(col) or "") for col in HEADER]


def run(args: argparse.Namespace) -> int:
    trades = Path(args.trades) if args.trades else Path(args.results) / "trades.csv"
    if not trades.exists():
        print(f"ERROR: {trades} does not exist.")
        return 1
    if trades.stat().st_size == 0:
        print(f"{trades} is empty; TradeLogger will write a fresh full header itself. Nothing to do.")
        return 0

    current = read_csv_header(trades)
    print("=" * 78)
    print("trades.csv header migration")
    print("=" * 78)
    print(f"File:    {trades}  ({trades.stat().st_size / 1e6:.1f} MB)")
    print(f"Current: {len(current)} columns: {','.join(current)}")
    print(f"Target:  {len(HEADER)} columns: {','.join(HEADER)}")

    if current == HEADER:
        print("\nHeader already current. Nothing to do.")
        return 0

    unknown = [c for c in current if c not in HEADER]
    missing = [c for c in HEADER if c not in current]
    print(f"Columns to add (padded with empty cells): {missing or 'none'}")
    if unknown:
        print(
            f"\nABORT: existing header has columns not in the current schema: {unknown}. "
            "By-name migration would drop their data; refusing. Migrate manually."
        )
        return 2

    stats = _scan(trades, current)
    print(
        f"Data rows: {stats['rows']:,} "
        f"(short rows to pad: {stats['short_rows']:,}, "
        f"rows with extra cells to drop: {stats['extra_cell_rows']:,})"
    )

    if not args.apply:
        print("\nReport-only. Re-run with --apply to rewrite the header.")
        return 0

    live, why = grid_lock_is_live(trades)
    if live and not args.force:
        print(f"\nABORT: grid appears to be running ({why}). Stop the grid or pass --force.")
        return 2

    age = time.time() - trades.stat().st_mtime
    if age < args.min_quiet_seconds and not args.force:
        print(
            f"\nABORT: {trades.name} modified {age:.1f}s ago "
            f"(< {args.min_quiet_seconds}s). Stop the grid or pass --force."
        )
        return 2

    free = shutil.disk_usage(trades.parent).free
    needed = int(trades.stat().st_size * DISK_HEADROOM_FACTOR)
    if free < needed:
        print(
            f"\nABORT: insufficient free disk space "
            f"({free / 1e9:.1f} GB free < {needed / 1e9:.1f} GB needed). Not bypassable."
        )
        return 2

    backup = backup_trades(trades)
    print(f"\nBacked up -> {backup.name}")

    expected_rows = stats["rows"]
    atomic_write_csv_rows(trades, HEADER, _padded_rows(trades, stats))
    if stats["extra_cell_rows"]:
        print(
            f"NOTE: {stats['extra_cell_rows']:,} rows had cells beyond the old header; "
            f"extras ignored (originals preserved in {backup.name})."
        )

    new_header = read_csv_header(trades)
    new_rows = count_data_rows(trades)
    if new_header != HEADER or new_rows != expected_rows:
        print(
            f"\nERROR: post-rewrite verification failed "
            f"(header ok: {new_header == HEADER}, rows {new_rows:,} vs {expected_rows:,}). "
            f"Original preserved in {backup.name}; do NOT restart the grid until resolved."
        )
        return 1

    print(f"Verified: header OK ({len(HEADER)} cols); rows {expected_rows:,} -> {new_rows:,}.")
    print("Restart the grid; the legacy-header warning should no longer appear.")
    return 0


def main(argv: Optional[list[str]] = None) -> int:
    p = argparse.ArgumentParser(
        prog="migrate_trades_header",
        description="Rewrite a legacy trades.csv with the full TradeLogger header (grid must be stopped).",
    )
    p.add_argument("--results", default="results", help="results dir (default: results)")
    p.add_argument("--trades", help="explicit trades.csv path (overrides --results)")
    p.add_argument("--apply", action="store_true", help="rewrite the file (default: report only)")
    p.add_argument("--force", action="store_true", help="bypass the grid-lock and quiet-period guards")
    p.add_argument("--min-quiet-seconds", type=float, default=DEFAULT_MIN_QUIET_SECONDS)
    return run(p.parse_args(argv))


if __name__ == "__main__":
    sys.exit(main())
