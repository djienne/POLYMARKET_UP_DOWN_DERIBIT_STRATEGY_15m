"""Shared safety helpers for host-run maintenance CLIs that touch results/.

These CLIs (phantom cleaner, trades-header migration) rewrite or append to
files the grid is normally the sole writer of, so they must refuse to run
while a grid writer is plausibly alive. Lock-mtime freshness is the primary
liveness signal because it works across the Docker boundary, where PIDs do
not.
"""
from __future__ import annotations

import csv
import gzip
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path

from scripts.trader.grid_runtime import is_pid_alive
from scripts.trader.process_lock import lock_owner_can_use_host_pid, read_lock_owner

# trader.lock heartbeat: the grid touches it every 30s; the Docker healthcheck
# treats mtime < 120s as alive. Mirror that here.
LOCK_HEARTBEAT_FRESH_SECONDS = 120.0


def grid_lock_is_live(trades_path: Path) -> tuple[bool, str]:
    """Is a grid writer plausibly alive? Lock-mtime freshness is the primary
    signal (works across the Docker boundary, where PIDs do not); host-namespace
    PID liveness is the fallback for a stale heartbeat."""
    lock = trades_path.parent / "trader.lock"
    if not lock.exists():
        return False, "no trader.lock"
    try:
        age = time.time() - lock.stat().st_mtime
    except OSError as exc:
        return True, f"trader.lock unreadable ({exc}); assuming live"
    if age < LOCK_HEARTBEAT_FRESH_SECONDS:
        return True, f"trader.lock heartbeat {age:.0f}s old (grid touches it every 30s)"
    owner = read_lock_owner(lock)
    if owner and lock_owner_can_use_host_pid(owner):
        try:
            pid = int(owner.get("pid") or 0)
        except (TypeError, ValueError):
            pid = 0
        if is_pid_alive(pid):
            return True, f"trader.lock pid {pid} alive (heartbeat stale {age:.0f}s)"
    return False, f"stale trader.lock (heartbeat {age:.0f}s)"


def backup_trades(trades_path: Path) -> Path:
    """Gzip-copy the trades log to a timestamped sibling before mutating it."""
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    dst = trades_path.with_name(f"{trades_path.name}.{ts}.bak.gz")
    with trades_path.open("rb") as src, gzip.open(dst, "wb") as out:
        shutil.copyfileobj(src, out)
    return dst


def read_csv_header(trades_path: Path) -> list[str]:
    """Return the first line of a CSV as a list of column names."""
    with trades_path.open("r", newline="", encoding="utf-8") as fh:
        first = fh.readline().strip()
    return [c.strip() for c in first.split(",") if c.strip()]


def count_data_rows(path: Path) -> int:
    """Count CSV data rows (excluding the header) with a streaming reader.

    Uses ``csv.reader`` rather than line counting so quoted newlines and mixed
    line endings are handled correctly.
    """
    with path.open("r", newline="", encoding="utf-8") as fh:
        reader = csv.reader(fh)
        next(reader, None)  # header
        return sum(1 for _ in reader)
