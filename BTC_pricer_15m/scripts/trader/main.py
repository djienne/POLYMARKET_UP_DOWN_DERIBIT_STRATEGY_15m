#!/usr/bin/env python3
"""CLI entry point for the 15-minute BTC Up/Down trading system.

Two modes:
  --mode grid    (default) Run the configured paper parameter grid.
  --mode trader  Run single-instance paper + optional live trader.
"""

import argparse
import atexit
import copy
import csv
import gzip
import json
import logging
import math
import os
import secrets
import signal
import shutil
import socket
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from collections import deque
from dataclasses import replace

# State / trades / equity files should be world-readable so other processes
# (like scp-as-ubuntu for VPS→local sync) can read them even when the
# container runs as root. tempfile.mkstemp defaults to 0600; umask 022 makes
# the rename land at 0644.
os.umask(0o022)
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path

from dotenv import load_dotenv
from btc_pricer.constants import MAX_CONTRACT_PRICE
from scripts.trader._io import atomic_write_json
from scripts.trader.process_lock import (
    CompositeProcessLock,
    RedisLeaseLock,
    build_lock_owner,
    lock_owner_can_use_host_pid,
    lock_owner_is_current_host,
    parse_lock_owner,
)

# Load environment variables from .env file
load_dotenv()


# --- Lockfile ---

_lock_file: Path = Path("results/trader.lock")
_lock_fd: int | None = None
_lock_owner_token: str | None = None
_lock_owner_mode: str = "trader"
_lock_owner_kind: str = "file"


def _inside_docker() -> bool:
    return Path("/.dockerenv").exists()


def _is_pid_alive(pid: int) -> bool:
    """Check if a PID is alive. Works on both Windows and Linux."""
    import subprocess
    if sys.platform == "win32":
        try:
            kwargs = {"capture_output": True, "text": True, "timeout": 5}
            kwargs["creationflags"] = getattr(subprocess, "CREATE_NO_WINDOW", 0)
            result = subprocess.run(
                ["tasklist", "/FI", f"PID eq {pid}", "/FO", "CSV", "/NH"],
                **kwargs
            )
            if result.returncode != 0:
                return False
            for row in csv.reader(result.stdout.splitlines()):
                if len(row) > 1:
                    try:
                        if int(row[1]) == pid:
                            return True
                    except ValueError:
                        continue
            return False
        except Exception:
            return False
    else:
        try:
            os.kill(pid, 0)
            return True
        except OSError:
            return False


def _lock_owner_record() -> dict:
    return build_lock_owner(
        token=_lock_owner_token,
        mode=_lock_owner_mode,
        lock_kind=_lock_owner_kind,
    )


def _parse_lock_owner(content: str) -> dict:
    return parse_lock_owner(content)


def _write_lock_owner(fd: int) -> None:
    os.ftruncate(fd, 0)
    os.lseek(fd, 0, os.SEEK_SET)
    payload = json.dumps(_lock_owner_record(), sort_keys=True)
    os.write(fd, (payload + "\n").encode("utf-8"))
    os.fsync(fd)


def _acquire_posix_lock(lock_path: Path) -> bool:
    global _lock_fd, _lock_owner_token
    import errno
    import fcntl

    fd = os.open(str(lock_path), os.O_RDWR | os.O_CREAT, 0o644)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as exc:
        os.close(fd)
        if exc.errno in (errno.EACCES, errno.EAGAIN):
            try:
                owner = _parse_lock_owner(lock_path.read_text(encoding="utf-8").strip())
                print(
                    "Lockfile held by another process "
                    f"({owner.get('hostname') or '?'}:{owner.get('pid') or '?'})"
                )
            except Exception:
                print("Lockfile held by another process")
            return False
        raise

    os.lseek(fd, 0, os.SEEK_SET)
    existing = os.read(fd, 4096).decode("utf-8", errors="replace").strip()
    if existing:
        try:
            owner = _parse_lock_owner(existing)
        except ValueError:
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)
            print(f"Lockfile exists but owner is unreadable: {lock_path}")
            return False
        legacy_foreign = (
            not owner.get("token")
            and owner.get("hostname")
            and owner.get("hostname") != socket.gethostname()
        )
        if legacy_foreign:
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)
            print(
                f"Legacy lockfile held by another host ({owner.get('hostname')}:{owner.get('pid')}); "
                "remove it manually after verifying no trader is running."
            )
            return False

    _lock_fd = fd
    _lock_owner_token = secrets.token_hex(16)
    _write_lock_owner(fd)
    atexit.register(_release_lock)
    return True


def _acquire_lock(lock_path: Path, *, mode: str = "trader", lock_kind: str = "file") -> bool:
    """Acquire a lockfile to prevent concurrent instances.

    Returns True if lock acquired, False if another instance is running.
    """
    global _lock_file, _lock_owner_token, _lock_owner_mode, _lock_owner_kind
    _lock_file = lock_path
    _lock_owner_mode = mode
    _lock_owner_kind = lock_kind
    _lock_file.parent.mkdir(parents=True, exist_ok=True)

    if sys.platform != "win32":
        return _acquire_posix_lock(_lock_file)

    try:
        fd = os.open(str(_lock_file), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        _lock_owner_token = secrets.token_hex(16)
        os.write(fd, (json.dumps(_lock_owner_record(), sort_keys=True) + "\n").encode("utf-8"))
        os.close(fd)
        atexit.register(_release_lock)
        return True
    except FileExistsError:
        try:
            owner = _parse_lock_owner(_lock_file.read_text(encoding="utf-8").strip())
            lock_host = owner.get("hostname") or ""
            old_pid = int(owner.get("pid") or 0)
            # Different host → live trader runs elsewhere; we cannot check liveness
            # remotely, so refuse rather than risk a dual run.
            if lock_host and not lock_owner_is_current_host(owner):
                print(f"Lockfile held by another host ({lock_host}:{old_pid}); "
                      "run `python manage.py live stop` to release.")
                return False
            if old_pid > 0 and lock_owner_can_use_host_pid(owner) and _is_pid_alive(old_pid):
                return False
            print(f"Removing stale lockfile (host={lock_host or '?'} PID {old_pid} is dead)")
            _lock_file.unlink(missing_ok=True)
            return _acquire_lock(lock_path, mode=mode, lock_kind=lock_kind)
        except (ValueError, OSError):
            print(f"Lockfile exists but owner is unreadable: {_lock_file}")
            return False


def _release_lock() -> None:
    """Release the lockfile if this process still owns it."""
    global _lock_fd, _lock_owner_token
    try:
        owned = False
        if _lock_owner_token:
            try:
                owner = _parse_lock_owner(_lock_file.read_text(encoding="utf-8").strip())
                owned = owner.get("token") == _lock_owner_token
            except Exception:
                owned = False
        if owned:
            _lock_file.unlink(missing_ok=True)
    except OSError:
        pass
    if _lock_fd is not None:
        try:
            if sys.platform != "win32":
                import fcntl
                fcntl.flock(_lock_fd, fcntl.LOCK_UN)
            os.close(_lock_fd)
        except OSError:
            pass
        _lock_fd = None
    _lock_owner_token = None


def _write_shutdown_breadcrumb(
    mode: str,
    reason: str,
    *,
    signal_num: int | None = None,
    exception: BaseException | None = None,
) -> None:
    """Append one line to results/shutdown_log.txt before the process exits.

    Persistent across container restarts (results/ is host-mounted), so we
    can post-mortem a flap when docker has discarded the prior container's
    stdout. Wrapped in a broad except so a breadcrumb failure can never
    block shutdown.
    """
    try:
        path = Path("results/shutdown_log.txt")
        path.parent.mkdir(parents=True, exist_ok=True)
        ts = datetime.now(timezone.utc).isoformat()
        parts = [ts, f"mode={mode}", f"pid={os.getpid()}", f"reason={reason}"]
        if signal_num is not None:
            try:
                sig_name = signal.Signals(signal_num).name
            except Exception:
                sig_name = str(signal_num)
            parts.append(f"signal={sig_name}({signal_num})")
        if exception is not None:
            import traceback
            tb_summary = "".join(
                traceback.format_exception_only(type(exception), exception)
            ).strip().replace("\n", " | ")
            parts.append(f"exc={tb_summary}")
        with path.open("a", encoding="utf-8") as f:
            f.write(" ".join(parts) + "\n")
    except Exception:
        pass


from scripts.trader.config import DEFAULT_DRY_RUN_FRICTION_PCT, GridConfig, InstanceParams
from scripts.trader.data_feed import SharedDataFeed
from scripts.trader.polymarket_status import (
    start_background_refresh as start_status_background_refresh,
)
from scripts.trader.grid_diagnostics import GridEntryDiagnosticsLogger
from scripts.trader.grid_execution import dispatch_grid_tick
from scripts.trader.grid_runtime import (
    GRID_LOCK_PATH,
    GRID_STOP_SENTINEL,
    clear_stale_lock,
    docker_compose_command,
    docker_compose_grid_service,
    docker_grid_is_active,
    ensure_history_preserved,
    find_grid_processes,
    grid_status_snapshot,
    history_state,
    start_grid_process,
    stop_grid_processes,
)
from scripts.trader.mirror_control import (
    MirrorTradeSignal,
    RedisMirrorAckReader,
    RedisMirrorSignalPublisher,
    ack_is_unplaceable_rejection,
    mirror_config_hash,
    resolve_mirror_instance_id,
    signal_trigger_prices,
)
from scripts.trader.leaderboard_ipc import build_leaderboard_publisher
from scripts.trader.trader import PaperInstance
from scripts.trader.trade_log import TradeLogger
from scripts.trader.position import Direction, MicroPosition, TradeEvent
from scripts.trader.summary import (
    print_market_summary,
    print_leaderboard,
    print_status,
    write_leaderboard_csv,
    snapshot_report_instances,
)

log = logging.getLogger("grid_trader")

GRID_PARAM_FIELDS = (
    "alpha_up",
    "alpha_down",
    "floor_up",
    "floor_down",
    "tp_percentage",
    "stop_loss_pct",
)


def setup_logging(verbose: bool = False):
    level = logging.DEBUG if verbose else logging.INFO
    fmt = "%(asctime)s %(levelname)-5s %(name)s: %(message)s"
    logging.basicConfig(level=level, format=fmt, datefmt="%H:%M:%S")
    # Quiet noisy loggers
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("websockets").setLevel(logging.WARNING)
    logging.getLogger("websockets.client").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("hpack").setLevel(logging.WARNING)
    logging.getLogger("h2").setLevel(logging.WARNING)


def run_discover():
    """Discovery mode: print matching 15-min markets and exit."""
    from scripts.polymarket_btc_15m import discover_markets
    markets = discover_markets(n=10)
    if not markets:
        print("No 15-minute BTC markets found on Polymarket.")
        return
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)
    print(f"Found {len(markets)} 15-minute BTC markets:\n")
    for m in markets:
        status = "CLOSED" if m["closed"] else "ACTIVE"
        remaining = ""
        if m["end_date_utc"] and not m["closed"]:
            secs = (m["end_date_utc"] - now).total_seconds()
            if secs > 0:
                remaining = f" ({int(secs // 60)}m {int(secs % 60)}s left)"
            else:
                remaining = " (expired)"
        prob = ""
        if m["prob_up"] is not None:
            prob = f"  UP={m['prob_up']:.1%}  DOWN={m['prob_down']:.1%}"
        print(f"  [{status}] {m['title']}{remaining}")
        print(f"    Slug: {m['slug']}{prob}")
        if m.get("token_ids"):
            for k, v in m["token_ids"].items():
                print(f"    {k} token: {v[:16]}...")
        print()


SNAPSHOT_PATH = Path("results/state_snapshot.json")
LEADERBOARD_PATH = Path("results/leaderboard.csv")
TRADES_PATH = Path("results/trades.csv")
GRID_BACKUP_DIR = Path("results/backups/grid_state")
GRID_BACKUP_INTERVAL_SECONDS = 300
GRID_BACKUP_GZIP_LEVEL = 6
# Tiered retention: keep every backup for the last hour, one per hour for
# the prior 24h, one per day for the prior 7d, drop the rest. Steady-state
# disk footprint per backed-up file kind is ~12+23+6 = ~41 entries.
GRID_BACKUP_RECENT_DENSE_SECONDS = 3600
GRID_BACKUP_HOURLY_HORIZON_SECONDS = 24 * 3600
GRID_BACKUP_DAILY_HORIZON_SECONDS = 7 * 86400
_MIGRATED_BACKUP_DIRS: set[Path] = set()

GRID_CLOSE_EVENTS = {
    "TP_FILLED",
    "TP_SIGNAL_MARKET_EXIT",
    "WIN_EXPIRY",
    "LOSS_EXPIRY",
    "STOP_LOSS",
    "TRAILING_STOP",
    "UNRESOLVED_RESTART",
}
GRID_WIN_EVENTS = {"TP_FILLED", "WIN_EXPIRY"}
GRID_LOSS_EVENTS = {"STOP_LOSS", "LOSS_EXPIRY"}
# Append-only correction marker (written by the offline cleaner and the
# near-real-time auto-amender) that voids a phantom dry-run entry which live
# could not actually place. Recovery replay drops the voided ENTRY and any
# paired close so the leaderboard reflects live-achievable fills.
GRID_VOID_EVENT = "ENTRY_VOID"
_LAST_CSV_RECOVERY: tuple[Path, int] | None = None


def _float_or_zero(value) -> float:
    if value is None or value == "":
        return 0.0
    try:
        return float(str(value).rstrip("%"))
    except (TypeError, ValueError):
        return 0.0


def _parse_iso(value: str | None):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _load_json(path: Path) -> dict:
    import json as _json
    with open(path, encoding="utf-8") as f:
        return _json.load(f)


def _snapshot_trade_total(path: Path) -> int:
    try:
        data = _load_json(path)
    except Exception:
        return 0
    instances = data.get("instances", {})
    if not isinstance(instances, dict):
        return 0
    total = 0
    for raw in instances.values():
        if not isinstance(raw, dict):
            continue
        try:
            total += int(raw.get("trades_count", 0))
        except (TypeError, ValueError):
            pass
    return total


def _leaderboard_trade_total(path: Path) -> int:
    if not path.exists():
        return 0
    total = 0
    try:
        with path.open("r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    total += int(row.get("trades", 0) or 0)
                except ValueError:
                    pass
    except OSError:
        return 0
    return total


def _gzip_copy(src: Path, dst: Path, *, level: int = GRID_BACKUP_GZIP_LEVEL) -> None:
    """Stream src into dst as a gzip file. dst should end in .gz."""
    with src.open("rb") as fin, gzip.open(dst, "wb", compresslevel=level) as fout:
        shutil.copyfileobj(fin, fout)


def _migrate_uncompressed_backups(directory: Path) -> None:
    """One-shot per-process migration: gzip any leftover ``*.bak`` files in
    ``directory`` to ``*.bak.gz`` and remove the originals. Idempotent —
    after the sweep the dir contains only gzipped backups.
    """
    if directory in _MIGRATED_BACKUP_DIRS:
        return
    _MIGRATED_BACKUP_DIRS.add(directory)
    if not directory.exists():
        return
    for src in directory.glob("*.bak"):
        dst = src.with_name(src.name + ".gz")
        try:
            _gzip_copy(src, dst)
            src.unlink(missing_ok=True)
        except OSError as exc:
            log.warning("Could not migrate backup %s: %s", src, exc)


def _select_backups_to_keep(paths: list[Path], now: float) -> set[Path]:
    """Tiered retention selection.

    Keeps everything from the last ``GRID_BACKUP_RECENT_DENSE_SECONDS``
    (typically 1h), one entry per hour bucket up to the daily horizon,
    one entry per day bucket up to the long horizon, and drops the rest.
    User-pinned ``*.manual.*`` files are always preserved.
    """
    keep: set[Path] = set()
    by_hour: dict[int, Path] = {}
    by_day: dict[int, Path] = {}
    for p in paths:
        try:
            mt = p.stat().st_mtime
        except OSError:
            continue
        if ".manual." in p.name:
            keep.add(p)
            continue
        age = now - mt
        if age < 0 or age < GRID_BACKUP_RECENT_DENSE_SECONDS:
            keep.add(p)
        elif age < GRID_BACKUP_HOURLY_HORIZON_SECONDS:
            bucket = int(mt // 3600)
            current = by_hour.get(bucket)
            if current is None or mt > current.stat().st_mtime:
                by_hour[bucket] = p
        elif age < GRID_BACKUP_DAILY_HORIZON_SECONDS:
            bucket = int(mt // 86400)
            current = by_day.get(bucket)
            if current is None or mt > current.stat().st_mtime:
                by_day[bucket] = p
        # else: drop (older than the daily horizon)
    keep.update(by_hour.values())
    keep.update(by_day.values())
    return keep


def _backup_grid_file(
    path: Path,
    min_interval_seconds: int = GRID_BACKUP_INTERVAL_SECONDS,
) -> None:
    """Compress and archive a small derived grid file before it gets
    overwritten. Uses gzip + tiered retention to keep disk usage bounded.
    """
    if not path.exists():
        return
    try:
        GRID_BACKUP_DIR.mkdir(parents=True, exist_ok=True)
        _migrate_uncompressed_backups(GRID_BACKUP_DIR)
        prefix = f"{path.name}."
        existing = sorted(
            GRID_BACKUP_DIR.glob(f"{prefix}*.bak.gz"),
            key=lambda p: p.stat().st_mtime,
        )
        now = time.time()
        if (
            min_interval_seconds > 0
            and existing
            and now - existing[-1].stat().st_mtime < min_interval_seconds
        ):
            return
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        dst = GRID_BACKUP_DIR / f"{path.name}.{ts}.bak.gz"
        _gzip_copy(path, dst)
        existing = list(GRID_BACKUP_DIR.glob(f"{prefix}*.bak.gz"))
        keep = _select_backups_to_keep(existing, now)
        for old in existing:
            if old not in keep:
                old.unlink(missing_ok=True)
    except OSError as exc:
        log.warning("Could not back up %s: %s", path, exc)


def _assert_grid_state_not_regressing(instances) -> None:
    new_total = sum(i.trades_count for i in instances)
    previous_total = max(
        _snapshot_trade_total(SNAPSHOT_PATH),
        _leaderboard_trade_total(LEADERBOARD_PATH),
    )
    if previous_total > new_total and TRADES_PATH.exists() and TRADES_PATH.stat().st_size > 0:
        if (
            _LAST_CSV_RECOVERY is not None
            and _LAST_CSV_RECOVERY[0] == TRADES_PATH.resolve()
            and new_total >= _LAST_CSV_RECOVERY[1]
        ):
            log.warning(
                "Allowing grid history repair save after trades.csv recovery "
                "(%s recovered trades < %s stale derived trades)",
                new_total,
                previous_total,
            )
            return
        raise RuntimeError(
            "Refusing to overwrite grid history with fewer trades "
            f"({new_total} < {previous_total}). Recover from {TRADES_PATH} first."
        )


def void_open_entry(inst, market_id: str, *, now: datetime):
    """Auto-amend (WS6): void the instance's OPEN paper entry for ``market_id``.

    Inverse of ``_commit_entry_plan``: refunds the cost basis, clears the
    position, and returns an ``ENTRY_VOID`` ``TradeEvent`` for the caller to log
    (recovery replay then drops the entry permanently). ``last_entered_market_id``
    is intentionally left set so the grid does not re-enter the same window the
    live trader just failed in. Returns ``None`` if there is no matching open
    position (already closed / never entered) — those are left to the offline
    cleaner, keeping this idempotent.
    """
    pos = getattr(inst, "position", None)
    if pos is None or getattr(pos, "market_id", None) != market_id:
        return None
    cost_basis = float(getattr(pos, "cost_basis", 0.0) or 0.0)
    inst.capital = float(getattr(inst, "capital", 0.0) or 0.0) + cost_basis
    event = TradeEvent(
        instance_id=inst.id,
        timestamp=now,
        event=GRID_VOID_EVENT,
        direction=pos.direction.value if hasattr(pos.direction, "value") else str(pos.direction),
        market_id=market_id,
        entry_price=float(getattr(pos, "entry_price", 0.0) or 0.0),
        shares=float(getattr(pos, "shares", 0.0) or 0.0),
        pnl=0.0,
        capital=inst.capital,
        cost_basis=cost_basis,
        fee_schedule=getattr(pos, "fee_schedule", None),
        contract_metadata=dict(getattr(pos, "contract_metadata", {}) or {}),
        experiment_id=getattr(inst.shared_config, "experiment_id", "legacy"),
    )
    inst.position = None
    return event


def recover_grid_instances_from_csv(
    instances: list[PaperInstance],
    trades_path: Path = TRADES_PATH,
    *,
    starting_capital: float = 1000.0,
    friction_pct: float = DEFAULT_DRY_RUN_FRICTION_PCT,
) -> tuple[int, int]:
    """Rebuild grid stats and open positions from the append-only event log."""
    global _LAST_CSV_RECOVERY
    if not trades_path.exists() or trades_path.stat().st_size == 0:
        return 0, 0

    experiments = {getattr(inst.shared_config, "experiment_id", "legacy") for inst in instances}
    if len(experiments) > 1:
        raise ValueError("cannot recover multiple grid experiments from one trade history")
    expected_experiment = next(iter(experiments), "legacy")

    aggregates: dict[int, dict] = {}
    resolved_market_ids: set[str] = set()

    with trades_path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if not any(value for value in row.values()):
                continue
            row_experiment = str(row.get("experiment_id") or "legacy")
            if row_experiment != expected_experiment:
                raise RuntimeError(
                    f"Trade history experiment {row_experiment!r} does not match configured "
                    f"{expected_experiment!r}; archive the old paper outputs before starting a new experiment"
                )
            try:
                inst_id = int(row.get("instance_id") or -1)
            except ValueError:
                continue
            if inst_id < 0:
                continue
            event = (row.get("event") or "").strip()
            agg = aggregates.setdefault(
                inst_id,
                {
                    "trade_pnls": [],
                    "wins": 0,
                    "losses": 0,
                    "last_tp_sl_time": None,
                    "settled_market_ids": set(),
                    "closed_market_ids": set(),
                    "open_entries": [],
                    "last_entered_market_id": None,
                    "voided_market_ids": set(),
                    "applied_closes": {},
                },
            )

            if event == GRID_VOID_EVENT:
                # Drop a phantom entry (and reverse its close, if already
                # applied) so a fill live could not place leaves no trace.
                market_id = row.get("market_id") or ""
                if not market_id or market_id in agg["voided_market_ids"]:
                    continue  # idempotent: a duplicate void marker is a no-op
                agg["voided_market_ids"].add(market_id)
                agg["open_entries"] = [
                    entry
                    for entry in agg["open_entries"]
                    if (entry.get("market_id") or "") != market_id
                ]
                applied = agg["applied_closes"].pop(market_id, None)
                if applied is not None:
                    # Void arrived after the exit was already counted: reverse it.
                    try:
                        agg["trade_pnls"].remove(applied["pnl"])
                    except ValueError:
                        pass
                    if applied["event"] in GRID_WIN_EVENTS:
                        agg["wins"] = max(0, agg["wins"] - 1)
                    elif applied["event"] in GRID_LOSS_EVENTS:
                        agg["losses"] = max(0, agg["losses"] - 1)
                agg["closed_market_ids"].discard(market_id)
                agg["settled_market_ids"].discard(market_id)
                # Keep last_entered_market_id set (the ENTRY row already set it),
                # matching the runtime void_open_entry: the grid must NOT re-enter
                # the window the live trader just failed in, even after a restart.
                continue

            if event == "ENTRY":
                market_id = row.get("market_id") or ""
                if market_id and market_id in agg["voided_market_ids"]:
                    continue  # entry was voided (e.g. void written before restart)
                if market_id and (
                    market_id in agg["closed_market_ids"]
                    or any((entry.get("market_id") or "") == market_id for entry in agg["open_entries"])
                ):
                    continue
                agg["last_entered_market_id"] = row.get("market_id") or None
                agg["open_entries"].append(row)
                continue

            if event not in GRID_CLOSE_EVENTS:
                continue

            market_id = row.get("market_id") or ""
            if market_id and market_id in agg["voided_market_ids"]:
                continue  # close belongs to a voided entry — ignore
            if market_id and market_id in agg["closed_market_ids"]:
                continue
            matching_entry_idx = None
            if market_id:
                close_entry_price = _float_or_zero(row.get("entry_price"))
                for idx, entry in enumerate(agg["open_entries"]):
                    if (
                        (entry.get("market_id") or "") == market_id
                        and (entry.get("direction") or "") == (row.get("direction") or "")
                        and abs(_float_or_zero(entry.get("entry_price")) - close_entry_price) < 1e-9
                    ):
                        matching_entry_idx = idx
                        break
                if matching_entry_idx is None:
                    # Fallback: the exact entry_price/direction-string match
                    # failed (formatting or precision drift). The ENTRY handler
                    # above guarantees at most one open entry per market_id, so
                    # a (market_id, direction) match cannot mis-pair. The price
                    # tolerance stays small (1e-3) so a close carrying a genuinely
                    # different entry_price — e.g. one paired with a duplicate
                    # ENTRY row this replay already discarded — is still dropped.
                    row_dir = (row.get("direction") or "").strip().upper()
                    for idx, entry in enumerate(agg["open_entries"]):
                        if (
                            (entry.get("market_id") or "") == market_id
                            and (entry.get("direction") or "").strip().upper() == row_dir
                            and abs(
                                _float_or_zero(entry.get("entry_price")) - close_entry_price
                            ) < 1e-3
                        ):
                            matching_entry_idx = idx
                            log.warning(
                                "recovery: close matched by (market_id, direction) "
                                "fallback (inst=%s market=%s event=%s "
                                "close_entry_price=%s entry_entry_price=%s)",
                                inst_id,
                                market_id,
                                event,
                                row.get("entry_price"),
                                entry.get("entry_price"),
                            )
                            break
                if agg["open_entries"] and matching_entry_idx is None:
                    log.warning(
                        "recovery: dropping close row with no matching open entry "
                        "(inst=%s market=%s event=%s direction=%s entry_price=%s pnl=%s)",
                        inst_id,
                        market_id,
                        event,
                        row.get("direction"),
                        row.get("entry_price"),
                        row.get("pnl"),
                    )
                    continue

            pnl = _float_or_zero(row.get("pnl"))
            agg["trade_pnls"].append(pnl)
            if event in GRID_WIN_EVENTS:
                agg["wins"] += 1
            elif event in GRID_LOSS_EVENTS:
                agg["losses"] += 1

            ts = _parse_iso(row.get("timestamp"))
            if event in {"TP_FILLED", "STOP_LOSS"} and ts is not None:
                prev = agg["last_tp_sl_time"]
                if prev is None or ts > prev:
                    agg["last_tp_sl_time"] = ts

            if market_id:
                agg["closed_market_ids"].add(market_id)
                resolved_market_ids.add(market_id)
                if event in {"WIN_EXPIRY", "LOSS_EXPIRY"}:
                    agg["settled_market_ids"].add(market_id)
                # Remember this close so a later ENTRY_VOID can reverse its PnL.
                agg["applied_closes"][market_id] = {"pnl": pnl, "event": event, "timestamp": ts}

            if matching_entry_idx is not None:
                del agg["open_entries"][matching_entry_idx]

    applied = 0
    by_id = {inst.id: inst for inst in instances}
    for inst_id, agg in aggregates.items():
        inst = by_id.get(inst_id)
        if inst is None:
            continue

        trade_pnls = agg["trade_pnls"]
        total_pnl = sum(trade_pnls)
        inst.restore_pnl_history(trade_pnls, total_pnl=total_pnl, pnl_count=len(trade_pnls))
        inst.trades_count = len(trade_pnls)
        inst.total_pnl = total_pnl
        inst.wins = agg["wins"]
        inst.losses = agg["losses"]
        inst.last_tp_sl_time = agg["last_tp_sl_time"]
        dated_closes = [close for close in agg["applied_closes"].values() if close["timestamp"] is not None]
        latest_close = max(dated_closes, key=lambda close: close["timestamp"], default=None)
        inst.last_close_at = latest_close["timestamp"] if latest_close else None
        inst.last_close_result = latest_close["event"] if latest_close else ""
        inst.settled_market_ids.update(agg["settled_market_ids"])
        inst.last_entered_market_id = agg["last_entered_market_id"]

        open_entry = agg["open_entries"][-1] if agg["open_entries"] else None
        if open_entry is not None:
            previous_position = inst.position if inst.position and inst.position.market_id == open_entry.get("market_id") else None
            opened_at = _parse_iso(open_entry.get("timestamp")) or datetime.now(timezone.utc)
            entry_price = _float_or_zero(open_entry.get("entry_price"))
            shares = _float_or_zero(open_entry.get("shares"))
            direction = (open_entry.get("direction") or "UP").strip()
            market_id = open_entry.get("market_id") or ""
            tp_price = min(entry_price * (1 + inst.params.tp_percentage), MAX_CONTRACT_PRICE)
            stop_loss_price = (
                entry_price * (1 - inst.params.stop_loss_pct)
                if inst.params.stop_loss_pct > 0
                else 0.0
            )
            metadata = json.loads(open_entry.get("contract_metadata") or "{}")
            if not isinstance(metadata, dict):
                raise ValueError("invalid contract metadata in grid entry history")
            if not metadata and previous_position is not None:
                metadata = dict(previous_position.contract_metadata)
            fee_raw = open_entry.get("fee_schedule")
            fee_schedule = json.loads(fee_raw) if fee_raw else metadata.get("fee_schedule")
            if fee_schedule is None and previous_position is not None:
                fee_schedule = previous_position.fee_schedule
            raw_cost_basis = open_entry.get("cost_basis")
            if raw_cost_basis not in (None, ""):
                cost_basis = float(raw_cost_basis)
            elif previous_position is not None:
                cost_basis = previous_position.cost_basis
            elif fee_schedule is not None:
                raise ValueError("grid entry with market fees has no recorded cost basis")
            else:
                cost_basis = entry_price * shares * (1 + friction_pct)
            if not (math.isfinite(cost_basis) and cost_basis >= 0):
                raise ValueError("invalid grid entry cost basis")
            inst.position = MicroPosition(
                direction=Direction(direction),
                token_id="",
                entry_price=entry_price,
                shares=shares,
                cost_basis=cost_basis,
                tp_price=tp_price,
                stop_loss_price=stop_loss_price,
                opened_at=opened_at,
                market_id=market_id,
                peak_price=entry_price,
                reference_price=_float_or_zero(open_entry.get("barrier")),
                effective_tp_pct=(
                    (tp_price - entry_price) / entry_price if entry_price > 0 else 0.0
                ),
                fee_schedule=fee_schedule,
                decision_at=_parse_iso(open_entry.get("decision_at")) or opened_at,
                condition_id=str(metadata.get("condition_id") or ""),
                payoff_kind=str(metadata.get("payoff_kind") or "legacy"),
                averaging_window_seconds=int(metadata.get("averaging_window_seconds") or 0),
                contract_metadata=metadata,
            )
            inst.last_entered_market_id = market_id
            inst.capital = starting_capital + total_pnl - cost_basis
        else:
            inst.position = None
            inst.capital = starting_capital + total_pnl
        applied += 1

    if applied:
        _LAST_CSV_RECOVERY = (
            trades_path.resolve(),
            sum(inst.trades_count for inst in instances),
        )
    return applied, len(resolved_market_ids)


def _params_snapshot_dict(params: InstanceParams) -> dict[str, float]:
    return {field: float(getattr(params, field)) for field in GRID_PARAM_FIELDS}


def _snapshot_params_match(snapshot_row: dict, params: InstanceParams) -> bool | None:
    raw = snapshot_row.get("params")
    if raw in (None, ""):
        return None
    if not isinstance(raw, dict):
        return False
    current = _params_snapshot_dict(params)
    try:
        for field, expected in current.items():
            if field not in raw:
                return False
            if abs(float(raw[field]) - expected) > 1e-9:
                return False
    except (TypeError, ValueError):
        return False
    return True


# One worker handles immutable snapshots and reports. Pending saves coalesce
# to the latest state; only the final shutdown flush waits for completion.
_STATE_IO_POOL = ThreadPoolExecutor(max_workers=1, thread_name_prefix="state-io")
_state_io_future = None
_state_io_lock = threading.Lock()
_state_io_running = False
_pending_state_write = None


def _write_snapshot_files(snapshot: dict) -> None:
    _backup_grid_file(SNAPSHOT_PATH)
    try:
        atomic_write_json(
            SNAPSHOT_PATH, {**snapshot, "written_at": datetime.now(timezone.utc).isoformat()},
            indent=None, retries=10,
        )
    except PermissionError as exc:
        log.warning(
            "state snapshot write failed; continuing with in-memory grid state: %s",
            exc,
        )


def _write_state_report(job) -> None:
    snapshot, report_instances, liquidity_mode, allow_history_repair, publisher = job
    if not allow_history_repair:
        _assert_grid_state_not_regressing(report_instances)
    _backup_grid_file(LEADERBOARD_PATH)
    _backup_grid_file(LEADERBOARD_PATH.with_suffix(".txt"))
    _write_snapshot_files(snapshot)
    write_leaderboard_csv(
        report_instances, LEADERBOARD_PATH, liquidity_mode=liquidity_mode,
        as_of=datetime.fromisoformat(snapshot["timestamp"]),
    )
    if publisher is not None:
        try:
            publisher.publish_from_csv(LEADERBOARD_PATH)
        except Exception as exc:  # noqa: BLE001
            log.warning("failed to publish leaderboard snapshot: %s", exc)
    log.info("State saved: %s instances, %s markets resolved (as of %s)",
             len(report_instances), snapshot["markets_resolved"], snapshot["timestamp"])


def _drain_state_writes(job) -> None:
    """At most one active job and one latest replacement; no unbounded queue."""
    global _state_io_running, _pending_state_write
    error = None
    while job is not None:
        try:
            _write_state_report(job)
        except Exception as exc:  # noqa: BLE001 - finish any newer queued snapshot
            error = exc
            log.exception("grid state/report write failed")
        with _state_io_lock:
            job = _pending_state_write
            _pending_state_write = None
            if job is None:
                _state_io_running = False
    if error is not None:
        raise error


def save_state(
    instances,
    markets_resolved=0,
    liquidity_mode: str = "",
    *,
    allow_history_repair: bool = False,
    leaderboard_publisher=None,
    async_io: bool = False,
):
    """Copy a consistent state, then write snapshot and reports on one worker.

    Periodic callers coalesce to the latest pending copy. Final callers wait
    for their copy; neither history scanning nor serialization blocks a tick.
    """
    global _state_io_future, _state_io_running, _pending_state_write
    snapshot = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "experiment_id": getattr(instances[0].shared_config, "experiment_id", "legacy") if instances else "legacy",
        "n_instances": len(instances),
        "markets_resolved": markets_resolved,
        "instances": {},
    }
    for inst in instances:
        snapshot["instances"][str(inst.id)] = inst.to_snapshot()

    job = (snapshot, snapshot_report_instances(instances), liquidity_mode,
           allow_history_repair, leaderboard_publisher)
    with _state_io_lock:
        if _state_io_running:
            _pending_state_write = job
        else:
            _state_io_running = True
            _state_io_future = _STATE_IO_POOL.submit(_drain_state_writes, job)
        future = _state_io_future
    if not async_io:
        future.result()


def try_recover(instances):
    """Try to recover instance state from snapshot. Returns markets_resolved count."""
    if not SNAPSHOT_PATH.exists():
        applied, csv_markets = recover_grid_instances_from_csv(instances)
        if applied:
            total_trades = sum(i.trades_count for i in instances)
            total_pnl = sum(i.total_pnl for i in instances)
            log.warning(
                "No snapshot found; rebuilt %s instances from trades.csv: "
                "%s trades, PnL=$%+,.0f",
                applied,
                total_trades,
                total_pnl,
            )
            return csv_markets
        return 0
    try:
        snapshot = _load_json(SNAPSHOT_PATH)
        expected_experiment = getattr(instances[0].shared_config, "experiment_id", "legacy") if instances else "legacy"
        if snapshot.get("experiment_id", "legacy") != expected_experiment:
            raise RuntimeError(
                "Grid snapshot belongs to a different experiment; archive old paper outputs before starting a new one"
            )
        inst_data = snapshot.get("instances", {})
        if not isinstance(inst_data, dict):
            inst_data = {}

        try:
            n_snap = int(snapshot.get("n_instances", len(inst_data)) or 0)
        except (TypeError, ValueError):
            n_snap = len(inst_data)
        if n_snap > len(instances):
            log.warning(f"Snapshot has {n_snap} instances but config has {len(instances)} "
                        f"— starting fresh")
            applied, csv_markets = recover_grid_instances_from_csv(instances)
            return csv_markets if applied else 0
        if n_snap < len(instances):
            log.warning(
                "Snapshot has %s instances but config has %s; recovering "
                "overlapping ids and starting new ids fresh",
                n_snap,
                len(instances),
            )

        recovered = 0
        legacy_recovered = 0
        skipped_param_mismatch = 0
        skipped_invalid = 0
        for inst in instances:
            key = str(inst.id)
            if key in inst_data:
                row = inst_data[key]
                if not isinstance(row, dict):
                    skipped_invalid += 1
                    log.warning("Snapshot row for instance %s is invalid; skipping", inst.id)
                    continue
                param_match = _snapshot_params_match(row, inst.params)
                if param_match is False:
                    skipped_param_mismatch += 1
                    log.warning(
                        "Snapshot params mismatch for instance %s; saved=%s current=%s; skipping",
                        inst.id,
                        row.get("params"),
                        _params_snapshot_dict(inst.params),
                    )
                    continue
                if param_match is None:
                    legacy_recovered += 1
                inst.load_snapshot(row)
                recovered += 1

        fresh_appended = max(0, len(instances) - n_snap) if n_snap < len(instances) else 0
        if skipped_param_mismatch or skipped_invalid or legacy_recovered or fresh_appended:
            log.warning(
                "Grid snapshot recovery: recovered=%s legacy_missing_params=%s "
                "param_mismatches=%s invalid_rows=%s fresh_appended=%s",
                recovered,
                legacy_recovered,
                skipped_param_mismatch,
                skipped_invalid,
                fresh_appended,
            )

        markets = snapshot.get("markets_resolved", 0) if recovered else 0
        csv_applied, csv_markets = recover_grid_instances_from_csv(instances)
        if csv_applied:
            markets = max(markets, csv_markets)
            log.info(
                "Reconciled grid state from trades.csv: %s instances, "
                "%s closed/open-history markets",
                csv_applied,
                csv_markets,
            )
        total_trades = sum(i.trades_count for i in instances)
        total_pnl = sum(i.total_pnl for i in instances)
        traded = sum(1 for i in instances if i.trades_count > 0)
        log.info(f"Recovered {recovered}/{len(instances)} instances from snapshot: "
                 f"{markets} markets, {total_trades} trades, "
                 f"{traded} active traders, PnL=${total_pnl:+,.0f}")
        return markets
    except Exception as e:
        raise RuntimeError(f"Grid history recovery failed; refusing to start fresh: {e}") from e


_GRID_PENDING_SETTLEMENT_CACHE: dict[tuple[str, str], tuple[float, dict]] = {}


def settle_recovered_grid_positions(instances, tick, settlements_dir: Path) -> list:
    """Resolve old positions after downtime using only their own payout records."""
    pending = {}
    for inst in instances:
        pos = inst.position
        if pos is not None and pos.market_id != tick.market_id:
            pending.setdefault(pos.market_id, []).append(inst)
    events = []
    for market_id, market_instances in pending.items():
        if Path(market_id).name != market_id or "/" in market_id or "\\" in market_id:
            continue
        key = str(Path(settlements_dir).resolve()), market_id
        cached_at, record = _GRID_PENDING_SETTLEMENT_CACHE.get(key, (float("-inf"), {}))
        if time.monotonic() - cached_at >= 10:
            try:
                record = json.loads((Path(settlements_dir) / f"{market_id}.json").read_text(encoding="utf-8"))
                if not isinstance(record, dict):
                    record = {}
            except (OSError, ValueError):
                record = {}
            if len(_GRID_PENDING_SETTLEMENT_CACHE) >= 128:
                _GRID_PENDING_SETTLEMENT_CACHE.pop(next(iter(_GRID_PENDING_SETTLEMENT_CACHE)))
            _GRID_PENDING_SETTLEMENT_CACHE[key] = time.monotonic(), record
        if record.get("market_id") != market_id or record.get("settlement_status") != "finalized":
            continue
        payouts = record.get("settlement_payouts")
        if not isinstance(payouts, dict):
            continue
        expiry_tick = replace(
            tick, market_id=market_id, is_expired=True, is_new_market=False, settlement_pending=False,
            settlement_payouts=payouts, settlement_source=record.get("settlement_source") or "",
        )
        for inst in market_instances:
            if inst.position.condition_id and record.get("condition_id") != inst.position.condition_id:
                continue
            event = inst._settle_expiry(expiry_tick)
            if event is not None:
                event.liquidity_mode = inst.shared_config.liquidity_mode
                events.append(event)
    return events


def _print_grid_status(config_path: str = "config_grid.json"):
    """Show running process and snapshot state for the grid."""
    state = grid_status_snapshot(Path.cwd())
    mains = state["main_processes"]
    children = state["child_processes"]
    snapshot = state["snapshot"]
    docker_service = state.get("docker_service")

    print("\nGRID STATUS")
    print("=" * 50)
    if docker_service:
        docker_line = (
            f"Docker Compose: service={docker_service.get('service')} | "
            f"container={docker_service.get('name')} | "
            f"state={docker_service.get('state') or 'unknown'}"
        )
        if docker_service.get("health"):
            docker_line += f" | health={docker_service['health']}"
        print(docker_line)

    lock_pid = state.get("lock_pid") or "none"
    if docker_grid_is_active(docker_service) and lock_pid != "none":
        print(f"Lock PID: {lock_pid} (container PID namespace; not a host PID)")
        print(f"Host grid processes: {len(mains)}")
        print(f"Host cli_terminal children: {len(children)}")
    else:
        print(f"Lock PID: {lock_pid}")
        print(f"Main grid processes: {len(mains)}")
        print(f"cli_terminal children: {len(children)}")

    for proc in mains:
        print(f"  PID {proc['pid']}: {proc['command_line']}")
    for proc in children:
        print(f"  PID {proc['pid']} <- {proc['ppid']}")

    if snapshot.get("exists"):
        print(
            f"Snapshot: {snapshot.get('timestamp')} | "
            f"instances={snapshot.get('n_instances')} | "
            f"markets={snapshot.get('markets_resolved')}"
        )
    else:
        print("Snapshot: missing")

    cfg_path = Path(config_path)
    if cfg_path.exists():
        try:
            cfg = GridConfig.from_json(cfg_path)
            print(
                f"Config: {cfg_path} | liquidity_mode={cfg.liquidity_mode} | "
                f"recheck={cfg.paper_recheck_delay_seconds}s | settlement_wait={cfg.settlement_wait_seconds}s"
            )
        except Exception as e:
            print(f"Config: {cfg_path} (failed to load: {e})")


def _active_docker_grid_service():
    """Return Docker Compose grid state when the container is actively running."""
    docker_service = docker_compose_grid_service(Path.cwd())
    if docker_grid_is_active(docker_service):
        return docker_service
    return None


def _forward_docker_grid_command(args: list[str], verb: str) -> None:
    """Forward grid lifecycle commands to Docker Compose."""
    print(f"Grid is running under Docker Compose; forwarding to `docker compose {' '.join(args)}`.")
    result = docker_compose_command(Path.cwd(), args, timeout=300)
    if result.returncode != 0:
        raise RuntimeError(f"Docker Compose {verb} failed with exit code {result.returncode}")


def _stop_grid_cli(args):
    """Stop the grid runner without touching history files."""
    docker_service = _active_docker_grid_service()
    if docker_service:
        _forward_docker_grid_command(["stop", "grid"], "stop")
        return

    result = stop_grid_processes(Path.cwd())
    print(f"Grid stop requested. Stopped={result['stopped']} Forced={result['forced']}")
    remaining = result["remaining"]
    if remaining["main_processes"]:
        print("Remaining grid processes:")
        for proc in remaining["main_processes"]:
            print(f"  PID {proc['pid']}: {proc['command_line']}")


def _restart_grid_cli(args):
    """Restart the grid runner while preserving history files."""
    docker_service = _active_docker_grid_service()
    if docker_service:
        _forward_docker_grid_command(["restart", "grid"], "restart")
        return

    before = history_state(Path.cwd())
    result = stop_grid_processes(Path.cwd())
    after_stop = history_state(Path.cwd())
    ensure_history_preserved(before, after_stop)

    if not result["stopped"]:
        raise RuntimeError("Grid processes are still running after stop attempt")

    pid = start_grid_process(Path.cwd(), config_path=args.config, verbose=True)
    print(f"Restarted grid runner (launcher PID {pid}).")


def run_grid(args):
    """Main grid search loop."""
    docker_service = _active_docker_grid_service()
    if docker_service:
        status = docker_service.get("status") or docker_service.get("state") or "running"
        print(f"ERROR: Grid is already running under Docker Compose ({status}).")
        print("Use `docker compose logs -f grid`, `docker compose stop grid`, or `docker compose restart grid`.")
        sys.exit(1)

    lock_path = Path(GRID_LOCK_PATH)
    if not _acquire_lock(lock_path):
        print(f"ERROR: Another BTC_pricer_15m instance is already running (lockfile: {lock_path})")
        sys.exit(1)
    Path(GRID_STOP_SENTINEL).unlink(missing_ok=True)
    clear_stale_lock(Path.cwd())

    config_path = Path(args.config)
    if not config_path.exists():
        print(f"Config not found: {config_path}")
        sys.exit(1)

    config = GridConfig.from_json(config_path)
    leaderboard_publisher = build_leaderboard_publisher(
        config_path,
        fallback_redis_url=config.redis_url,
    )
    params_list = config.generate_params()
    print(f"Generated {len(params_list)} parameter combinations")

    instances = [
        PaperInstance(id=i, params=p, shared_config=config)
        for i, p in enumerate(params_list)
    ]
    mirror_publisher = None
    mirror_instance_id = None
    mirror_instance_hash = ""
    if getattr(config, "mirror_signal_publish_enabled", False):
        from scripts.trader.config import TraderConfig

        live_config_path = Path("config.yaml") if Path("config.yaml").exists() else config_path
        live_config = TraderConfig.from_json(live_config_path)
        mirror_instance_id = resolve_mirror_instance_id(
            grid_config=config,
            live_config=live_config,
            requested=getattr(config, "mirror_signal_instance_id", "auto_from_live"),
        )
        mirror_instance_hash = mirror_config_hash(instances[mirror_instance_id].params)
        mirror_publisher = RedisMirrorSignalPublisher(
            redis_url=config.redis_url,
            stream=getattr(config, "mirror_signal_stream", "trade.mirror.signals"),
        )
        print(
            f"Mirror signals: publishing instance {mirror_instance_id} "
            f"to {config.mirror_signal_stream}"
        )

    # Near-real-time auto-amend (WS6): read the live trader's rejection acks and
    # void the mirror instance's just-committed phantom entry. Opt-in + best-effort.
    mirror_ack_reader = None
    if (
        getattr(config, "grid_auto_amend_enabled", False)
        and mirror_publisher is not None
    ):
        try:
            mirror_ack_reader = RedisMirrorAckReader(
                redis_url=config.redis_url,
                ack_stream=getattr(config, "mirror_ack_stream", "trade.mirror.acks"),
            )
            print(
                f"Mirror auto-amend: voiding phantom entries for instance "
                f"{mirror_instance_id} on live rejection acks"
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("Could not start mirror ack reader (auto-amend disabled): %s", exc)
            mirror_ack_reader = None

    markets_resolved = try_recover(instances)
    if markets_resolved == 0:
        print(f"Starting capital per instance: ${config.starting_capital:,.2f}")
    print(f"No-trade zones: first {config.no_trade_first_seconds}s, "
          f"last {config.no_trade_last_seconds}s")
    print(f"Liquidity mode: {config.liquidity_mode}")
    print()

    feed = SharedDataFeed(
        config,
        hybrid_calibration_enabled=config.hybrid_calibration_enabled,
        hybrid_remote_inbox_enabled=config.hybrid_remote_inbox_enabled,
        hybrid_local_worker_enabled=config.hybrid_local_worker_enabled,
        hybrid_local_worker_delay_min_seconds=config.hybrid_local_worker_delay_min_seconds,
        hybrid_local_worker_delay_max_seconds=config.hybrid_local_worker_delay_max_seconds,
        hybrid_inbox_dir=Path(config.hybrid_inbox_dir) if config.hybrid_inbox_dir else None,
        ipc_backend=config.ipc_backend,
        redis_url=config.redis_url,
        redis_stream=config.redis_stream,
        redis_consumer_group=config.redis_consumer_group,
        redis_consumer_name=config.redis_consumer_name,
        ipc_file_fallback_enabled=config.ipc_file_fallback_enabled,
        hybrid_push_consumer_enabled=getattr(
            config, "hybrid_push_consumer_enabled", True
        ),
        local_prob_eval_enabled=getattr(config, "local_prob_eval_enabled", False),
        local_eval_event_max_age_s=getattr(config, "local_eval_event_max_age_s", 30.0),
        local_eval_spot_max_age_s=getattr(config, "local_eval_spot_max_age_s", 2.0),
    )
    # Pre-warm the Polymarket operational + CLOB-reads probe caches from a
    # background thread so entry gates read cached verdicts instead of
    # blocking mid-dispatch on an expired TTL.
    start_status_background_refresh()
    trade_logger = TradeLogger(Path("results/trades.csv"))
    grid_diag_logger = GridEntryDiagnosticsLogger(Path("results/15m_grid_entry_eval.current.csv"))

    # Graceful shutdown
    shutdown = False
    def _signal_handler(sig, frame):
        nonlocal shutdown
        shutdown = True
        print("\nShutting down...")
        _write_shutdown_breadcrumb(mode="grid", reason="signal", signal_num=sig)
    signal.signal(signal.SIGINT, _signal_handler)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _signal_handler)

    last_market_id = None
    last_status_time = 0
    last_save_time = 0
    tick_count = 0
    _last_lock_touch = time.monotonic()
    _tick_intervals = deque(maxlen=300)
    _tick_durations = deque(maxlen=300)
    _previous_tick_started = None
    _last_cadence_write = 0.0

    print("Starting grid search... (Ctrl+C to stop and see leaderboard)\n")
    _write_shutdown_breadcrumb(mode="grid", reason="started")

    try:
        while not shutdown:
            _tick_started = time.monotonic()
            _now_mono = time.monotonic()
            if _now_mono - _last_lock_touch >= 30:
                try:
                    lock_path.touch(exist_ok=True)
                except OSError:
                    pass
                _last_lock_touch = _now_mono

            if Path(GRID_STOP_SENTINEL).exists():
                log.info("Grid stop sentinel detected - shutting down after current cycle")
                _write_shutdown_breadcrumb(mode="grid", reason="sentinel")
                shutdown = True
                continue

            tick = feed.tick()

            if tick is None:
                if args.verbose:
                    log.info("No active market, scanning...")
                sleep_time = feed.next_sleep()
                if sleep_time > 0:
                    time.sleep(sleep_time)
                continue

            tick_count += 1

            entries_this_tick = 0
            exits_this_tick = 0
            grid_diagnostics = []
            events = settle_recovered_grid_positions(instances, tick, Path("results/settlements"))
            events.extend(dispatch_grid_tick(
                instances,
                tick,
                config,
                diagnostics=grid_diagnostics,
            ))
            grid_diag_logger.log_many(
                grid_diagnostics,
                liquidity_mode=config.liquidity_mode,
            )
            for ev in events:
                trade_logger.log(ev)
                if mirror_publisher is not None and ev.instance_id == mirror_instance_id:
                    bid, ask, mid = signal_trigger_prices(ev, tick)
                    mirror_publisher.publish(
                        MirrorTradeSignal.from_trade_event(
                            ev,
                            market_title=tick.market_title,
                            source_config_hash=mirror_instance_hash,
                            max_age_seconds=getattr(
                                config, "mirror_signal_max_age_seconds", 30.0
                            ),
                            trigger_bid=bid,
                            trigger_ask=ask,
                            trigger_mid=mid,
                        )
                    )
                if ev.event == "ENTRY":
                    entries_this_tick += 1
                else:
                    exits_this_tick += 1

            if entries_this_tick > 0 or exits_this_tick > 0:
                log.info(f"  >> {entries_this_tick} entries, {exits_this_tick} exits this tick")
                trade_logger.flush()
                grid_diag_logger.flush()
            elif grid_diagnostics:
                grid_diag_logger.flush()

            # WS6 auto-amend: drain live rejection acks and void the mirror
            # instance's phantom entries live could not place. Best-effort.
            if mirror_ack_reader is not None and mirror_instance_id is not None:
                try:
                    voided_any = False
                    for ack in mirror_ack_reader.read_new():
                        try:
                            if int(ack.get("source_instance_id", -1)) != mirror_instance_id:
                                continue
                        except (TypeError, ValueError):
                            continue
                        if not ack_is_unplaceable_rejection(ack):
                            continue
                        void_ev = void_open_entry(
                            instances[mirror_instance_id],
                            ack.get("market_id", "") or "",
                            now=datetime.now(timezone.utc),
                        )
                        if void_ev is not None:
                            trade_logger.log(void_ev)
                            voided_any = True
                            log.warning(
                                "auto-amend: voided phantom entry inst=%s market=%s "
                                "(live rejected: %s)",
                                mirror_instance_id,
                                void_ev.market_id,
                                ack.get("reason", ""),
                            )
                    if voided_any:
                        trade_logger.flush()
                except Exception as exc:  # noqa: BLE001 - never break the tick loop
                    log.warning("mirror auto-amend poll failed: %s", exc)

            if mirror_publisher is not None:
                try:
                    mirror_publisher.publish_heartbeat(
                        key=getattr(config, "mirror_heartbeat_key", "trade.mirror.heartbeat"),
                        ttl_seconds=getattr(config, "mirror_heartbeat_ttl_seconds", 120.0),
                        market_id=tick.market_id,
                        market_title=tick.market_title,
                        source_instance_id=mirror_instance_id,
                        source_config_hash=mirror_instance_hash,
                        tick_count=tick_count,
                        entries_this_tick=entries_this_tick,
                        exits_this_tick=exits_this_tick,
                    )
                except Exception as exc:  # noqa: BLE001
                    log.warning("failed to publish mirror heartbeat: %s", exc)

            if tick.is_expired and tick.market_id != last_market_id:
                trade_logger.flush()
                grid_diag_logger.flush()
                markets_resolved += 1
                print_market_summary(
                    instances,
                    tick.market_id,
                    tick.market_title,
                    liquidity_mode=config.liquidity_mode,
                )
                save_state(
                    instances,
                    markets_resolved,
                    liquidity_mode=config.liquidity_mode,
                    leaderboard_publisher=leaderboard_publisher,
                    async_io=True,
                )
                last_market_id = tick.market_id

            # Periodic status with edge info (every 60s)
            now = time.time()
            if now - last_status_time > 60:
                active = sum(1 for i in instances if i.position is not None)
                total_trades = sum(i.trades_count for i in instances)
                total_pnl = sum(i.total_pnl for i in instances)
                settling = " SETTLING" if tick.settlement_pending else ""
                final_source = f" final={tick.final_spot_source}" if tick.final_spot_source else ""
                log.info(
                    f"Tick #{tick_count} ({tick.calibration_seconds:.0f}s calib)  |  "
                    f"Spot=${tick.spot_price:,.0f}  Barrier=${tick.barrier:,.0f}{settling}  "
                    f"[{tick.model_used}]  |  "
                    f"Model: UP={tick.prob_above:.1%} DOWN={tick.prob_below:.1%}  |  "
                    f"Poly: UP={tick.poly_prob_up:.1%} DOWN={tick.poly_prob_down:.1%}  |  "
                    f"Active={active}  Trades={total_trades}  "
                    f"PnL=${total_pnl:+,.0f}  |  "
                    f"Elapsed={tick.elapsed_seconds:.0f}s  "
                    f"Remaining={tick.time_remaining_seconds:.0f}s  "
                    f"Mode={config.liquidity_mode}{final_source}"
                )
                last_status_time = now

                # Capture every 60s; completed writes keep their capture and
                # write timestamps separate so delayed persistence stays visible.
                if now - last_save_time > 60:
                    save_state(
                        instances,
                        markets_resolved,
                        liquidity_mode=config.liquidity_mode,
                        leaderboard_publisher=leaderboard_publisher,
                        async_io=True,
                    )
                    last_save_time = now

            _tick_finished = time.monotonic()
            _tick_durations.append(_tick_finished - _tick_started)
            if _previous_tick_started is not None:
                _tick_intervals.append(_tick_started - _previous_tick_started)
            _previous_tick_started = _tick_started
            if _tick_finished - _last_cadence_write >= 30:
                def percentile(values, fraction):
                    ranked = sorted(values)
                    return ranked[min(len(ranked) - 1, math.ceil(fraction * len(ranked)) - 1)] if ranked else None

                try:
                    atomic_write_json(Path("results/grid_runtime.health.json"), {
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "input_timestamp": tick.timestamp.isoformat(),
                        "market_id": tick.market_id,
                        "experiment_id": getattr(config, "experiment_id", "legacy"),
                        "tick_count": tick_count,
                        "sample_count": len(_tick_intervals),
                        "tick_interval_p50_s": percentile(_tick_intervals, .50),
                        "tick_interval_p95_s": percentile(_tick_intervals, .95),
                        "tick_interval_p99_s": percentile(_tick_intervals, .99),
                        "processing_p95_s": percentile(_tick_durations, .95),
                    })
                except OSError as exc:
                    log.warning("grid cadence report failed: %s", exc)
                _last_cadence_write = _tick_finished

            sleep_time = feed.next_sleep()
            if sleep_time > 0:
                time.sleep(sleep_time)

    except Exception as e:
        log.error(f"Unexpected error: {e}", exc_info=True)
        _write_shutdown_breadcrumb(mode="grid", reason="exception", exception=e)
    finally:
        Path(GRID_STOP_SENTINEL).unlink(missing_ok=True)
        if mirror_publisher is not None:
            mirror_publisher.close()
        if mirror_ack_reader is not None:
            mirror_ack_reader.close()
        trade_logger.close()
        grid_diag_logger.close()
        save_state(
            instances,
            markets_resolved,
            liquidity_mode=config.liquidity_mode,
            leaderboard_publisher=leaderboard_publisher,
        )
        if leaderboard_publisher is not None:
            leaderboard_publisher.close()
        print_leaderboard(instances, top_n=20, liquidity_mode=config.liquidity_mode)


# ---------------------------------------------------------------------------
# Trader mode (single instance, paper + optional live)
# ---------------------------------------------------------------------------

class _TeeStream:
    """Write to both a stream (stdout/stderr) and a logging handler."""

    def __init__(self, original_stream, logger, level):
        self._original = original_stream
        self._logger = logger
        self._level = level

    def write(self, message: str) -> int:
        self._original.write(message)
        if message and message.strip():
            self._logger.log(self._level, message.rstrip())
        return len(message)

    def flush(self) -> None:
        self._original.flush()

    @property
    def encoding(self):
        return getattr(self._original, "encoding", "utf-8")

    def fileno(self):
        return self._original.fileno()

    def isatty(self):
        return self._original.isatty()


def _setup_trader_logging() -> None:
    """Set up rotating log file that captures all trader output.

    Attaches a RotatingFileHandler to the root logger so log.*() calls
    reach the file directly.  A separate tee logger (same handler, no
    propagation) captures print() output via a wrapped stdout.  stderr
    is NOT wrapped — log.*() already reaches the file via the root
    handler, so wrapping would double-log.
    """
    log_path = Path("results/15m_trading.log")
    log_path.parent.mkdir(parents=True, exist_ok=True)

    handler = RotatingFileHandler(
        str(log_path), maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8"
    )
    handler.setFormatter(logging.Formatter("%(asctime)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))

    logging.getLogger().addHandler(handler)

    tee_logger = logging.getLogger("trading_tee")
    tee_logger.setLevel(logging.DEBUG)
    tee_logger.propagate = False
    tee_logger.addHandler(handler)

    sys.stdout = _TeeStream(sys.stdout, tee_logger, logging.INFO)


def _load_trader_state(trader, args) -> bool:
    """Load or initialize state for a trader. Returns True if OK."""
    mode = "LIVE" if trader.executor.is_live else "PAPER"

    if args.reset:
        print(f"[{mode}] Resetting state...")
        trader.state.reset()
        trader.state.save()
        print(f"[{mode}] State reset complete")

    if trader.state.load():
        print(f"[{mode}] Loaded existing state")
    else:
        load_error = getattr(trader.state, "last_load_error", "")
        if trader.executor.is_live and load_error:
            print(f"[{mode}] ERROR: Live state could not be loaded: {load_error}")
            print(f"  Options: --recover or --reset")
            return False
        if trader.state.state_file.exists():
            print(
                f"[{mode}] ERROR: State file {trader.state.state_file} exists but could not "
                f"be loaded (corrupt or version mismatch)."
            )
            print(f"  Options: --recover or --reset")
            return False
        else:
            print(f"[{mode}] Starting fresh")
            trader.state.save()

    if (not trader.state.closed_positions
            and trader.state.trades_file.exists()
            and trader.state.trades_file.stat().st_size > 0):
        print(
            f"[{mode}] WARNING: State has 0 closed trades but trades CSV exists. "
            "Run with --recover to rebuild from CSV."
        )
    return True


def _normalize_trader_config(config, label: str):
    """Resolve contradictory mode flags before constructing executors."""
    if getattr(config, "paper_mode", False) and getattr(config, "live_trading_enabled", False):
        log.warning(
            "%s config has paper_mode=true and live_trading_enabled=true; "
            "using paper mode and disabling live_trading_enabled for this process",
            label,
        )
        config.live_trading_enabled = False
    return config


LIVE_CONFIG_PARITY_FIELDS = (
    "edge_alpha_up",
    "edge_alpha_down",
    "edge_floor_up",
    "edge_floor_down",
    "max_entry_price",
    "order_size_pct",
    "tp_percentage",
    "no_trade_first_seconds",
    "no_trade_last_seconds",
    "min_order_size_usd",
    "min_position_shares",
    "max_slippage_estimate_pct",
    "live_recheck_delay_seconds",
    "live_entry_spot_recheck_enabled",
    "live_entry_spot_recheck_buffer_usd",
    "entry_cancel_timeout_s",
    "live_entry_order_retries",
    "live_entry_retry_window_seconds",
    "live_entry_retry_delay_seconds",
    "live_entry_retry_price_guard",
)


def _config_values_differ(left, right) -> bool:
    if isinstance(left, bool) or isinstance(right, bool):
        return bool(left) != bool(right)
    if isinstance(left, (int, float)) or isinstance(right, (int, float)):
        try:
            return abs(float(left) - float(right)) > 1e-9
        except (TypeError, ValueError):
            return left != right
    return left != right


def _verify_calc_live_config_alignment(
    live_config,
    live_config_path: Path,
    *,
    fail_on_drift: bool = False,
) -> list[str]:
    """Warn or fail if calc_daemon live signals differ from live trader config."""
    from scripts.trader.config import TraderConfig

    config_yaml = Path("config.yaml")
    if not config_yaml.exists():
        message = "config.yaml missing - calc_daemon cannot emit canonical live signals"
        if fail_on_drift:
            raise ValueError(message)
        log.warning(message)
        return [message]
    try:
        calc_live = TraderConfig.from_json(config_yaml)
    except Exception as exc:
        message = f"failed to load calc live signal config from {config_yaml}: {exc}"
        if fail_on_drift:
            raise ValueError(message) from exc
        log.warning(message)
        return [message]

    drift_messages: list[str] = []
    for key in LIVE_CONFIG_PARITY_FIELDS:
        trader_val = getattr(live_config, key)
        calc_val = getattr(calc_live, key)
        if _config_values_differ(calc_val, trader_val):
            message = (
                f"CONFIG DRIFT: calc live signal {key}={calc_val!r} from {config_yaml} "
                f"vs trader {key}={trader_val!r} from {live_config_path}"
            )
            drift_messages.append(message)
            log.warning(message)
    if drift_messages and fail_on_drift:
        raise ValueError(
            "live config drift detected; use --live-config config.yaml or pass "
            "--allow-live-config-drift for a deliberate experiment:\n"
            + "\n".join(drift_messages)
        )
    log.info(
        "live config parity: trader=%s calc=%s alpha=(%.2f/%.2f) floor=(%.2f/%.2f) max_entry=%.2f",
        live_config_path,
        config_yaml,
        live_config.edge_alpha_up,
        live_config.edge_alpha_down,
        live_config.edge_floor_up,
        live_config.edge_floor_down,
        live_config.max_entry_price,
    )
    return drift_messages


def _make_live_shadow_config(live_config):
    shadow = copy.deepcopy(live_config)
    shadow.paper_mode = True
    shadow.live_trading_enabled = False
    shadow.live_shadow_mode = True
    shadow.paper_starting_capital = live_config.starting_capital
    shadow.state_file = Path("results/15m_live_shadow_state.json")
    shadow.trades_file = Path("results/15m_live_shadow_trades.csv")
    shadow.equity_file = Path("results/15m_live_shadow_equity.csv")
    shadow.sqlite_db_file = Path("results/db/15m_live_shadow.sqlite")
    return shadow


def run_trader(args):
    """Single-instance trader mode (paper + optional live)."""
    from scripts.trader.config import TraderConfig
    from scripts.trader.single_trader import SingleTrader
    from scripts.trader.data_feed import SharedDataFeed

    _setup_trader_logging()

    # --- Load configs ---
    trader_config_path = Path(args.trader_config)
    live_config_path = Path(args.live_config)

    if args.paper_only and args.live_only:
        print("ERROR: --paper-only and --live-only are mutually exclusive")
        sys.exit(2)
    if args.live_shadow and args.live_only:
        print("ERROR: --live-shadow is a paper-mode comparator and cannot be combined with --live-only")
        sys.exit(2)

    paper_config = None
    if not args.live_only:
        if trader_config_path.exists():
            paper_config = TraderConfig.from_json(trader_config_path)
        else:
            paper_config = TraderConfig()
            paper_config.to_json(trader_config_path)
            print(f"Created default paper config: {trader_config_path}")
        paper_config = _normalize_trader_config(paper_config, "paper")

    live_config = None
    if not args.paper_only and live_config_path.exists():
        if live_config_path.name == "config_trader_live.json":
            log.warning(
                "config_trader_live.json is legacy; prefer --live-config config.yaml "
                "so calc and live use one source of truth"
            )
        live_config = TraderConfig.from_json(live_config_path)
        if live_config.paper_mode:
            log.warning(
                "live config %s has paper_mode=true; disabling live trader for safety",
                live_config_path,
            )
            live_config = None

    if live_config:
        try:
            _verify_calc_live_config_alignment(
                live_config,
                live_config_path,
                fail_on_drift=not getattr(args, "allow_live_config_drift", False),
            )
        except ValueError as exc:
            print(f"ERROR: {exc}")
            sys.exit(2)

    if getattr(args, "live_shadow", False):
        if live_config is not None:
            paper_config = _make_live_shadow_config(live_config)
            print("Live-shadow mode: paper trader uses live config + live entry gates")
        elif paper_config is not None:
            paper_config.live_shadow_mode = True
            print("Live-shadow mode: no live config loaded; applying live gates to paper config")

    if paper_config is not None:
        print(f"Loading paper config from {trader_config_path}")
    else:
        print("Paper trading: disabled (--live-only)")
    if live_config:
        print(f"Loading live config from {live_config_path}")
    else:
        print("Live trading: disabled (no live config or --paper-only)")

    if paper_config is None and live_config is None:
        print("ERROR: no trader configured — paper and live both disabled.")
        sys.exit(2)

    # --- Create traders ---
    trader_paper = (
        SingleTrader(paper_config, verbose=args.verbose) if paper_config is not None else None
    )
    trader_live = SingleTrader(live_config, verbose=args.verbose) if live_config else None

    traders: list[SingleTrader] = []
    if trader_paper:
        traders.append(trader_paper)
    if trader_live:
        traders.append(trader_live)

    # --- Recovery mode ---
    if args.recover:
        for trader in traders:
            mode = "LIVE" if trader.executor.is_live else "PAPER"
            print(f"[{mode}] Recovering state from trades CSV...")

            override_capital = None
            if trader.executor.is_live:
                try:
                    usdc_balance = trader.executor.get_usdc_balance()
                    if usdc_balance is not None:
                        override_capital = usdc_balance
                        print(f"[{mode}] USDC balance on Polymarket: ${usdc_balance:.2f}")
                except Exception as e:
                    print(f"[{mode}] WARNING: Failed to query USDC balance: {e}")

            if trader.state.recover_from_csv(
                last_n=args.last,
                override_capital=override_capital,
                preserve_open_positions=trader.executor.is_live,
            ):
                print(f"[{mode}] Recovery complete")
            else:
                print(f"[{mode}] Recovery failed")
                sys.exit(1)
        return

    # --- Load state ---
    for trader in traders:
        if not _load_trader_state(trader, args):
            sys.exit(1)

    # --- Status mode ---
    if args.status:
        for trader in traders:
            mode = "LIVE" if trader.executor.is_live else "PAPER"
            print(f"\n{'='*50}")
            print(f"  {mode} TRADING STATUS (15-min)")
            print(f"{'='*50}")
            live_equity = trader._get_live_equity()
            trader.state.display_status(
                trader.get_open_position_value, live_equity=live_equity
            )
        return

    # --- Acquire lockfile (separate from grid) ---
    lock_path = Path("results/single_trader.lock")
    shutdown = False
    lock_loss_reason: str | None = None
    process_lock: CompositeProcessLock | None = None
    file_lock_acquired = False
    live_runtime = any(trader.executor.is_live for trader in traders)

    def _on_process_lock_lost(reason: str) -> None:
        nonlocal shutdown, lock_loss_reason
        lock_loss_reason = reason
        shutdown = True
        log.critical("Live process lock lost: %s", reason)
        _write_shutdown_breadcrumb(mode="trader", reason=f"process_lock_lost:{reason}")

    def _release_process_guard() -> None:
        nonlocal file_lock_acquired
        if process_lock is not None:
            process_lock.release()
        elif file_lock_acquired:
            _release_lock()
            file_lock_acquired = False

    if live_runtime and live_config and live_config.redis_lock_enabled:
        redis_owner = build_lock_owner(mode="live", lock_kind="redis_lease")
        redis_lock = RedisLeaseLock(
            redis_url=live_config.redis_url,
            key=live_config.redis_lock_key,
            ttl_ms=live_config.redis_lock_ttl_ms,
            owner=redis_owner,
        )
        process_lock = CompositeProcessLock(
            redis_lock=redis_lock,
            file_acquire=lambda: _acquire_lock(lock_path, mode="live", lock_kind="file_flock"),
            file_release=_release_lock,
            renew_interval_ms=live_config.redis_lock_renew_ms,
            fail_closed=live_config.redis_lock_fail_closed,
        )
        if not process_lock.acquire():
            owner_hint = ""
            try:
                owner = redis_lock.current_owner()
                if owner:
                    owner_hint = (
                        f" (redis owner host={owner.get('hostname') or '?'} "
                        f"pid={owner.get('pid') or '?'} container={owner.get('container_id') or '?'})"
                    )
            except Exception:
                owner_hint = ""
            print(
                "ERROR: Another live trader instance is already running "
                f"(redis lock: {live_config.redis_lock_key}, file lock: {lock_path}){owner_hint}"
            )
            sys.exit(1)
        process_lock.start_renewal(_on_process_lock_lost)
    else:
        if not _acquire_lock(lock_path, mode="trader", lock_kind="file"):
            print(f"ERROR: Another trader instance is already running (lockfile: {lock_path})")
            sys.exit(1)
        file_lock_acquired = True

    # --- Startup ---
    for trader in traders:
        if not trader.startup(confirm_live=True):
            print("Live trading declined. Exiting.")
            _release_process_guard()
            sys.exit(0)

    # --- Create shared data feed ---
    config_yaml_path = Path("config.yaml")
    if config_yaml_path.exists():
        grid_config = GridConfig.from_json(config_yaml_path)
    else:
        grid_config = GridConfig()
    hybrid_cfg = live_config if live_config and not args.paper_only else None
    feed = SharedDataFeed(
        grid_config,
        hybrid_calibration_enabled=bool(
            hybrid_cfg and hybrid_cfg.hybrid_calibration_enabled
        ),
        hybrid_remote_inbox_enabled=bool(
            hybrid_cfg and hybrid_cfg.hybrid_remote_inbox_enabled
        ),
        hybrid_local_worker_enabled=bool(
            hybrid_cfg and hybrid_cfg.hybrid_local_worker_enabled
        ),
        hybrid_local_worker_delay_min_seconds=(
            hybrid_cfg.hybrid_local_worker_delay_min_seconds if hybrid_cfg else 0.0
        ),
        hybrid_local_worker_delay_max_seconds=(
            hybrid_cfg.hybrid_local_worker_delay_max_seconds if hybrid_cfg else 0.0
        ),
        hybrid_inbox_dir=(
            Path(hybrid_cfg.hybrid_inbox_dir)
            if hybrid_cfg and hybrid_cfg.hybrid_inbox_dir
            else None
        ),
        ipc_backend=hybrid_cfg.ipc_backend if hybrid_cfg else "file",
        redis_url=hybrid_cfg.redis_url if hybrid_cfg else None,
        redis_stream=hybrid_cfg.redis_stream if hybrid_cfg else "market.calibration",
        redis_consumer_group=hybrid_cfg.redis_consumer_group if hybrid_cfg else "live",
        redis_consumer_name=hybrid_cfg.redis_consumer_name if hybrid_cfg else "live",
        ipc_file_fallback_enabled=(
            hybrid_cfg.ipc_file_fallback_enabled if hybrid_cfg else True
        ),
        hybrid_push_consumer_enabled=(
            getattr(hybrid_cfg, "hybrid_push_consumer_enabled", True)
            if hybrid_cfg
            else True
        ),
        local_prob_eval_enabled=(
            getattr(hybrid_cfg, "local_prob_eval_enabled", False)
            if hybrid_cfg
            else False
        ),
        local_eval_event_max_age_s=(
            getattr(hybrid_cfg, "local_eval_event_max_age_s", 30.0)
            if hybrid_cfg
            else 30.0
        ),
        local_eval_spot_max_age_s=(
            getattr(hybrid_cfg, "local_eval_spot_max_age_s", 2.0)
            if hybrid_cfg
            else 2.0
        ),
    )
    start_status_background_refresh()

    # --- Run once mode ---
    if args.once:
        tick = feed.tick()
        if tick:
            for trader in traders:
                trader.on_tick(tick)
            for trader in traders:
                mode = "LIVE" if trader.executor.is_live else "PAPER"
                print(f"\n--- {mode} ---")
                trader.state.display_status(
                    trader.get_open_position_value, live_equity=trader._get_live_equity()
                )
        else:
            print("No active market found.")
        feed.shutdown()
        _release_process_guard()
        return

    # --- Main loop ---
    def _signal_handler(sig, frame):
        nonlocal shutdown
        shutdown = True
        print("\nShutting down...")
        _write_shutdown_breadcrumb(mode="trader", reason="signal", signal_num=sig)
    signal.signal(signal.SIGINT, _signal_handler)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _signal_handler)

    print("Starting trader... (Ctrl+C to stop)\n")
    _write_shutdown_breadcrumb(mode="trader", reason="started")
    _last_lock_touch = time.monotonic()

    try:
        while not shutdown:
            _now_mono = time.monotonic()
            if _now_mono - _last_lock_touch >= 30:
                try:
                    lock_path.touch(exist_ok=True)
                except OSError:
                    pass
                _last_lock_touch = _now_mono

            tick = feed.tick()
            if shutdown:
                break

            if tick is None:
                sleep_time = feed.next_sleep()
                if sleep_time > 0:
                    time.sleep(sleep_time)
                continue

            # Dispatch tick to all traders
            for trader in traders:
                trader.on_tick(tick)

            sleep_time = feed.next_sleep()
            if sleep_time > 0:
                time.sleep(sleep_time)

    except Exception as e:
        log.error(f"Unexpected error: {e}", exc_info=True)
        _write_shutdown_breadcrumb(mode="trader", reason="exception", exception=e)
    finally:
        if lock_loss_reason:
            print(f"Stopping trader because process lock was lost: {lock_loss_reason}")
        feed.shutdown()
        for trader in traders:
            trader.shutdown()
        _release_process_guard()


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="15-minute BTC Up/Down trading system"
    )
    parser.add_argument(
        "--mode", choices=["grid", "trader"], default="grid",
        help="Mode: 'grid' (configured paper instances) or 'trader' (single paper+live)"
    )
    parser.add_argument(
        "--config", default="config_grid.json",
        help="Path to grid config YAML/JSON (default: config_grid.json)"
    )
    parser.add_argument(
        "--discover", action="store_true",
        help="Search Gamma API for 15-min markets and exit"
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true",
        help="Verbose logging"
    )
    # Trader-mode arguments
    parser.add_argument(
        "--trader-config", default="config_trader.json",
        help="Path to paper trader config (default: config_trader.json)"
    )
    parser.add_argument(
        "--live-config", default="config.yaml",
        help="Path to live trader config (default: config.yaml live: section)"
    )
    parser.add_argument(
        "--paper-only", action="store_true",
        help="[trader mode] Skip live even if config exists"
    )
    parser.add_argument(
        "--live-only", action="store_true",
        help="[trader mode] Skip the paper SingleTrader; run live only. "
             "Useful for the dedicated live-real-money container so paper "
             "trader noise doesn't share the calibration/log stream."
    )
    parser.add_argument(
        "--live-shadow", action="store_true",
        help="[trader mode] Paper mode with live entry gates (slippage, delayed book, spot drift). "
             "Reveals entries that paper would take but live would reject."
    )
    parser.add_argument(
        "--allow-live-config-drift", action="store_true",
        help="[trader mode] Permit live trader config to differ from config.yaml live: signal params."
    )
    parser.add_argument(
        "--status", action="store_true",
        help="[grid/trader mode] Show current status and exit"
    )
    parser.add_argument(
        "--stop", action="store_true",
        help="[grid mode] Gracefully stop the running grid without touching history files"
    )
    parser.add_argument(
        "--restart", action="store_true",
        help="[grid mode] Stop then relaunch the grid while preserving history"
    )
    parser.add_argument(
        "--reset", action="store_true",
        help="[trader mode] Reset state and start fresh"
    )
    parser.add_argument(
        "--recover", action="store_true",
        help="[trader mode] Recover state from trades CSV"
    )
    parser.add_argument(
        "--last", type=int, metavar="N", default=None,
        help="[trader mode] With --recover: only recover last N trades"
    )
    parser.add_argument(
        "--once", action="store_true",
        help="[trader mode] Run one tick and exit"
    )

    args = parser.parse_args()

    setup_logging(args.verbose)

    if args.discover:
        run_discover()
    elif args.mode == "trader":
        run_trader(args)
    elif args.stop:
        _stop_grid_cli(args)
    elif args.restart:
        _restart_grid_cli(args)
    elif args.status:
        _print_grid_status(config_path=args.config)
    else:
        run_grid(args)


if __name__ == "__main__":
    main()
