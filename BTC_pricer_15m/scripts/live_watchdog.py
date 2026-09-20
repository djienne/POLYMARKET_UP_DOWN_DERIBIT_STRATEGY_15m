#!/usr/bin/env python
"""Reactive watchdog for the local live trader container."""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import os
import queue
import re
import shutil
import subprocess
import threading
import time
from dataclasses import dataclass
from pathlib import Path

from scripts.trader.config import GridConfig, TraderConfig
from scripts.trader.mirror_control import resolve_mirror_instance_id
from scripts.live_runtime_state import latch_live_stop, safety_trigger_path


FATAL_RE = re.compile(
    r"("
    r"\bERROR\s|Traceback \(most recent call last\)|\bCRITICAL\s|"
    r"\bphantom\b|\bVOID\b|\bREJECT\b|settlement_abandoned|"
    r"tp_post_close_balance_unreadable|Could not determine position|"
    r"buy order may still be live|sell order may still be live"
    r")",
    re.IGNORECASE,
)

IGNORE_RE = re.compile(
    r"("
    r"Cloudflare|Attention Required|status=403|auth/api-key|"
    r"cf\.errors|cf_styles|cf-[\w-]*error|cf-alert|cf-wrapper|"
    r"cf-ray|cdn-cgi|enable_cookies|Please enable cookies|"
    r"rejecting shared_calc event|"
    r"SSVI surface fit failed.*fell back|"
    r"no fresh calibration.*holding uninformative|"
    r"awaiting calc|"
    r"No orderbook exists|"
    r"py_clob_client.*status=404|"
    r"PolyApiException.*404|"
    r"\border book empty\b|"
    r"Error fetching order book|"
    r"Network is unreachable|"
    r"Failed to establish a new connection|"
    r"PTB fetch error|"
    r"429.*Too Many Requests|"
    r"CLOB collateral query failed|"
    r"Request exception|"
    r"PolyApiException"
    r")",
    re.IGNORECASE,
)

HTML_MARKUP_RE = re.compile(r"(^|\|\s*)</?[A-Za-z!]", re.IGNORECASE)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Stop local live trading and capture diagnostics on fatal log patterns."
    )
    parser.add_argument("--since-seconds", type=int, default=10)
    parser.add_argument("--service", default="live")
    parser.add_argument("--profile", default="local-live")
    parser.add_argument("--diagnostics-root", default="results/diagnostics")
    parser.add_argument("--log-tail", type=int, default=600)
    parser.add_argument("--container-name", default="btc_pricer_15m_live")
    parser.add_argument("--parity-enabled", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--parity-instance", type=int, default=None)
    parser.add_argument("--parity-interval-seconds", type=float, default=15.0)
    parser.add_argument("--parity-lookback-minutes", type=float, default=15.0)
    parser.add_argument("--parity-entry-grace-seconds", type=float, default=20.0)
    parser.add_argument("--parity-close-grace-seconds", type=float, default=60.0)
    parser.add_argument("--parity-close-time-tol-seconds", type=float, default=90.0)
    parser.add_argument("--parity-time-tol-seconds", type=float, default=30.0)
    parser.add_argument("--parity-price-tol", type=float, default=0.10)
    return parser.parse_args()


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def timestamp() -> str:
    return dt.datetime.now().strftime("%Y%m%d_%H%M%S")


def now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).astimezone().isoformat()


def run_capture(args: list[str], out_file: Path, timeout: int = 60) -> int:
    out_file.parent.mkdir(parents=True, exist_ok=True)
    try:
        proc = subprocess.run(
            args,
            cwd=repo_root(),
            text=True,
            encoding="utf-8",
            errors="replace",
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=timeout,
        )
        text = proc.stdout or ""
        rc = proc.returncode
        if rc:
            text += f"\nCOMMAND EXITED {rc}: {' '.join(args)}\n"
    except Exception as exc:  # noqa: BLE001 - diagnostic capture must not fail hard
        text = f"COMMAND FAILED: {' '.join(args)}\n{exc!r}\n"
        rc = -1
    out_file.write_text(text, encoding="utf-8")
    return rc


def copy_if_exists(src: str, dst_dir: Path) -> None:
    path = repo_root() / src
    if path.exists():
        shutil.copy2(path, dst_dir / path.name)


def write_pid_file(args: argparse.Namespace, diagnostics_root: Path) -> None:
    diagnostics_root.mkdir(parents=True, exist_ok=True)
    (diagnostics_root / "live_watchdog.pid").write_text(
        "\n".join(
            [
                f"started_at={now_iso()}",
                f"pid={os.getpid()}",
                f"repo={repo_root()}",
                f"service={args.service}",
                f"profile={args.profile}",
                f"since_seconds={args.since_seconds}",
                "implementation=python",
                f"parity_enabled={args.parity_enabled}",
                f"parity_instance={resolve_parity_instance(args)}",
                "",
            ]
        ),
        encoding="utf-8",
    )


def trigger_matches(line: str) -> bool:
    if HTML_MARKUP_RE.search(line):
        return False
    return bool(FATAL_RE.search(line) and not IGNORE_RE.search(line))


@dataclass(frozen=True)
class GridTrade:
    opened_at: dt.datetime
    direction: str
    market_id: str
    entry_price: float | None
    close_event: str | None = None
    closed_at: dt.datetime | None = None


@dataclass(frozen=True)
class LiveTrade:
    opened_at: dt.datetime
    direction: str
    market_id: str | None
    entry_price: float | None
    result: str | None
    closed_at: dt.datetime | None
    id: str | None


def parse_dt(value: str | None) -> dt.datetime | None:
    if not value:
        return None
    try:
        parsed = dt.datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=dt.timezone.utc)
    return parsed


def parse_float(value: str | None) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def resolve_parity_instance(args: argparse.Namespace) -> int | None:
    if args.parity_instance is not None:
        return args.parity_instance
    unified_path = repo_root() / "config.yaml"
    try:
        if unified_path.exists():
            live_cfg = TraderConfig.from_json(unified_path)
            grid_cfg = GridConfig.from_json(unified_path)
            return resolve_mirror_instance_id(
                grid_config=grid_cfg,
                live_config=live_cfg,
                requested="auto",
            )
        else:
            live_path = repo_root() / "config_trader_live.json"
            grid_path = repo_root() / "config_grid.json"
            live_cfg = TraderConfig.from_json(live_path)
            grid_cfg = GridConfig.from_json(grid_path)
            return resolve_mirror_instance_id(
                grid_config=grid_cfg,
                live_config=live_cfg,
                requested="auto",
            )
    except (OSError, json.JSONDecodeError, ValueError):
        return None


def read_grid_trades(path: Path, instance_id: int, since: dt.datetime) -> list[GridTrade]:
    if not path.exists():
        return []
    rows = []
    iid = str(instance_id)
    with path.open("r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            if row.get("instance_id") != iid:
                continue
            ts = parse_dt(row.get("timestamp"))
            if ts is None or ts < since:
                continue
            rows.append((ts, row))
    rows.sort(key=lambda x: x[0])

    out: list[GridTrade] = []
    open_by_market: dict[str, GridTrade] = {}
    close_events = {
        "TP_FILLED",
        "TP_SIGNAL_MARKET_EXIT",
        "WIN_EXPIRY",
        "LOSS_EXPIRY",
        "STOP_LOSS",
        "TRAILING_STOP",
        "UNRESOLVED_RESTART",
    }
    for ts, row in rows:
        event = (row.get("event") or "").upper()
        market_id = row.get("market_id") or ""
        direction = (row.get("direction") or "").upper()
        if event == "ENTRY":
            if market_id in open_by_market:
                out.append(open_by_market.pop(market_id))
            open_by_market[market_id] = GridTrade(
                opened_at=ts,
                direction=direction,
                market_id=market_id,
                entry_price=parse_float(row.get("entry_price")),
            )
            continue
        if event in close_events and market_id in open_by_market:
            entry = open_by_market.pop(market_id)
            out.append(
                GridTrade(
                    opened_at=entry.opened_at,
                    direction=entry.direction,
                    market_id=entry.market_id,
                    entry_price=entry.entry_price,
                    close_event=event,
                    closed_at=ts,
                )
            )
    out.extend(open_by_market.values())
    out.sort(key=lambda t: t.opened_at)
    return out


def read_live_trades(path: Path, since: dt.datetime) -> list[LiveTrade]:
    if not path.exists():
        return []
    out: list[LiveTrade] = []
    with path.open("r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            opened_at = parse_dt(row.get("opened_at"))
            if opened_at is None or opened_at < since:
                continue
            out.append(
                LiveTrade(
                    opened_at=opened_at,
                    direction=(row.get("direction") or "").upper(),
                    market_id=row.get("market_id") or None,
                    entry_price=parse_float(row.get("entry_price")),
                    result=(row.get("result") or "").upper() or None,
                    closed_at=parse_dt(row.get("closed_at")),
                    id=row.get("id") or None,
                )
            )
    out.sort(key=lambda t: t.opened_at)
    return out


def read_live_state_trades(path: Path, since: dt.datetime) -> list[LiveTrade]:
    if not path.exists():
        return []
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []

    out: list[LiveTrade] = []
    for pos in raw.get("open_positions") or []:
        if not isinstance(pos, dict):
            continue
        opened_at = parse_dt(pos.get("opened_at"))
        if opened_at is None or opened_at < since:
            continue
        out.append(
            LiveTrade(
                opened_at=opened_at,
                direction=(pos.get("direction") or "").upper(),
                market_id=pos.get("market_id") or None,
                entry_price=parse_float(str(pos.get("entry_price"))),
                result=None,
                closed_at=None,
                id=pos.get("id") or None,
            )
        )

    for pos in raw.get("closed_positions") or []:
        if not isinstance(pos, dict):
            continue
        opened_at = parse_dt(pos.get("opened_at"))
        if opened_at is None or opened_at < since:
            continue
        out.append(
            LiveTrade(
                opened_at=opened_at,
                direction=(pos.get("direction") or "").upper(),
                market_id=pos.get("market_id") or None,
                entry_price=parse_float(str(pos.get("entry_price"))),
                result=(pos.get("result") or "").upper() or None,
                closed_at=parse_dt(pos.get("closed_at")),
                id=pos.get("id") or None,
            )
        )
    return out


def merge_live_trades(*groups: list[LiveTrade]) -> list[LiveTrade]:
    by_key: dict[tuple[str | None, str, str], LiveTrade] = {}
    for group in groups:
        for trade in group:
            key = (trade.id, trade.direction, trade.opened_at.isoformat())
            prev = by_key.get(key)
            if prev is None:
                by_key[key] = trade
                continue
            if prev.result is None and trade.result is not None:
                by_key[key] = trade
            elif prev.market_id is None and trade.market_id is not None:
                by_key[key] = trade
    out = list(by_key.values())
    out.sort(key=lambda t: t.opened_at)
    return out


def find_live_match(
    grid: GridTrade,
    live_trades: list[LiveTrade],
    used: set[int],
    time_tol_seconds: float,
    price_tol: float,
) -> tuple[int, LiveTrade] | None:
    best: tuple[int, LiveTrade] | None = None
    best_delta = float("inf")
    for idx, live in enumerate(live_trades):
        if idx in used:
            continue
        if live.direction != grid.direction:
            continue
        if live.market_id and grid.market_id and live.market_id != grid.market_id:
            continue
        delta = abs((live.opened_at - grid.opened_at).total_seconds())
        if delta > time_tol_seconds:
            continue
        if (
            live.entry_price is not None
            and grid.entry_price is not None
            and abs(live.entry_price - grid.entry_price) > price_tol
        ):
            continue
        if delta < best_delta:
            best_delta = delta
            best = (idx, live)
    return best


def find_near_live_entry(
    grid: GridTrade,
    live_trades: list[LiveTrade],
    time_tol_seconds: float,
) -> tuple[int, LiveTrade, float] | None:
    best: tuple[int, LiveTrade, float] | None = None
    best_delta = float("inf")
    for idx, live in enumerate(live_trades):
        if live.market_id and grid.market_id and live.market_id != grid.market_id:
            continue
        delta = abs((live.opened_at - grid.opened_at).total_seconds())
        if delta > time_tol_seconds:
            continue
        if delta < best_delta:
            best_delta = delta
            best = (idx, live, delta)
    return best


def find_near_grid_entry(
    live: LiveTrade,
    grid_trades: list[GridTrade],
    time_tol_seconds: float,
) -> tuple[GridTrade, float] | None:
    best: tuple[GridTrade, float] | None = None
    best_delta = float("inf")
    for grid in grid_trades:
        if live.market_id and grid.market_id and live.market_id != grid.market_id:
            continue
        delta = abs((live.opened_at - grid.opened_at).total_seconds())
        if delta > time_tol_seconds:
            continue
        if delta < best_delta:
            best_delta = delta
            best = (grid, delta)
    return best


def parity_check(args: argparse.Namespace, since: dt.datetime) -> dict | None:
    instance_id = resolve_parity_instance(args)
    if instance_id is None:
        return None

    now = dt.datetime.now(dt.timezone.utc)
    grid_trades = read_grid_trades(repo_root() / "results" / "trades.csv", instance_id, since)
    live_trades = merge_live_trades(
        read_live_trades(repo_root() / "results" / "15m_live_trades.csv", since),
        read_live_state_trades(repo_root() / "results" / "15m_live_state.json", since),
    )
    used_live: set[int] = set()
    mismatches = []
    matched = 0

    for grid in grid_trades:
        near_live = find_near_live_entry(
            grid,
            live_trades,
            args.parity_time_tol_seconds,
        )
        if near_live is not None and near_live[1].direction != grid.direction:
            live_idx, live, delta = near_live
            used_live.add(live_idx)
            mismatches.append(
                {
                    "kind": "entry_direction_mismatch",
                    "instance_id": instance_id,
                    "market_id": grid.market_id,
                    "grid_direction": grid.direction,
                    "grid_opened_at": grid.opened_at.isoformat(),
                    "grid_entry_price": grid.entry_price,
                    "live_direction": live.direction,
                    "live_opened_at": live.opened_at.isoformat(),
                    "live_entry_price": live.entry_price,
                    "live_id": live.id,
                    "delta_seconds": round(delta, 1),
                    "time_tol_seconds": args.parity_time_tol_seconds,
                }
            )
            continue

        match = find_live_match(
            grid,
            live_trades,
            used_live,
            args.parity_time_tol_seconds,
            args.parity_price_tol,
        )
        age = (now - grid.opened_at).total_seconds()
        if match is None:
            if age >= args.parity_entry_grace_seconds:
                mismatches.append(
                    {
                        "kind": "grid_entry_missing_live",
                        "instance_id": instance_id,
                        "market_id": grid.market_id,
                        "direction": grid.direction,
                        "grid_opened_at": grid.opened_at.isoformat(),
                        "grid_entry_price": grid.entry_price,
                        "age_seconds": round(age, 1),
                        "entry_grace_seconds": args.parity_entry_grace_seconds,
                    }
                )
            continue
        live_idx, live = match
        used_live.add(live_idx)
        matched += 1

        if grid.close_event is None:
            continue
        close_age = (
            (now - grid.closed_at).total_seconds()
            if grid.closed_at is not None
            else float("inf")
        )
        if live.result is None or live.closed_at is None:
            if close_age >= args.parity_close_grace_seconds:
                mismatches.append(
                    {
                        "kind": "grid_close_missing_live",
                        "instance_id": instance_id,
                        "market_id": grid.market_id,
                        "direction": grid.direction,
                        "grid_close_event": grid.close_event,
                        "grid_closed_at": grid.closed_at.isoformat() if grid.closed_at else None,
                        "live_id": live.id,
                        "live_opened_at": live.opened_at.isoformat(),
                        "close_age_seconds": round(close_age, 1),
                        "close_grace_seconds": args.parity_close_grace_seconds,
                    }
                )
            continue
        if live.result != grid.close_event:
            mismatches.append(
                {
                    "kind": "close_result_mismatch",
                    "instance_id": instance_id,
                    "market_id": grid.market_id,
                    "direction": grid.direction,
                    "grid_close_event": grid.close_event,
                    "grid_closed_at": grid.closed_at.isoformat() if grid.closed_at else None,
                    "live_result": live.result,
                    "live_closed_at": live.closed_at.isoformat(),
                    "live_id": live.id,
                }
            )
            continue

        close_delta = abs((live.closed_at - grid.closed_at).total_seconds()) if grid.closed_at else 0.0
        if close_delta > args.parity_close_time_tol_seconds:
            mismatches.append(
                {
                    "kind": "close_time_mismatch",
                    "instance_id": instance_id,
                    "market_id": grid.market_id,
                    "direction": grid.direction,
                    "result": live.result,
                    "grid_closed_at": grid.closed_at.isoformat() if grid.closed_at else None,
                    "live_closed_at": live.closed_at.isoformat(),
                    "delta_seconds": round(close_delta, 1),
                    "close_time_tol_seconds": args.parity_close_time_tol_seconds,
                    "live_id": live.id,
                }
            )

    for idx, live in enumerate(live_trades):
        if idx in used_live:
            continue
        age = (now - live.opened_at).total_seconds()
        if age < args.parity_entry_grace_seconds:
            continue
        near_grid = find_near_grid_entry(
            live,
            grid_trades,
            args.parity_time_tol_seconds,
        )
        if near_grid is not None and near_grid[0].direction != live.direction:
            grid, delta = near_grid
            mismatches.append(
                {
                    "kind": "entry_direction_mismatch",
                    "instance_id": instance_id,
                    "market_id": live.market_id or grid.market_id,
                    "grid_direction": grid.direction,
                    "grid_opened_at": grid.opened_at.isoformat(),
                    "grid_entry_price": grid.entry_price,
                    "live_direction": live.direction,
                    "live_opened_at": live.opened_at.isoformat(),
                    "live_entry_price": live.entry_price,
                    "live_id": live.id,
                    "delta_seconds": round(delta, 1),
                    "time_tol_seconds": args.parity_time_tol_seconds,
                }
            )
            continue
        mismatches.append(
            {
                "kind": "live_entry_missing_grid",
                "instance_id": instance_id,
                "market_id": live.market_id,
                "direction": live.direction,
                "live_opened_at": live.opened_at.isoformat(),
                "live_entry_price": live.entry_price,
                "live_id": live.id,
                "age_seconds": round(age, 1),
                "entry_grace_seconds": args.parity_entry_grace_seconds,
            }
        )

    return {
        "status": "mismatch" if mismatches else "ok",
        "instance_id": instance_id,
        "since": since.isoformat(),
        "grid_trades": len(grid_trades),
        "live_trades": len(live_trades),
        "matched": matched,
        "mismatches": mismatches,
    }


def parity_loop(
    args: argparse.Namespace,
    triggers: "queue.Queue[str]",
    stop_event: threading.Event,
) -> None:
    since = dt.datetime.now(dt.timezone.utc) - dt.timedelta(
        minutes=args.parity_lookback_minutes
    )
    while not stop_event.is_set():
        try:
            report = parity_check(args, since)
        except Exception as exc:  # noqa: BLE001 - watchdog must keep running
            print(f"parity check failed: {exc!r}", flush=True)
            stop_event.wait(args.parity_interval_seconds)
            continue
        if report is None:
            print("parity check skipped: could not resolve mirror instance", flush=True)
            stop_event.wait(args.parity_interval_seconds)
            continue
        if report["mismatches"]:
            triggers.put("PARITY_MISMATCH " + json.dumps(report, sort_keys=True))
            return
        stop_event.wait(args.parity_interval_seconds)


HEARTBEAT_TIMEOUT_SECONDS = 300

_last_log_line_at: float = 0.0


def log_loop(
    proc: subprocess.Popen[str],
    triggers: "queue.Queue[str]",
    stop_event: threading.Event,
) -> None:
    global _last_log_line_at
    assert proc.stdout is not None
    _last_log_line_at = time.monotonic()
    for line in proc.stdout:
        if stop_event.is_set():
            break
        _last_log_line_at = time.monotonic()
        line = line.rstrip("\n")
        print(line, flush=True)
        if trigger_matches(line):
            triggers.put(line)
            return


def heartbeat_loop(
    triggers: "queue.Queue[str]",
    stop_event: threading.Event,
) -> None:
    while not stop_event.wait(30):
        silence = time.monotonic() - _last_log_line_at
        if _last_log_line_at > 0 and silence > HEARTBEAT_TIMEOUT_SECONDS:
            triggers.put(
                f"HEARTBEAT_TIMEOUT: no log output for {silence:.0f}s "
                f"(threshold {HEARTBEAT_TIMEOUT_SECONDS}s)"
            )
            return


def diagnose_and_stop(args: argparse.Namespace, trigger_line: str) -> int:
    diagnostics_root = repo_root() / args.diagnostics_root
    diag_dir = diagnostics_root / f"live_watchdog_{timestamp()}"
    diag_dir.mkdir(parents=True, exist_ok=True)

    trigger_text = "\n".join(
        [
            f"triggered_at={now_iso()}",
            f"pid={os.getpid()}",
            f"service={args.service}",
            f"profile={args.profile}",
            f"trigger_line={trigger_line}",
            "",
        ]
    )
    (diag_dir / "trigger.txt").write_text(trigger_text, encoding="utf-8")
    if trigger_line.startswith("PARITY_MISMATCH "):
        try:
            report = json.loads(trigger_line.removeprefix("PARITY_MISMATCH "))
        except json.JSONDecodeError:
            report = None
        if report is not None:
            (diag_dir / "parity_report.json").write_text(
                json.dumps(report, indent=2, sort_keys=True),
                encoding="utf-8",
            )

    last_trigger = safety_trigger_path(repo_root() / "results")
    latch_live_stop(
        repo_root() / "results",
        f"STOPPING live due to watchdog trigger. Diagnosis: {diag_dir}",
    )

    stop_rc = run_capture(
        ["docker", "compose", "--profile", args.profile, "stop", args.service],
        diag_dir / "stop_live.txt",
        timeout=90,
    )
    if stop_rc != 0:
        print(f"WARNING: docker compose stop exited {stop_rc} — container may still be running", flush=True)
    run_capture(
        ["docker", "compose", "--profile", args.profile, "ps", "--all"],
        diag_dir / "compose_ps.txt",
    )
    run_capture(
        ["docker", "inspect", "--format",
         '{"State":{{json .State}},"RestartCount":{{json .RestartCount}},"Image":{{json .Image}}}',
         args.container_name],
        diag_dir / "inspect_live.json",
    )
    run_capture(
        ["docker", "compose", "--profile", args.profile, "logs", f"--tail={args.log_tail}", args.service],
        diag_dir / "live_tail.log",
    )
    run_capture(["docker", "compose", "logs", f"--tail={args.log_tail}", "calc"], diag_dir / "calc_tail.log")
    run_capture(["docker", "compose", "logs", f"--tail={args.log_tail}", "grid"], diag_dir / "grid_tail.log")

    for rel_path in [
        "results/15m_live_state.json",
        "results/15m_live_trades.csv",
        "results/trades.csv",
        "results/leaderboard.csv",
        "results/15m_risk_events.csv",
        "results/15m_risk_events.current.csv",
        "results/15m_live_tick_eval.current.csv",
        "config.yaml",
        "config_trader_live.json",
        "config_grid.json",
    ]:
        copy_if_exists(rel_path, diag_dir)

    outcome = "STOPPED" if stop_rc == 0 else "STOP FAILED; safety latch remains active for"
    msg = f"{outcome} live due to watchdog trigger. Diagnosis: {diag_dir}"
    print(msg, flush=True)
    last_trigger.write_text(msg + "\n", encoding="utf-8")
    return 2


MAX_LOG_FOLLOW_RETRIES = 10
LOG_FOLLOW_RETRY_DELAY = 5.0


def _watch_once(args: argparse.Namespace) -> int | None:
    """Attach to the live container log stream once.

    Returns an exit code (2 = triggered, stops live) or ``None`` when the
    ``docker compose logs -f`` process exits on its own (Docker restart,
    container recreation, pipe error) so the caller can retry.
    """
    global _last_log_line_at

    cmd = [
        "docker",
        "compose",
        "--profile",
        args.profile,
        "logs",
        f"--since={args.since_seconds}s",
        "-f",
        args.service,
    ]

    triggers: queue.Queue[str] = queue.Queue()
    stop_event = threading.Event()
    _last_log_line_at = time.monotonic()

    with subprocess.Popen(
        cmd,
        cwd=repo_root(),
        text=True,
        encoding="utf-8",
        errors="replace",
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        bufsize=1,
    ) as proc:
        threads = [
            threading.Thread(
                target=log_loop,
                args=(proc, triggers, stop_event),
                name="live-log-watch",
                daemon=True,
            ),
            threading.Thread(
                target=heartbeat_loop,
                args=(triggers, stop_event),
                name="live-heartbeat-watch",
                daemon=True,
            ),
        ]
        if args.parity_enabled:
            threads.append(
                threading.Thread(
                    target=parity_loop,
                    args=(args, triggers, stop_event),
                    name="live-parity-watch",
                    daemon=True,
                )
            )
        for thread in threads:
            thread.start()

        try:
            while proc.poll() is None:
                try:
                    trigger = triggers.get(timeout=0.5)
                except queue.Empty:
                    continue
                stop_event.set()
                proc.terminate()
                return diagnose_and_stop(args, trigger)
            # Also drain triggers posted just before proc exited (heartbeat, parity).
            try:
                trigger = triggers.get_nowait()
            except queue.Empty:
                trigger = None
            if trigger is not None:
                stop_event.set()
                return diagnose_and_stop(args, trigger)
            return None
        finally:
            stop_event.set()


def main() -> int:
    args = parse_args()
    os.chdir(repo_root())
    diagnostics_root = repo_root() / args.diagnostics_root
    write_pid_file(args, diagnostics_root)

    print(f"watching live service={args.service} profile={args.profile}", flush=True)

    for attempt in range(MAX_LOG_FOLLOW_RETRIES):
        result = _watch_once(args)
        if result is not None:
            return result
        print(
            f"docker compose logs -f exited (attempt {attempt + 1}/{MAX_LOG_FOLLOW_RETRIES}); "
            f"retrying in {LOG_FOLLOW_RETRY_DELAY}s...",
            flush=True,
        )
        time.sleep(LOG_FOLLOW_RETRY_DELAY)

    msg = f"docker compose logs -f exited {MAX_LOG_FOLLOW_RETRIES} times; giving up"
    print(msg, flush=True)
    return 3


if __name__ == "__main__":
    raise SystemExit(main())
