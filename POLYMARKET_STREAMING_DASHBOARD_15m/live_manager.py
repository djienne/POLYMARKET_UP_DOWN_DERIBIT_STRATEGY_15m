"""Local-Docker live-trader orchestration for BTC_pricer_15m.

The 15m lane is intentionally local-only. This module never opens remote
shells and all live controls operate on the local Docker Compose ``live``
service.
"""
from __future__ import annotations

import os
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Optional

from manage import (
    BOT_ROOT,
    CommandResult,
    is_pid_running,
    kill_pid,
    log,
    read_pid,
)
from scripts.live_runtime_state import prepare_live_start, safety_stop_active, set_live_desired


_NO_WINDOW = 0x08000000 if os.name == "nt" else 0

ROOT = Path(__file__).resolve().parent
RESULTS_DIR = BOT_ROOT / "results"
LOCATION_PATH = RESULTS_DIR / ".live_location"
LEGACY_SYNC_PID_PATH = RESULTS_DIR / ".vps_sync.pid"
LEGACY_SYNC_HEARTBEAT_PATH = RESULTS_DIR / ".vps_sync_last"
LIVE_HISTORY_BACKUP_DIR = RESULTS_DIR / "live_history_backups"

LIVE_CONTAINER = "btc_pricer_15m_live"
LEGACY_OFFLOAD_CONTAINER = "BTC_pricer_15m_offload"
LIVE_HISTORY_FILES = (
    "15m_live_state.json",
    "15m_live_trades.csv",
    "15m_live_equity.csv",
)
LIVE_HISTORY_BACKUP_KEEP = 288


def _run(
    args,
    *,
    capture: bool = True,
    cwd=None,
    env=None,
) -> CommandResult:
    kwargs: dict = dict(
        text=True,
        capture_output=capture,
        cwd=str(cwd) if cwd else None,
        env=env,
    )
    if os.name == "nt":
        kwargs["creationflags"] = _NO_WINDOW
    completed = subprocess.run([str(a) for a in args], **kwargs)
    return CommandResult(
        returncode=completed.returncode,
        stdout=completed.stdout or "",
        stderr=completed.stderr or "",
    )


def _read_location_raw() -> str:
    try:
        return LOCATION_PATH.read_text(encoding="utf-8").strip()
    except OSError:
        return "stopped"


def set_location(kind: str) -> None:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    LOCATION_PATH.write_text(kind, encoding="ascii")


def _docker_ps_names(filter_name: str) -> list[str]:
    result = _run(
        ["docker", "ps", "--filter", f"name={filter_name}", "--format", "{{.Names}}"],
        capture=True,
    )
    if result.returncode != 0:
        return []
    return [line.strip() for line in result.stdout.splitlines() if line.strip()]


def local_live_running() -> bool:
    return LIVE_CONTAINER in _docker_ps_names(LIVE_CONTAINER)


def legacy_offload_running() -> bool:
    return LEGACY_OFFLOAD_CONTAINER in _docker_ps_names(LEGACY_OFFLOAD_CONTAINER)


def _docker_compose(args: list[str]) -> None:
    result = _run(["docker", "compose", *args], cwd=BOT_ROOT, capture=False)
    if result.returncode != 0:
        raise RuntimeError(
            f"docker compose {' '.join(args)} failed with exit code {result.returncode}"
        )


def start_local_live() -> None:
    _docker_compose(["--profile", "local-live", "up", "-d", "live"])


def stop_local_live() -> None:
    if local_live_running():
        _docker_compose(["--profile", "local-live", "stop", "live"])


def stop_legacy_vps_sync_loop() -> None:
    pid = read_pid(LEGACY_SYNC_PID_PATH)
    if pid is not None and is_pid_running(pid):
        kill_pid(pid, "legacy remote_state_sync")
    for path in (LEGACY_SYNC_PID_PATH, LEGACY_SYNC_HEARTBEAT_PATH):
        try:
            path.unlink()
        except OSError:
            pass


def stop_legacy_offload() -> None:
    if not legacy_offload_running():
        return
    result = _run(["docker", "stop", LEGACY_OFFLOAD_CONTAINER], capture=True)
    if result.returncode == 0:
        log("stopped legacy calibration offload container", prefix="live")
    else:
        log(
            f"docker stop {LEGACY_OFFLOAD_CONTAINER} failed (exit {result.returncode})",
            prefix="live",
        )


def _safe_reason(reason: str) -> str:
    return "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in reason)[:40]


def _prune_live_history_backups() -> None:
    try:
        backups = sorted(
            [p for p in LIVE_HISTORY_BACKUP_DIR.iterdir() if p.is_dir()],
            key=lambda p: p.name,
            reverse=True,
        )
    except OSError:
        return
    for old in backups[LIVE_HISTORY_BACKUP_KEEP:]:
        try:
            shutil.rmtree(old)
        except OSError:
            pass


def backup_live_history(reason: str) -> Optional[Path]:
    existing = [RESULTS_DIR / name for name in LIVE_HISTORY_FILES if (RESULTS_DIR / name).exists()]
    if not existing:
        return None

    stamp = time.strftime("%Y%m%d_%H%M%S", time.gmtime())
    dest = LIVE_HISTORY_BACKUP_DIR / f"{stamp}_{_safe_reason(reason)}"
    try:
        dest.mkdir(parents=True, exist_ok=False)
        for path in existing:
            shutil.copy2(path, dest / path.name)
        _prune_live_history_backups()
    except OSError as exc:
        log(f"live history backup failed ({reason}): {exc}", prefix="live")
        return None
    return dest


def switch_stop() -> None:
    log("stopping LOCAL Docker live trader", prefix="live")
    set_live_desired(RESULTS_DIR, running=False, reason="manage.py live stop")
    stop_local_live()
    stop_legacy_vps_sync_loop()
    stop_legacy_offload()
    set_location("stopped")
    log("live is now STOPPED", prefix="live")


def switch_local(*, acknowledge_safety_stop: bool = False) -> None:
    log("starting LOCAL Docker live trader", prefix="live")
    prepare_live_start(RESULTS_DIR, acknowledge_safety_stop=acknowledge_safety_stop)
    stop_legacy_vps_sync_loop()
    stop_legacy_offload()
    start_local_live()
    set_location("local")
    log("LIVE is now LOCAL Docker", prefix="live")


def show_status() -> None:
    raw = _read_location_raw()
    print(f"location: {raw}", flush=True)
    print(f"safety hold:      {'yes' if safety_stop_active(RESULTS_DIR) else 'no'}", flush=True)
    try:
        local_up = "yes" if local_live_running() else "no"
    except Exception:
        local_up = "unknown"
    print(f"local docker:      {local_up}", flush=True)
    print("remote live:      removed", flush=True)


def dispatch(live_args: list[str]) -> int:
    if not live_args:
        live_args = ["status"]
    cmd = live_args[0].lower()
    options = live_args[1:]
    if any(option != "--acknowledge-safety-stop" for option in options):
        log("unknown live option", prefix="live")
        return 2
    acknowledge = "--acknowledge-safety-stop" in options
    try:
        if cmd == "status":
            show_status()
            return 0
        if cmd in {"local", "start"}:
            switch_local(acknowledge_safety_stop=acknowledge)
            return 0
        if cmd == "restart":
            switch_stop()
            switch_local(acknowledge_safety_stop=acknowledge)
            return 0
        if cmd == "stop":
            switch_stop()
            return 0
        if cmd in {"vps", "ireland", "heal"}:
            log("remote live control has been removed; use local Docker only", prefix="live")
            return 2
        log("unknown live command. Use: status | local | start | restart | stop", prefix="live")
        return 2
    except Exception as exc:
        log(f"ERROR: {exc}", prefix="live")
        return 1


if __name__ == "__main__":
    sys.exit(dispatch(sys.argv[1:]))
