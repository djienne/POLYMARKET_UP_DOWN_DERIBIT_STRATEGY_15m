#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import socket
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence


ROOT = Path(__file__).resolve().parent
FREQTRADE_ROOT = ROOT.parent
BOT_ROOT = FREQTRADE_ROOT / "BTC_pricer_15m"
sys.path.insert(0, str(BOT_ROOT))
from scripts.stack_services import SERVICE_CONTAINERS, STACK_SERVICES, STOP_SERVICES
BACKEND_DIR = ROOT / "backend"
FRONTEND_DIR = ROOT / "frontend"
RUNTIME_DIR = ROOT / "runtime"
LOG_DIR = ROOT / "logs"
TMP_DIR = ROOT / ".codex_tmp"
ALL_START_SERVICES = tuple(service for service in STACK_SERVICES if service != "live")
# Watchdog stops first: it is the supervisor, and leaving it running while the
# rest go down would have it racing the shutdown.
ALL_STOP_SERVICES = tuple(service for service in STOP_SERVICES if service != "live")
GRID_START_SERVICES = ("redis", "spot", "calc", "grid")
GRID_STOP_SERVICES = ("grid", "calc", "spot", "redis")
ALL_CONTAINERS = tuple(SERVICE_CONTAINERS.items())

# Tells the liveness watchdog (BTC_pricer_15m/scripts/stack_liveness.py) that
# this stop was deliberate, so it does not immediately restart what we just
# stopped. Expires on its own so a forgotten marker cannot disable the guard
# permanently -- see the watchdog's maintenance_hold().
MAINTENANCE_MARKER = BOT_ROOT / "results" / ".maintenance_stop"
MAINTENANCE_HOURS = 12

BACKEND_PID = RUNTIME_DIR / "dashboard_backend.pid"
FRONTEND_PID = RUNTIME_DIR / "dashboard_frontend.pid"
BACKEND_PORT = 8798
FRONTEND_PORT = 5175

CREATE_NO_WINDOW = 0x08000000 if os.name == "nt" else 0
CREATE_NEW_PROCESS_GROUP = 0x00000200 if os.name == "nt" else 0


@dataclass(frozen=True)
class CommandResult:
    returncode: int
    stdout: str
    stderr: str


def log(message: str, *, prefix: str = "manage") -> None:
    print(f"[{prefix}] {message}", flush=True)


def ensure_dirs() -> None:
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    TMP_DIR.mkdir(parents=True, exist_ok=True)


def run(
    args: Sequence[str | os.PathLike[str]],
    *,
    cwd: Path | None = None,
    env: dict[str, str] | None = None,
    check: bool = False,
    capture: bool = True,
) -> CommandResult:
    text_args = [str(a) for a in args]
    completed = subprocess.run(
        text_args,
        cwd=str(cwd) if cwd else None,
        env=env,
        text=True,
        capture_output=capture,
        check=False,
    )
    result = CommandResult(
        returncode=completed.returncode,
        stdout=completed.stdout or "",
        stderr=completed.stderr or "",
    )
    if check and result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip()
        raise RuntimeError(f"{text_args[0]} failed with exit code {result.returncode}: {detail}")
    return result


def is_port_listening(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(0.35)
        return sock.connect_ex(("127.0.0.1", port)) == 0


def pids_listening_on(port: int) -> list[int]:
    if os.name != "nt":
        return []
    result = run(["netstat.exe", "-ano", "-p", "tcp"], capture=True)
    if result.returncode != 0:
        return []
    pids: set[int] = set()
    needle = f":{port}"
    for line in result.stdout.splitlines():
        parts = line.split()
        if len(parts) < 5:
            continue
        local_addr = parts[1]
        state = parts[3].upper()
        pid_text = parts[4]
        if local_addr.endswith(needle) and state == "LISTENING" and pid_text.isdigit():
            pids.add(int(pid_text))
    return sorted(pids)


def is_pid_running(pid: int) -> bool:
    if pid <= 0:
        return False
    if os.name == "nt":
        result = run(["tasklist.exe", "/FI", f"PID eq {pid}", "/FO", "CSV", "/NH"], capture=True)
        return result.returncode == 0 and str(pid) in result.stdout
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def kill_pid(pid: int, name: str) -> None:
    if pid <= 0 or pid == os.getpid():
        return
    if os.name == "nt":
        run(["taskkill.exe", "/PID", str(pid), "/T", "/F"], capture=True)
    else:
        try:
            os.kill(pid, 15)
        except OSError:
            pass
    log(f"stopped {name} pid={pid}", prefix="stop")


def read_pid(path: Path) -> int | None:
    try:
        text = path.read_text(encoding="ascii").strip()
    except OSError:
        return None
    return int(text) if text.isdigit() else None


def write_pid(path: Path, pid: int) -> None:
    ensure_dirs()
    path.write_text(str(pid), encoding="ascii")


def stop_pid_file(path: Path, name: str) -> None:
    pid = read_pid(path)
    if pid is not None and is_pid_running(pid):
        kill_pid(pid, name)
    try:
        path.unlink()
    except OSError:
        pass


def stop_port(port: int, name: str) -> None:
    pids = [pid for pid in pids_listening_on(port) if pid != os.getpid()]
    if not pids:
        log(f"{name} not listening on :{port}", prefix="stop")
        return
    for pid in pids:
        kill_pid(pid, f"{name} listener")


def start_hidden(
    *,
    name: str,
    cwd: Path,
    args: Sequence[str],
    pid_file: Path,
    stdout_log: Path,
    stderr_log: Path,
) -> int:
    ensure_dirs()
    stdout = stdout_log.open("ab")
    stderr = stderr_log.open("ab")
    creationflags = CREATE_NO_WINDOW | CREATE_NEW_PROCESS_GROUP
    try:
        proc = subprocess.Popen(
            list(args),
            cwd=str(cwd),
            stdout=stdout,
            stderr=stderr,
            stdin=subprocess.DEVNULL,
            close_fds=True,
            creationflags=creationflags,
        )
    finally:
        stdout.close()
        stderr.close()
    write_pid(pid_file, proc.pid)
    log(f"{name} process pid={proc.pid}", prefix="start")
    return proc.pid


def docker_compose(args: Sequence[str], *, prefix: str, warn_only: bool = True) -> bool:
    if not BOT_ROOT.exists():
        log(f"bot repo not found: {BOT_ROOT}", prefix=prefix)
        return False
    result = run(["docker", "compose", *args], cwd=BOT_ROOT, capture=True)
    if result.returncode == 0:
        return True
    detail = (result.stderr or result.stdout).strip()
    message = f"docker compose {' '.join(args)} failed"
    if detail:
        message += f": {detail}"
    if warn_only:
        log(message, prefix=prefix)
        return False
    raise RuntimeError(message)


def docker_container_status(name: str) -> str:
    result = run(
        ["docker", "ps", "-a", "--filter", f"name={name}", "--format", "{{.Status}}"],
        cwd=BOT_ROOT if BOT_ROOT.exists() else None,
        capture=True,
    )
    if result.returncode != 0:
        return "unknown (docker unavailable)"
    status = result.stdout.strip().splitlines()
    return status[0] if status else "not found"


def _start_native_dashboard() -> bool:
    """Launch backend + frontend as hidden native processes (dev mode)."""
    if is_port_listening(BACKEND_PORT):
        log(f"backend already listening on :{BACKEND_PORT}", prefix="start")
    else:
        log(f"launching hidden backend uvicorn on :{BACKEND_PORT} ...", prefix="start")
        start_hidden(
            name="backend",
            cwd=BACKEND_DIR,
            args=[
                sys.executable,
                "-m",
                "uvicorn",
                "app.main:app",
                "--host",
                "127.0.0.1",
                "--port",
                str(BACKEND_PORT),
                "--reload",
            ],
            pid_file=BACKEND_PID,
            stdout_log=LOG_DIR / "dashboard_backend.out.log",
            stderr_log=LOG_DIR / "dashboard_backend.err.log",
        )

    if is_port_listening(FRONTEND_PORT):
        log(f"frontend already listening on :{FRONTEND_PORT}", prefix="start")
    else:
        log(f"launching hidden frontend vite on :{FRONTEND_PORT} ...", prefix="start")
        if os.name == "nt":
            frontend_cmd = ["cmd.exe", "/d", "/c", "npm", "run", "dev"]
        else:
            frontend_cmd = ["npm", "run", "dev"]
        start_hidden(
            name="frontend",
            cwd=FRONTEND_DIR,
            args=frontend_cmd,
            pid_file=FRONTEND_PID,
            stdout_log=LOG_DIR / "dashboard_frontend.out.log",
            stderr_log=LOG_DIR / "dashboard_frontend.err.log",
        )

    backend_ok = wait_for_port(BACKEND_PORT, timeout_s=45)
    frontend_ok = wait_for_port(FRONTEND_PORT, timeout_s=45)
    if not backend_ok:
        log(
            f"backend did not start on :{BACKEND_PORT}; see logs/dashboard_backend.err.log",
            prefix="start",
        )
    if not frontend_ok:
        log(
            f"frontend did not start on :{FRONTEND_PORT}; see logs/dashboard_frontend.err.log",
            prefix="start",
        )
    return backend_ok and frontend_ok


def set_maintenance_marker(reason: str, *, prefix: str) -> None:
    now = dt.datetime.now(dt.timezone.utc)
    payload = {
        "stopped_at": now.isoformat(timespec="seconds"),
        "expires_at": (now + dt.timedelta(hours=MAINTENANCE_HOURS)).isoformat(timespec="seconds"),
        "reason": reason,
    }
    try:
        MAINTENANCE_MARKER.parent.mkdir(parents=True, exist_ok=True)
        MAINTENANCE_MARKER.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        log(f"watchdog paused for {MAINTENANCE_HOURS}h ({MAINTENANCE_MARKER.name})", prefix=prefix)
    except OSError as exc:
        log(f"WARNING could not write {MAINTENANCE_MARKER}: {exc}", prefix=prefix)


def clear_maintenance_marker(*, prefix: str) -> None:
    try:
        MAINTENANCE_MARKER.unlink(missing_ok=True)
    except OSError as exc:
        log(f"WARNING could not remove {MAINTENANCE_MARKER}: {exc}", prefix=prefix)


def start(args: argparse.Namespace) -> int:
    ensure_dirs()
    clear_maintenance_marker(prefix="start")
    native = getattr(args, "native", False)

    if args.no_grid:
        # Dashboard only, always native
        ok = _start_native_dashboard()
        log(f"done. Dashboard: http://127.0.0.1:{FRONTEND_PORT}  (backend API: :{BACKEND_PORT})", prefix="start")
        return 0 if ok else 1

    if native:
        log("docker compose up -d redis calc grid ...", prefix="start")
        docker_compose(["up", "-d", *GRID_START_SERVICES], prefix="start", warn_only=True)
        ok = _start_native_dashboard()
        log(f"done. Dashboard: http://127.0.0.1:{FRONTEND_PORT}  (backend API: :{BACKEND_PORT})", prefix="start")
        return 0 if ok else 1

    # Default: everything in Docker including dashboard
    log("docker compose up -d redis calc grid dashboard ...", prefix="start")
    docker_compose(["up", "-d", *ALL_START_SERVICES], prefix="start", warn_only=True)
    ok = wait_for_port(FRONTEND_PORT, timeout_s=60)
    if not ok:
        log(
            f"dashboard did not start on :{FRONTEND_PORT}; check: docker logs polymarket_dashboard_15m",
            prefix="start",
        )
    log(f"done. Dashboard: http://127.0.0.1:{FRONTEND_PORT}", prefix="start")
    return 0 if ok else 1


def wait_for_port(port: int, *, timeout_s: float) -> bool:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if is_port_listening(port):
            return True
        time.sleep(0.25)
    return False


def stop(args: argparse.Namespace) -> int:
    # Always try to stop native processes (safe no-op if not running)
    stop_pid_file(BACKEND_PID, "backend")
    stop_pid_file(FRONTEND_PID, "frontend")
    stop_port(BACKEND_PORT, "backend")
    stop_port(FRONTEND_PORT, "frontend")
    set_maintenance_marker("dashboard manage.py stop", prefix="stop")
    if not args.no_grid:
        log("docker compose stop watchdog dashboard grid calc redis ...", prefix="stop")
        docker_compose(["stop", *ALL_STOP_SERVICES], prefix="stop", warn_only=True)
    else:
        # --no-grid: also stop the dashboard container if it's running. The
        # watchdog goes too, else it would restart the dashboard behind us.
        docker_compose(["stop", "watchdog", "dashboard"], prefix="stop", warn_only=True)
    log("done. Live trading was not touched.", prefix="stop")
    return 0


def restart(args: argparse.Namespace) -> int:
    stop_args = argparse.Namespace(no_grid=args.no_grid)
    start_args = argparse.Namespace(no_grid=args.no_grid, native=getattr(args, "native", False))
    stop(stop_args)
    time.sleep(2.0)
    return start(start_args)


def pid_file_status(path: Path, name: str) -> str:
    pid = read_pid(path)
    if pid is None:
        return f"{name} pid: none"
    return f"{name} pid: {pid} ({'running' if is_pid_running(pid) else 'stale'})"


def status(_: argparse.Namespace) -> int:
    # Native process status
    backend_pid = read_pid(BACKEND_PID)
    native_backend = backend_pid is not None and is_pid_running(backend_pid)
    frontend_pid = read_pid(FRONTEND_PID)
    native_frontend = frontend_pid is not None and is_pid_running(frontend_pid)
    if native_backend or native_frontend:
        backend_state = f"running (:{BACKEND_PORT})" if is_port_listening(BACKEND_PORT) else "stopped"
        frontend_state = f"running (:{FRONTEND_PORT})" if is_port_listening(FRONTEND_PORT) else "stopped"
        print(f"[status] dashboard backend (native): {backend_state}", flush=True)
        print(f"[status] dashboard frontend (native): {frontend_state}", flush=True)
        print(f"[status] {pid_file_status(BACKEND_PID, 'backend')}", flush=True)
        print(f"[status] {pid_file_status(FRONTEND_PID, 'frontend')}", flush=True)
    else:
        port_state = f"running (:{FRONTEND_PORT})" if is_port_listening(FRONTEND_PORT) else "stopped"
        print(f"[status] dashboard (:{FRONTEND_PORT}): {port_state}", flush=True)

    for label, container in ALL_CONTAINERS:
        print(f"[status] {label} container:     {docker_container_status(container)}", flush=True)
    print(flush=True)
    print("[status] live switch:", flush=True)
    return live(argparse.Namespace(live_args=["status"]))


def live(args: argparse.Namespace) -> int:
    from live_manager import dispatch as live_dispatch

    live_args = list(args.live_args or ["status"])
    return live_dispatch(live_args)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Manage the Polymarket dashboard and bot helpers.")
    sub = parser.add_subparsers(dest="command")

    p_start = sub.add_parser("start", help="Start all services (Docker by default).")
    p_start.add_argument("--no-grid", action="store_true", help="Start only dashboard (native backend/frontend).")
    p_start.add_argument("--native", action="store_true", help="Run dashboard as native processes instead of Docker container.")
    p_start.set_defaults(func=start)

    p_stop = sub.add_parser("stop", help="Stop dashboard and dry-run stack. Live trading is untouched.")
    p_stop.add_argument("--no-grid", action="store_true", help="Stop only dashboard (native + container).")
    p_stop.set_defaults(func=stop)

    p_restart = sub.add_parser("restart", help="Restart dashboard/dry-run stack.")
    p_restart.add_argument("--no-grid", action="store_true", help="Restart only dashboard.")
    p_restart.add_argument("--native", action="store_true", help="Run dashboard as native processes instead of Docker container.")
    p_restart.set_defaults(func=restart)

    p_status = sub.add_parser("status", help="Show dashboard, dry-run stack, and live-switch status.")
    p_status.set_defaults(func=status)

    p_live = sub.add_parser("live", help="Delegate live-trader switching/status to the BTC_pricer script.")
    p_live.add_argument("live_args", nargs=argparse.REMAINDER)
    p_live.set_defaults(func=live)

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not hasattr(args, "func"):
        args = parser.parse_args(["status"])
    try:
        return int(args.func(args))
    except KeyboardInterrupt:
        return 130
    except Exception as exc:
        print(f"[manage] ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
