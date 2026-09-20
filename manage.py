#!/usr/bin/env python3
"""Small ops wrapper for the local 15m Polymarket stack.

The live trader is intentionally included in these commands. This wrapper
always uses Docker Compose's ``local-live`` profile and names every service so
start/stop/restart do not accidentally omit the real-money bot.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import platform
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path


ROOT = Path(__file__).resolve().parent
STACK_DIR = ROOT / "BTC_pricer_15m"
sys.path.insert(0, str(STACK_DIR))
from scripts.live_runtime_state import prepare_live_start, set_live_desired
from scripts.stack_services import STACK_SERVICES, STOP_SERVICES

SERVICES = STACK_SERVICES
# Watchdog stops first: it is the supervisor, and leaving it running while the
# rest go down would have it racing the shutdown.
STOP_ORDER = STOP_SERVICES
DASHBOARD_HEALTH_URL = "http://127.0.0.1:5175/api/health"
DASHBOARD_BOOTSTRAP_URL = "http://127.0.0.1:5175/api/bootstrap"

# Tells the liveness watchdog (BTC_pricer_15m/scripts/stack_liveness.py) that
# this stop was deliberate, so it does not immediately restart what we just
# stopped. Expires on its own so a forgotten marker cannot disable the guard
# permanently -- see the watchdog's maintenance_hold().
MAINTENANCE_MARKER = STACK_DIR / "results" / ".maintenance_stop"
MAINTENANCE_HOURS = 12


def _set_maintenance_marker(reason: str) -> None:
    now = dt.datetime.now(dt.timezone.utc)
    payload = {
        "stopped_at": now.isoformat(timespec="seconds"),
        "expires_at": (now + dt.timedelta(hours=MAINTENANCE_HOURS)).isoformat(timespec="seconds"),
        "reason": reason,
    }
    try:
        MAINTENANCE_MARKER.parent.mkdir(parents=True, exist_ok=True)
        MAINTENANCE_MARKER.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        print(f"[ops] watchdog paused for {MAINTENANCE_HOURS}h ({MAINTENANCE_MARKER.name})", flush=True)
    except OSError as exc:
        print(f"[ops] WARNING could not write {MAINTENANCE_MARKER}: {exc}", file=sys.stderr, flush=True)


def _clear_maintenance_marker() -> None:
    try:
        MAINTENANCE_MARKER.unlink(missing_ok=True)
    except OSError as exc:
        print(f"[ops] WARNING could not remove {MAINTENANCE_MARKER}: {exc}", file=sys.stderr, flush=True)


def _creationflags() -> int:
    if platform.system() != "Windows":
        return 0
    return getattr(subprocess, "CREATE_NO_WINDOW", 0)


def _run(
    args: list[str],
    *,
    check: bool = False,
    capture: bool = False,
    announce: bool = False,
) -> subprocess.CompletedProcess:
    if announce:
        print("[ops] " + " ".join(args), flush=True)
    return subprocess.run(
        args,
        cwd=STACK_DIR,
        check=check,
        text=True,
        capture_output=capture,
        creationflags=_creationflags(),
    )


def _compose(*args: str, check: bool = False, capture: bool = False) -> subprocess.CompletedProcess:
    return _run(
        ["docker", "compose", "--profile", "local-live", *args],
        check=check,
        capture=capture,
        announce=not capture,
    )


def _docker_ready() -> bool:
    try:
        result = _run(["docker", "info"], capture=True)
    except FileNotFoundError:
        print("docker CLI was not found on PATH", file=sys.stderr)
        return False
    return result.returncode == 0


def _start_docker_desktop() -> bool:
    if platform.system() != "Windows":
        return False
    docker_desktop = Path(r"C:\Program Files\Docker\Docker\Docker Desktop.exe")
    if not docker_desktop.exists():
        return False
    print("Docker is not reachable; starting Docker Desktop...")
    subprocess.Popen(
        [str(docker_desktop)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        creationflags=_creationflags(),
    )
    return True


def ensure_docker(timeout_s: int = 120) -> bool:
    if _docker_ready():
        return True
    started = _start_docker_desktop()
    if not started:
        return False
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if _docker_ready():
            return True
        time.sleep(2)
    return False


def require_docker() -> None:
    if not ensure_docker():
        print("Docker is not reachable. Start Docker Desktop and retry.", file=sys.stderr)
        raise SystemExit(1)


def cmd_start(args: argparse.Namespace) -> int:
    require_docker()
    try:
        prepare_live_start(STACK_DIR / "results", acknowledge_safety_stop=args.acknowledge_safety_stop)
    except (OSError, RuntimeError) as exc:
        print(f"[ops] {exc}", file=sys.stderr)
        return 1
    _clear_maintenance_marker()
    compose_args = ["up", "-d"]
    if args.build:
        compose_args.append("--build")
    compose_args.extend(SERVICES)
    return _compose(*compose_args).returncode


def cmd_restart(args: argparse.Namespace) -> int:
    require_docker()
    try:
        prepare_live_start(STACK_DIR / "results", acknowledge_safety_stop=args.acknowledge_safety_stop)
    except (OSError, RuntimeError) as exc:
        print(f"[ops] {exc}", file=sys.stderr)
        return 1
    _clear_maintenance_marker()
    compose_args = ["up", "-d", "--force-recreate"]
    if args.build:
        compose_args.append("--build")
    compose_args.extend(SERVICES)
    return _compose(*compose_args).returncode


def cmd_stop(_args: argparse.Namespace) -> int:
    set_live_desired(STACK_DIR / "results", running=False, reason="manage.py stop")
    if not ensure_docker(timeout_s=20):
        print("Docker is not reachable; nothing is running on this local engine.")
        return 0
    _set_maintenance_marker("manage.py stop")
    return _compose("stop", *STOP_ORDER).returncode


def cmd_status(_args: argparse.Namespace) -> int:
    if not ensure_docker(timeout_s=20):
        print("Docker is not reachable.")
        return 1
    return _compose("ps").returncode


def cmd_logs(args: argparse.Namespace) -> int:
    require_docker()
    compose_args = ["logs", "--tail", str(args.tail)]
    if args.follow:
        compose_args.append("-f")
    if args.service:
        compose_args.append(args.service)
    return _compose(*compose_args).returncode


def _get_json(url: str, timeout_s: float = 3.0) -> dict | None:
    try:
        with urllib.request.urlopen(url, timeout=timeout_s) as response:
            return json.loads(response.read().decode("utf-8"))
    except (OSError, urllib.error.URLError, json.JSONDecodeError):
        return None


def cmd_doctor(_args: argparse.Namespace) -> int:
    status = cmd_status(_args)
    health = _get_json(DASHBOARD_HEALTH_URL)
    bootstrap = _get_json(DASHBOARD_BOOTSTRAP_URL)
    if health is None:
        print("\ndashboard health: unavailable")
        return status or 1
    print(f"\ndashboard health: ok={health.get('ok')} mode={health.get('mode')} bot_live={health.get('bot_live')}")
    live = (bootstrap or {}).get("liveness") or {}
    if live:
        print(
            "liveness: "
            f"bot_live={live.get('bot_live')} "
            f"lock_exists={live.get('lock_exists')} "
            f"age_s={live.get('terminal_age_s')} "
            f"location={live.get('execution_label') or live.get('execution_location')}"
        )
    return status


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Manage the local 15m Docker stack")
    sub = parser.add_subparsers(dest="command", required=True)

    start = sub.add_parser("start", help="Start the complete local stack, including live")
    start.add_argument("--build", action="store_true", help="Build images before starting")
    start.add_argument("--acknowledge-safety-stop", action="store_true", help="Acknowledge a reviewed live watchdog diagnosis and resume")
    start.set_defaults(func=cmd_start)

    restart = sub.add_parser("restart", help="Force-recreate all local-live services")
    restart.add_argument("--build", action="store_true", help="Build images before restarting")
    restart.add_argument("--acknowledge-safety-stop", action="store_true", help="Acknowledge a reviewed live watchdog diagnosis and resume")
    restart.set_defaults(func=cmd_restart)

    stop = sub.add_parser("stop", help="Stop every local stack service")
    stop.set_defaults(func=cmd_stop)

    status = sub.add_parser("status", help="Show Docker Compose service status")
    status.set_defaults(func=cmd_status)

    logs = sub.add_parser("logs", help="Show Docker Compose logs")
    logs.add_argument("service", nargs="?", choices=SERVICES)
    logs.add_argument("--tail", type=int, default=120)
    logs.add_argument("-f", "--follow", action="store_true")
    logs.set_defaults(func=cmd_logs)

    doctor = sub.add_parser("doctor", help="Show service status plus dashboard liveness")
    doctor.set_defaults(func=cmd_doctor)

    return parser


def main(argv: list[str] | None = None) -> int:
    if not STACK_DIR.exists():
        print(f"Stack directory not found: {STACK_DIR}", file=sys.stderr)
        return 1
    args = build_parser().parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
