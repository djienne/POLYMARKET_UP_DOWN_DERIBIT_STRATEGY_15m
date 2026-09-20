"""Runtime helpers for the 15-minute grid search."""

from __future__ import annotations

import json
import os
import csv
import logging
import subprocess
import shlex
import sys
import time
from pathlib import Path
from typing import Any

from scripts.trader.process_lock import (
    lock_owner_can_use_host_pid,
    lock_owner_is_container_namespace,
    read_lock_owner,
)
from scripts.trader._platform import subprocess_kwargs as _subprocess_kwargs


GRID_LOCK_PATH = Path("results/trader.lock")
GRID_STOP_SENTINEL = Path("results/grid.stop")
GRID_LOG_PATH = Path("results/trader.log")
COMPOSE_FILE_CANDIDATES = (
    "docker-compose.yml",
    "docker-compose.yaml",
    "compose.yml",
    "compose.yaml",
)
GRID_HISTORY_FILES = [
    Path("results/trades.csv"),
    Path("results/state_snapshot.json"),
    Path("results/leaderboard.csv"),
    Path("results/leaderboard.txt"),
]
CONDA_ENV_NAME = os.environ.get("BTC_PRICER_CONDA_ENV", "btc_pricer")

log = logging.getLogger(__name__)


def _running_inside_docker() -> bool:
    """Return True when executing inside a Docker container."""
    return Path("/.dockerenv").exists()


def _compose_files_exist(workdir: Path) -> bool:
    """Return True if this repo has a Compose file."""
    return any((workdir / name).exists() for name in COMPOSE_FILE_CANDIDATES)


def docker_compose_command(
    workdir: Path,
    args: list[str],
    *,
    capture_output: bool = False,
    timeout: int = 120,
) -> subprocess.CompletedProcess[str]:
    """Run a docker compose command rooted at this repo."""
    kwargs: dict[str, Any] = {
        "cwd": str(Path(workdir).resolve()),
        "text": True,
        "timeout": timeout,
    }
    if capture_output:
        kwargs["capture_output"] = True
    return subprocess.run(
        ["docker", "compose", *args],
        **_subprocess_kwargs(**kwargs),
    )


def _parse_compose_ps_output(payload: str) -> list[dict[str, Any]]:
    """Parse docker compose ps JSON output across Compose variants."""
    payload = payload.strip()
    if not payload:
        return []

    try:
        parsed = json.loads(payload)
    except json.JSONDecodeError:
        rows = []
        for line in payload.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                item = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(item, dict):
                rows.append(item)
        return rows

    if isinstance(parsed, dict):
        return [parsed]
    if isinstance(parsed, list):
        return [item for item in parsed if isinstance(item, dict)]
    return []


def docker_compose_grid_service(workdir: Path) -> dict[str, Any] | None:
    """Return Docker Compose state for the grid service when available."""
    workdir = Path(workdir).resolve()
    if _running_inside_docker() or not _compose_files_exist(workdir):
        return None

    try:
        result = docker_compose_command(
            workdir,
            ["ps", "-a", "--format", "json"],
            capture_output=True,
            timeout=20,
        )
    except (OSError, subprocess.SubprocessError):
        return None

    if result.returncode != 0:
        return None

    for row in _parse_compose_ps_output(result.stdout):
        service = str(row.get("Service") or row.get("service") or "")
        name = str(row.get("Name") or row.get("Names") or row.get("name") or "")
        if service != "grid" and name != "btc_pricer_15m_grid":
            continue
        return {
            "service": service or "grid",
            "name": name or "btc_pricer_15m_grid",
            "state": str(row.get("State") or row.get("state") or "").lower(),
            "status": str(row.get("Status") or row.get("status") or ""),
            "health": row.get("Health") or row.get("health"),
        }
    return None


def docker_grid_is_active(service: dict[str, Any] | None) -> bool:
    """Return True if the Compose-managed grid is actively running."""
    if not service:
        return False
    return str(service.get("state") or "").lower() in {"running", "restarting"}


def read_lock_pid(lock_path: Path) -> int | None:
    """Read an integer PID from a lock file."""
    owner = read_lock_owner(lock_path)
    if not owner:
        return None
    try:
        return int(owner.get("pid") or 0) or None
    except (TypeError, ValueError):
        return None


def _taskkill(pid: int, force: bool = False) -> None:
    args = ["taskkill", "/PID", str(pid), "/T"]
    if force:
        args.append("/F")
    subprocess.run(args, **_subprocess_kwargs(capture_output=True, text=True, timeout=15))


def _kill_posix(pid: int, force: bool = False) -> bool:
    import signal

    sig = signal.SIGKILL if force else signal.SIGTERM
    try:
        os.killpg(pid, sig)
        return True
    except PermissionError as exc:
        log.error("Permission denied killing process group %s: %s", pid, exc)
        return False
    except ProcessLookupError:
        return True
    except OSError:
        try:
            os.kill(pid, sig)
            return True
        except PermissionError as exc:
            log.error("Permission denied killing process %s: %s", pid, exc)
            return False
        except ProcessLookupError:
            return True
        except OSError as exc:
            log.warning("Failed to kill process %s: %s", pid, exc)
            return False


def is_pid_alive(pid: int) -> bool:
    """Check if a PID is still running."""
    if pid <= 0:
        return False
    if sys.platform == "win32":
        try:
            result = subprocess.run(
                ["tasklist", "/FI", f"PID eq {pid}", "/FO", "CSV", "/NH"],
                **_subprocess_kwargs(capture_output=True, text=True, timeout=5),
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
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def _normalize_process_rows(payload: Any) -> list[dict[str, Any]]:
    if payload is None:
        return []
    if isinstance(payload, dict):
        payload = [payload]

    rows = []
    for item in payload:
        try:
            rows.append({
                "pid": int(item.get("ProcessId", 0) or 0),
                "ppid": int(item.get("ParentProcessId", 0) or 0),
                "command_line": item.get("CommandLine", "") or "",
            })
        except (TypeError, ValueError):
            continue
    return rows


def list_python_processes() -> list[dict[str, Any]]:
    """Return Python processes with parent PIDs and command lines."""
    if sys.platform == "win32":
        script = (
            "Get-CimInstance Win32_Process | "
            "Where-Object { $_.Name -like 'python*' } | "
            "Select-Object ProcessId,ParentProcessId,CommandLine | "
            "ConvertTo-Json -Compress"
        )
        result = subprocess.run(
            ["powershell", "-NoProfile", "-Command", script],
            **_subprocess_kwargs(capture_output=True, text=True, timeout=15),
        )
        if result.returncode != 0 or not result.stdout.strip():
            return []
        try:
            return _normalize_process_rows(json.loads(result.stdout))
        except json.JSONDecodeError:
            return []

    result = subprocess.run(
        ["ps", "-eo", "pid=,ppid=,args="],
        capture_output=True,
        text=True,
        timeout=10,
    )
    rows = []
    for line in result.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        parts = line.split(None, 2)
        if len(parts) != 3:
            continue
        pid, ppid, cmd = parts
        if "python" not in cmd:
            continue
        try:
            rows.append({"pid": int(pid), "ppid": int(ppid), "command_line": cmd})
        except ValueError:
            continue
    return rows


def _is_grid_main_command(command_line: str) -> bool:
    try:
        tokens = shlex.split(command_line, posix=sys.platform != "win32")
    except ValueError:
        tokens = command_line.split()
    normalized = [token.strip("\"'").lower() for token in tokens]

    is_main = any(
        token == "scripts.trader.main"
        or token.endswith("scripts/trader/main.py")
        or token.endswith("scripts\\trader\\main.py")
        for token in normalized
    )
    if not is_main:
        return False

    for idx, token in enumerate(normalized):
        if token == "--mode" and idx + 1 < len(normalized) and normalized[idx + 1] == "trader":
            return False
        if token == "--mode=trader":
            return False

    return not any(
        flag in normalized for flag in ("--stop", "--restart", "--status", "--discover")
    )


def find_grid_processes(workdir: Path) -> dict[str, Any]:
    """Discover grid main processes plus any cli_terminal children."""
    workdir = Path(workdir).resolve()
    workdir_str = str(workdir).lower()
    lock_pid = read_lock_pid(workdir / GRID_LOCK_PATH)
    processes = [p for p in list_python_processes() if p["pid"] != os.getpid()]

    repo_cli_children = [
        p for p in processes
        if "cli_terminal.py" in p["command_line"].lower() and workdir_str in p["command_line"].lower()
    ]
    preferred_main_pids = {p["ppid"] for p in repo_cli_children if p["ppid"] > 0}
    if lock_pid:
        preferred_main_pids.add(lock_pid)

    main_processes = [p for p in processes if _is_grid_main_command(p["command_line"])]
    if preferred_main_pids:
        preferred = [p for p in main_processes if p["pid"] in preferred_main_pids]
        others = [p for p in main_processes if p["pid"] not in preferred_main_pids]
        if preferred:
            main_processes = preferred + others

    main_pids = {p["pid"] for p in main_processes}
    child_processes = [
        p for p in processes
        if p["ppid"] in main_pids and "cli_terminal.py" in p["command_line"].lower()
    ]

    return {
        "lock_pid": lock_pid,
        "main_processes": sorted(main_processes, key=lambda p: p["pid"]),
        "child_processes": sorted(child_processes, key=lambda p: p["pid"]),
    }


def _wait_for_pids(pids: set[int], timeout_seconds: float) -> bool:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        if not any(is_pid_alive(pid) for pid in pids):
            return True
        time.sleep(1.0)
    return not any(is_pid_alive(pid) for pid in pids)


def clear_stale_lock(workdir: Path) -> None:
    """Remove the grid lock if its PID is gone."""
    workdir = Path(workdir).resolve()
    lock_path = workdir / GRID_LOCK_PATH
    owner = read_lock_owner(lock_path)
    lock_pid = read_lock_pid(lock_path)
    if docker_grid_is_active(docker_compose_grid_service(workdir)):
        return

    if owner and lock_owner_is_container_namespace(owner):
        lock_path.unlink(missing_ok=True)
        return

    if owner and lock_pid and not lock_owner_can_use_host_pid(owner):
        return

    if lock_pid and not is_pid_alive(lock_pid):
        lock_path.unlink(missing_ok=True)


def stop_grid_processes(workdir: Path, wait_seconds: float = 45.0) -> dict[str, Any]:
    """Stop grid runners without touching history files."""
    workdir = Path(workdir).resolve()
    docker_service = docker_compose_grid_service(workdir)
    if docker_grid_is_active(docker_service):
        return {
            "docker_managed": True,
            "docker_service": docker_service,
            "initial": find_grid_processes(workdir),
            "forced": False,
            "stopped": False,
            "remaining": find_grid_processes(workdir),
        }

    sentinel_path = workdir / GRID_STOP_SENTINEL
    sentinel_path.parent.mkdir(parents=True, exist_ok=True)
    sentinel_path.write_text("stop\n")

    initial = find_grid_processes(workdir)
    pids = {p["pid"] for p in initial["main_processes"] + initial["child_processes"]}
    if not pids:
        clear_stale_lock(workdir)
        sentinel_path.unlink(missing_ok=True)
        remaining = find_grid_processes(workdir)
        return {
            "docker_managed": False,
            "docker_service": docker_service,
            "initial": initial,
            "forced": False,
            "stopped": not remaining["main_processes"] and not remaining["child_processes"],
            "remaining": remaining,
        }

    stopped = _wait_for_pids(pids, wait_seconds)
    forced = False

    if not stopped:
        forced = True
        for pid in sorted({p["pid"] for p in initial["main_processes"]}):
            if sys.platform == "win32":
                _taskkill(pid, force=False)
            else:
                _kill_posix(pid, force=False)
        stopped = _wait_for_pids(pids, 10.0)

    if not stopped:
        for pid in sorted({p["pid"] for p in initial["main_processes"]}):
            if sys.platform == "win32":
                _taskkill(pid, force=True)
            else:
                _kill_posix(pid, force=True)
        stopped = _wait_for_pids(pids, 10.0)

    clear_stale_lock(workdir)
    if stopped:
        sentinel_path.unlink(missing_ok=True)

    remaining = find_grid_processes(workdir)
    fully_stopped = stopped and not remaining["main_processes"] and not remaining["child_processes"]
    return {
        "docker_managed": False,
        "docker_service": docker_service,
        "initial": initial,
        "forced": forced,
        "stopped": fully_stopped,
        "remaining": remaining,
    }


def _candidate_env_pythons() -> list[Path]:
    suffix = "python.exe" if sys.platform == "win32" else "bin/python"
    candidates: list[Path] = []

    conda_prefix = os.environ.get("CONDA_PREFIX")
    if conda_prefix and Path(conda_prefix).name == CONDA_ENV_NAME:
        candidates.append(Path(conda_prefix) / suffix)

    conda_exe = os.environ.get("CONDA_EXE")
    if conda_exe:
        base = Path(conda_exe).resolve().parent.parent
        candidates.append(base / "envs" / CONDA_ENV_NAME / suffix)

    home = Path.home()
    candidates.extend([
        home / "miniconda3" / "envs" / CONDA_ENV_NAME / suffix,
        home / "anaconda3" / "envs" / CONDA_ENV_NAME / suffix,
    ])

    current = Path(sys.executable)
    if CONDA_ENV_NAME in current.as_posix():
        candidates.append(current)

    seen = set()
    unique = []
    for candidate in candidates:
        key = str(candidate).lower()
        if key not in seen:
            seen.add(key)
            unique.append(candidate)
    return unique


def resolve_grid_python() -> Path:
    """Resolve the configured grid environment Python executable."""
    for candidate in _candidate_env_pythons():
        if candidate.exists():
            return candidate
    raise FileNotFoundError(f"Could not locate the {CONDA_ENV_NAME} Python executable")


def start_grid_process(workdir: Path, config_path: str = "config_grid.json", verbose: bool = True) -> int:
    """Launch one detached grid runner in the configured environment."""
    workdir = Path(workdir).resolve()
    python_path = resolve_grid_python()
    log_path = workdir / GRID_LOG_PATH
    log_path.parent.mkdir(parents=True, exist_ok=True)
    (workdir / GRID_STOP_SENTINEL).unlink(missing_ok=True)

    cmd = [str(python_path), "-u", "-m", "scripts.trader.main", "--mode", "grid", "--config", config_path]
    if verbose:
        cmd.append("-v")

    stdout_handle = open(log_path, "a", encoding="utf-8")
    kwargs: dict[str, Any] = {
        "cwd": str(workdir),
        "stdin": subprocess.DEVNULL,
        "stdout": stdout_handle,
        "stderr": subprocess.STDOUT,
        "close_fds": True,
    }

    if sys.platform == "win32":
        kwargs["creationflags"] = (
            subprocess.CREATE_NEW_PROCESS_GROUP
            | subprocess.DETACHED_PROCESS
            | getattr(subprocess, "CREATE_NO_WINDOW", 0)
        )
    else:
        kwargs["start_new_session"] = True

    try:
        proc = subprocess.Popen(cmd, **kwargs)
    finally:
        stdout_handle.close()

    return proc.pid


def history_state(workdir: Path) -> dict[str, dict[str, Any]]:
    """Capture existence and sizes for history files."""
    workdir = Path(workdir).resolve()
    state = {}
    for rel_path in GRID_HISTORY_FILES:
        path = workdir / rel_path
        exists = path.exists()
        state[str(rel_path)] = {
            "exists": exists,
            "size": path.stat().st_size if exists else None,
        }
    return state


def ensure_history_preserved(before: dict[str, dict[str, Any]], after: dict[str, dict[str, Any]]) -> None:
    """Raise if a previously existing history file disappeared."""
    for path, meta in before.items():
        if meta["exists"] and not after.get(path, {}).get("exists"):
            raise RuntimeError(f"History file disappeared during restart: {path}")


def grid_status_snapshot(workdir: Path) -> dict[str, Any]:
    """Return current process and snapshot status for grid mode."""
    workdir = Path(workdir).resolve()
    snapshot_path = workdir / "results/state_snapshot.json"
    snapshot_meta = {"exists": snapshot_path.exists()}
    if snapshot_path.exists():
        try:
            data = json.loads(snapshot_path.read_text())
            snapshot_meta.update({
                "timestamp": data.get("timestamp"),
                "n_instances": data.get("n_instances"),
                "markets_resolved": data.get("markets_resolved"),
            })
        except (OSError, json.JSONDecodeError):
            snapshot_meta["corrupt"] = True

    process_state = find_grid_processes(workdir)
    process_state["docker_service"] = docker_compose_grid_service(workdir)
    process_state["snapshot"] = snapshot_meta
    process_state["history"] = history_state(workdir)
    return process_state
