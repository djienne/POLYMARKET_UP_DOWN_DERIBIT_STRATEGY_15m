#!/usr/bin/env python
"""Passive liveness watchdog: restarts 15m stack containers that were stopped.

Counterpart to ``live_watchdog.py``, which is *reactive* and STOPS live on fatal
log patterns. This one is *passive* and STARTS things back up. Both can run at
once; the interlock below keeps them from fighting.

Why this exists
---------------
Between 2026-07-27 and 2026-08-14 the whole stack sat stopped for ~18 days and
nobody noticed. Every service is ``restart: unless-stopped``, which deliberately
does not bring a container back once it has been explicitly stopped -- not even
across daemon restarts -- and nothing else watched the stack.

Why running inside the stack still works
----------------------------------------
This container is the only one marked ``restart: always``. Unlike
``unless-stopped``, that policy DOES restart a manually-stopped container when
the Docker daemon restarts, and Docker Desktop has AutoStart=True. So a
``docker compose stop`` that takes this container down with everything else is
undone at the next sign-in, and the watchdog then repairs the rest of the stack.

Scope: it starts containers that are STOPPED. It does not restart containers
that are running-but-unhealthy (their own healthchecks and ``live_watchdog.py``
own that case) and it cannot recreate containers that were ``docker compose
down``-ed, since it holds no compose context -- both are logged, not actioned.
"""

from __future__ import annotations

import datetime as dt
import json
import os
import subprocess
import sys
import time
from pathlib import Path

try:  # package import in tests; direct script in the watchdog image
    from scripts.live_runtime_state import desired_live_running, initialize_live_desired, safety_stop_active
    from scripts.stack_services import SERVICE_CONTAINERS, SERVICE_DEPENDENCIES
except ModuleNotFoundError:
    from live_runtime_state import desired_live_running, initialize_live_desired, safety_stop_active
    from stack_services import SERVICE_CONTAINERS, SERVICE_DEPENDENCIES


# Start order mirrors the compose `depends_on` graph. `docker start` bypasses
# compose's `condition: service_healthy` gating, so we re-impose it here: each
# tier must be healthy before the next is started.
EXPECTED = tuple(name for service, name in SERVICE_CONTAINERS.items() if service != "watchdog")
DEPENDENCIES = {
    SERVICE_CONTAINERS[service]: tuple(SERVICE_CONTAINERS[dep] for dep in dependencies)
    for service, dependencies in SERVICE_DEPENDENCIES.items()
}

LIVE_CONTAINER = "btc_pricer_15m_live"

# States worth acting on. `restarting` is excluded on purpose: the container's
# own restart policy is already working on it, and `docker start` would fight
# it. `running` covers healthy and unhealthy alike -- see module docstring.
DEAD_STATES = frozenset({"exited", "created", "dead"})

CSV_HEADER = "timestamp;event;down;action;result;detail"


def env_str(key: str, default: str) -> str:
    return os.environ.get(key, default).strip()


def env_int(key: str, default: int) -> int:
    try:
        return int(env_str(key, str(default)))
    except ValueError:
        return default


def env_flag(key: str, default: bool = True) -> bool:
    return env_str(key, "1" if default else "0").lower() not in {"0", "false", "no"}


RESULTS_DIR = Path(env_str("WATCHDOG_RESULTS_DIR", "/data/results"))
POLL_SECONDS = env_int("WATCHDOG_POLL_SECONDS", 60)
STARTUP_GRACE_SECONDS = env_int("WATCHDOG_STARTUP_GRACE_SECONDS", 120)
HEALTH_WAIT_SECONDS = env_int("WATCHDOG_HEALTH_WAIT_SECONDS", 120)
MAX_RETRY_SECONDS = 300
ENABLED = env_flag("WATCHDOG_ENABLED", True)

HEARTBEAT_PATH = RESULTS_DIR / "watchdog.health"
LOG_PATH = RESULTS_DIR / "liveness_log.csv"
MARKER_PATH = RESULTS_DIR / ".maintenance_stop"


def now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")


def say(message: str) -> None:
    print(f"{now_iso()} {message}", flush=True)


def log_row(event: str, down: str = "", action: str = "", result: str = "", detail: str = "") -> None:
    """Append one semicolon-delimited row, matching monitoring/mem_check.ps1."""
    def clean(value: str) -> str:
        return str(value).replace(";", ",").replace("\n", " ").strip()

    try:
        new = not LOG_PATH.exists()
        LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        with LOG_PATH.open("a", encoding="utf-8", newline="") as handle:
            if new:
                handle.write(CSV_HEADER + "\n")
            handle.write(
                ";".join([now_iso(), clean(event), clean(down), clean(action), clean(result), clean(detail)]) + "\n"
            )
    except OSError as exc:  # never let logging kill the watchdog
        say(f"WARN could not write {LOG_PATH}: {exc!r}")


def docker(*args: str, timeout: int = 60) -> tuple[int, str]:
    try:
        proc = subprocess.run(
            ["docker", *args],
            text=True,
            encoding="utf-8",
            errors="replace",
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=timeout,
        )
        return proc.returncode, (proc.stdout or "").strip()
    except (OSError, subprocess.SubprocessError) as exc:
        return -1, repr(exc)


def snapshot() -> tuple[dict[str, tuple[str, str]], str | None]:
    """Map container name -> (state, status). Second item is an error, if any.

    One `docker ps -a` for everything, then filter. Passing explicit names makes
    docker error out when any one is absent, which would drop the whole sample
    -- the same trap monitoring/mem_check.ps1 documents at lines 40-45.
    """
    rc, out = docker("ps", "-a", "--format", "{{.Names}};{{.State}};{{.Status}}")
    if rc != 0:
        return {}, out or f"docker ps exited {rc}"
    found: dict[str, tuple[str, str]] = {}
    for line in out.splitlines():
        parts = line.split(";", 2)
        if len(parts) < 3:
            continue
        name, state, status = parts[0].strip(), parts[1].strip().lower(), parts[2].strip()
        if name in EXPECTED:
            found[name] = (state, status)
    return found, None


def is_healthy(name: str, containers: dict[str, tuple[str, str]]) -> bool:
    """Running, and not reporting an explicit unhealthy/starting health state."""
    entry = containers.get(name)
    if entry is None:
        return False
    state, status = entry
    if state != "running":
        return False
    lowered = status.lower()
    if "(unhealthy)" in lowered or "health: starting" in lowered:
        return False
    return True


def stopped_containers(containers: dict[str, tuple[str, str]]) -> list[str]:
    """Expected containers that are stopped or missing, in canonical order."""
    out = []
    for name in EXPECTED:
        entry = containers.get(name)
        if entry is None:
            out.append(name)
        elif entry[0] in DEAD_STATES:
            out.append(name)
    return out


def maintenance_hold() -> str | None:
    """Reason to stay idle, or None. Written by manage.py stop; see plan doc.

    Format: {"stopped_at": iso, "expires_at": iso, "reason": str}. An EXPIRED
    marker is deliberately NOT a hold -- otherwise a forgotten marker would
    disable the guard permanently.
    """
    if not MARKER_PATH.exists():
        return None
    try:
        raw = json.loads(MARKER_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        # Unreadable marker: treat as a hold. Someone put it there on purpose.
        return "maintenance marker present but unreadable"
    if not isinstance(raw, dict):
        return "maintenance marker is not an object"
    expires = raw.get("expires_at")
    if not expires:
        return f"maintenance marker (no expiry): {raw.get('reason') or 'unspecified'}"
    try:
        deadline = dt.datetime.fromisoformat(str(expires))
    except ValueError:
        return "maintenance marker with unparseable expires_at"
    if deadline.tzinfo is None:
        deadline = deadline.replace(tzinfo=dt.timezone.utc)
    if dt.datetime.now(dt.timezone.utc) < deadline:
        return f"maintenance marker until {expires}: {raw.get('reason') or 'unspecified'}"
    return None


def live_trigger_active(_last_recovery: float = 0.0) -> bool:
    """A safety trigger remains active until an explicit resume acknowledges it."""
    return safety_stop_active(RESULTS_DIR)


def live_hold_reason() -> str | None:
    if live_trigger_active():
        return "unacknowledged live safety stop"
    if not desired_live_running(RESULTS_DIR):
        return "operator intent is stopped"
    return None


def retry_delay(failures: int) -> float:
    return min(MAX_RETRY_SECONDS, max(1, POLL_SECONDS) * 2 ** min(max(failures - 1, 0), 8))


def wait_for_health(names: list[str], budget: int) -> list[str]:
    """Block until every name is healthy or the budget runs out. Returns stragglers."""
    deadline = time.monotonic() + budget
    pending = list(names)
    while pending and time.monotonic() < deadline:
        beat()
        time.sleep(3)
        containers, err = snapshot()
        if err:
            continue
        pending = [n for n in pending if not is_healthy(n, containers)]
    return pending


def recover(down: list[str], hold_live: bool) -> tuple[bool, str]:
    """Start only with healthy dependencies and re-read live intent before start."""
    targets = [n for n in down if not (hold_live and n == LIVE_CONTAINER)]
    if not targets:
        return True, "nothing to start (live held)"

    failures: list[str] = []
    started = []
    for name in EXPECTED:
        if name not in targets:
            continue
        if maintenance_hold():
            return False, "maintenance requested during recovery"
        if name == LIVE_CONTAINER and live_hold_reason():
            continue
        current, error = snapshot()
        if error:
            failures.append(f"{name}: Docker state unavailable")
            continue
        blocked = [dep for dep in DEPENDENCIES.get(name, ()) if not is_healthy(dep, current)]
        if blocked:
            failures.append(f"{name}: waiting for healthy {','.join(blocked)}")
            continue
        rc, out = docker("start", name, timeout=90)
        if rc != 0:
            failures.append(f"{name}: {out.splitlines()[0] if out else f'exit {rc}'}")
            say(f"ERROR could not start {name}: {out}")
            continue
        started.append(name)
        say(f"started {name}")
        stragglers = wait_for_health([name], HEALTH_WAIT_SECONDS)
        if stragglers:
            failures.append(f"{name}: unhealthy after start")
            say(f"WARN not healthy within {HEALTH_WAIT_SECONDS}s: {', '.join(stragglers)}")

    if failures:
        return False, "; ".join(failures)
    return True, f"started {len(started)}"


def beat() -> None:
    try:
        HEARTBEAT_PATH.parent.mkdir(parents=True, exist_ok=True)
        HEARTBEAT_PATH.write_text(str(time.time()), encoding="utf-8")
    except OSError as exc:
        say(f"WARN heartbeat write failed: {exc!r}")


def main() -> int:
    say(
        f"stack liveness watchdog starting: poll={POLL_SECONDS}s "
        f"results={RESULTS_DIR} watching={len(EXPECTED)} containers"
    )
    if not ENABLED:
        say("WATCHDOG_ENABLED=0 -- heartbeat only, no recovery")

    # Don't fight a `compose up` that is still in progress.
    beat()
    time.sleep(STARTUP_GRACE_SECONDS)

    last_state: str | None = None
    consecutive_failures = 0
    retry_at = 0.0
    recovery_state = None
    held_logged = False

    while True:
        beat()
        containers, err = snapshot()

        if err:
            if last_state != "docker_unreachable":
                say(f"docker unreachable: {err}")
                log_row("docker_unreachable", detail=err)
                last_state = "docker_unreachable"
            time.sleep(POLL_SECONDS)
            continue

        try:
            initialize_live_desired(
                RESULTS_DIR, observed_running=containers.get(LIVE_CONTAINER, ("", ""))[0] == "running"
            )
        except OSError as exc:
            say(f"cannot persist live intent; recovery held: {exc}")
            time.sleep(POLL_SECONDS)
            continue

        hold = maintenance_hold()
        if hold:
            if last_state != "maintenance":
                say(f"idle: {hold}")
                log_row("maintenance_skip", detail=hold)
                last_state = "maintenance"
            time.sleep(POLL_SECONDS)
            continue

        down = stopped_containers(containers)
        hold_reason = live_hold_reason()
        hold_live = bool(hold_reason)
        recoverable = [name for name in down if not (hold_live and name == LIVE_CONTAINER)]

        if hold_live and LIVE_CONTAINER in down and not held_logged:
            say(f"HOLDING {LIVE_CONTAINER}: {hold_reason}")
            log_row("live_held", down=",".join(down), action="hold_live", detail=hold_reason or "")
            held_logged = True
        elif not hold_live:
            held_logged = False

        state_key = tuple(
            (name, entry[0], is_healthy(name, containers)) for name, entry in sorted(containers.items())
        ), hold_reason
        if state_key != recovery_state:
            consecutive_failures = 0
            retry_at = 0.0
            recovery_state = state_key

        if not recoverable:
            if last_state != "ok":
                say("all desired containers up")
                log_row("ok", result="all desired containers up")
                last_state = "ok"
            consecutive_failures = 0
            time.sleep(POLL_SECONDS)
            continue

        down_str = ",".join(down)

        if not ENABLED:
            if last_state != f"disabled:{down_str}":
                log_row("down_detected", down=down_str, action="none", detail="WATCHDOG_ENABLED=0")
                last_state = f"disabled:{down_str}"
            time.sleep(POLL_SECONDS)
            continue

        if time.monotonic() < retry_at:
            time.sleep(POLL_SECONDS)
            continue

        say(f"down: {down_str} -- recovering")
        log_row("down_detected", down=down_str, action="docker start")

        ok, detail = recover(down, hold_live)

        after, after_err = snapshot()
        still_down = [] if after_err else [
            n for n in stopped_containers(after) if not (hold_live and n == LIVE_CONTAINER)
        ]

        if ok and not after_err and not still_down:
            consecutive_failures = 0
            retry_at = 0.0
            say("recovered")
            log_row("recovered", down=down_str, action="docker start", result="ok", detail=detail)
            last_state = "recovered"
        else:
            consecutive_failures += 1
            delay = retry_delay(consecutive_failures)
            retry_at = time.monotonic() + delay
            result = f"still down: {','.join(still_down)}" if still_down else "start failed"
            say(f"recovery incomplete (attempt {consecutive_failures}, retry in {delay}s): {result}")
            log_row("recover_failed", down=down_str, action="docker start", result=result, detail=detail)
            last_state = f"failed:{down_str}"

        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        sys.exit(0)
