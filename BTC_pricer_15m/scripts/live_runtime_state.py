"""Persistent operator intent and the independent live-trading safety latch.

Container state is an observation, never authorization to resume trading.
The legacy location file remains a display/migration format; the JSON marker
is authoritative once written. A watchdog trigger is acknowledged explicitly.
"""

from __future__ import annotations

import datetime as dt
import json
import os
import uuid
from pathlib import Path


DESIRED_NAME = ".live_desired.json"
TRIGGER_NAME = "live_watchdog_last_trigger.txt"


def _atomic_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        temporary.write_text(text, encoding="utf-8")
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def safety_trigger_path(results_dir: Path) -> Path:
    return Path(results_dir) / "diagnostics" / TRIGGER_NAME


def safety_stop_active(results_dir: Path) -> bool:
    return safety_trigger_path(results_dir).exists()


def desired_live_running(results_dir: Path) -> bool:
    """Fail toward stopped for missing/corrupt intent; migrate separately."""
    results_dir = Path(results_dir)
    marker = results_dir / DESIRED_NAME
    if marker.exists():
        try:
            payload = json.loads(marker.read_text(encoding="utf-8"))
            return isinstance(payload, dict) and payload.get("desired") == "running"
        except (OSError, ValueError):
            return False
    try:
        return (results_dir / ".live_location").read_text(encoding="ascii").strip() == "local"
    except OSError:
        return False


def set_live_desired(results_dir: Path, *, running: bool, reason: str) -> None:
    """Write intent before issuing a Docker command; errors must reach caller."""
    results_dir = Path(results_dir)
    payload = {
        "schema_version": 1,
        "desired": "running" if running else "stopped",
        "updated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "reason": reason,
    }
    _atomic_text(results_dir / DESIRED_NAME, json.dumps(payload, sort_keys=True) + "\n")
    _atomic_text(results_dir / ".live_location", "local" if running else "stopped")


def initialize_live_desired(results_dir: Path, *, observed_running: bool) -> None:
    """One-time migration; never revive an ambiguous stopped container."""
    results_dir = Path(results_dir)
    if (results_dir / DESIRED_NAME).exists():
        return
    try:
        legacy = (results_dir / ".live_location").read_text(encoding="ascii").strip()
    except OSError:
        legacy = ""
    running = legacy == "local" if legacy in {"local", "stopped"} else observed_running
    if safety_stop_active(results_dir):
        running = False
    # Exclusive creation prevents migration from overwriting an operator stop
    # written after the initial existence check. Partial creation fails stopped.
    results_dir.mkdir(parents=True, exist_ok=True)
    try:
        with (results_dir / DESIRED_NAME).open("x", encoding="utf-8") as handle:
            json.dump({
                "schema_version": 1,
                "desired": "running" if running else "stopped",
                "updated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                "reason": "migrate existing live intent",
            }, handle, sort_keys=True)
            handle.write("\n")
    except FileExistsError:
        return


def prepare_live_start(results_dir: Path, *, acknowledge_safety_stop: bool = False) -> None:
    """Explicitly authorize running, preserving any unacknowledged fault."""
    results_dir = Path(results_dir)
    trigger = safety_trigger_path(results_dir)
    if trigger.exists():
        if not acknowledge_safety_stop:
            raise RuntimeError(
                "Live is held by a safety stop. Review its diagnosis, then resume "
                "with --acknowledge-safety-stop."
            )
        # Keep the diagnosis after acknowledgment; a later trigger creates a new latch.
        acknowledged = trigger.with_name(f"live_watchdog_acknowledged_{uuid.uuid4().hex}.txt")
        os.replace(trigger, acknowledged)
    set_live_desired(results_dir, running=True, reason="explicit live start")


def latch_live_stop(results_dir: Path, reason: str) -> None:
    """Persist the fault and stopped intent before stopping a live container."""
    _atomic_text(safety_trigger_path(results_dir), reason.rstrip() + "\n")
    set_live_desired(results_dir, running=False, reason=reason)
