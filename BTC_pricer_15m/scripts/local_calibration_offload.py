#!/usr/bin/env python3
from __future__ import annotations

import json
import logging
import os
import random
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.polymarket_edge import run_terminal_script
from scripts.trader.hybrid_calibration import CalibrationEvent, MarketContext

log = logging.getLogger("local_calibration_offload")

RESULTS_DIR = ROOT / "results"
OUTPUT_JSON = RESULTS_DIR / "terminal_data.offload_local.json"


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-5s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )


def parse_market_context(raw: dict) -> Optional[MarketContext]:
    market = raw.get("current_market") or {}
    market_id = market.get("market_id")
    barrier = market.get("reference_price")
    if not market_id or barrier is None:
        return None

    try:
        start_unix = int(str(market_id).rsplit("-", 1)[1])
    except (IndexError, ValueError):
        return None

    end_dt = datetime.fromtimestamp(start_unix + 900, tz=timezone.utc)
    return MarketContext(
        market_id=str(market_id),
        barrier=float(barrier),
        market_end_time_iso=end_dt.isoformat(),
    )


def load_remote_market_context(
    *,
    host: str,
    user: str,
    remote_dir: str,
    ssh_key: str,
) -> Optional[MarketContext]:
    remote_state = f"{remote_dir}/results/15m_live_state.json"
    cmd = [
        "ssh",
        "-i",
        ssh_key,
        "-o",
        "StrictHostKeyChecking=accept-new",
        "-o",
        "ConnectTimeout=10",
        f"{user}@{host}",
        f"test -f '{remote_state}' && cat '{remote_state}'",
    ]
    try:
        completed = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True,
            timeout=15,
        )
        raw = json.loads(completed.stdout)
    except (OSError, json.JSONDecodeError, subprocess.SubprocessError):
        return None
    return parse_market_context(raw)


def enqueue_remote_event(
    event: CalibrationEvent,
    *,
    host: str,
    user: str,
    remote_dir: str,
    ssh_key: str,
) -> None:
    inbox_dir = f"{remote_dir}/results/calibration_inbox"
    remote_name = f"{event.ready_at_unix_ms}-{event.event_id}.json"
    remote_tmp = f"{inbox_dir}/.{remote_name}.tmp"
    remote_final = f"{inbox_dir}/{remote_name}"
    payload = json.dumps(event.to_dict(), indent=2)
    cmd = [
        "ssh",
        "-i",
        ssh_key,
        "-o",
        "StrictHostKeyChecking=accept-new",
        "-o",
        "ConnectTimeout=10",
        f"{user}@{host}",
        f"mkdir -p '{inbox_dir}' && cat > '{remote_tmp}' && mv '{remote_tmp}' '{remote_final}'",
    ]
    subprocess.run(cmd, input=payload, text=True, check=True, timeout=15)


def prepare_ssh_key(source_path: str) -> str:
    src = Path(source_path)
    target = RESULTS_DIR / ".offload_ssh_key"
    target.write_bytes(src.read_bytes())
    target.chmod(0o600)
    if os.name == "nt":
        user = os.environ.get("USERNAME")
        if user:
            for cmd in (
                ["icacls", str(target), "/inheritance:r"],
                ["icacls", str(target), "/grant:r", f"{user}:R"],
            ):
                try:
                    subprocess.run(cmd, capture_output=True, text=True, timeout=5)
                except (OSError, subprocess.SubprocessError) as exc:
                    log.warning("could not tighten SSH key ACLs with icacls: %s", exc)
    return str(target)


def main() -> int:
    setup_logging()

    host = os.environ.get("VPS_HOST", "").strip()
    user = os.environ.get("VPS_USER", "ubuntu").strip()
    remote_dir = os.environ.get("VPS_DIR", "/opt/btc_pricer_15m_live").strip()
    ssh_key = os.environ.get("SSH_KEY", "").strip()

    if not host or not ssh_key:
        log.error("VPS_HOST and SSH_KEY are required")
        return 2
    prepared_key = prepare_ssh_key(ssh_key)

    while True:
        context = load_remote_market_context(
            host=host,
            user=user,
            remote_dir=remote_dir,
            ssh_key=prepared_key,
        )
        if context is None:
            time.sleep(1.0)
            continue

        now = datetime.now(timezone.utc)
        hours_remaining = max(
            0.0,
            (context.market_end_time - now).total_seconds() / 3600.0,
        )
        if context.barrier <= 0 or hours_remaining <= 0:
            time.sleep(1.0)
            continue

        model_data = run_terminal_script(
            barrier=context.barrier,
            hours=hours_remaining,
            skip_heston=True,
            json_path=OUTPUT_JSON,
            market_end_time=context.market_end_time_iso,
        )
        if model_data.get("prob_above") is not None and OUTPUT_JSON.exists():
            try:
                terminal_payload = json.loads(OUTPUT_JSON.read_text(encoding="utf-8"))
                ready_at = datetime.now(timezone.utc)
                event = CalibrationEvent.from_parts(
                    source="local_offload",
                    context=context,
                    ready_at=ready_at,
                    model_data=model_data,
                    terminal_payload=terminal_payload,
                )
                enqueue_remote_event(
                    event,
                    host=host,
                    user=user,
                    remote_dir=remote_dir,
                    ssh_key=prepared_key,
                )
                log.info(
                    "sent local calibration for %s (fit %.2fs, mc %.2fs)",
                    context.market_id,
                    float((terminal_payload.get("timing") or {}).get("surface_fit_s") or 0.0),
                    float((terminal_payload.get("timing") or {}).get("mc_s") or 0.0),
                )
            except Exception as exc:
                log.warning("failed to send local calibration offload: %s", exc)

        time.sleep(random.uniform(0.0, 3.0))


if __name__ == "__main__":
    raise SystemExit(main())
