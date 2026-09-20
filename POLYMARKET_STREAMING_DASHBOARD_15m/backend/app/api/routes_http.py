from __future__ import annotations

import csv
import json
from pathlib import Path

from fastapi import APIRouter, Depends, Query

from ..config import settings
from .auth import require_api_token
from .state_hub import _read_simple_yaml_section, get_hub

router = APIRouter(prefix="/api")


def _read_csv_dicts(path: Path, limit: int) -> list[dict]:
    """Read the last ``limit`` rows of a CSV as dicts. Fail-soft -> []."""
    try:
        if not path.exists():
            return []
        with path.open("r", newline="", encoding="utf-8") as fh:
            rows = list(csv.DictReader(fh))
        return rows[-limit:]
    except (OSError, ValueError):
        return []


@router.get("/health")
async def health() -> dict:
    hub = get_hub()
    info = hub.liveness_info()
    dashboard_health = hub.dashboard_health()
    payload = {
        "ok": True,
        "mode": hub.effective_mode,
        "results_dir_ok": settings.resolved_results_dir.exists(),
        "bot_live": info.bot_live,
        "lock_exists": info.lock_exists,
        "last_terminal_age_s": info.terminal_age_s,
        **dashboard_health,
    }
    if settings.debug:
        payload["results_dir"] = str(settings.resolved_results_dir)
    return payload


@router.get("/bootstrap", dependencies=[Depends(require_api_token)])
async def bootstrap(
    instance_id: int = Query(default=None),
    source: str | None = Query(default=None),
) -> dict:
    hub = get_hub()
    view_source = hub.normalize_source(source)
    iid = instance_id if instance_id is not None else hub.recommended_instance_id(view_source)
    payload = hub.build_bootstrap(iid, view_source)
    return payload.model_dump()


@router.get("/instances", dependencies=[Depends(require_api_token)])
async def instances() -> list[dict]:
    hub = get_hub()
    return [r.model_dump() for r in hub.leaderboard.rows]


@router.get("/availability", dependencies=[Depends(require_api_token)])
async def availability(limit: int = Query(default=200, ge=1, le=2000)) -> dict:
    """Live-tradability ledger + recent phantom-void corrections (read-only).

    Surfaces ``availability_ledger.csv`` (normalized intervals of when live
    trading was impossible) and ``trades_void_audit.csv`` (phantom dry-run fills
    corrected by the cleaner / auto-amend), so the UI can render an availability
    timeline and mark voided trades. Fail-soft: missing files yield empty lists.
    """
    results = settings.resolved_results_dir
    return {
        "intervals": _read_csv_dicts(results / "availability_ledger.csv", limit),
        "voids": _read_csv_dicts(results / "trades_void_audit.csv", limit),
    }


@router.get("/instance/{instance_id}", dependencies=[Depends(require_api_token)])
async def instance_detail(
    instance_id: int,
    source: str | None = Query(default=None),
) -> dict:
    hub = get_hub()
    return hub.build_bootstrap(instance_id, hub.normalize_source(source)).model_dump()


@router.get("/config", dependencies=[Depends(require_api_token)])
async def merged_config() -> dict:
    out: dict = {"mode": settings.mode}
    yaml_path = settings.config_yaml_path()
    if yaml_path.exists():
        for key in ("grid", "live"):
            try:
                out[key] = _read_simple_yaml_section(yaml_path, key)
            except OSError:
                out[key] = None
    else:
        for key, path in [("grid", settings.grid_config_path()), ("live", settings.live_config_path())]:
            try:
                out[key] = json.loads(path.read_text(encoding="utf-8"))
            except (FileNotFoundError, json.JSONDecodeError):
                out[key] = None
    return out
