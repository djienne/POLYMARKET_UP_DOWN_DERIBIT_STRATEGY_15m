"""Watches Polymarket's Instatus status page for ongoing maintenance / outages.

Polls https://status.polymarket.com/summary.json (the same JSON feed the public
status page consumes) on a slow cadence. When the page reports a non-UP state
or an active maintenance, the dashboard surfaces a banner and stops treating
the empty CLOB books as real prices.

Why we need this: during platform upgrades the CLOB `/book` returns empty
bids/asks and `/prices-history` returns synthetic 0.5 placeholders. The chart
without context looks like a bug; with the banner it reads as "Polymarket is
down for maintenance".
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Callable, Optional

import httpx

from ..config import settings
from ..events.bus import bus
from ..models import PolymarketStatus

log = logging.getLogger(__name__)

STATUS_URL = "https://status.polymarket.com/summary.json"
COMPONENTS_URL = "https://status.polymarket.com/v3/components.json"
EMPIRICAL_CLOB_OK_POLLS = 2
COMPLETED_MAINTENANCE_STATUSES = {"COMPLETED", "RESOLVED"}
SCHEDULED_MAINTENANCE_STATUSES = {"NOTSTARTEDYET", "SCHEDULED", "UPCOMING"}


def _parse_start_at(value: object) -> Optional[datetime]:
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _maintenance_is_active(item: dict, now: datetime) -> bool:
    status = str(item.get("status") or "").upper()
    if status in COMPLETED_MAINTENANCE_STATUSES:
        return False
    if status in SCHEDULED_MAINTENANCE_STATUSES:
        return False
    start_at = _parse_start_at(item.get("start"))
    if start_at is not None and start_at > now:
        return False
    return True


def _active_maintenance_name(data: dict) -> Optional[str]:
    active_list = data.get("activeMaintenances")
    if not isinstance(active_list, list):
        return None
    now = datetime.now(timezone.utc)
    for item in active_list:
        if not isinstance(item, dict):
            continue
        if not _maintenance_is_active(item, now):
            continue
        name = item.get("name")
        if isinstance(name, str) and name:
            return name
        return "active maintenance"
    return None


def _component_status(data: Optional[dict], component_name: str) -> Optional[str]:
    components = data.get("components") if isinstance(data, dict) else None
    if not isinstance(components, list):
        return None
    wanted = component_name.casefold()
    for item in components:
        if not isinstance(item, dict):
            continue
        name = item.get("name")
        if isinstance(name, str) and name.casefold() == wanted:
            status = item.get("status")
            return str(status) if isinstance(status, str) else None
    return None


def _health_value(health: Any, key: str, default: Any = None) -> Any:
    if health is None:
        return default
    return getattr(health, key, default)


def _empirical_health_ready(health: Any) -> bool:
    return (
        _health_value(health, "healthy", False) is True
        and int(_health_value(health, "consecutive_healthy_polls", 0) or 0)
        >= EMPIRICAL_CLOB_OK_POLLS
    )


def _build_status(
    data: dict,
    components_data: Optional[dict],
    health: Any,
) -> PolymarketStatus:
    page = data.get("page") if isinstance(data, dict) else None
    page_status = (page or {}).get("status") if isinstance(page, dict) else None
    if not isinstance(page_status, str):
        page_status = "UP"

    active_name = _active_maintenance_name(data) if isinstance(data, dict) else None
    clob_status = _component_status(components_data, "CLOB API")
    page_upper = page_status.upper()
    clob_upper = clob_status.upper() if isinstance(clob_status, str) else None

    empirical_ok = bool(_health_value(health, "healthy", False))
    empirical_count = int(_health_value(health, "consecutive_healthy_polls", 0) or 0)
    health_reason = str(_health_value(health, "reason", "") or "")
    maintenance_flag = (
        active_name is not None
        or page_upper == "UNDERMAINTENANCE"
        or clob_upper == "UNDERMAINTENANCE"
    )
    if maintenance_flag:
        if _empirical_health_ready(health):
            return PolymarketStatus(
                status=page_status,
                page_status=page_status,
                clob_status=clob_status,
                effective_status="WARNING",
                is_operational=True,
                active_maintenance=active_name,
                empirical_clob_healthy=empirical_ok,
                empirical_clob_consecutive_ok=empirical_count,
                status_reason="official CLOB maintenance flag; live books reachable",
                fetched_at=datetime.now(timezone.utc).isoformat(),
            )
        return PolymarketStatus(
            status=page_status,
            page_status=page_status,
            clob_status=clob_status,
            effective_status="UNAVAILABLE",
            is_operational=False,
            active_maintenance=active_name,
            empirical_clob_healthy=empirical_ok,
            empirical_clob_consecutive_ok=empirical_count,
            status_reason=health_reason or active_name or "official CLOB maintenance flag",
            fetched_at=datetime.now(timezone.utc).isoformat(),
        )

    non_up_component = clob_upper not in (None, "OPERATIONAL")
    if page_upper != "UP" or non_up_component:
        # A page-level incident with the CLOB component fine and live books
        # empirically reachable is a warning, not an outage — e.g. a website
        # chart glitch flips the page to HASISSUES while trading works. An
        # explicitly non-operational CLOB component is never overridden.
        if not non_up_component and _empirical_health_ready(health):
            return PolymarketStatus(
                status=page_status,
                page_status=page_status,
                clob_status=clob_status,
                effective_status="WARNING",
                is_operational=True,
                active_maintenance=active_name,
                empirical_clob_healthy=empirical_ok,
                empirical_clob_consecutive_ok=empirical_count,
                status_reason=f"page {page_status}; CLOB operational, books reachable",
                fetched_at=datetime.now(timezone.utc).isoformat(),
            )
        return PolymarketStatus(
            status=page_status,
            page_status=page_status,
            clob_status=clob_status,
            effective_status="UNAVAILABLE",
            is_operational=False,
            active_maintenance=active_name,
            empirical_clob_healthy=empirical_ok,
            empirical_clob_consecutive_ok=empirical_count,
            status_reason=clob_status if non_up_component else page_status,
            fetched_at=datetime.now(timezone.utc).isoformat(),
        )

    return PolymarketStatus(
        status=page_status,
        page_status=page_status,
        clob_status=clob_status,
        effective_status="UP",
        is_operational=True,
        active_maintenance=active_name,
        empirical_clob_healthy=empirical_ok,
        empirical_clob_consecutive_ok=empirical_count,
        status_reason=None,
        fetched_at=datetime.now(timezone.utc).isoformat(),
    )


class PolymarketStatusWatcher:
    def __init__(self, health_fn: Optional[Callable[[], Any]] = None) -> None:
        self._client: Optional[httpx.AsyncClient] = None
        self._health_fn = health_fn
        self._status = PolymarketStatus()
        self._consecutive_failures = 0

    @property
    def status(self) -> PolymarketStatus:
        return self._status

    async def _ensure_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=settings.polymarket_request_timeout_seconds,
                headers={"User-Agent": "polymarket-streaming-dashboard/0.1"},
            )
        return self._client

    async def close(self) -> None:
        if self._client is not None:
            try:
                await self._client.aclose()
            finally:
                self._client = None

    async def poll(self) -> bool:
        """Fetch the status summary. Returns True if the published status changed."""
        try:
            client = await self._ensure_client()
            r = await client.get(STATUS_URL)
            r.raise_for_status()
            data = r.json()
        except (httpx.HTTPError, ValueError) as e:
            self._consecutive_failures += 1
            if self._consecutive_failures <= 2:
                log.warning("polymarket status fetch failed: %s", e)
            # Keep last known status; don't flip the banner on a transient miss.
            return False
        self._consecutive_failures = 0

        components_data: Optional[dict] = None
        try:
            cr = await client.get(COMPONENTS_URL)
            cr.raise_for_status()
            raw_components = cr.json()
            if isinstance(raw_components, dict):
                components_data = raw_components
        except (httpx.HTTPError, ValueError) as e:
            log.debug("polymarket components fetch failed: %s", e)

        health = self._health_fn() if self._health_fn is not None else None
        new_status = _build_status(data, components_data, health)
        changed = (
            new_status.status != self._status.status
            or new_status.is_operational != self._status.is_operational
            or new_status.active_maintenance != self._status.active_maintenance
            or new_status.clob_status != self._status.clob_status
            or new_status.effective_status != self._status.effective_status
        )
        self._status = new_status
        if changed:
            log.info(
                "polymarket status: page=%s clob=%s effective=%s operational=%s "
                "maintenance=%s empirical=%s/%s reason=%s",
                new_status.status,
                new_status.clob_status,
                new_status.effective_status,
                new_status.is_operational,
                new_status.active_maintenance,
                new_status.empirical_clob_healthy,
                new_status.empirical_clob_consecutive_ok,
                new_status.status_reason,
            )
        return changed


async def run_polymarket_status_loop(
    watcher: PolymarketStatusWatcher,
    stop: asyncio.Event,
) -> None:
    try:
        while not stop.is_set():
            try:
                if await watcher.poll():
                    await bus.publish(
                        "polymarket_status.update",
                        watcher.status.model_dump(),
                    )
            except Exception:  # noqa: BLE001
                log.exception("polymarket status poll error")
            try:
                await asyncio.wait_for(
                    stop.wait(),
                    timeout=settings.polymarket_status_poll_interval_seconds,
                )
            except asyncio.TimeoutError:
                pass
    finally:
        await watcher.close()
