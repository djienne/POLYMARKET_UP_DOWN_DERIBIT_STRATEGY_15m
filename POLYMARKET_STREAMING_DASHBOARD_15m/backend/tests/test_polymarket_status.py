"""Verify status watcher recovers from maintenance → operational."""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from app.collector.polymarket_status import PolymarketStatusWatcher


class _FakeResp:
    def __init__(self, body: dict) -> None:
        self._body = body

    def raise_for_status(self) -> None:  # pragma: no cover
        pass

    def json(self) -> dict:
        return self._body


class _FakeClient:
    """Yields a queue of responses so a single watcher can see status flip."""

    def __init__(self, responses: list[dict], components: list[dict] | None = None) -> None:
        self._responses = list(responses)
        self._components = list(components or [])

    async def get(self, url: str) -> _FakeResp:
        if "components" in url:
            if self._components:
                return _FakeResp(self._components.pop(0))
            return _FakeResp({"components": []})
        return _FakeResp(self._responses.pop(0))

    async def aclose(self) -> None:
        pass


@pytest.mark.asyncio
async def test_watcher_flips_back_to_operational_after_maintenance() -> None:
    """Watcher must not get stuck in DOWN state when Polymarket recovers."""
    w = PolymarketStatusWatcher()
    w._client = _FakeClient([
        # Tick 1: maintenance starts.
        {"page": {"status": "UNDERMAINTENANCE"}, "activeMaintenances": [
            {"id": "x", "name": "CLOB v2 exchange upgrade", "status": "INPROGRESS"},
        ]},
        # Tick 2: maintenance still ongoing, but no named entry yet.
        {"page": {"status": "UNDERMAINTENANCE"}, "activeMaintenances": None},
        # Tick 3: Polymarket comes back online.
        {"page": {"status": "UP"}, "activeMaintenances": []},
        # Tick 4: still up — should remain operational.
        {"page": {"status": "UP"}},
    ])

    changed1 = await w.poll()
    assert changed1 is True
    assert w.status.is_operational is False
    assert w.status.status == "UNDERMAINTENANCE"
    assert w.status.active_maintenance == "CLOB v2 exchange upgrade"

    changed2 = await w.poll()
    assert changed2 is True  # active_maintenance went None → field changed
    assert w.status.is_operational is False

    changed3 = await w.poll()
    assert changed3 is True
    assert w.status.is_operational is True
    assert w.status.status == "UP"
    assert w.status.active_maintenance is None

    changed4 = await w.poll()
    assert changed4 is False  # idempotent — same UP state
    assert w.status.is_operational is True


@pytest.mark.asyncio
async def test_watcher_keeps_last_status_on_fetch_failure() -> None:
    """Transient network failure must not flip the banner off."""
    import httpx

    class _FailClient:
        def __init__(self) -> None:
            self.calls = 0

        async def get(self, url: str):
            self.calls += 1
            raise httpx.ConnectError("network down")

        async def aclose(self) -> None:
            pass

    w = PolymarketStatusWatcher()
    # Seed with a "down" status.
    w._client = _FakeClient([
        {"page": {"status": "UNDERMAINTENANCE"}, "activeMaintenances": []},
    ])
    await w.poll()
    assert w.status.is_operational is False

    # Now simulate fetch failures.
    w._client = _FailClient()
    changed = await w.poll()
    assert changed is False
    # Status preserved despite failure.
    assert w.status.is_operational is False
    assert w.status.status == "UNDERMAINTENANCE"


@pytest.mark.asyncio
@pytest.mark.parametrize("page_status", ["UNDERMAINTENANCE", "DEGRADEDPERFORMANCE", "MINOROUTAGE"])
async def test_watcher_treats_any_non_up_page_status_as_non_operational(page_status: str) -> None:
    w = PolymarketStatusWatcher()
    w._client = _FakeClient([
        {"page": {"status": page_status}, "activeMaintenances": []},
    ])

    await w.poll()

    assert w.status.status == page_status
    assert w.status.is_operational is False


@pytest.mark.asyncio
async def test_watcher_allows_clob_maintenance_when_empirical_health_is_ready() -> None:
    health = SimpleNamespace(
        healthy=True,
        consecutive_healthy_polls=2,
        reason="live CLOB books reachable",
    )
    w = PolymarketStatusWatcher(health_fn=lambda: health)
    w._client = _FakeClient(
        [{"page": {"status": "UNDERMAINTENANCE"}, "activeMaintenances": []}],
        components=[{"components": [{"name": "CLOB API", "status": "UNDERMAINTENANCE"}]}],
    )

    await w.poll()

    assert w.status.status == "UNDERMAINTENANCE"
    assert w.status.clob_status == "UNDERMAINTENANCE"
    assert w.status.is_operational is True
    assert w.status.effective_status == "WARNING"
    assert w.status.empirical_clob_healthy is True
    assert w.status.empirical_clob_consecutive_ok == 2
    assert "live books reachable" in (w.status.status_reason or "")


@pytest.mark.asyncio
async def test_watcher_keeps_clob_maintenance_blocked_until_health_threshold() -> None:
    health = SimpleNamespace(
        healthy=True,
        consecutive_healthy_polls=1,
        reason="live CLOB books reachable",
    )
    w = PolymarketStatusWatcher(health_fn=lambda: health)
    w._client = _FakeClient(
        [{"page": {"status": "UNDERMAINTENANCE"}, "activeMaintenances": []}],
        components=[{"components": [{"name": "CLOB API", "status": "UNDERMAINTENANCE"}]}],
    )

    await w.poll()

    assert w.status.is_operational is False
    assert w.status.effective_status == "UNAVAILABLE"
    assert w.status.empirical_clob_consecutive_ok == 1


@pytest.mark.asyncio
async def test_watcher_warns_on_page_incident_when_clob_operational_and_books_reachable() -> None:
    """A page-level incident (e.g. website chart glitch) with the CLOB component
    OPERATIONAL and live books empirically reachable must degrade to WARNING,
    not claim the CLOB is unavailable while the bot is trading."""
    health = SimpleNamespace(
        healthy=True,
        consecutive_healthy_polls=2,
        reason="live CLOB books reachable",
    )
    w = PolymarketStatusWatcher(health_fn=lambda: health)
    w._client = _FakeClient(
        [{"page": {"status": "HASISSUES"}, "activeMaintenances": []}],
        components=[{"components": [{"name": "CLOB API", "status": "OPERATIONAL"}]}],
    )

    await w.poll()

    assert w.status.status == "HASISSUES"
    assert w.status.clob_status == "OPERATIONAL"
    assert w.status.effective_status == "WARNING"
    assert w.status.is_operational is True
    assert "books reachable" in (w.status.status_reason or "")


@pytest.mark.asyncio
async def test_watcher_keeps_page_incident_unavailable_when_clob_component_down() -> None:
    """An explicitly non-operational CLOB component is never overridden by the
    empirical probe."""
    health = SimpleNamespace(
        healthy=True,
        consecutive_healthy_polls=5,
        reason="live CLOB books reachable",
    )
    w = PolymarketStatusWatcher(health_fn=lambda: health)
    w._client = _FakeClient(
        [{"page": {"status": "HASISSUES"}, "activeMaintenances": []}],
        components=[{"components": [{"name": "CLOB API", "status": "DEGRADEDPERFORMANCE"}]}],
    )

    await w.poll()

    assert w.status.effective_status == "UNAVAILABLE"
    assert w.status.is_operational is False
    assert w.status.status_reason == "DEGRADEDPERFORMANCE"


@pytest.mark.asyncio
async def test_watcher_keeps_page_incident_unavailable_below_health_threshold() -> None:
    health = SimpleNamespace(
        healthy=True,
        consecutive_healthy_polls=1,
        reason="live CLOB books reachable",
    )
    w = PolymarketStatusWatcher(health_fn=lambda: health)
    w._client = _FakeClient(
        [{"page": {"status": "HASISSUES"}, "activeMaintenances": []}],
        components=[{"components": [{"name": "CLOB API", "status": "OPERATIONAL"}]}],
    )

    await w.poll()

    assert w.status.effective_status == "UNAVAILABLE"
    assert w.status.is_operational is False


@pytest.mark.asyncio
async def test_watcher_treats_active_maintenance_as_non_operational() -> None:
    w = PolymarketStatusWatcher()
    w._client = _FakeClient([
        {"page": {"status": "UP"}, "activeMaintenances": [
            {"name": "API upgrade", "status": "INPROGRESS"},
        ]},
    ])

    await w.poll()

    assert w.status.status == "UP"
    assert w.status.is_operational is False
    assert w.status.active_maintenance == "API upgrade"


@pytest.mark.asyncio
async def test_watcher_ignores_future_scheduled_maintenance() -> None:
    w = PolymarketStatusWatcher()
    w._client = _FakeClient([
        {"page": {"status": "UP"}, "activeMaintenances": [
            {
                "name": "Clob restart - release",
                "status": "NOTSTARTEDYET",
                "start": "2999-01-01T13:10:00.000Z",
            },
        ]},
    ])

    await w.poll()

    assert w.status.status == "UP"
    assert w.status.is_operational is True
    assert w.status.active_maintenance is None


@pytest.mark.asyncio
async def test_watcher_ignores_unknown_future_maintenance() -> None:
    w = PolymarketStatusWatcher()
    w._client = _FakeClient([
        {"page": {"status": "UP"}, "activeMaintenances": [
            {
                "name": "Future CLOB work",
                "status": "PLANNED",
                "start": "2999-01-01T13:10:00.000Z",
            },
        ]},
    ])

    await w.poll()

    assert w.status.status == "UP"
    assert w.status.is_operational is True
    assert w.status.active_maintenance is None


@pytest.mark.asyncio
async def test_watcher_treats_unknown_undated_maintenance_as_active() -> None:
    w = PolymarketStatusWatcher()
    w._client = _FakeClient([
        {"page": {"status": "UP"}, "activeMaintenances": [
            {"name": "Ambiguous CLOB work", "status": "PLANNED"},
        ]},
    ])

    await w.poll()

    assert w.status.status == "UP"
    assert w.status.is_operational is False
    assert w.status.active_maintenance == "Ambiguous CLOB work"
