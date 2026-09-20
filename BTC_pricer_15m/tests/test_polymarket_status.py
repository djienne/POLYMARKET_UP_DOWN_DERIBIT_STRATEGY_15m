from __future__ import annotations

import pytest

from scripts.trader import polymarket_status


class _Resp:
    def __init__(self, payload: object) -> None:
        self._payload = payload

    def raise_for_status(self) -> None:
        pass

    def json(self) -> object:
        return self._payload


@pytest.fixture(autouse=True)
def _reset_status_cache(monkeypatch) -> None:
    monkeypatch.setattr(polymarket_status, "_cached_is_up", False)
    monkeypatch.setattr(polymarket_status, "_cached_at", 0.0)
    monkeypatch.setattr(polymarket_status, "_cached_label", polymarket_status.UNKNOWN_STATUS_LABEL)
    monkeypatch.setattr(polymarket_status, "_has_successful_refresh", False)
    monkeypatch.setattr(polymarket_status, "_consecutive_failures", 0)
    monkeypatch.setattr(polymarket_status, "_consecutive_empirical_ok", 0)
    monkeypatch.setattr(polymarket_status, "_last_empirical_slug", None)


def test_trader_polymarket_status_fails_closed_on_cold_fetch_failure(monkeypatch) -> None:
    def _raise(*args, **kwargs):
        raise polymarket_status.requests.RequestException("simulated status outage")

    monkeypatch.setattr(polymarket_status.requests, "get", _raise)

    assert polymarket_status.is_polymarket_operational(force_refresh=True) is False
    assert polymarket_status.polymarket_status_label() == polymarket_status.FETCH_FAILED_STATUS_LABEL


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"page": {}, "activeMaintenances": []},
        {"page": {"status": ""}, "activeMaintenances": []},
    ],
)
def test_trader_polymarket_status_fails_closed_on_cold_malformed_payload(
    monkeypatch,
    payload: object,
) -> None:
    monkeypatch.setattr(polymarket_status.requests, "get", lambda *args, **kwargs: _Resp(payload))

    assert polymarket_status.is_polymarket_operational(force_refresh=True) is False
    assert polymarket_status.polymarket_status_label() == polymarket_status.FETCH_FAILED_STATUS_LABEL


def test_trader_polymarket_status_preserves_confirmed_up_on_later_fetch_failure(monkeypatch) -> None:
    monkeypatch.setattr(
        polymarket_status.requests,
        "get",
        lambda *args, **kwargs: _Resp({"page": {"status": "UP"}, "activeMaintenances": []}),
    )
    assert polymarket_status.is_polymarket_operational(force_refresh=True) is True
    assert polymarket_status.polymarket_status_label() is None

    def _raise(*args, **kwargs):
        raise polymarket_status.requests.RequestException("simulated status outage")

    monkeypatch.setattr(polymarket_status.requests, "get", _raise)

    assert polymarket_status.is_polymarket_operational(force_refresh=True) is True
    assert polymarket_status.polymarket_status_label() is None


def test_trader_polymarket_status_preserves_confirmed_up_on_later_malformed_payload(monkeypatch) -> None:
    monkeypatch.setattr(
        polymarket_status.requests,
        "get",
        lambda *args, **kwargs: _Resp({"page": {"status": "UP"}, "activeMaintenances": []}),
    )
    assert polymarket_status.is_polymarket_operational(force_refresh=True) is True
    assert polymarket_status.polymarket_status_label() is None

    monkeypatch.setattr(polymarket_status.requests, "get", lambda *args, **kwargs: _Resp({}))

    assert polymarket_status.is_polymarket_operational(force_refresh=True) is True
    assert polymarket_status.polymarket_status_label() is None


def test_trader_polymarket_status_preserves_confirmed_down_on_later_fetch_failure(monkeypatch) -> None:
    monkeypatch.setattr(
        polymarket_status.requests,
        "get",
        lambda *args, **kwargs: _Resp({
            "page": {"status": "UNDERMAINTENANCE"},
            "activeMaintenances": [],
        }),
    )
    monkeypatch.setattr(
        polymarket_status,
        "_empirical_clob_health",
        lambda: polymarket_status.ClobHealth(False, "CLOB books are empty"),
    )
    assert polymarket_status.is_polymarket_operational(force_refresh=True) is False
    assert polymarket_status.polymarket_status_label() == "UNDERMAINTENANCE"

    def _raise(*args, **kwargs):
        raise polymarket_status.requests.RequestException("simulated status outage")

    monkeypatch.setattr(polymarket_status.requests, "get", _raise)

    assert polymarket_status.is_polymarket_operational(force_refresh=True) is False
    assert polymarket_status.polymarket_status_label() == "UNDERMAINTENANCE"


def test_trader_polymarket_status_preserves_confirmed_down_on_later_malformed_payload(monkeypatch) -> None:
    monkeypatch.setattr(
        polymarket_status.requests,
        "get",
        lambda *args, **kwargs: _Resp({
            "page": {"status": "UNDERMAINTENANCE"},
            "activeMaintenances": [],
        }),
    )
    monkeypatch.setattr(
        polymarket_status,
        "_empirical_clob_health",
        lambda: polymarket_status.ClobHealth(False, "CLOB books are empty"),
    )
    assert polymarket_status.is_polymarket_operational(force_refresh=True) is False
    assert polymarket_status.polymarket_status_label() == "UNDERMAINTENANCE"

    monkeypatch.setattr(polymarket_status.requests, "get", lambda *args, **kwargs: _Resp({}))

    assert polymarket_status.is_polymarket_operational(force_refresh=True) is False
    assert polymarket_status.polymarket_status_label() == "UNDERMAINTENANCE"


@pytest.mark.parametrize("page_status", ["UNDERMAINTENANCE", "DEGRADEDPERFORMANCE", "MINOROUTAGE"])
def test_trader_polymarket_status_requires_page_up(monkeypatch, page_status: str) -> None:
    monkeypatch.setattr(polymarket_status, "_cached_at", 0.0)
    monkeypatch.setattr(polymarket_status, "_consecutive_empirical_ok", 0)
    monkeypatch.setattr(
        polymarket_status.requests,
        "get",
        lambda *args, **kwargs: _Resp({"page": {"status": page_status}, "activeMaintenances": []}),
    )
    # Keep hermetic: the degraded-state probe must not hit the network.
    monkeypatch.setattr(
        polymarket_status,
        "_empirical_clob_health",
        lambda: polymarket_status.ClobHealth(False, "unavailable"),
    )

    assert polymarket_status.is_polymarket_operational(force_refresh=True) is False
    assert polymarket_status.polymarket_status_label() == page_status


def test_trader_polymarket_status_blocks_active_maintenance(monkeypatch) -> None:
    monkeypatch.setattr(polymarket_status, "_cached_at", 0.0)
    monkeypatch.setattr(polymarket_status, "_consecutive_empirical_ok", 0)
    monkeypatch.setattr(
        polymarket_status.requests,
        "get",
        lambda *args, **kwargs: _Resp({
            "page": {"status": "UP"},
            "activeMaintenances": [{"name": "CLOB upgrade", "status": "INPROGRESS"}],
        }),
    )

    assert polymarket_status.is_polymarket_operational(force_refresh=True) is False
    assert polymarket_status.polymarket_status_label() == "CLOB upgrade"


def test_trader_polymarket_status_allows_clob_maintenance_after_empirical_health(
    monkeypatch,
) -> None:
    monkeypatch.setattr(polymarket_status, "_cached_at", 0.0)
    monkeypatch.setattr(polymarket_status, "_consecutive_empirical_ok", 0)

    def _get(url, *args, **kwargs):
        if url == polymarket_status.COMPONENTS_URL:
            return _Resp({"components": [{"name": "CLOB API", "status": "UNDERMAINTENANCE"}]})
        return _Resp({"page": {"status": "UNDERMAINTENANCE"}, "activeMaintenances": []})

    monkeypatch.setattr(polymarket_status.requests, "get", _get)
    monkeypatch.setattr(
        polymarket_status,
        "_empirical_clob_health",
        lambda: polymarket_status.ClobHealth(True, "live CLOB books reachable"),
    )

    assert polymarket_status.is_polymarket_operational(force_refresh=True) is False
    assert polymarket_status.is_polymarket_operational(force_refresh=True) is True
    assert polymarket_status.polymarket_status_label() == "CLOB maintenance; empirical health OK"


def test_trader_polymarket_status_blocks_clob_maintenance_when_health_fails(
    monkeypatch,
) -> None:
    monkeypatch.setattr(polymarket_status, "_cached_at", 0.0)
    monkeypatch.setattr(polymarket_status, "_consecutive_empirical_ok", 0)

    def _get(url, *args, **kwargs):
        if url == polymarket_status.COMPONENTS_URL:
            return _Resp({"components": [{"name": "CLOB API", "status": "UNDERMAINTENANCE"}]})
        return _Resp({"page": {"status": "UNDERMAINTENANCE"}, "activeMaintenances": []})

    monkeypatch.setattr(polymarket_status.requests, "get", _get)
    monkeypatch.setattr(
        polymarket_status,
        "_empirical_clob_health",
        lambda: polymarket_status.ClobHealth(False, "CLOB books are empty"),
    )

    assert polymarket_status.is_polymarket_operational(force_refresh=True) is False
    assert polymarket_status.polymarket_status_label() == "UNDERMAINTENANCE"


def test_trader_polymarket_status_ignores_future_scheduled_maintenance(monkeypatch) -> None:
    monkeypatch.setattr(polymarket_status, "_cached_at", 0.0)
    monkeypatch.setattr(polymarket_status, "_consecutive_empirical_ok", 0)
    monkeypatch.setattr(
        polymarket_status.requests,
        "get",
        lambda *args, **kwargs: _Resp({
            "page": {"status": "UP"},
            "activeMaintenances": [
                {
                    "name": "Clob restart - release",
                    "status": "NOTSTARTEDYET",
                    "start": "2999-01-01T13:10:00.000Z",
                },
            ],
        }),
    )

    assert polymarket_status.is_polymarket_operational(force_refresh=True) is True
    assert polymarket_status.polymarket_status_label() is None


def test_trader_polymarket_status_ignores_unknown_future_maintenance(monkeypatch) -> None:
    monkeypatch.setattr(polymarket_status, "_cached_at", 0.0)
    monkeypatch.setattr(polymarket_status, "_consecutive_empirical_ok", 0)
    monkeypatch.setattr(
        polymarket_status.requests,
        "get",
        lambda *args, **kwargs: _Resp({
            "page": {"status": "UP"},
            "activeMaintenances": [
                {
                    "name": "Future CLOB work",
                    "status": "PLANNED",
                    "start": "2999-01-01T13:10:00.000Z",
                },
            ],
        }),
    )

    assert polymarket_status.is_polymarket_operational(force_refresh=True) is True
    assert polymarket_status.polymarket_status_label() is None


def test_trader_polymarket_status_blocks_unknown_undated_maintenance(monkeypatch) -> None:
    monkeypatch.setattr(polymarket_status, "_cached_at", 0.0)
    monkeypatch.setattr(polymarket_status, "_consecutive_empirical_ok", 0)
    monkeypatch.setattr(
        polymarket_status.requests,
        "get",
        lambda *args, **kwargs: _Resp({
            "page": {"status": "UP"},
            "activeMaintenances": [{"name": "Ambiguous CLOB work", "status": "PLANNED"}],
        }),
    )

    assert polymarket_status.is_polymarket_operational(force_refresh=True) is False
    assert polymarket_status.polymarket_status_label() == "Ambiguous CLOB work"


def test_trader_polymarket_status_page_incident_overridden_by_empirical_health(monkeypatch) -> None:
    """A page-level incident (e.g. website chart glitch -> HASISSUES) with the
    CLOB component OPERATIONAL and live books reachable must not block entries
    once the empirical threshold is met."""

    def _get(url, *args, **kwargs):
        if url == polymarket_status.COMPONENTS_URL:
            return _Resp({"components": [{"name": "CLOB API", "status": "OPERATIONAL"}]})
        return _Resp({"page": {"status": "HASISSUES"}, "activeMaintenances": []})

    monkeypatch.setattr(polymarket_status.requests, "get", _get)
    monkeypatch.setattr(
        polymarket_status,
        "_empirical_clob_health",
        lambda: polymarket_status.ClobHealth(True, "live CLOB books reachable"),
    )

    # First refresh: only one empirical success - still blocked.
    assert polymarket_status.is_polymarket_operational(force_refresh=True) is False
    assert polymarket_status.polymarket_status_label() == "HASISSUES"

    # Second refresh meets EMPIRICAL_CLOB_OK_POLLS - entries allowed.
    assert polymarket_status.is_polymarket_operational(force_refresh=True) is True
    assert polymarket_status.polymarket_status_label() == "page HASISSUES; CLOB books reachable"


def test_trader_polymarket_status_page_incident_with_clob_down_never_overridden(monkeypatch) -> None:
    def _get(url, *args, **kwargs):
        if url == polymarket_status.COMPONENTS_URL:
            return _Resp({"components": [{"name": "CLOB API", "status": "DEGRADEDPERFORMANCE"}]})
        return _Resp({"page": {"status": "HASISSUES"}, "activeMaintenances": []})

    monkeypatch.setattr(polymarket_status.requests, "get", _get)
    monkeypatch.setattr(
        polymarket_status,
        "_empirical_clob_health",
        lambda: polymarket_status.ClobHealth(True, "live CLOB books reachable"),
    )

    for _ in range(3):
        assert polymarket_status.is_polymarket_operational(force_refresh=True) is False
    assert polymarket_status.polymarket_status_label() == "HASISSUES"
