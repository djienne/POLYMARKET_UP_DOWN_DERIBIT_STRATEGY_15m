"""Tests for the shared CLOB taker-health flag and the dry-run/live entry gate.

Covers the file-flag round trip, self-expiry, retry_after clamping, fail-open on
missing/corrupt files, the error classifiers, the read-only probe verdicts, and
the grid ``try_entry`` gate that keeps dry-run matched with live.
"""
from __future__ import annotations

import csv
import dataclasses
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from scripts.trader import clob_health, polymarket_status
from scripts.trader.config import GridConfig, InstanceParams
from scripts.trader.trader import PaperInstance
from tests.conftest import make_tick


@pytest.fixture(autouse=True)
def _reset_caches() -> None:
    clob_health.reset_cache()
    polymarket_status.reset_reads_cache()
    yield
    clob_health.reset_cache()
    polymarket_status.reset_reads_cache()


def _now() -> datetime:
    return datetime.now(timezone.utc)


# --------------------------------------------------------------------------- #
# Flag file round trip + expiry
# --------------------------------------------------------------------------- #

def test_unhealthy_then_active_then_cleared(tmp_path: Path) -> None:
    flag = tmp_path / "clob_health.json"
    clob_health.mark_unhealthy("order_post_refused", path=flag, retry_after=60)

    active, reason, until = clob_health.taker_outage_active(flag)
    assert active is True
    assert reason == "order_post_refused"
    assert until is not None and until > _now()

    clob_health.mark_healthy(path=flag)
    active, reason, until = clob_health.taker_outage_active(flag)
    assert active is False
    assert reason is None


def test_outage_self_expires(tmp_path: Path) -> None:
    flag = tmp_path / "clob_health.json"
    # Flag already in the past => treated as healthy (fail-open on a stale writer).
    past = _now() - timedelta(seconds=5)
    clob_health.mark_unhealthy(
        "order_post_refused", path=flag, retry_after=1, default_cooldown=1, max_cooldown=1, now=past
    )
    clob_health.reset_cache()
    active, _reason, _until = clob_health.taker_outage_active(flag)
    assert active is False


def test_missing_file_fails_open(tmp_path: Path) -> None:
    active, reason, until = clob_health.taker_outage_active(tmp_path / "does_not_exist.json")
    assert (active, reason, until) == (False, None, None)


def test_corrupt_file_fails_open(tmp_path: Path) -> None:
    flag = tmp_path / "clob_health.json"
    flag.write_text("{ this is not json", encoding="utf-8")
    clob_health.reset_cache()
    active, _reason, _until = clob_health.taker_outage_active(flag)
    assert active is False


# --------------------------------------------------------------------------- #
# Cooldown clamping
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize(
    "retry_after, default, maximum, expected",
    [
        (None, 90.0, 120.0, 90.0),   # no retry_after -> default
        (30.0, 90.0, 120.0, 90.0),   # short retry_after floored to default
        (100.0, 90.0, 120.0, 100.0), # honored within bounds
        (999.0, 90.0, 120.0, 120.0), # long retry_after capped at max
    ],
)
def test_cooldown_clamped(tmp_path: Path, retry_after, default, maximum, expected) -> None:
    flag = tmp_path / "clob_health.json"
    now = _now()
    until = clob_health.mark_unhealthy(
        "x", path=flag, retry_after=retry_after,
        default_cooldown=default, max_cooldown=maximum, now=now,
    )
    assert abs((until - now).total_seconds() - expected) < 1e-6


# --------------------------------------------------------------------------- #
# Error classifiers
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize(
    "msg",
    [
        "Limit buy failed: PolyApiException[status_code=503, error_message={'code': 'post_only_mode', 'retry_after_seconds': 36}]",
        "PolyApiException[status_code=425, error_message=service not ready]",
        "Limit buy failed: PolyApiException[status_code=None, error_message=Request exception!]",
        "connection reset by peer",
    ],
)
def test_taker_blocking_error_detected(msg: str) -> None:
    assert clob_health.is_taker_blocking_error(msg) is True


def test_taker_blocking_error_ignores_unrelated(tmp_path: Path) -> None:
    assert clob_health.is_taker_blocking_error("insufficient balance") is False
    assert clob_health.is_taker_blocking_error("") is False
    assert clob_health.is_taker_blocking_error(None) is False


def test_hard_outage_is_subset() -> None:
    # post-only / 425 / 503 are hard outages (retry loop should fast-fail)...
    assert clob_health.is_hard_taker_outage("post_only_mode") is True
    assert clob_health.is_hard_taker_outage("status_code=425, service not ready") is True
    # ...but a transient timeout/connection blip is not (let the retry loop run).
    assert clob_health.is_hard_taker_outage("read timed out") is False
    assert clob_health.is_taker_blocking_error("read timed out") is True


@pytest.mark.parametrize(
    "msg, expected",
    [
        ("{'code': 'post_only_mode', 'retry_after_seconds': 36}", 36.0),
        ("retry_after_seconds=88", 88.0),
        ("no backoff here", None),
    ],
)
def test_parse_retry_after(msg: str, expected) -> None:
    assert clob_health.parse_retry_after(msg) == expected


@pytest.mark.parametrize(
    "msg, expected",
    [
        ("PolyApiException[status_code=425| error_message=service not ready]", 425),
        ("PolyApiException[status_code=503]", 503),
        ("rate limited, status_code=429", 429),
        ("got a 425 from the exchange", 425),
        # transport failure: status_code=None → no code, leave scope to the text
        ("PolyApiException[status_code=None error_message=Request exception!]", None),
        ("no code here", None),
        ("", None),
    ],
)
def test_parse_status_code(msg: str, expected) -> None:
    assert clob_health.parse_status_code(msg) == expected


# --------------------------------------------------------------------------- #
# Read-only probe (clob_reads_healthy) — no orders, no capital
# --------------------------------------------------------------------------- #

class _Resp:
    def __init__(self, payload: object = None, status_code: int = 200) -> None:
        self._payload = payload
        self.status_code = status_code

    def raise_for_status(self) -> None:
        pass

    def json(self) -> object:
        return self._payload


def _gamma_market(*, accepting: bool = True, enable_book: bool = True) -> list:
    return [{
        "acceptingOrders": accepting,
        "enableOrderBook": enable_book,
        "clobTokenIds": ["111", "222"],
        "outcomes": ["Up", "Down"],
    }]


def _book_with_mid() -> dict:
    return {"bids": [{"price": "0.49", "size": "100"}], "asks": [{"price": "0.51", "size": "100"}]}


def test_reads_healthy_when_accepting(monkeypatch) -> None:
    def fake_get(url, **kwargs):
        if url == polymarket_status.CLOB_OK_URL:
            return _Resp("OK", status_code=200)
        if url == polymarket_status.GAMMA_MARKETS_URL:
            return _Resp(_gamma_market(), status_code=200)
        return _Resp(_book_with_mid(), status_code=200)

    monkeypatch.setattr(polymarket_status._SESSION, "get", fake_get)
    healthy, label = polymarket_status.clob_reads_healthy(ttl=0.0)
    assert healthy is True
    assert label is None


def test_reads_block_when_not_accepting(monkeypatch) -> None:
    def fake_get(url, **kwargs):
        if url == polymarket_status.CLOB_OK_URL:
            return _Resp("OK", status_code=200)
        if url == polymarket_status.GAMMA_MARKETS_URL:
            return _Resp(_gamma_market(accepting=False), status_code=200)
        return _Resp(_book_with_mid(), status_code=200)

    monkeypatch.setattr(polymarket_status._SESSION, "get", fake_get)
    healthy, label = polymarket_status.clob_reads_healthy(ttl=0.0)
    assert healthy is False
    assert label == "market not accepting orders"


def test_reads_block_when_ok_endpoint_not_ready(monkeypatch) -> None:
    monkeypatch.setattr(
        polymarket_status._SESSION, "get",
        lambda url, **kwargs: _Resp(None, status_code=425),
    )
    healthy, label = polymarket_status.clob_reads_healthy(ttl=0.0)
    assert healthy is False
    assert label == "CLOB /ok not ready"


def test_reads_fail_open_on_transport_error(monkeypatch) -> None:
    # A probe transport failure must NOT block trading (reactive flag covers it).
    def boom(url, **kwargs):
        raise polymarket_status.requests.RequestException("network down")

    monkeypatch.setattr(polymarket_status._SESSION, "get", boom)
    healthy, _label = polymarket_status.clob_reads_healthy(ttl=0.0)
    assert healthy is True


# --------------------------------------------------------------------------- #
# Grid try_entry gate — dry-run skips while the shared flag marks an outage
# --------------------------------------------------------------------------- #

def _grid_instance(flag: Path, *, gate_enabled: bool = True) -> PaperInstance:
    cfg = GridConfig(
        clob_health_gate_enabled=gate_enabled,
        clob_health_shared_path=str(flag),
    )
    params = InstanceParams(
        alpha_up=1.5, alpha_down=1.5, floor_up=0.55, floor_down=0.55,
        tp_percentage=0.15, stop_loss_pct=0.0,
    )
    return PaperInstance(id=0, params=params, shared_config=cfg)


def _tick_with_signal():
    # The grid suppresses the entry diagnostic when there is no matching signal,
    # so attach one for instance 0 to exercise (and observe) the gate.
    signal = {
        "instance_id": 0,
        "direction": "UP",
        "executable_price": 0.50,
        "model_prob": 0.60,
        "required_prob": 0.45,
        "edge_ratio": 1.2,
    }
    return dataclasses.replace(make_tick(), entry_signals=(signal,))


def _try_entry(inst: PaperInstance, tick, diagnostics: list):
    return inst.try_entry(
        tick,
        tick.up_order_book,
        tick.up_order_book_delayed,
        tick.down_order_book,
        tick.down_order_book_delayed,
        diagnostics=diagnostics,
    )


def test_grid_try_entry_skips_on_clob_taker_outage(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr("scripts.trader.trader.is_polymarket_operational", lambda: True)
    # Isolate the reactive file-flag path from the read-only probe (no network).
    monkeypatch.setattr("scripts.trader.trader.clob_reads_healthy", lambda *a, **k: (True, None))
    flag = tmp_path / "15m_clob_health.json"
    tick = _tick_with_signal()

    # Healthy: the CLOB gate does not fire (no flag file present).
    healthy_diags: list = []
    _try_entry(_grid_instance(flag), tick, healthy_diags)
    assert all(d.rejection_reason != "clob_taker_outage" for d in healthy_diags)

    # Outage active: entry is skipped specifically for the CLOB taker outage.
    clob_health.mark_unhealthy("order_post_refused", path=flag, retry_after=60)
    outage_diags: list = []
    result = _try_entry(_grid_instance(flag), tick, outage_diags)
    assert result is None
    assert any(d.rejection_reason == "clob_taker_outage" for d in outage_diags)


def test_grid_try_entry_skips_when_clob_reads_unhealthy(tmp_path: Path, monkeypatch) -> None:
    # No flag file written (live trader down / never ran) — the grid must still
    # skip when its own read-only probe finds the CLOB not accepting orders.
    monkeypatch.setattr("scripts.trader.trader.is_polymarket_operational", lambda: True)
    monkeypatch.setattr(
        "scripts.trader.trader.clob_reads_healthy",
        lambda *a, **k: (False, "market not accepting orders"),
    )
    flag = tmp_path / "15m_clob_health.json"
    diags: list = []
    result = _try_entry(_grid_instance(flag), _tick_with_signal(), diags)
    assert result is None
    assert any(d.rejection_reason == "clob_unavailable" for d in diags)


def test_grid_gate_disabled_does_not_skip(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr("scripts.trader.trader.is_polymarket_operational", lambda: True)
    monkeypatch.setattr(
        "scripts.trader.trader.clob_reads_healthy",
        lambda *a, **k: (False, "market not accepting orders"),
    )
    flag = tmp_path / "15m_clob_health.json"
    clob_health.mark_unhealthy("order_post_refused", path=flag, retry_after=60)

    diags: list = []
    _try_entry(_grid_instance(flag, gate_enabled=False), _tick_with_signal(), diags)
    assert all(
        d.rejection_reason not in ("clob_taker_outage", "clob_unavailable") for d in diags
    )


# --- Unavailability audit log -------------------------------------------------


def _read_audit_rows(flag: Path) -> list[dict]:
    log_path = clob_health.health_log_path(flag)
    if not log_path.exists():
        return []
    with log_path.open(encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def test_audit_log_records_outage_start_once(tmp_path):
    flag = tmp_path / "15m_clob_health.json"
    t0 = _now()
    clob_health.mark_unhealthy("order_post_refused", path=flag, status_code=425, now=t0)
    # Re-mark while still active (e.g. another failed order in the same outage):
    # must NOT append a duplicate start row.
    clob_health.mark_unhealthy(
        "order_post_refused", path=flag, status_code=425, now=t0 + timedelta(seconds=10)
    )

    starts = [r for r in _read_audit_rows(flag) if r["event"] == "outage_start"]
    assert len(starts) == 1
    assert starts[0]["status_code"] == "425"
    assert starts[0]["reason"] == "order_post_refused"
    assert starts[0]["source"] == "live_order_post"


def test_audit_log_records_cleared_with_duration(tmp_path):
    flag = tmp_path / "15m_clob_health.json"
    t0 = _now()
    clob_health.mark_unhealthy("order_post_refused", path=flag, now=t0)
    clob_health.mark_healthy(path=flag, now=t0 + timedelta(seconds=30))

    cleared = [r for r in _read_audit_rows(flag) if r["event"] == "outage_cleared"]
    assert len(cleared) == 1
    assert float(cleared[0]["duration_seconds"]) == 30.0


def test_audit_log_no_clear_row_when_not_active(tmp_path):
    flag = tmp_path / "15m_clob_health.json"
    # Clearing a flag that was never unhealthy must not append a row.
    clob_health.mark_healthy(path=flag, now=_now())
    assert _read_audit_rows(flag) == []


def test_audit_log_header_written_once(tmp_path):
    flag = tmp_path / "15m_clob_health.json"
    t0 = _now()
    clob_health.mark_unhealthy("a", path=flag, now=t0)
    clob_health.mark_healthy(path=flag, now=t0 + timedelta(seconds=5))
    clob_health.mark_unhealthy("b", path=flag, now=t0 + timedelta(seconds=10))

    text = clob_health.health_log_path(flag).read_text(encoding="utf-8")
    assert text.count(",".join(clob_health._HEALTH_LOG_HEADER)) == 1


def test_audit_log_records_polymarket_edges(tmp_path):
    flag = tmp_path / "15m_clob_health.json"
    t0 = _now()
    clob_health.log_availability_event("polymarket_down", path=flag, reason="maintenance", now=t0)
    clob_health.log_availability_event(
        "polymarket_up", path=flag, reason="operational", duration_seconds=42.0, now=t0
    )

    rows = _read_audit_rows(flag)
    assert [r["event"] for r in rows] == ["polymarket_down", "polymarket_up"]
    assert rows[0]["source"] == "polymarket_status"
    assert float(rows[1]["duration_seconds"]) == 42.0


def test_audit_log_write_failure_is_swallowed(tmp_path):
    flag = tmp_path / "15m_clob_health.json"
    # Make the CSV path a directory so the append open() fails — mark_unhealthy
    # must still return normally (best-effort, never breaks the order path).
    clob_health.health_log_path(flag).mkdir(parents=True)
    until = clob_health.mark_unhealthy("x", path=flag, now=_now())
    assert until is not None


# --------------------------------------------------------------------------- #
# Scope classification in the flag payload (consumed by the 5m sibling)
# --------------------------------------------------------------------------- #

def _flag_scope(tmp_path: Path, reason: str, **kw) -> str:
    import json

    flag = tmp_path / "flag.json"
    clob_health.mark_unhealthy(reason, path=flag, **kw)
    return json.loads(flag.read_text(encoding="utf-8")).get("scope")


def test_scope_platform_for_425(tmp_path: Path) -> None:
    assert _flag_scope(tmp_path, "service not ready", status_code=425) == "platform"


def test_scope_account_for_rate_limit(tmp_path: Path) -> None:
    assert _flag_scope(tmp_path, "too many requests", status_code=429) == "account"


def test_scope_bot_host_for_transport_failure(tmp_path: Path) -> None:
    assert _flag_scope(
        tmp_path, "PolyApiException[status_code=None| error_message=Request exception!]"
    ) == "bot_host"


def test_scope_market_for_reads_probe_book(tmp_path: Path) -> None:
    # The read probe's gamma/book checks always concern the live trader's own
    # current market — scope=market lets the 5m sibling ignore them (its
    # markets are different; it probes them itself).
    assert _flag_scope(
        tmp_path, "reads:CLOB books are empty", source="live_reads_probe"
    ) == "market"


def test_scope_market_for_reads_probe_not_accepting(tmp_path: Path) -> None:
    assert _flag_scope(
        tmp_path, "reads:market not accepting orders", source="live_reads_probe"
    ) == "market"


def test_scope_platform_for_reads_probe_ok_endpoint(tmp_path: Path) -> None:
    # /ok is CLOB-wide: stays platform even from the reads probe.
    assert _flag_scope(
        tmp_path, "reads:CLOB /ok not ready", source="live_reads_probe"
    ) == "platform"
