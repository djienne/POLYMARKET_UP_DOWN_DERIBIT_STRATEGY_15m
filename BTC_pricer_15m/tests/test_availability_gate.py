"""WS4 — the optional availability-ledger forward gate (grid).

Default off; when enabled it skips entries whose tick falls inside a confirmed
unavailability interval. Fail-open on a missing/corrupt ledger.
"""
from __future__ import annotations

import dataclasses
from datetime import timedelta
from pathlib import Path

import pytest

from scripts.trader import availability as av, clob_health, polymarket_status
from scripts.trader.config import GridConfig, InstanceParams
from scripts.trader.trader import PaperInstance
from tests.conftest import make_tick


@pytest.fixture(autouse=True)
def _reset_caches():
    av.reset_cache()
    clob_health.reset_cache()
    polymarket_status.reset_reads_cache()
    yield
    av.reset_cache()


def _instance(ledger: Path, *, gate_enabled: bool) -> PaperInstance:
    cfg = GridConfig(
        clob_health_gate_enabled=True,
        clob_health_shared_path=str(ledger.parent / "flag.json"),
        availability_gate_enabled=gate_enabled,
        availability_ledger_path=str(ledger),
    )
    params = InstanceParams(
        alpha_up=1.5, alpha_down=1.5, floor_up=0.55, floor_down=0.55,
        tp_percentage=0.15, stop_loss_pct=0.0,
    )
    return PaperInstance(id=0, params=params, shared_config=cfg)


def _tick():
    signal = {
        "instance_id": 0, "direction": "UP", "executable_price": 0.50,
        "model_prob": 0.60, "required_prob": 0.45, "edge_ratio": 1.2,
    }
    return dataclasses.replace(make_tick(), entry_signals=(signal,))


def _try(inst, tick, diags):
    return inst.try_entry(
        tick, tick.up_order_book, tick.up_order_book_delayed,
        tick.down_order_book, tick.down_order_book_delayed, diagnostics=diags,
    )


def _confirmed_ledger(path: Path, ts) -> None:
    iv = av.AvailabilityInterval(
        scope=av.SCOPE_PLATFORM, market_id="", start_ts=ts - timedelta(minutes=1),
        end_ts=ts + timedelta(minutes=1), status="closed", reason_code="425",
        evidence=av.EVIDENCE_ORDER_POST, confidence=av.CONFIDENCE_CONFIRMED, source_bot="15m",
    )
    av.write_ledger([iv], ledger_file=path)


def test_gate_skips_on_confirmed_interval(tmp_path, monkeypatch):
    monkeypatch.setattr("scripts.trader.trader.is_polymarket_operational", lambda: True)
    monkeypatch.setattr("scripts.trader.trader.clob_reads_healthy", lambda *a, **k: (True, None))
    ledger = tmp_path / "availability_ledger.csv"
    tick = _tick()
    _confirmed_ledger(ledger, tick.timestamp)

    diags: list = []
    result = _try(_instance(ledger, gate_enabled=True), tick, diags)
    assert result is None
    assert any(d.rejection_reason == "availability_ledger_outage" for d in diags)


def test_gate_disabled_does_not_skip(tmp_path, monkeypatch):
    monkeypatch.setattr("scripts.trader.trader.is_polymarket_operational", lambda: True)
    monkeypatch.setattr("scripts.trader.trader.clob_reads_healthy", lambda *a, **k: (True, None))
    ledger = tmp_path / "availability_ledger.csv"
    tick = _tick()
    _confirmed_ledger(ledger, tick.timestamp)

    diags: list = []
    _try(_instance(ledger, gate_enabled=False), tick, diags)
    assert all(d.rejection_reason != "availability_ledger_outage" for d in diags)


def test_gate_fail_open_on_missing_ledger(tmp_path, monkeypatch):
    monkeypatch.setattr("scripts.trader.trader.is_polymarket_operational", lambda: True)
    monkeypatch.setattr("scripts.trader.trader.clob_reads_healthy", lambda *a, **k: (True, None))
    ledger = tmp_path / "does_not_exist.csv"

    diags: list = []
    _try(_instance(ledger, gate_enabled=True), _tick(), diags)
    assert all(d.rejection_reason != "availability_ledger_outage" for d in diags)


def test_gate_ignores_inferred_interval(tmp_path, monkeypatch):
    monkeypatch.setattr("scripts.trader.trader.is_polymarket_operational", lambda: True)
    monkeypatch.setattr("scripts.trader.trader.clob_reads_healthy", lambda *a, **k: (True, None))
    ledger = tmp_path / "availability_ledger.csv"
    tick = _tick()
    iv = av.AvailabilityInterval(
        scope=av.SCOPE_PLATFORM, market_id="", start_ts=tick.timestamp - timedelta(minutes=1),
        end_ts=tick.timestamp + timedelta(minutes=1), status="open", reason_code="reads",
        evidence=av.EVIDENCE_BOOK, confidence=av.CONFIDENCE_INFERRED, source_bot="15m",
    )
    av.write_ledger([iv], ledger_file=ledger)

    diags: list = []
    _try(_instance(ledger, gate_enabled=True), tick, diags)
    # confirmed-only gate: an inferred interval must not block
    assert all(d.rejection_reason != "availability_ledger_outage" for d in diags)
