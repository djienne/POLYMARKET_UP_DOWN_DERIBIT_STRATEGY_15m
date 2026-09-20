"""Tests for the grid entry confirmation hold (dry-run / live parity).

When ``grid_entry_confirm_seconds > 0`` the grid stages an entry instead of
committing it instantly, then finalizes it a few wall-clock seconds later only
if the shared CLOB taker-health flag is still healthy. This lets the live
trader's real ``POST /order`` discover a 425 / post-only outage first, so the
grid drops entries the live trader could not place.

Regression anchor: 2026-06-03 instance 1271 recorded 2 phantom wins (12:39:28
and 12:52:03) that the live trader hit a 425 on ~3s later. With the hold, those
entries are dropped.
"""
from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from btc_pricer.edge import required_model_prob
from scripts.trader import clob_health
from scripts.trader.config import GridConfig, InstanceParams
from scripts.trader.trader import PaperInstance
from tests.conftest import make_order_book, make_tick

T0 = datetime(2026, 6, 3, 12, 39, 28, 251000, tzinfo=timezone.utc)


@pytest.fixture(autouse=True)
def _operational(monkeypatch):
    """Keep Polymarket-operational and the read probe healthy so the only gate
    under test is the staged-entry confirmation hold."""
    monkeypatch.setattr(
        "scripts.trader.trader.is_polymarket_operational", lambda: True, raising=False
    )


def _signal(direction, model_prob, market_prob, alpha, floor):
    return {
        "instance_id": 0,
        "direction": direction,
        "edge_ratio": round(model_prob / market_prob, 4) if market_prob > 0 else 0,
        "executable_price": market_prob,
        "required_prob": round(required_model_prob(market_prob, alpha, floor), 4),
        "model_prob": round(model_prob, 4),
    }


def _book(bid, ask, ask_size=1000.0):
    return make_order_book(
        best_bid=bid, best_ask=ask, bid_size=1000.0, ask_size=ask_size, levels=1
    )


def _config(tmp_path, confirm=4.0):
    return GridConfig(
        starting_capital=1000.0,
        order_size_pct=0.10,
        no_trade_first_seconds=300,
        no_trade_last_seconds=180,
        grid_entry_confirm_seconds=confirm,
        clob_health_shared_path=str(tmp_path / "15m_clob_health.json"),
    )


def _instance(cfg):
    params = InstanceParams(
        alpha_up=1.0, alpha_down=1.0, floor_up=0.45, floor_down=0.45,
        tp_percentage=0.15, stop_loss_pct=0.20,
    )
    return PaperInstance(id=0, params=params, shared_config=cfg)


def _entry_tick(ts=T0, ask=0.50, time_remaining=400.0):
    sig = _signal("UP", 0.85, 0.50, 1.0, 0.45)
    base = make_tick(
        prob_above=0.85, poly_prob_up=0.50,
        elapsed_seconds=400.0, time_remaining_seconds=time_remaining,
    )
    bid = round(ask - 0.02, 4)  # keep the book uncrossed for any ask
    return replace(
        base,
        timestamp=ts,
        up_order_book=_book(bid, ask, ask_size=500),
        up_order_book_delayed=_book(bid, ask, ask_size=500),
        entry_signals=(sig,),
    )


def test_staged_entry_holds_then_finalizes(tmp_path):
    cfg = _config(tmp_path, confirm=4.0)
    inst = _instance(cfg)
    cap0 = inst.capital
    diags = []

    ev1 = inst.on_tick(_entry_tick(ts=T0), diagnostics=diags)
    assert ev1 == []
    assert inst.position is None
    assert inst.staged_entry is not None
    assert inst.capital == cap0  # no debit while staged
    assert any(d.rejection_reason == "entry_staged" for d in diags)

    ev2 = inst.on_tick(_entry_tick(ts=T0 + timedelta(seconds=5)), diagnostics=diags)
    assert len(ev2) == 1
    assert ev2[0].event == "ENTRY"
    assert ev2[0].timestamp == T0 + timedelta(seconds=5)
    assert ev2[0].decision_at == T0
    assert inst.position.opened_at == T0 + timedelta(seconds=5)
    assert inst.position is not None
    assert inst.staged_entry is None
    assert inst.capital < cap0


def test_staged_entry_waits_until_confirm_elapsed(tmp_path):
    cfg = _config(tmp_path, confirm=4.0)
    inst = _instance(cfg)
    inst.on_tick(_entry_tick(ts=T0))
    # Only 2s later — hold not yet elapsed: still staged, no event.
    ev = inst.on_tick(_entry_tick(ts=T0 + timedelta(seconds=2)))
    assert ev == []
    assert inst.position is None
    assert inst.staged_entry is not None


def test_staged_entry_dropped_on_clob_outage(tmp_path):
    """The 2026-06-03 regression: live POSTs a 425 ~3s in, the flag goes
    unhealthy, and the staged grid entry is dropped instead of recorded."""
    cfg = _config(tmp_path, confirm=4.0)
    inst = _instance(cfg)
    cap0 = inst.capital
    diags = []

    inst.on_tick(_entry_tick(ts=T0), diagnostics=diags)
    assert inst.staged_entry is not None

    clob_health.mark_unhealthy(
        "order_post_refused",
        path=cfg.clob_health_shared_path,
        status_code=425,
        now=T0 + timedelta(seconds=1.5),
    )

    ev = inst.on_tick(_entry_tick(ts=T0 + timedelta(seconds=5)), diagnostics=diags)
    assert ev == []
    assert inst.position is None
    assert inst.staged_entry is None
    assert inst.capital == cap0  # no phantom debit/fill
    assert any(d.rejection_reason == "staged_dropped_clob_outage" for d in diags)


def test_staged_entry_dropped_on_market_rollover(tmp_path):
    cfg = _config(tmp_path, confirm=4.0)
    inst = _instance(cfg)
    inst.on_tick(_entry_tick(ts=T0))
    assert inst.staged_entry is not None

    rolled = replace(
        make_tick(
            market_id="btc-updown-15m-9999999999",
            is_new_market=True,
            elapsed_seconds=400.0,
            time_remaining_seconds=400.0,
        ),
        timestamp=T0 + timedelta(seconds=5),
        entry_signals=(),  # no signal in the new market -> no re-stage
    )
    ev = inst.on_tick(rolled)
    assert ev == []
    assert inst.staged_entry is None
    assert inst.position is None


def test_staged_entry_dropped_when_hold_crosses_last_window(tmp_path):
    cfg = _config(tmp_path, confirm=4.0)
    inst = _instance(cfg)
    cap0 = inst.capital
    diags = []

    # Stage just above the late-window cutoff (182 > 180)...
    inst.on_tick(_entry_tick(ts=T0, time_remaining=182.0), diagnostics=diags)
    assert inst.staged_entry is not None
    # ...finalize tick has crossed it (178 < 180) -> drop.
    ev = inst.on_tick(_entry_tick(ts=T0 + timedelta(seconds=5), time_remaining=178.0), diagnostics=diags)
    assert ev == []
    assert inst.position is None
    assert inst.staged_entry is None
    assert inst.capital == cap0
    assert any(d.rejection_reason == "staged_dropped_last_window" for d in diags)


def test_staged_entry_reprices_to_worse_finalize_book(tmp_path):
    cfg = _config(tmp_path, confirm=4.0)
    inst = _instance(cfg)
    inst.on_tick(_entry_tick(ts=T0, ask=0.50))
    assert round(inst.staged_entry.plan.entry_price, 4) == 0.50

    # Book worsened during the hold: record the worse finalize price.
    ev = inst.on_tick(_entry_tick(ts=T0 + timedelta(seconds=5), ask=0.60))
    assert len(ev) == 1
    assert round(ev[0].entry_price, 4) == 0.60


def test_staged_entry_keeps_worse_staged_price_when_finalize_improves(tmp_path):
    cfg = _config(tmp_path, confirm=4.0)
    inst = _instance(cfg)
    inst.on_tick(_entry_tick(ts=T0, ask=0.50))

    # Book improved during the hold (still above the 0.45 floor): keep the
    # conservative (worse) staged price rather than crediting the improvement.
    ev = inst.on_tick(_entry_tick(ts=T0 + timedelta(seconds=5), ask=0.46))
    assert len(ev) == 1
    assert round(ev[0].entry_price, 4) == 0.50


def test_confirm_seconds_zero_is_instant(tmp_path):
    cfg = _config(tmp_path, confirm=0.0)
    inst = _instance(cfg)
    ev = inst.on_tick(_entry_tick(ts=T0))
    assert len(ev) == 1
    assert ev[0].event == "ENTRY"
    assert inst.staged_entry is None
    assert inst.position is not None


def test_staged_entry_not_persisted(tmp_path):
    cfg = _config(tmp_path, confirm=4.0)
    inst = _instance(cfg)
    inst.on_tick(_entry_tick(ts=T0))
    assert inst.staged_entry is not None

    snap = inst.to_snapshot()
    assert "staged_entry" not in snap

    restored = _instance(cfg)
    restored.load_snapshot(snap)
    assert restored.staged_entry is None


def test_grid_entry_confirm_seconds_rejects_negative():
    with pytest.raises(ValueError):
        GridConfig(grid_entry_confirm_seconds=-1.0)
