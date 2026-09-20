"""Restart regression: absent contract metadata never authorizes a price proxy."""
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts.trader.contracts import MarketResolution, fetch_market_contract, parse_market_contract
from scripts.trader.market_orchestrator import MarketOrchestrator
from scripts.trader.persistence import SqliteStore
from tests.test_calc_daemon import _build_calc_config
from tests.test_twap_contracts import gamma_market


def _pending_row(store, market_id="btc-updown-15m-inherited"):
    end = datetime.now(timezone.utc)-timedelta(seconds=60)
    start = end-timedelta(minutes=15)
    store.write_strike(market_id=market_id, window_start=start, window_end=end,
                       strike=79_631.21, strike_offset_s=0, strike_source="polymarket")
    return start, end


def test_boot_pending_row_fetches_actual_contract_then_authoritative_payout(tmp_path, monkeypatch):
    import scripts.trader.calc_daemon as module
    store = MarketOrchestrator(settlements_dir=tmp_path/"settlements")
    market_id = "btc-updown-15m-inherited"
    _pending_row(store, market_id)
    daemon = module.CalcDaemon(_build_calc_config(tmp_path), terminal_path=tmp_path/"terminal.json")
    assert market_id in daemon._pending_settlements
    contract = parse_market_contract(gamma_market())
    calls = []
    def contract_lookup(slug, **kwargs):
        calls.append(slug)
        return contract
    monkeypatch.setattr(module, "fetch_market_contract", contract_lookup)
    monkeypatch.setattr(module, "fetch_market_resolution", lambda value, **kwargs: MarketResolution(
        value.condition_id, {"up-token": 0.0, "down-token": 1.0},
        "polymarket_resolved_outcome", datetime.now(timezone.utc).isoformat()))
    daemon._polymarket = SimpleNamespace(ptb_for_window=lambda *args: pytest.fail("next-window open proxy"))
    daemon._chainlink = SimpleNamespace(is_healthy=True, get_first_at_or_after=lambda *args, **kwargs: pytest.fail("instant oracle proxy"))
    daemon._archive = SimpleNamespace(get_round_at_or_after=lambda *args, **kwargs: pytest.fail("archive proxy"))
    daemon._resolve_pending_settlements()
    daemon._settlement_job[1].result(timeout=2)
    daemon._resolve_pending_settlements()
    saved = store.read_market(market_id)
    assert calls == [market_id]
    assert saved["contract_metadata"]["condition_id"] == contract.condition_id
    assert saved["settlement_payouts"] == {"up-token": 0.0, "down-token": 1.0}
    assert saved["settlement_price"] is None
    assert saved["settlement_source"] == "polymarket_resolved_outcome"
    assert market_id not in daemon._pending_settlements
    daemon._settlement_pool.shutdown(wait=True)


def test_contract_lookup_requires_exact_slug_and_mapping():
    raw = {**gamma_market(), "slug": "expected"}
    session = SimpleNamespace(get=lambda *args, **kwargs: SimpleNamespace(raise_for_status=lambda: None, json=lambda: raw))
    assert fetch_market_contract("expected", session=session) is not None
    assert fetch_market_contract("different", session=session) is None
    raw["clobTokenIds"] = "[]"
    assert fetch_market_contract("expected", session=session) is None


def test_finalized_history_is_not_requeued_or_enriched(tmp_path, monkeypatch):
    import scripts.trader.calc_daemon as module
    store = MarketOrchestrator(settlements_dir=tmp_path/"settlements")
    start, end = _pending_row(store, "historical")
    payload = store.read_market("historical")
    payload["settlement_status"] = "finalized"
    path = store.settlement_path("historical")
    path.write_text(json.dumps(payload), encoding="utf-8")
    original = path.read_bytes()
    assert not store.restore_state()[1]
    assert not store.attach_pending_contract("historical", parse_market_contract(gamma_market()))
    daemon = module.CalcDaemon(_build_calc_config(tmp_path), terminal_path=tmp_path/"terminal.json")
    daemon._pending_settlements["historical"] = (start, end, payload["strike"], 0)
    monkeypatch.setattr(module, "fetch_market_contract", lambda *args, **kwargs: pytest.fail("historical lookup"))
    daemon._resolve_pending_settlements()
    assert path.read_bytes() == original
    assert "historical" not in daemon._pending_settlements


def test_bulk_sqlite_restore_skips_exports_but_keeps_json_only_rows(tmp_path, monkeypatch):
    database = SqliteStore(tmp_path/"markets.sqlite")
    store = MarketOrchestrator(settlements_dir=tmp_path/"settlements", sqlite_store=database)
    _pending_row(store, "canonical")
    canonical = store.settlement_path("canonical")
    # The stale/corrupt export must never be opened over a valid canonical row.
    canonical.write_text("not JSON", encoding="utf-8")
    legacy_store = MarketOrchestrator(settlements_dir=store.settlements_dir)
    _pending_row(legacy_store, "json-only")
    actual_read = Path.read_text
    opened = []
    def read(path, *args, **kwargs):
        if path == canonical:
            pytest.fail("opened an already-covered SQLite export")
        opened.append(path.name)
        return actual_read(path, *args, **kwargs)
    monkeypatch.setattr(Path, "read_text", read)
    rows = store.iter_markets()
    assert {row["market_id"] for row in rows} == {"canonical", "json-only"}
    assert opened == ["json-only.json"]


def test_pending_resolution_read_does_not_block_pricing_loop(tmp_path, monkeypatch):
    import threading
    import scripts.trader.calc_daemon as module
    store = MarketOrchestrator(settlements_dir=tmp_path/"settlements")
    _pending_row(store)
    daemon = module.CalcDaemon(_build_calc_config(tmp_path), terminal_path=tmp_path/"terminal.json")
    started, release = threading.Event(), threading.Event()
    def delayed_read(*args, **kwargs):
        started.set()
        release.wait(2)
        return None
    monkeypatch.setattr(module, "fetch_market_contract", delayed_read)
    try:
        daemon._resolve_pending_settlements()
        assert started.wait(1)
        assert daemon._settlement_job is not None
        assert not daemon._settlement_job[1].done()
        daemon._resolve_pending_settlements()  # returns while network read remains pending
        assert not daemon._settlement_job[1].done()
    finally:
        release.set()
        daemon._settlement_pool.shutdown(wait=True)
