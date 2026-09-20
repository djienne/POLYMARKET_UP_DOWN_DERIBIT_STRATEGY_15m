import asyncio
import json
import os

import pytest

from app.api.state_hub import Hub
from app.collector.state_reader import run_state_loop
from app.collector.trades_tail import TradesTail, run_trades_loop
from app.config import settings
from app.events.bus import bus
from tests.test_trades_tail import HEADER, _row


def test_seed_waits_for_complete_final_record(tmp_path):
    path = tmp_path / "trades.csv"
    complete = HEADER + _row(773, "ENTRY")
    close = _row(773, "TP_FILLED", "5.00")
    cut = len(close) - 5
    path.write_text(complete + close[:cut])
    tail = TradesTail(lambda: path)
    assert [ev.event for ev in tail.seed()] == ["ENTRY"]
    with path.open("a") as file:
        file.write(close[cut:])
    assert [ev.event for ev in tail.poll()] == ["TP_FILLED"]
    assert tail.poll() == []


def test_entry_void_replay_removes_original_close_from_equity(tmp_path):
    path = tmp_path / "trades.csv"
    path.write_text(HEADER + _row(773, "ENTRY") + _row(773, "TP_FILLED", "5")
                    + _row(773, "STOP_LOSS", "-2").replace(",slug,", ",other,"))
    tail = TradesTail(lambda: path)
    tail.seed()
    assert sum(ev.pnl or 0 for ev in tail.realized_history(773)) == 3
    with path.open("a") as file:
        file.write(_row(773, "ENTRY_VOID"))
    assert tail.poll() == []
    assert tail.take_seed_request() == "entry_void"
    tail.seed()
    assert tail.take_pending_reset()
    assert sum(ev.pnl or 0 for ev in tail.realized_history(773)) == -2
    assert [ev.event for ev in tail.recent(773)] == ["ENTRY_VOID", "STOP_LOSS"]
    summary = tail.today_summary(773, "2026-04-20")
    assert (summary.entries, summary.closed, summary.pnl) == (0, 1, -2)


@pytest.mark.asyncio
@pytest.mark.parametrize("atomic,size", [(True, "grow"), (True, "same"), (True, "shrink"), (False, "grow")])
async def test_corrected_csv_resets_once_then_continues_appending(tmp_path, monkeypatch, atomic, size):
    path = tmp_path / "trades.csv"
    old = HEADER + _row(773, "TP_FILLED", "5.00")
    if size == "shrink":
        old += _row(774, "TP_FILLED", "5.00")
    path.write_text(old)
    tail = TradesTail(lambda: path)
    tail.seed()
    corrected = HEADER + _row(773, "VOIDED_PHANTOM_TP" if size != "same" else "STOP_LOSS", "0.00")
    if atomic:
        replacement = tmp_path / "replacement"
        replacement.write_text(corrected)
        replacement.replace(path)
    else:
        path.write_text(corrected)
    published = []
    reset = asyncio.Event()
    async def capture(topic, data):
        published.append(topic)
        if topic == "trades.reset":
            reset.set()
    unsub = bus.subscribe("*", capture)
    stop = asyncio.Event()
    monkeypatch.setattr(settings, "poll_interval_seconds", .005)
    task = asyncio.create_task(run_trades_loop(tail, stop))
    try:
        await asyncio.wait_for(reset.wait(), timeout=1)
    finally:
        stop.set()
        await task
        unsub()
    assert published == ["trades.reset"]
    assert sum(ev.pnl or 0 for ev in tail.realized_history(773)) == 0
    assert all(ev.event != "TP_FILLED" for ev in tail.recent(773))
    with path.open("a") as file:
        file.write(_row(775, "TP_FILLED", "2.00"))
    assert [(ev.instance_id, ev.pnl) for ev in tail.poll()] == [(775, 2.0)]
    assert tail.poll() == []


def test_atomic_replace_during_seed_keeps_previous_snapshot(tmp_path, monkeypatch):
    path = tmp_path / "trades.csv"
    path.write_text(HEADER + _row(773, "TP_FILLED", "5.00"))
    tail = TradesTail(lambda: path)
    tail.seed()
    original_stat = tail._stat
    calls = 0
    def replace_after_read(p):
        nonlocal calls
        calls += 1
        if calls == 2:
            replacement = tmp_path / "replacement"
            replacement.write_text(HEADER + _row(773, "STOP_LOSS", "-2.00"))
            replacement.replace(p)
        return original_stat(p)
    monkeypatch.setattr(tail, "_stat", replace_after_read)
    assert tail.seed() == []
    assert tail.recent(773)[0].pnl == 5
    assert tail.take_seed_request() == "replaced_during_seed"
    tail.seed()
    assert tail.recent(773)[0].pnl == -2


@pytest.mark.asyncio
async def test_bootstrap_and_slug_reads_cannot_consume_live_entry(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "mode", "live")
    monkeypatch.setattr(settings, "results_dir", tmp_path)
    monkeypatch.setattr(settings, "config_dir", tmp_path)
    monkeypatch.setattr(settings, "state_poll_interval_seconds", .005)
    path = tmp_path / "15m_live_state.json"
    raw = {"capital": {"starting": 100, "current": 100, "total_pnl": 0}, "open_positions": []}
    path.write_text(json.dumps(raw))
    hub = Hub()
    hub.state_live.read_if_changed()
    published = []
    entry = asyncio.Event()
    async def capture(topic, data):
        published.append(topic)
        if topic == "trade.append":
            entry.set()
    unsub = bus.subscribe("*", capture)
    stop = asyncio.Event()
    task = asyncio.create_task(run_state_loop(hub.state_live, stop))
    try:
        await asyncio.sleep(0)
        raw["open_positions"] = [{"opened_at": "2026-09-05T10:00:00+00:00", "direction": "UP", "entry_price": .5, "shares": 10}]
        path.write_text(json.dumps(raw))
        os.utime(path, (path.stat().st_atime, hub.state_live._last_mtime + 1))
        hub._current_slug()
        assert hub.build_bootstrap(1271).mode == "live"
        await asyncio.wait_for(entry.wait(), timeout=1)
    finally:
        stop.set()
        await task
        unsub()
    assert published == ["state.update", "trade.append"]
    assert hub.build_bootstrap(1271).position.open.direction == "UP"


def test_live_equity_uses_observed_balance_not_trade_pnl(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "mode", "live")
    monkeypatch.setattr(settings, "results_dir", tmp_path)
    monkeypatch.setattr(settings, "config_dir", tmp_path)
    (tmp_path / "15m_live_state.json").write_text(json.dumps({
        "capital": {"starting": 100, "current": 118.9997, "total_pnl": -1.0003},
        "closed_positions": [{"result": "STOP_LOSS", "pnl": -1.0003, "cost_basis": 5, "proceeds": 3.9997,
                              "opened_at": "2026-09-05T09:59:00Z", "closed_at": "2026-09-05T10:00:00Z"}],
        "open_positions": [],
    }))
    equity_path = tmp_path / "15m_live_equity.csv"
    equity_path.write_text("timestamp,event,capital,equity,position_value\n2026-09-04T20:00:00Z,RECONCILED,120,120,0\n2026-09-05T10:00:00Z,STOP_LOSS,119,119,0\n")
    hub = Hub()
    hub.state_live.read_if_changed()
    hub.equity_live.read_if_changed()
    snapshot = hub.build_bootstrap(1271)
    assert snapshot.instance.capital == 118.9997
    assert snapshot.instance.total_pnl == -1.0003
    assert snapshot.equity_series[-1].model_dump() == {"t": "2026-09-05T10:00:00Z", "v": 119}
    day = hub._live_today_summary("2026-09-05", 100, 118.9997)
    assert day.pnl == -1.0003
    assert day.pnl_pct == pytest.approx(-1.0003 / 120 * 100)
    equity_path.write_text("timestamp,event,capital,equity,position_value\n2026-09-05T11:00:00Z,ENTRY,114,")
    assert not hub.equity_live.read_if_changed()
    assert hub.equity_live.series() == [point.model_dump() for point in snapshot.equity_series]
