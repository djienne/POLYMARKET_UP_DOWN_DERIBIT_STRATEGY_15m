import asyncio
import logging
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from app.derive.equity import equity_timeseries

from app.config import settings
from app.collector import entry_registry
from app.collector import trades_tail as trades_tail_module
from app.collector.trades_tail import TradesTail
from app.main import _active_trade_tails
from app.models import TradeEvent


HEADER = "instance_id,timestamp,event,direction,market_id,entry_price,exit_price,shares,pnl,pnl_pct,capital,model_prob,poly_prob,spot_price,barrier\n"


def _row(iid, event, pnl=""):
    return f"{iid},2026-04-20T10:00:00+00:00,{event},UP,slug,0.5,,10,{pnl},,1000,0.6,0.5,70000,70100\n"


def _same_size_voided_payload(original: bytes) -> bytes:
    voided_row = _row(773, "VOIDED_PHANTOM_TP", "0.00")
    voided = (HEADER + voided_row).encode("utf-8")
    if len(voided) < len(original):
        pad = b" " * (len(original) - len(voided))
        voided = voided[:-1] + pad + b"\n"
    elif len(voided) > len(original):
        voided = voided[: len(original) - 1] + b"\n"
    assert len(voided) == len(original)
    return voided


def test_tail_picks_up_appended_rows(tmp_path: Path):
    p = tmp_path / "trades.csv"
    p.write_text(HEADER + _row(773, "ENTRY"))
    t = TradesTail(path_fn=lambda: p)
    assert len(t.seed()) == 1
    assert t.recent(773)[0].event == "ENTRY"

    with p.open("a") as f:
        f.write(_row(773, "TP_FILLED", "5.00"))
    new = t.poll()
    assert len(new) == 1
    assert new[0].event == "TP_FILLED"
    assert new[0].pnl == 5.00
    # Newest-first recent list
    r = t.recent(773, 5)
    assert r[0].event == "TP_FILLED"
    assert r[1].event == "ENTRY"


def test_seed_does_not_skip_row_appended_before_final_stat(tmp_path: Path, monkeypatch):
    p = tmp_path / "trades.csv"
    p.write_text(HEADER + _row(773, "ENTRY"), encoding="utf-8")
    parsed_size = p.stat().st_size
    original_stat = TradesTail._stat
    stat_calls = 0

    def append_during_final_seed_stat(path):
        nonlocal stat_calls
        stat_calls += 1
        if stat_calls == 2:
            with path.open("a", encoding="utf-8", newline="") as f:
                f.write(_row(773, "TP_FILLED", "5.00"))
        return original_stat(path)

    monkeypatch.setattr(TradesTail, "_stat", staticmethod(append_during_final_seed_stat))

    t = TradesTail(path_fn=lambda: p)
    seeded = t.seed()

    assert [ev.event for ev in seeded] == ["ENTRY"]
    assert t._offset == parsed_size
    assert t._last_size == parsed_size
    assert t._last_file_size_bytes == p.stat().st_size

    new = t.poll()

    assert [ev.event for ev in new] == ["TP_FILLED"]
    assert [ev.event for ev in t.recent(773, 5)] == ["TP_FILLED", "ENTRY"]


def test_tail_ignores_partial_line(tmp_path: Path):
    p = tmp_path / "trades.csv"
    p.write_text(HEADER + _row(773, "ENTRY"))
    t = TradesTail(path_fn=lambda: p)
    t.seed()
    # Append a partial line (no newline yet)
    with p.open("a") as f:
        f.write("773,2026-04-20T10:01:00+00:00,TP_FILLED,UP,slug,0.5,,10,1.00")
    new = t.poll()
    assert new == []
    # Complete the line
    with p.open("a") as f:
        f.write(",,1001,0.6,0.5,70000,70100\n")
    new = t.poll()
    assert len(new) == 1
    assert new[0].event == "TP_FILLED"


def test_lazy_seed_request_is_ignored_while_seed_is_busy(tmp_path: Path):
    p = tmp_path / "trades.csv"
    p.write_text(HEADER + _row(773, "ENTRY"))
    t = TradesTail(path_fn=lambda: p)

    t.request_seed("startup")
    assert t.take_seed_request() == "startup"
    t._busy = True

    assert t.recent(773) == []

    t._busy = False
    assert t.take_seed_request() is None


def test_tail_scoped_by_instance(tmp_path: Path):
    p = tmp_path / "trades.csv"
    p.write_text(HEADER + _row(773, "ENTRY") + _row(100, "ENTRY"))
    t = TradesTail(path_fn=lambda: p)
    t.seed()
    assert len(t.recent(773)) == 1
    assert len(t.recent(100)) == 1
    assert t.recent(999) == []


def test_tail_tags_events_with_run_source(tmp_path: Path):
    p = tmp_path / "trades.csv"
    p.write_text(HEADER + _row(773, "ENTRY"))
    t = TradesTail(path_fn=lambda: p, run_source="live")

    events = t.seed()

    assert events[0].run_source == "live"
    assert t.snapshot_all_events()[0]["run_source"] == "live"


def test_entry_registry_scopes_duplicate_entries_by_run_source():
    opened_at = "2026-04-29T21:03:43+00:00"
    entry_registry.reset()

    entry_registry.mark_emitted("live", opened_at)

    assert entry_registry.was_emitted("live", opened_at)
    assert not entry_registry.was_emitted("dry_run", opened_at)
    entry_registry.reset()


def test_live_entry_mark_does_not_suppress_dry_run_entry():
    opened_at = "2026-04-29T21:03:43+00:00"
    entry_registry.reset()
    entry_registry.mark_emitted("live", opened_at)
    live_event = TradeEvent(
        instance_id=773,
        timestamp=opened_at,
        event="ENTRY",
        run_source="live",
    )
    dry_event = TradeEvent(
        instance_id=773,
        timestamp=opened_at,
        event="ENTRY",
        run_source="dry_run",
    )

    assert entry_registry.was_emitted(live_event.run_source, live_event.timestamp)
    assert not entry_registry.was_emitted(dry_event.run_source, dry_event.timestamp)
    entry_registry.reset()


def test_realized_history_keeps_full_close_history(tmp_path: Path):
    p = tmp_path / "trades.csv"
    rows = "".join(_row(773, "TP_FILLED", "1.00") for _ in range(205))
    p.write_text(HEADER + rows)
    t = TradesTail(path_fn=lambda: p)
    t.seed()
    assert len(t.recent(773, 300)) == 200
    assert len(t.realized_history(773)) == 205


def test_realized_history_includes_unresolved_restart(tmp_path: Path):
    p = tmp_path / "trades.csv"
    p.write_text(HEADER + _row(773, "UNRESOLVED_RESTART"))
    t = TradesTail(path_fn=lambda: p)
    t.seed()
    history = t.realized_history(773)
    assert len(history) == 1
    assert history[0].event == "UNRESOLVED_RESTART"


def test_today_summary_uses_full_day_history(tmp_path: Path):
    p = tmp_path / "trades.csv"
    rows = [
        "773,2026-04-22T08:00:00+00:00,ENTRY,UP,slug,0.5,,10,,,,70000,70100\n",
        "773,2026-04-22T08:01:00+00:00,TP_FILLED,UP,slug,0.5,0.8,10,3.00,,1003,0.6,0.5,70000,70100\n",
        "773,2026-04-22T08:02:00+00:00,ENTRY,DOWN,slug,0.5,,10,,,,70000,70100\n",
        "773,2026-04-22T08:03:00+00:00,STOP_LOSS,DOWN,slug,0.5,0.2,10,-2.00,,1001,0.4,0.5,70000,70100\n",
    ]
    p.write_text(HEADER + "".join(rows))
    t = TradesTail(path_fn=lambda: p)
    t.seed()
    summary = t.today_summary(773, "2026-04-22")
    assert summary.entries == 2
    assert summary.closed == 2
    assert summary.wins == 1
    assert summary.losses == 1
    assert summary.pnl == 1.0


def test_single_trader_rows_follow_configured_default_instance(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(settings, "default_instance_id", 9999)
    live_csv = tmp_path / "15m_live_trades.csv"
    live_csv.write_text(
        "id,direction,shares,entry_price,exit_price,cost_basis,proceeds,pnl,pnl_pct,result,"
        "opened_at,closed_at,market_title,reference_price,final_btc_price,mode,model_prob,"
        "poly_price,edge_ratio,spot_price,model_used,ttm_hours,required_prob,"
        "entry_order_id,exit_order_id\n"
        "abc123,UP,10.0,0.66,0.76,6.6,7.6,1.0,15.15%,TP_FILLED,"
        "2026-04-29T21:03:43+00:00,2026-04-29T21:04:05+00:00,m,75648.0,,live,0.815,0.665,"
        "1.234,75680.0,SSVI_SURFACE,0.023,0.754,o1,o2\n",
        encoding="utf-8",
    )
    t = TradesTail(path_fn=lambda: live_csv)
    t.seed()

    assert len(t.recent(9999, 100)) == 2
    assert len(t.realized_history(9999)) == 1
    assert t.recent(100, 100) == []


def test_single_trader_rows_not_duplicated_when_unrelated_instance_polled(tmp_path: Path, monkeypatch):
    """Regression: in live mode the dashboard polls with its UI-selected
    grid instance_id (e.g. 574), but live trades land under the configured
    synthetic live bucket. Pre-fix, every miss-keyed lookup
    re-ran ``seed()`` and re-pushed the live rows into that bucket (single-trader
    rows have ``market_id=None`` so the grid-only dedup didn't catch them).
    """
    monkeypatch.setattr(settings, "default_instance_id", 9999)
    live_csv = tmp_path / "15m_live_trades.csv"
    live_csv.write_text(
        "id,direction,shares,entry_price,exit_price,cost_basis,proceeds,pnl,pnl_pct,result,"
        "opened_at,closed_at,market_title,reference_price,final_btc_price,mode,model_prob,"
        "poly_price,edge_ratio,spot_price,model_used,ttm_hours,required_prob,"
        "entry_order_id,exit_order_id\n"
        "abc123,UP,10.0,0.66,0.76,6.6,7.6,1.0,15.15%,TP_FILLED,"
        "2026-04-29T21:03:43+00:00,2026-04-29T21:04:05+00:00,m,75648.0,,live,0.815,0.665,"
        "1.234,75680.0,SSVI_SURFACE,0.023,0.754,o1,o2\n",
        encoding="utf-8",
    )
    t = TradesTail(path_fn=lambda: live_csv)
    t.seed()

    # Initial state: synthetic live bucket [9999] has ENTRY + TP_FILLED.
    assert len(t.recent(9999, 100)) == 2

    # Repeatedly poll an unrelated instance (mimics live-mode dashboard
    # bootstrapping with an unrelated grid-like instance). Pre-fix this duplicated
    # the [100] bucket on every call.
    for _ in range(5):
        t.recent(574, 50)
        t.realized_history(574)
        t.today_summary(574, "2026-04-29")

    assert len(t.recent(9999, 100)) == 2, (
        "single-trader bucket should remain at 2 events; duplication regressed"
    )


def test_active_trade_tails_dedupes_live_same_path(tmp_path: Path):
    primary = SimpleNamespace(path=tmp_path / "15m_live_trades.csv")
    live = SimpleNamespace(path=tmp_path / "15m_live_trades.csv")
    hub = SimpleNamespace(trades=primary, trades_live=live)

    assert _active_trade_tails(hub) == [primary]


def test_active_trade_tails_keeps_dry_run_live_secondary(tmp_path: Path):
    primary = SimpleNamespace(path=tmp_path / "trades.csv")
    live = SimpleNamespace(path=tmp_path / "15m_live_trades.csv")
    hub = SimpleNamespace(trades=primary, trades_live=live)

    assert _active_trade_tails(hub) == [primary, live]


def test_in_place_rewrite_triggers_reseed_and_pending_reset(tmp_path: Path):
    """Live trader can rewrite a phantom-TP_FILLED row in place to
    VOIDED_PHANTOM_TP at the same byte offset (file size unchanged).
    The tail must detect this via content hash and re-seed, and must
    flag a pending_reset so the WS publisher can emit ``trades.reset``.
    """
    p = tmp_path / "trades.csv"

    original = (HEADER + _row(773, "TP_FILLED", "5.00")).encode("utf-8")
    p.write_bytes(original)

    t = TradesTail(path_fn=lambda: p)
    t.seed()
    assert any(ev.event == "TP_FILLED" for ev in t.recent(773, 10))
    assert t.take_pending_reset() is False

    # Construct a same-byte-count rewrite that swaps TP_FILLED for
    # VOIDED_PHANTOM_TP and zeroes the pnl. Pad the result to exactly
    # match the original byte count so size-based change detection
    # CANNOT see the rewrite — the tail must rely on content hash.
    voided = _same_size_voided_payload(original)

    time.sleep(0.05)
    p.write_bytes(voided)

    new = t.poll()
    assert new == []
    assert t.take_seed_request() == "consumed_tail_rewrite"
    t.seed()
    assert t.take_pending_reset() is True
    assert t.take_pending_reset() is False
    events = [ev.event for ev in t.recent(773, 20)]
    assert "TP_FILLED" not in events
    assert "VOIDED_PHANTOM_TP" in events


def test_same_size_unchanged_file_does_not_hash_tail(tmp_path: Path, monkeypatch):
    p = tmp_path / "trades.csv"
    p.write_text(HEADER + _row(773, "ENTRY"), encoding="utf-8")
    t = TradesTail(path_fn=lambda: p)
    t.seed()

    calls = 0
    original_tail_hash = TradesTail._tail_hash

    def counted_tail_hash(path, size):
        nonlocal calls
        calls += 1
        return original_tail_hash(path, size)

    monkeypatch.setattr(TradesTail, "_tail_hash", staticmethod(counted_tail_hash))

    assert t.poll() == []
    assert calls == 0


def test_hash_reads_only_the_bounded_consumed_tail(tmp_path: Path, monkeypatch):
    import hashlib

    cap = 64
    monkeypatch.setattr(trades_tail_module, "TAIL_REWRITE_HASH_BYTES", cap)
    p = tmp_path / "large.csv"
    payload = b"old data" * 100 + b"z" * cap
    p.write_bytes(payload)
    bytes_read = 0
    open_path = Path.open

    class MeasuredFile:
        def __enter__(self):
            self.file = open_path(p, "rb")
            return self

        def __exit__(self, *args):
            self.file.close()

        def seek(self, offset):
            return self.file.seek(offset)

        def read(self, size=-1):
            nonlocal bytes_read
            data = self.file.read(size)
            bytes_read += len(data)
            return data

    monkeypatch.setattr(Path, "open", lambda *args, **kwargs: MeasuredFile())
    assert TradesTail._tail_hash(p, len(payload)) == hashlib.sha256(b"z" * cap).hexdigest()
    assert bytes_read == cap


def test_run_trades_loop_reset_does_not_replay_same_size_reseed(
    tmp_path: Path,
    monkeypatch,
):
    p = tmp_path / "trades.csv"
    original = (HEADER + _row(773, "TP_FILLED", "5.00")).encode("utf-8")
    p.write_bytes(original)
    tail = TradesTail(path_fn=lambda: p)
    tail.seed()
    time.sleep(0.05)
    p.write_bytes(_same_size_voided_payload(original))

    async def run_once():
        published = []
        stop = asyncio.Event()

        async def fake_publish(topic, data):
            published.append((topic, data))
            if topic == "trades.reset":
                stop.set()

        monkeypatch.setattr(trades_tail_module.bus, "publish", fake_publish)
        monkeypatch.setattr(trades_tail_module.settings, "poll_interval_seconds", 0.01)
        await asyncio.wait_for(
            trades_tail_module.run_trades_loop(tail, stop),
            timeout=1,
        )
        return published

    published = asyncio.run(run_once())

    assert [topic for topic, _ in published] == ["trades.reset"]
    assert published[0][1]["events"] is None
    assert published[0][1]["reason"] == "consumed_tail_rewrite"
    assert any(ev.event == "VOIDED_PHANTOM_TP" for ev in tail.recent(773, 20))


def test_run_trades_loop_reset_does_not_replay_truncated_reseed(
    tmp_path: Path,
    monkeypatch,
):
    p = tmp_path / "trades.csv"
    p.write_text(
        HEADER
        + _row(773, "ENTRY")
        + "773,2026-04-20T10:01:00+00:00,TP_FILLED,UP,slug,0.5,0.8,10,3.00,,1003,0.6,0.5,70000,70100\n",
        encoding="utf-8",
    )
    tail = TradesTail(path_fn=lambda: p)
    tail.seed()
    p.write_text(HEADER + _row(773, "ENTRY"), encoding="utf-8")

    async def run_once():
        published = []
        stop = asyncio.Event()

        async def fake_publish(topic, data):
            published.append((topic, data))
            if topic == "trades.reset":
                stop.set()

        monkeypatch.setattr(trades_tail_module.bus, "publish", fake_publish)
        monkeypatch.setattr(trades_tail_module.settings, "poll_interval_seconds", 0.01)
        await asyncio.wait_for(
            trades_tail_module.run_trades_loop(tail, stop),
            timeout=1,
        )
        return published

    published = asyncio.run(run_once())

    assert [topic for topic, _ in published] == ["trades.reset"]
    assert published[0][1]["events"] is None
    assert published[0][1]["reason"] == "truncated"
    assert [ev.event for ev in tail.recent(773, 20)] == ["ENTRY"]


def test_grid_duplicate_entry_and_close_for_same_market_are_suppressed(
    tmp_path: Path,
    caplog,
):
    p = tmp_path / "trades.csv"
    rows = [
        "773,2026-04-22T08:00:00+00:00,ENTRY,UP,slug,0.5,,10,,,,70000,70100\n",
        "773,2026-04-22T08:00:10+00:00,ENTRY,UP,slug,0.6,,10,,,,70000,70100\n",
        "773,2026-04-22T08:01:00+00:00,TP_FILLED,UP,slug,0.6,0.9,10,4.00,,1007,0.6,0.5,70000,70100\n",
        "773,2026-04-22T08:01:10+00:00,TP_FILLED,UP,slug,0.5,0.8,10,3.00,,1003,0.6,0.5,70000,70100\n",
    ]
    p.write_text(HEADER + "".join(rows))
    t = TradesTail(path_fn=lambda: p)
    caplog.set_level(logging.INFO, logger="app.collector.trades_tail")

    seeded = t.seed()

    assert [ev.event for ev in seeded] == ["ENTRY", "TP_FILLED"]
    assert len(t.recent(773, 10)) == 2
    assert len(t.realized_history(773)) == 1
    summary = t.today_summary(773, "2026-04-22")
    assert summary.entries == 1
    assert summary.closed == 1
    assert summary.pnl == 3.0
    duplicate_logs = [
        record.getMessage()
        for record in caplog.records
        if "trades_tail skipped" in record.getMessage()
    ]
    assert len(duplicate_logs) == 1
    assert "2 duplicate grid event(s)" in duplicate_logs[0]
    assert "while seeding dry_run trades.csv" in duplicate_logs[0]
    assert "ENTRY=1" in duplicate_logs[0]
    assert "TP_FILLED=1" in duplicate_logs[0]


def test_grid_duplicate_append_logs_one_poll_summary(tmp_path: Path, caplog):
    p = tmp_path / "trades.csv"
    p.write_text(
        HEADER
        + "773,2026-04-22T08:00:00+00:00,ENTRY,UP,slug,0.5,,10,,,,70000,70100\n"
        + "773,2026-04-22T08:01:00+00:00,TP_FILLED,UP,slug,0.5,0.8,10,3.00,,1003,0.6,0.5,70000,70100\n",
        encoding="utf-8",
    )
    t = TradesTail(path_fn=lambda: p)
    t.seed()
    caplog.clear()
    caplog.set_level(logging.INFO, logger="app.collector.trades_tail")

    with p.open("a", encoding="utf-8", newline="") as f:
        f.write(
            "773,2026-04-22T08:02:00+00:00,ENTRY,UP,slug,0.6,,10,,,,70000,70100\n"
        )
        f.write(
            "773,2026-04-22T08:03:00+00:00,TP_FILLED,UP,slug,0.6,0.9,10,4.00,,1007,0.6,0.5,70000,70100\n"
        )

    assert t.poll() == []
    duplicate_logs = [
        record.getMessage()
        for record in caplog.records
        if "trades_tail skipped" in record.getMessage()
    ]
    assert len(duplicate_logs) == 1
    assert "2 duplicate grid event(s)" in duplicate_logs[0]
    assert "while polling dry_run trades.csv" in duplicate_logs[0]
    assert "ENTRY=1" in duplicate_logs[0]
    assert "TP_FILLED=1" in duplicate_logs[0]


def _close_row(iid, idx, pnl, capital, market=None):
    """A grid TP_FILLED close row (unique market per idx avoids grid dedup)."""
    ts = f"2026-04-20T10:{idx % 60:02d}:{idx // 60:02d}+00:00"
    mkt = market if market is not None else f"slug-{idx}"
    return (
        f"{iid},{ts},TP_FILLED,UP,{mkt},0.5,0.9,10,"
        f"{pnl:.2f},,{capital:.2f},0.6,0.5,70000,70100\n"
    )


def test_realized_history_is_bounded_lightweight_and_span_preserving(
    tmp_path: Path, monkeypatch
):
    """Regression: the realized-history buffer must stay bounded (it was an
    unbounded list of pydantic TradeEvents — the dashboard's multi-GB leak),
    hold lightweight RealizedRecords, and PRESERVE the full history span: when
    it fills it downsamples (merges interior closes) instead of dropping early
    history, keeping the first and last points exact so the equity curve keeps
    its full time span and correct endpoints."""
    monkeypatch.setattr(trades_tail_module, "MAX_REALIZED_PER_INSTANCE", 4)
    n = 50
    rows = [_close_row(773, i, pnl=1.0, capital=1000.0 + i + 1) for i in range(n)]
    p = tmp_path / "trades.csv"
    p.write_text(HEADER + "".join(rows))

    t = TradesTail(path_fn=lambda: p)
    t.seed()

    hist = t.realized_history(773)
    assert 0 < len(hist) <= 4  # bounded, despite 50 closes
    assert all(isinstance(r, trades_tail_module.RealizedRecord) for r in hist)
    # Full span preserved: first close (1001) and latest close (1050) kept exact.
    assert hist[0].capital == 1000.0 + 1
    assert hist[-1].capital == 1000.0 + n
    # Cumulative PnL preserved through merges: equity curve still ends at total.
    series = equity_timeseries(hist, starting_capital=1000.0, prefer_event_capital=False)
    assert series[-1]["v"] == pytest.approx(1000.0 + n * 1.0)


def test_realized_history_equity_curve_matches_cumulative_pnl(tmp_path: Path):
    """The lightweight record must carry event/timestamp/pnl/capital through to
    the equity timeseries so a non-truncated curve still ends at cumulative PnL."""
    pnls = [5.0, -2.0, 3.0, 10.0]
    capital = 1000.0
    rows = []
    for i, pnl in enumerate(pnls):
        capital += pnl
        rows.append(_close_row(773, i, pnl=pnl, capital=capital))
    p = tmp_path / "trades.csv"
    p.write_text(HEADER + "".join(rows))

    t = TradesTail(path_fn=lambda: p)
    t.seed()

    hist = t.realized_history(773)
    assert len(hist) == len(pnls)
    series = equity_timeseries(hist, starting_capital=1000.0, prefer_event_capital=False)
    assert series[-1]["v"] == pytest.approx(1000.0 + sum(pnls))


def test_grid_dedup_sets_are_bounded(tmp_path: Path, monkeypatch):
    """Regression: grid de-dup keys must not grow without bound with the number
    of markets in trades.csv (the slow RSS creep that remained after the
    realized-buffer fix). Oldest (settled) keys are evicted; recent markets are
    still de-duplicated."""
    monkeypatch.setattr(trades_tail_module, "MAX_SEEN_GRID_KEYS", 5)
    rows = []
    for i in range(50):  # 50 distinct markets, one ENTRY each
        ts = f"2026-04-20T10:{i % 60:02d}:{i // 60:02d}+00:00"
        rows.append(f"773,{ts},ENTRY,UP,mkt-{i},0.50,,10,,,,70000,70100\n")
    # Duplicate ENTRY for the most-recent market: still within the cap window,
    # so it must be suppressed even though older keys were evicted.
    rows.append("773,2026-04-20T10:50:00+00:00,ENTRY,UP,mkt-49,0.50,,10,,,,70000,70100\n")
    p = tmp_path / "trades.csv"
    p.write_text(HEADER + "".join(rows))

    t = TradesTail(path_fn=lambda: p)
    seeded = t.seed()

    assert len(t._seen_grid_entries) <= 5  # bounded, despite 50 markets
    entry_events = [e for e in seeded if e.event == "ENTRY"]
    assert len(entry_events) == 50  # 50 unique kept, recent duplicate suppressed
