"""WS2/WS3 live-trader tests: the read-probe now closes its own outages and the
availability ledger is rebuilt on transitions.

Before this change the read probe only ever wrote ``outage_start`` rows (454
starts / 0 clears), so the audit log could not be turned into real intervals.
The probe must now write a recovery edge when reads recover — but only for
outages IT opened (an order-POST taker outage can persist while reads look fine).
"""
from __future__ import annotations

import csv
from pathlib import Path

import pytest

from scripts.trader import availability as av, clob_health
from scripts.trader.executor import OrderResult
from tests.conftest import make_test_single_trader


class _StubExecutor:
    is_live = True

    def get_usdc_balance(self):
        return 100.0


def _read_health_rows(flag: Path) -> list[dict]:
    log_path = clob_health.health_log_path(flag)
    if not log_path.exists():
        return []
    with log_path.open(encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def _make(tmp_path, monkeypatch, *, reads_healthy, label=None):
    flag = tmp_path / "15m_clob_health.json"
    ledger = tmp_path / "availability_ledger.csv"
    monkeypatch.setattr(
        "scripts.trader.single_trader.clob_reads_healthy",
        lambda *a, **k: (reads_healthy, label),
    )
    trader = make_test_single_trader(
        tmp_path,
        executor=_StubExecutor(),
        order_book_client=None,
        clob_health_gate_enabled=True,
        clob_health_shared_path=str(flag),
        availability_ledger_enabled=True,
        availability_ledger_path=str(ledger),
    )
    return trader, flag, ledger


def test_read_probe_clears_its_own_outage(tmp_path, monkeypatch):
    # Pre-existing read-probe outage, then reads recover.
    trader, flag, ledger = _make(tmp_path, monkeypatch, reads_healthy=True)
    clob_health.mark_unhealthy("reads:CLOB books are empty", path=flag, source="live_reads_probe")
    trader._maybe_refresh_clob_taker_health()

    cleared = [r for r in _read_health_rows(flag) if r["event"] == "outage_cleared"]
    assert len(cleared) == 1  # recovery edge written (fixes 0-clears)
    active, _r, _u = clob_health.taker_outage_active(flag)
    assert active is False
    # ledger rebuilt with a closed interval
    intervals = av.load_intervals(ledger)
    assert intervals and intervals[0].status == "closed"


def test_read_probe_does_not_clear_order_post_outage(tmp_path, monkeypatch):
    # An order-POST 425 taker outage must NOT be cleared by a healthy read probe
    # (post-only: reads look fine while takers are refused) — parity preserved.
    trader, flag, _ledger = _make(tmp_path, monkeypatch, reads_healthy=True)
    clob_health.mark_unhealthy("order_post_refused", path=flag, source="live_order_post", status_code=425)
    trader._maybe_refresh_clob_taker_health()

    cleared = [r for r in _read_health_rows(flag) if r["event"] == "outage_cleared"]
    assert cleared == []  # left for a successful order / self-expiry
    active, _r, _u = clob_health.taker_outage_active(flag)
    assert active is True


def test_read_probe_unhealthy_marks_and_builds_interval(tmp_path, monkeypatch):
    trader, flag, ledger = _make(
        tmp_path, monkeypatch, reads_healthy=False, label="CLOB books are empty"
    )
    trader._maybe_refresh_clob_taker_health()

    starts = [r for r in _read_health_rows(flag) if r["event"] == "outage_start"]
    assert len(starts) == 1
    active, _r, _u = clob_health.taker_outage_active(flag)
    assert active is True
    assert ledger.exists()  # ledger refreshed (open interval present)
    assert av.load_intervals(ledger)


def test_refresh_ledger_never_raises_on_bad_path(tmp_path, monkeypatch):
    trader, _flag, _ledger = _make(tmp_path, monkeypatch, reads_healthy=True)
    # Point the ledger at a directory so the write fails — must be swallowed.
    bad = tmp_path / "as_dir"
    bad.mkdir()
    trader.config.availability_ledger_path = str(bad)
    trader._refresh_availability_ledger()  # no exception


# --------------------------------------------------------------------------- #
# Startup seeding of the polymarket status-edge recorder
# --------------------------------------------------------------------------- #

_HEALTH_HEADER = [
    "event", "timestamp", "reason", "source", "status_code",
    "retry_after_seconds", "cooldown_seconds", "unhealthy_until", "duration_seconds",
]


def _write_status_edges(flag: Path, rows: list[dict]) -> None:
    log_path = clob_health.health_log_path(flag)
    with log_path.open("w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=_HEALTH_HEADER)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in _HEALTH_HEADER})


def test_seed_resumes_unclosed_outage_when_still_down(tmp_path, monkeypatch):
    # Restart during a Polymarket outage: the edge-triggered recorder's state is
    # gone, so without seeding the recovery would never write its close edge.
    trader, flag, _ledger = _make(tmp_path, monkeypatch, reads_healthy=True)
    _write_status_edges(flag, [{
        "event": "polymarket_down", "timestamp": "2026-06-10T05:00:00+00:00",
        "reason": "HASISSUES", "source": "polymarket_status",
    }])
    monkeypatch.setattr(
        "scripts.trader.single_trader.is_polymarket_operational", lambda *a, **k: False
    )
    trader._seed_polymarket_status_state()
    assert trader._polymarket_available_last is False
    assert trader._polymarket_down_since is not None

    # The actual recovery now fires the transition and closes the period.
    monkeypatch.setattr(
        "scripts.trader.single_trader.is_polymarket_operational", lambda *a, **k: True
    )
    trader._record_polymarket_availability()
    ups = [r for r in _read_health_rows(flag) if r["event"] == "polymarket_up"]
    assert len(ups) == 1
    assert ups[0]["duration_seconds"]  # carries the true outage duration


def test_seed_noop_when_recovered_while_off(tmp_path, monkeypatch):
    # Polymarket recovered while the trader was off: writing a close edge at
    # startup would falsely cover the whole offline span — the backfill's
    # STATUS_DOWN_SELF_EXPIRY_SECONDS estimate bounds the unclosed edge instead.
    trader, flag, _ledger = _make(tmp_path, monkeypatch, reads_healthy=True)
    _write_status_edges(flag, [{
        "event": "polymarket_down", "timestamp": "2026-06-10T05:00:00+00:00",
        "reason": "HASISSUES", "source": "polymarket_status",
    }])
    monkeypatch.setattr(
        "scripts.trader.single_trader.is_polymarket_operational", lambda *a, **k: True
    )
    trader._seed_polymarket_status_state()
    assert getattr(trader, "_polymarket_available_last", None) is None
    assert [r for r in _read_health_rows(flag) if r["event"] == "polymarket_up"] == []


def test_seed_noop_when_last_edge_is_closed(tmp_path, monkeypatch):
    trader, flag, _ledger = _make(tmp_path, monkeypatch, reads_healthy=True)
    _write_status_edges(flag, [
        {"event": "polymarket_down", "timestamp": "2026-06-10T05:00:00+00:00",
         "reason": "HASISSUES", "source": "polymarket_status"},
        {"event": "polymarket_up", "timestamp": "2026-06-10T05:02:00+00:00",
         "reason": "operational", "source": "polymarket_status"},
    ])
    monkeypatch.setattr(
        "scripts.trader.single_trader.is_polymarket_operational", lambda *a, **k: False
    )
    trader._seed_polymarket_status_state()
    assert getattr(trader, "_polymarket_available_last", None) is None


def test_seed_noop_when_log_missing(tmp_path, monkeypatch):
    trader, _flag, _ledger = _make(tmp_path, monkeypatch, reads_healthy=True)
    trader._seed_polymarket_status_state()  # no log file → no-op, no exception
    assert getattr(trader, "_polymarket_available_last", None) is None
