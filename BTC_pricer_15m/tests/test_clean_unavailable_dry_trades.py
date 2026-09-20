"""Tests for the generic phantom dry-run cleaner.

Includes the regression fixture: exactly the five known instance-1271 phantoms
are flagged and the accepted control (1780667100) is not.
"""
from __future__ import annotations

import csv
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from scripts.trader import availability as av
from scripts.trader.cli import clean_unavailable_dry_trades as cleaner


UTC = timezone.utc

GRID_HEADER = [
    "instance_id", "timestamp", "event", "direction", "market_id", "entry_price",
    "exit_price", "shares", "pnl", "pnl_pct", "capital", "model_prob", "poly_prob",
    "spot_price", "barrier",
]

# (market, direction, entry, dry pnl, close event, reject detail or None=accepted)
PHANTOMS = [
    ("btc-updown-15m-1780489800", "DOWN", "0.5575", "70.04", "WIN_EXPIRY",
     "PolyApiException[status_code=425| error_message=service not ready]"),
    ("btc-updown-15m-1780490700", "UP", "0.5561", "25.73", "TP_FILLED",
     "PolyApiException[status_code=425| error_message=service not ready]"),
    ("btc-updown-15m-1780499700", "DOWN", "0.7600", "-95.11", "LOSS_EXPIRY",
     "PolyApiException[status_code=None| error_message=Request exception!]"),
    ("btc-updown-15m-1780500600", "DOWN", "0.7281", "32.50", "WIN_EXPIRY",
     "PolyApiException[status_code=None| error_message=Request exception!]"),
    ("btc-updown-15m-1780662600", "DOWN", "0.5610", "70.32", "WIN_EXPIRY",
     "PolyApiException[status_code=425| error_message=service not ready]"),
]
ACCEPTED = ("btc-updown-15m-1780667100", "DOWN", "0.9600", "1.03", "TP_FILLED")


def _t(base: str) -> datetime:
    return datetime.fromisoformat(base).replace(tzinfo=UTC) if "+" not in base else datetime.fromisoformat(base)


def _write_trades(path: Path) -> None:
    rows = [GRID_HEADER]
    ts = datetime(2026, 6, 3, 12, 0, 0, tzinfo=UTC)
    for i, (market, direction, entry, pnl, close, _detail) in enumerate(PHANTOMS):
        et = (ts + timedelta(minutes=10 * i)).isoformat()
        ct = (ts + timedelta(minutes=10 * i + 5)).isoformat()
        rows.append(["1271", et, "ENTRY", direction, market, entry, "", "10.0", "", "", "1000", "0.7", "0.5", "83000", "83100"])
        rows.append(["1271", ct, close, direction, market, entry, "1.0", "10.0", pnl, "", "1000", "0.7", "0.5", "83000", "83100"])
    # accepted control
    market, direction, entry, pnl, close = ACCEPTED
    et = (ts + timedelta(hours=2)).isoformat()
    ct = (ts + timedelta(hours=2, minutes=5)).isoformat()
    rows.append(["1271", et, "ENTRY", direction, market, entry, "", "6.0", "", "", "1000", "0.7", "0.96", "83000", "83100"])
    rows.append(["1271", ct, close, direction, market, entry, "1.0", "6.0", pnl, "", "1000", "0.7", "0.96", "83000", "83100"])
    with path.open("w", newline="", encoding="utf-8") as fh:
        csv.writer(fh).writerows(rows)


MIRROR_HEADER = [
    "timestamp", "stage", "status", "reason", "signal_id", "event", "market_id",
    "direction", "source_instance_id", "stream_id", "attempt", "signal_age_s",
    "publish_to_receive_s", "receive_to_attempt_s", "publish_to_attempt_s",
    "trigger_bid", "trigger_ask", "trigger_mid", "dry_entry_price",
    "rechecked_price", "order_id", "live_position_id", "details",
]


def _write_mirror_audit(path: Path) -> None:
    rows = [MIRROR_HEADER]
    ts = datetime(2026, 6, 3, 12, 0, 0, tzinfo=UTC)
    for i, (market, direction, _entry, _pnl, _close, detail) in enumerate(PHANTOMS):
        rt = (ts + timedelta(minutes=10 * i, seconds=3)).isoformat()
        row = {
            "timestamp": rt, "stage": "attempt_result", "status": "rejected",
            "reason": "order_failed", "event": "ENTRY", "market_id": market,
            "direction": direction, "source_instance_id": "1271", "order_id": "",
            "live_position_id": "", "details": detail,
        }
        rows.append([row.get(c, "") for c in MIRROR_HEADER])
    # accepted control
    market, direction, _e, _p, _c = ACCEPTED
    at = (ts + timedelta(hours=2, seconds=4)).isoformat()
    row = {
        "timestamp": at, "stage": "attempt_result", "status": "accepted",
        "reason": "entered", "event": "ENTRY", "market_id": market,
        "direction": direction, "source_instance_id": "1271", "order_id": "0xabc",
        "live_position_id": "2bd16833", "details": "",
    }
    rows.append([row.get(c, "") for c in MIRROR_HEADER])
    with path.open("w", newline="", encoding="utf-8") as fh:
        csv.writer(fh).writerows(rows)


@pytest.fixture
def env(tmp_path):
    trades = tmp_path / "trades.csv"
    audit = tmp_path / "15m_mirror_signal_audit.csv"
    health = tmp_path / "15m_clob_health_log.csv"
    _write_trades(trades)
    _write_mirror_audit(audit)
    health.write_text("event,timestamp,reason,source,status_code,retry_after_seconds,cooldown_seconds,unhealthy_until,duration_seconds\n", encoding="utf-8")
    return {"dir": tmp_path, "trades": trades, "audit": audit, "health": health}


def _argv(env, *extra):
    return [
        "--bot", "15m", "--instance", "1271",
        "--trades", str(env["trades"]),
        "--mirror-audit", str(env["audit"]),
        "--health-log", str(env["health"]),
        "--events", str(env["dir"] / "events.csv"),
        "--ledger", str(env["dir"] / "ledger.csv"),
        *extra,
    ]


# ── regression: exactly five phantoms, not the accepted control ──────────────────
def test_tier_a_flags_five_phantoms_not_accepted(env, capsys):
    rc = cleaner.main(_argv(env))
    assert rc == 0
    out = capsys.readouterr().out
    for market, *_ in PHANTOMS:
        assert market in out
    # accepted control present in report but marked keep, not void
    assert "Selected for void: 5" in out
    # net pnl removed = sum of the five dry pnls
    expected = sum(float(p[3]) for p in PHANTOMS)
    assert f"{expected:+.2f}" in out


def test_classify_accepted_control_is_tier_d(env):
    entries, voided = cleaner.read_grid_entries(
        env["trades"], instance_ids={1271}, since=None, until=None,
    )
    intervals, rejected = cleaner.build_evidence(
        bot="15m",
        paths={"health_log": env["health"], "mirror_audit": env["audit"], "events": None},
        instance_ids={1271}, coalesce_gap_seconds=120.0,
    )
    by_market = {e.market_id: cleaner.classify_entry(e, bot="15m", intervals=intervals, rejected=rejected)
                 for e in entries}
    for market, *_ in PHANTOMS:
        assert by_market[market].tier == "A"
    assert by_market[ACCEPTED[0]].tier == "D"  # accepted → no evidence → keep


# ── apply writes ENTRY_VOID + idempotent re-run ──────────────────────────────────
def test_apply_writes_void_markers_and_is_idempotent(env, capsys):
    rc = cleaner.main(_argv(env, "--apply", "--force"))
    assert rc == 0
    # five ENTRY_VOID rows appended
    with env["trades"].open(newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    voids = [r for r in rows if r["event"] == "ENTRY_VOID"]
    assert len(voids) == 5
    assert {r["market_id"] for r in voids} == {p[0] for p in PHANTOMS}
    # backup created
    assert list(env["dir"].glob("trades.csv.*.bak.gz"))
    # void audit trail
    assert (env["dir"] / "trades_void_audit.csv").exists()

    # re-run: already voided → nothing selected
    capsys.readouterr()
    rc2 = cleaner.main(_argv(env, "--apply", "--force"))
    assert rc2 == 0
    out2 = capsys.readouterr().out
    assert "Nothing to apply." in out2
    with env["trades"].open(newline="", encoding="utf-8") as fh:
        rows2 = list(csv.DictReader(fh))
    assert len([r for r in rows2 if r["event"] == "ENTRY_VOID"]) == 5  # no duplicates


def test_recent_writer_guard_aborts_without_force(env):
    rc = cleaner.main(_argv(env, "--apply"))  # file just written → mtime now
    assert rc == 2


# ── Tier B (confirmed interval) and Tier C (inferred, report-only) ───────────────
def test_tier_b_confirmed_interval_without_mirror_evidence(env):
    # An entry with NO mirror rejection, but inside a confirmed platform interval
    # (order_post 425 in the health log).
    entry_ts = datetime(2026, 6, 4, 10, 0, 0, tzinfo=UTC)
    with env["trades"].open("a", newline="", encoding="utf-8") as fh:
        csv.writer(fh).writerow(
            ["1271", entry_ts.isoformat(), "ENTRY", "UP", "btc-updown-15m-NOMIRROR",
             "0.5", "", "10", "", "", "1000", "0.7", "0.5", "83000", "83100"]
        )
    with env["health"].open("a", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["outage_start", entry_ts.isoformat(), "order_post_refused",
                    "live_order_post", "425", "", "90", (entry_ts + timedelta(minutes=2)).isoformat(), ""])
    entries, _ = cleaner.read_grid_entries(env["trades"], instance_ids={1271}, since=None, until=None)
    intervals, rejected = cleaner.build_evidence(
        bot="15m",
        paths={"health_log": env["health"], "mirror_audit": env["audit"], "events": None},
        instance_ids={1271}, coalesce_gap_seconds=120.0,
    )
    v = next(cleaner.classify_entry(e, bot="15m", intervals=intervals, rejected=rejected)
             for e in entries if e.market_id == "btc-updown-15m-NOMIRROR")
    assert v.tier == "B"
    assert v.confidence == av.CONFIDENCE_CONFIRMED


def test_tier_c_inferred_reported_not_applied(env):
    entry_ts = datetime(2026, 6, 4, 11, 0, 0, tzinfo=UTC)
    with env["trades"].open("a", newline="", encoding="utf-8") as fh:
        csv.writer(fh).writerow(
            ["1271", entry_ts.isoformat(), "ENTRY", "UP", "btc-updown-15m-INFER",
             "0.5", "", "10", "", "", "1000", "0.7", "0.5", "83000", "83100"]
        )
    # read-probe "books empty" → unknown/inferred
    with env["health"].open("a", newline="", encoding="utf-8") as fh:
        csv.writer(fh).writerow(["outage_start", entry_ts.isoformat(), "reads:CLOB books are empty",
                                 "live_reads_probe", "", "", "90", (entry_ts + timedelta(minutes=2)).isoformat(), ""])
    entries, _ = cleaner.read_grid_entries(env["trades"], instance_ids={1271}, since=None, until=None)
    intervals, rejected = cleaner.build_evidence(
        bot="15m",
        paths={"health_log": env["health"], "mirror_audit": env["audit"], "events": None},
        instance_ids={1271}, coalesce_gap_seconds=120.0,
    )
    v = next(cleaner.classify_entry(e, bot="15m", intervals=intervals, rejected=rejected)
             for e in entries if e.market_id == "btc-updown-15m-INFER")
    assert v.tier == "C"
    assert not v.applicable  # report-only by default


def test_5m_tier_b_from_shared_ledger(env, tmp_path):
    # 5m bot has no mirror verdicts, but consumes the 15m-exported shared platform
    # ledger. An entry inside a confirmed platform interval becomes Tier B.
    from datetime import datetime, timezone
    base = datetime(2026, 6, 3, 12, 0, 0, tzinfo=timezone.utc)
    shared = tmp_path / "shared_platform_ledger.csv"
    iv = av.AvailabilityInterval(
        scope=av.SCOPE_PLATFORM, market_id="",
        start_ts=base - timedelta(minutes=1), end_ts=base + timedelta(minutes=1),
        status="closed", reason_code="425", evidence=av.EVIDENCE_ORDER_POST,
        confidence=av.CONFIDENCE_CONFIRMED, source_bot="15m",
    )
    av.write_ledger([iv], ledger_file=shared)

    entries, _ = cleaner.read_grid_entries(env["trades"], instance_ids={1271}, since=None, until=None)
    intervals, rejected = cleaner.build_evidence(
        bot="5m",
        paths={"health_log": env["health"], "mirror_audit": env["audit"],
               "events": None, "shared_ledger": shared},
        instance_ids={1271}, coalesce_gap_seconds=120.0,
    )
    assert rejected == {}  # tier A still skipped for 5m
    # the first phantom entry (at base) falls inside the shared confirmed interval
    first_market = PHANTOMS[0][0]
    v = next(cleaner.classify_entry(e, bot="5m", intervals=intervals, rejected=rejected)
             for e in entries if e.market_id == first_market)
    assert v.tier == "B"
    assert v.confidence == av.CONFIDENCE_CONFIRMED


def test_5m_skips_tier_a(env):
    # Same data but --bot 5m: no mirror verdicts honored, so the phantoms are not
    # Tier A. With an empty health log they fall to Tier D (kept).
    entries, _ = cleaner.read_grid_entries(env["trades"], instance_ids={1271}, since=None, until=None)
    intervals, rejected = cleaner.build_evidence(
        bot="5m",
        paths={"health_log": env["health"], "mirror_audit": env["audit"], "events": None},
        instance_ids={1271}, coalesce_gap_seconds=120.0,
    )
    assert rejected == {}  # tier A not gathered for 5m
    tiers = {cleaner.classify_entry(e, bot="5m", intervals=intervals, rejected=rejected).tier for e in entries}
    assert tiers == {"D"}


# ── trader.lock liveness guard ────────────────────────────────────────────────
def test_apply_aborts_when_trader_lock_heartbeat_fresh(env, capsys):
    (env["dir"] / "trader.lock").write_text("12345", encoding="utf-8")  # fresh mtime

    rc = cleaner.main(_argv(env, "--apply"))

    assert rc == 2
    assert "grid appears to be running" in capsys.readouterr().out
    with env["trades"].open(newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    assert not [r for r in rows if r["event"] == "ENTRY_VOID"]


def test_apply_proceeds_when_trader_lock_stale(env, capsys):
    import json
    import os
    import time as time_mod

    lock = env["dir"] / "trader.lock"
    # Container-namespace owner: host PID check is skipped, so only the
    # (stale) heartbeat matters.
    lock.write_text(
        json.dumps({"schema_version": 2, "pid": 1, "hostname": "grid-ctr",
                    "container_id": "abc123"}),
        encoding="utf-8",
    )
    stale = time_mod.time() - 600
    os.utime(lock, (stale, stale))

    rc = cleaner.main(_argv(env, "--apply", "--min-quiet-seconds", "0"))

    assert rc == 0
    with env["trades"].open(newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    assert len([r for r in rows if r["event"] == "ENTRY_VOID"]) == 5


def test_apply_force_bypasses_trader_lock_guard(env, capsys):
    (env["dir"] / "trader.lock").write_text("12345", encoding="utf-8")  # fresh mtime

    rc = cleaner.main(_argv(env, "--apply", "--force"))

    assert rc == 0
    with env["trades"].open(newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    assert len([r for r in rows if r["event"] == "ENTRY_VOID"]) == 5


# ── open-ended interval guard (never auto-void on an unbounded interval) ─────────
def test_open_ended_interval_reported_not_auto_voided(env, capsys):
    # A confirmed platform interval with no end and no expiry estimate (e.g. an
    # unclosed polymarket_down edge) covers everything after its start —
    # auto-applying it would void the entire later history.
    base = datetime(2026, 6, 3, 11, 0, 0, tzinfo=UTC)
    open_iv = av.AvailabilityInterval(
        scope=av.SCOPE_PLATFORM, market_id="", start_ts=base, end_ts=None,
        status="open", reason_code="polymarket_down",
        evidence=av.EVIDENCE_STATUS_PAGE, confidence=av.CONFIDENCE_CONFIRMED,
        source_bot="15m",
    )
    entries, _ = cleaner.read_grid_entries(env["trades"], instance_ids={1271}, since=None, until=None)
    verdicts = [cleaner.classify_entry(e, bot="15m", intervals=[open_iv], rejected={}) for e in entries]
    assert verdicts and all(v.tier == "B" and v.unbounded for v in verdicts)

    selected, _pnl = cleaner._print_report(verdicts, include_inferred=False)
    assert selected == []
    out = capsys.readouterr().out
    assert "excluded" in out and "--allow-open-intervals" in out

    selected_forced, _pnl = cleaner._print_report(
        verdicts, include_inferred=False, allow_open_intervals=True
    )
    assert len(selected_forced) == len(verdicts)
