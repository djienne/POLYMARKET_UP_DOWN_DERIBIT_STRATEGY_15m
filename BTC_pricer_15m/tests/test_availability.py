"""Tests for the canonical availability (live-tradability) interval ledger.

Covers scope/confidence classification, evidence→interval coalescing,
``untradable_at`` scope/confidence/fail-open semantics, evidence round-trips,
shared-ledger scope filtering, and backfill from the legacy health log and the
mirror signal audit (the discovery-lag Tier-A case).
"""
from __future__ import annotations

import csv
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from scripts.trader import availability as av


UTC = timezone.utc


def _dt(minute: float) -> datetime:
    return datetime(2026, 6, 3, 12, 0, 0, tzinfo=UTC) + timedelta(minutes=minute)


def _ev(minute: float, event_type: str, scope: str, **kw) -> av.AvailabilityEvent:
    return av.AvailabilityEvent(
        observed_at=_dt(minute),
        event_type=event_type,
        scope=scope,
        confidence=kw.pop("confidence", av.CONFIDENCE_INFERRED),
        evidence=kw.pop("evidence", av.EVIDENCE_BOOK),
        **kw,
    )


# ── classify ───────────────────────────────────────────────────────────────────
def test_classify_platform_425():
    scope, conf = av.classify("PolyApiException[status_code=425]", source="order_post")
    assert scope == av.SCOPE_PLATFORM
    assert conf == av.CONFIDENCE_CONFIRMED


def test_classify_request_exception_is_bot_host():
    scope, conf = av.classify("Request exception!", source="order_post")
    assert scope == av.SCOPE_BOT_HOST


def test_classify_book_empty_market_vs_unknown():
    assert av.classify("reads:CLOB books are empty", has_market_id=True)[0] == av.SCOPE_MARKET
    assert av.classify("reads:CLOB books are empty", has_market_id=False)[0] == av.SCOPE_UNKNOWN


def test_classify_rate_limit_is_account():
    assert av.classify("rate limit exceeded", status_code=429)[0] == av.SCOPE_ACCOUNT


def test_classify_maintenance_is_platform():
    assert av.classify("Scheduled CLOB maintenance", source="polymarket_status")[0] == av.SCOPE_PLATFORM


def test_classify_read_probe_is_inferred():
    # read probe (not an order post / status page) is never "confirmed"
    _scope, conf = av.classify("service not ready", source="live_reads_probe")
    assert conf == av.CONFIDENCE_INFERRED


# ── build_intervals ──────────────────────────────────────────────────────────────
def test_coalesce_pings_within_gap_into_one_interval():
    # Five 90s-window pings 1 minute apart → one coalesced interval.
    events = [
        _ev(m, av.EVENT_UNAVAILABLE, av.SCOPE_PLATFORM, window_until=_dt(m + 1.5))
        for m in (0, 1, 2, 3, 4)
    ]
    intervals = av.build_intervals(events, coalesce_gap_seconds=120.0)
    assert len(intervals) == 1
    iv = intervals[0]
    assert iv.start_ts == _dt(0)
    assert iv.end_ts == _dt(5.5)  # last window_until
    assert iv.status == "open"  # never saw a recovery edge


def test_gap_exceeded_splits_intervals():
    events = [
        _ev(0, av.EVENT_UNAVAILABLE, av.SCOPE_PLATFORM, window_until=_dt(1.5)),
        _ev(10, av.EVENT_UNAVAILABLE, av.SCOPE_PLATFORM, window_until=_dt(11.5)),
    ]
    intervals = av.build_intervals(events, coalesce_gap_seconds=120.0)
    assert len(intervals) == 2


def test_recovery_edge_seals_interval_closed():
    events = [
        _ev(0, av.EVENT_UNAVAILABLE, av.SCOPE_PLATFORM, window_until=_dt(1.5)),
        _ev(2, av.EVENT_AVAILABLE, av.SCOPE_PLATFORM, evidence=av.EVIDENCE_CLOB_OK),
    ]
    intervals = av.build_intervals(events)
    assert len(intervals) == 1
    assert intervals[0].status == "closed"
    assert intervals[0].end_ts == _dt(2)


def test_confidence_is_strongest_member():
    events = [
        _ev(0, av.EVENT_UNAVAILABLE, av.SCOPE_PLATFORM, window_until=_dt(1.5)),
        _ev(
            1,
            av.EVENT_MIRROR_REJECTED,
            av.SCOPE_PLATFORM,
            confidence=av.CONFIDENCE_CONFIRMED,
            evidence=av.EVIDENCE_MIRROR,
            window_until=_dt(2.5),
        ),
    ]
    intervals = av.build_intervals(events)
    assert len(intervals) == 1
    assert intervals[0].confidence == av.CONFIDENCE_CONFIRMED


def test_market_and_platform_dont_coalesce_together():
    events = [
        _ev(0, av.EVENT_UNAVAILABLE, av.SCOPE_PLATFORM, window_until=_dt(1.5)),
        _ev(0, av.EVENT_UNAVAILABLE, av.SCOPE_MARKET, market_id="m1", window_until=_dt(1.5)),
    ]
    intervals = av.build_intervals(events)
    assert {iv.scope for iv in intervals} == {av.SCOPE_PLATFORM, av.SCOPE_MARKET}
    assert len(intervals) == 2


def test_effective_from_predates_start_for_discovery_lag():
    # Mirror rejection observed at min 1 but the dry fill opened at min 0.
    ev = av.AvailabilityEvent(
        observed_at=_dt(1),
        event_type=av.EVENT_MIRROR_REJECTED,
        scope=av.SCOPE_PLATFORM,
        confidence=av.CONFIDENCE_CONFIRMED,
        evidence=av.EVIDENCE_MIRROR,
        effective_from=_dt(0),
        window_until=_dt(2),
    )
    iv = av.build_intervals([ev])[0]
    lo, _hi = iv.applicability_window()
    assert lo == _dt(0)
    assert iv.covers(_dt(0.5))  # the phantom fill at 0.5 is covered


def test_status_down_without_recovery_stays_open():
    # A status-page down with no window_until and no status_up must remain an
    # ongoing (open-ended) interval, not collapse to a near-instant [t, t] point.
    ev = av.AvailabilityEvent(
        observed_at=_dt(0), event_type=av.EVENT_STATUS_DOWN, scope=av.SCOPE_PLATFORM,
        confidence=av.CONFIDENCE_CONFIRMED, evidence=av.EVIDENCE_STATUS_PAGE,
    )
    iv = av.build_intervals([ev])[0]
    assert iv.status == "open"
    assert iv.end_ts is None
    assert iv.covers(_dt(60))  # an hour later is still covered (down until recovery)


def test_status_down_then_up_closes_at_recovery():
    events = [
        av.AvailabilityEvent(
            observed_at=_dt(0), event_type=av.EVENT_STATUS_DOWN, scope=av.SCOPE_PLATFORM,
            confidence=av.CONFIDENCE_CONFIRMED, evidence=av.EVIDENCE_STATUS_PAGE,
        ),
        av.AvailabilityEvent(
            observed_at=_dt(3), event_type=av.EVENT_STATUS_UP, scope=av.SCOPE_PLATFORM,
            confidence=av.CONFIDENCE_CONFIRMED, evidence=av.EVIDENCE_STATUS_PAGE,
        ),
    ]
    iv = av.build_intervals(events)[0]
    assert iv.status == "closed"
    assert iv.end_ts == _dt(3)
    assert not iv.covers(_dt(60))


def test_self_expiring_unclosed_interval_keeps_finite_end():
    # Regression: an unclosed interval whose members DID carry window_until still
    # seals at that finite estimate (health-flag self-expiry), not open-ended.
    ev = av.AvailabilityEvent(
        observed_at=_dt(0), event_type=av.EVENT_UNAVAILABLE, scope=av.SCOPE_PLATFORM,
        confidence=av.CONFIDENCE_CONFIRMED, evidence=av.EVIDENCE_ORDER_POST,
        window_until=_dt(1.5),
    )
    iv = av.build_intervals([ev])[0]
    assert iv.status == "open"
    assert iv.end_ts == _dt(1.5)
    assert not iv.covers(_dt(60))


# ── untradable_at ────────────────────────────────────────────────────────────────
def _ledger(tmp_path: Path, intervals) -> Path:
    return av.write_ledger(intervals, ledger_file=tmp_path / "ledger.csv")


def test_untradable_platform_applies_to_all_markets(tmp_path):
    iv = av.AvailabilityInterval(
        scope=av.SCOPE_PLATFORM, market_id="", start_ts=_dt(0), end_ts=_dt(5),
        status="closed", reason_code="425", evidence=av.EVIDENCE_ORDER_POST,
        confidence=av.CONFIDENCE_CONFIRMED, source_bot="15m",
    )
    path = _ledger(tmp_path, [iv])
    active, reason, _ = av.untradable_at(_dt(2), market_id="anything", ledger_path=path)
    assert active and reason == "425"
    # outside the window
    assert not av.untradable_at(_dt(9), ledger_path=path)[0]


def test_untradable_market_requires_matching_market_id(tmp_path):
    iv = av.AvailabilityInterval(
        scope=av.SCOPE_MARKET, market_id="m1", start_ts=_dt(0), end_ts=_dt(5),
        status="closed", reason_code="book", evidence=av.EVIDENCE_BOOK,
        confidence=av.CONFIDENCE_CONFIRMED, source_bot="15m",
    )
    path = _ledger(tmp_path, [iv])
    assert av.untradable_at(_dt(2), market_id="m1", ledger_path=path)[0]
    assert not av.untradable_at(_dt(2), market_id="m2", ledger_path=path)[0]


def test_untradable_excludes_inferred_at_confirmed_threshold(tmp_path):
    iv = av.AvailabilityInterval(
        scope=av.SCOPE_PLATFORM, market_id="", start_ts=_dt(0), end_ts=_dt(5),
        status="open", reason_code="reads", evidence=av.EVIDENCE_BOOK,
        confidence=av.CONFIDENCE_INFERRED, source_bot="15m",
    )
    path = _ledger(tmp_path, [iv])
    assert not av.untradable_at(_dt(2), ledger_path=path, min_confidence=av.CONFIDENCE_CONFIRMED)[0]
    assert av.untradable_at(_dt(2), ledger_path=path, min_confidence=av.CONFIDENCE_INFERRED)[0]


def test_untradable_bot_host_only_matches_same_scope(tmp_path):
    iv = av.AvailabilityInterval(
        scope=av.SCOPE_BOT_HOST, market_id="", start_ts=_dt(0), end_ts=_dt(5),
        status="closed", reason_code="net", evidence=av.EVIDENCE_ORDER_POST,
        confidence=av.CONFIDENCE_CONFIRMED, source_bot="15m",
    )
    path = _ledger(tmp_path, [iv])
    # The default gate scopes (platform, market) must NOT see a bot-host failure —
    # so the dry-run-only 5m bot never blocks on a 15m host error.
    assert not av.untradable_at(_dt(2), ledger_path=path)[0]
    # Self-cleaning (all scopes) does see it.
    assert av.untradable_at(_dt(2), ledger_path=path, include_scopes=av.ALL_SCOPES)[0]


def test_untradable_fail_open_on_missing_ledger(tmp_path):
    assert av.untradable_at(_dt(2), ledger_path=tmp_path / "nope.csv") == (False, None, None)


# ── persistence round-trips + shared filtering ─────────────────────────────────────
def test_record_and_load_events_roundtrip(tmp_path):
    events_file = tmp_path / "events.csv"
    e = av.AvailabilityEvent(
        observed_at=_dt(0), event_type=av.EVENT_UNAVAILABLE, scope=av.SCOPE_PLATFORM,
        confidence=av.CONFIDENCE_CONFIRMED, evidence=av.EVIDENCE_ORDER_POST,
        reason_code="425", status_code=425, window_until=_dt(1.5),
        metadata={"k": "v"},
    )
    av.record_event(e, events_file=events_file)
    loaded = av.load_events(events_file)
    assert len(loaded) == 1
    assert loaded[0].scope == av.SCOPE_PLATFORM
    assert loaded[0].status_code == 425
    assert loaded[0].metadata == {"k": "v"}


def test_shared_ledger_only_gets_platform(tmp_path):
    events_file = tmp_path / "events.csv"
    shared = tmp_path / "shared.csv"
    av.record_event(
        av.AvailabilityEvent(
            observed_at=_dt(0), event_type=av.EVENT_UNAVAILABLE, scope=av.SCOPE_PLATFORM,
            confidence=av.CONFIDENCE_CONFIRMED, evidence=av.EVIDENCE_ORDER_POST,
        ),
        events_file=events_file, shared_path=shared,
    )
    av.record_event(
        av.AvailabilityEvent(
            observed_at=_dt(1), event_type=av.EVENT_UNAVAILABLE, scope=av.SCOPE_BOT_HOST,
            confidence=av.CONFIDENCE_CONFIRMED, evidence=av.EVIDENCE_ORDER_POST,
        ),
        events_file=events_file, shared_path=shared,
    )
    assert len(av.load_events(events_file)) == 2
    shared_events = av.load_events(shared)
    assert len(shared_events) == 1
    assert shared_events[0].scope == av.SCOPE_PLATFORM


def test_rebuild_ledger_end_to_end(tmp_path):
    events_file = tmp_path / "events.csv"
    ledger_file = tmp_path / "ledger.csv"
    for m in (0, 1, 2):
        av.record_event(
            av.AvailabilityEvent(
                observed_at=_dt(m), event_type=av.EVENT_UNAVAILABLE, scope=av.SCOPE_PLATFORM,
                confidence=av.CONFIDENCE_CONFIRMED, evidence=av.EVIDENCE_ORDER_POST,
                reason_code="425", window_until=_dt(m + 1.5),
            ),
            events_file=events_file,
        )
    intervals = av.rebuild_ledger(events_file=events_file, ledger_file=ledger_file, coalesce_gap_seconds=120.0)
    assert len(intervals) == 1
    assert av.untradable_at(_dt(1), ledger_path=ledger_file)[0]


# ── backfill from legacy health log ─────────────────────────────────────────────
def _write_health_log(path: Path, rows: list[dict]) -> None:
    header = (
        "event", "timestamp", "reason", "source", "status_code",
        "retry_after_seconds", "cooldown_seconds", "unhealthy_until", "duration_seconds",
    )
    with path.open("w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=header)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in header})


def test_backfill_health_log_coalesces_orphan_starts(tmp_path):
    log_path = tmp_path / "health_log.csv"
    rows = []
    for m in (0, 1, 2, 3, 4):
        rows.append({
            "event": "outage_start",
            "timestamp": _dt(m).isoformat(),
            "reason": "reads:CLOB books are empty",
            "source": "live_reads_probe",
            "unhealthy_until": _dt(m + 1.5).isoformat(),
        })
    _write_health_log(log_path, rows)
    events = av.events_from_health_log(log_path)
    assert len(events) == 5
    intervals = av.build_intervals(events, coalesce_gap_seconds=120.0)
    assert len(intervals) == 1
    # read-probe "books empty" with no market id → unknown / inferred (report-only)
    assert intervals[0].scope == av.SCOPE_UNKNOWN
    assert intervals[0].confidence == av.CONFIDENCE_INFERRED


def test_backfill_health_log_order_post_is_platform_confirmed(tmp_path):
    log_path = tmp_path / "health_log.csv"
    _write_health_log(log_path, [{
        "event": "outage_start",
        "timestamp": _dt(0).isoformat(),
        "reason": "order_post_refused",
        "source": "live_order_post",
        "status_code": "425",
        "unhealthy_until": _dt(1.5).isoformat(),
    }])
    events = av.events_from_health_log(log_path)
    assert events[0].scope == av.SCOPE_PLATFORM
    assert events[0].confidence == av.CONFIDENCE_CONFIRMED


def test_backfill_health_log_order_post_reason_text_classifies_platform(tmp_path):
    # The real _record_clob_order_outcome path embeds the executor error in the
    # reason and may leave status_code blank — classify must still find platform
    # from the reason text (regression for the 2026-06-05T12:35:09Z unknown row).
    log_path = tmp_path / "health_log.csv"
    _write_health_log(log_path, [{
        "event": "outage_start",
        "timestamp": _dt(0).isoformat(),
        "reason": "order_post_refused: PolyApiException[status_code=425| error_message=service not ready]",
        "source": "live_order_post",
        "status_code": "",  # blank, as the live row historically was
        "unhealthy_until": _dt(1.5).isoformat(),
    }])
    events = av.events_from_health_log(log_path)
    assert events[0].scope == av.SCOPE_PLATFORM
    assert events[0].confidence == av.CONFIDENCE_CONFIRMED


def test_backfill_health_log_order_post_request_exception_is_bot_host(tmp_path):
    # Transport failure (status_code=None) must classify bot_host, not platform —
    # a host-local error does not prove the CLOB refused everyone (Decision 3).
    log_path = tmp_path / "health_log.csv"
    _write_health_log(log_path, [{
        "event": "outage_start",
        "timestamp": _dt(0).isoformat(),
        "reason": "order_post_refused: PolyApiException[status_code=None error_message=Request exception!]",
        "source": "live_order_post",
        "status_code": "",
        "unhealthy_until": _dt(1.5).isoformat(),
    }])
    events = av.events_from_health_log(log_path)
    assert events[0].scope == av.SCOPE_BOT_HOST


# ── backfill from mirror audit (Tier A discovery-lag) ──────────────────────────────
def _write_mirror_audit(path: Path, rows: list[dict]) -> None:
    header = [
        "timestamp", "stage", "status", "reason", "signal_id", "event", "market_id",
        "direction", "source_instance_id", "stream_id", "attempt", "signal_age_s",
        "publish_to_receive_s", "receive_to_attempt_s", "publish_to_attempt_s",
        "trigger_bid", "trigger_ask", "trigger_mid", "dry_entry_price",
        "rechecked_price", "order_id", "live_position_id", "details",
    ]
    with path.open("w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=header)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in header})


def test_backfill_mirror_audit_rejected_and_accepted(tmp_path):
    audit = tmp_path / "audit.csv"
    _write_mirror_audit(audit, [
        {
            "timestamp": _dt(0).isoformat(), "stage": "attempt_result", "status": "rejected",
            "reason": "order_failed", "event": "ENTRY", "market_id": "btc-updown-15m-1780489800",
            "direction": "DOWN", "source_instance_id": "1271", "order_id": "", "live_position_id": "",
            "details": "PolyApiException[status_code=425 error_message=service not ready]",
        },
        {
            "timestamp": _dt(5).isoformat(), "stage": "attempt_result", "status": "accepted",
            "reason": "entered", "event": "ENTRY", "market_id": "btc-updown-15m-1780667100",
            "direction": "DOWN", "source_instance_id": "1271", "order_id": "0xabc",
            "live_position_id": "2bd16833",
        },
        {  # different instance — filtered out
            "timestamp": _dt(1).isoformat(), "stage": "attempt_result", "status": "rejected",
            "reason": "order_failed", "event": "ENTRY", "market_id": "btc-updown-15m-1780489800",
            "direction": "DOWN", "source_instance_id": "999", "order_id": "", "live_position_id": "",
            "details": "PolyApiException[status_code=425]",
        },
    ])
    events = av.events_from_mirror_audit(audit, instance_id=1271)
    kinds = sorted(e.event_type for e in events)
    assert kinds == [av.EVENT_MIRROR_ACCEPTED, av.EVENT_MIRROR_REJECTED]
    rej = next(e for e in events if e.event_type == av.EVENT_MIRROR_REJECTED)
    assert rej.scope == av.SCOPE_PLATFORM  # 425 → platform
    assert rej.confidence == av.CONFIDENCE_CONFIRMED
    assert rej.market_id == "btc-updown-15m-1780489800"


def test_backfill_mirror_audit_request_exception_is_bot_host(tmp_path):
    audit = tmp_path / "audit.csv"
    _write_mirror_audit(audit, [{
        "timestamp": _dt(0).isoformat(), "stage": "attempt_result", "status": "rejected",
        "reason": "order_failed", "event": "ENTRY", "market_id": "btc-updown-15m-1780499700",
        "direction": "DOWN", "source_instance_id": "1271", "order_id": "", "live_position_id": "",
        "details": "PolyApiException[status_code=None error_message=Request exception!]",
    }])
    events = av.events_from_mirror_audit(audit, instance_id=1271)
    assert events[0].scope == av.SCOPE_BOT_HOST


def test_mirror_audit_rejected_sets_discovery_lag_effective_from(tmp_path):
    # signal_age_s present → effective_from reaches back ~that far so the dry
    # fill that predates the live rejection is covered by Tier B intervals too.
    audit = tmp_path / "audit.csv"
    _write_mirror_audit(audit, [{
        "timestamp": _dt(1).isoformat(), "stage": "attempt_result", "status": "rejected",
        "reason": "order_failed", "event": "ENTRY", "market_id": "btc-updown-15m-1780662600",
        "direction": "UP", "source_instance_id": "1271", "order_id": "", "live_position_id": "",
        "signal_age_s": "30",
        "details": "PolyApiException[status_code=425 error_message=service not ready]",
    }])
    ev = next(e for e in av.events_from_mirror_audit(audit, instance_id=1271)
              if e.event_type == av.EVENT_MIRROR_REJECTED)
    assert ev.effective_from is not None and ev.effective_from < ev.observed_at
    assert (ev.observed_at - ev.effective_from).total_seconds() == pytest.approx(30.0)
    assert ev.window_until == ev.observed_at
    iv = av.build_intervals([ev])[0]
    assert iv.covers(_dt(1) - timedelta(seconds=20))  # dry fill 20s before rejection


def test_mirror_audit_rejected_lag_falls_back_to_gap(tmp_path):
    audit = tmp_path / "audit.csv"
    _write_mirror_audit(audit, [{
        "timestamp": _dt(1).isoformat(), "stage": "attempt_result", "status": "rejected",
        "reason": "order_failed", "event": "ENTRY", "market_id": "m1",
        "direction": "UP", "source_instance_id": "1271", "order_id": "", "live_position_id": "",
        "details": "PolyApiException[status_code=425]",
    }])
    ev = next(e for e in av.events_from_mirror_audit(audit, instance_id=1271)
              if e.event_type == av.EVENT_MIRROR_REJECTED)
    assert (ev.observed_at - ev.effective_from).total_seconds() == pytest.approx(
        av.DEFAULT_COALESCE_GAP_SECONDS
    )


def test_mirror_accepted_closes_open_interval():
    # A rejection then a later acceptance on the same platform market is a
    # confirmed recovery edge that seals the interval closed.
    events = [
        av.AvailabilityEvent(
            observed_at=_dt(0), event_type=av.EVENT_MIRROR_REJECTED, scope=av.SCOPE_PLATFORM,
            confidence=av.CONFIDENCE_CONFIRMED, evidence=av.EVIDENCE_MIRROR,
            market_id="m1", window_until=_dt(0),
        ),
        av.AvailabilityEvent(
            observed_at=_dt(2), event_type=av.EVENT_MIRROR_ACCEPTED, scope=av.SCOPE_PLATFORM,
            confidence=av.CONFIDENCE_CONFIRMED, evidence=av.EVIDENCE_MIRROR, market_id="m1",
        ),
    ]
    iv = av.build_intervals(events)[0]
    assert iv.status == "closed"
    assert iv.end_ts == _dt(2)


def test_backfill_polymarket_down_without_up_seals_at_estimate(tmp_path):
    # An unclosed polymarket_down edge (live trader restarted mid-outage — the
    # edge-triggered recorder's in-memory state resets, so the recovery edge is
    # never written) must NOT become an open-ended confirmed platform interval:
    # that would cover every later timestamp, wedging the forward gate and
    # letting the cleaner void the entire history after it. The backfill stamps
    # a self-expiry estimate instead.
    log_path = tmp_path / "health_log.csv"
    _write_health_log(log_path, [{
        "event": "polymarket_down",
        "timestamp": _dt(0).isoformat(),
        "reason": "HASISSUES",
        "source": "polymarket_status",
    }])
    [iv] = av.build_intervals(av.events_from_health_log(log_path))
    assert iv.status == "open"
    assert iv.scope == av.SCOPE_PLATFORM
    assert iv.confidence == av.CONFIDENCE_CONFIRMED
    assert iv.end_ts == _dt(0) + timedelta(seconds=av.STATUS_DOWN_SELF_EXPIRY_SECONDS)
    assert iv.covers(_dt(0) + timedelta(minutes=5))
    assert not iv.covers(_dt(0) + timedelta(seconds=av.STATUS_DOWN_SELF_EXPIRY_SECONDS + 60))


def test_backfill_polymarket_down_then_up_still_closes_exactly(tmp_path):
    # The estimate must never override a real recovery edge: a polymarket_up row
    # seals the interval at its exact time.
    log_path = tmp_path / "health_log.csv"
    _write_health_log(log_path, [
        {"event": "polymarket_down", "timestamp": _dt(0).isoformat(),
         "reason": "HASISSUES", "source": "polymarket_status"},
        {"event": "polymarket_up", "timestamp": _dt(2).isoformat(),
         "reason": "operational", "source": "polymarket_status"},
    ])
    [iv] = av.build_intervals(av.events_from_health_log(log_path))
    assert iv.status == "closed"
    assert iv.end_ts == _dt(2)
    assert not iv.covers(_dt(3))
