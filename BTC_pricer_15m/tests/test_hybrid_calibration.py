from __future__ import annotations

import json
import math
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

import pytest

import scripts.trader.hybrid_calibration as hybrid_calibration_module
from scripts.trader.hybrid_calibration import (
    CalibrationEvent,
    HybridCalibrationBroker,
    MarketContext,
)


def _make_event(
    market_id: str,
    ready_ms: int,
    *,
    source: str = "test",
    suffix: str = "a",
    fit_s: float = 1.0,
    market_end_ms: int | None = None,
) -> CalibrationEvent:
    ready_at = datetime.fromtimestamp(ready_ms / 1000.0, tz=timezone.utc)
    market_end_at = datetime.fromtimestamp(
        (market_end_ms if market_end_ms is not None else ready_ms + 900_000) / 1000.0,
        tz=timezone.utc,
    )
    return CalibrationEvent(
        schema_version=1,
        event_id=f"{ready_ms}-{suffix}",
        source=source,
        market_id=market_id,
        barrier=80000.0,
        market_end_time_iso=market_end_at.isoformat(),
        ready_at_iso=ready_at.isoformat(),
        ready_at_unix_ms=ready_ms,
        prob_above=0.6,
        prob_below=0.4,
        model_used="ssvi_surface",
        calibration_seconds=fit_s,
        terminal_payload={
            "timestamp": ready_at.isoformat(),
            "timing": {
                "surface_fit_s": fit_s,
                "mc_s": 0.5,
            },
            "avg_prob_above": 0.6,
            "avg_prob_below": 0.4,
        },
    )


def test_broker_prefers_newer_event_for_same_market(tmp_path: Path) -> None:
    broker = HybridCalibrationBroker(
        results_dir=tmp_path,
        enable_local_worker=False,
        # Synthetic 1970-era timestamps; disable skew check so tests focus on
        # ordering semantics. Real-clock skew tests live in their own cases.
        producer_skew_tolerance_ms=10**18,
    )
    broker.update_market_context(
        MarketContext(
            market_id="btc-updown-15m-1",
            barrier=80000.0,
            market_end_time_iso="2026-04-23T00:15:00+00:00",
        )
    )
    older = _make_event("btc-updown-15m-1", 1000, suffix="older", fit_s=9.0)
    newer = _make_event("btc-updown-15m-1", 2000, suffix="newer", fit_s=3.0)

    assert broker.publish(older) is True
    assert broker.publish(newer) is True
    latest = broker.latest_for_market("btc-updown-15m-1")
    assert latest is not None
    assert latest.event_id == newer.event_id

    terminal = json.loads((tmp_path / "terminal_data.json").read_text(encoding="utf-8"))
    assert terminal["timing"]["surface_fit_s"] == 3.0
    assert terminal["timing"]["used_gap_s"] == 1.0
    assert terminal["timing"]["used_source"] == "test"


@pytest.mark.parametrize(
    ("pa", "pb"),
    [
        (math.nan, 0.5),
        (0.5, math.nan),
        (math.inf, 0.5),
        (-0.1, 1.1),
        (1.1, -0.1),
        (0.6, 0.2),  # sums to 0.8 — non-complementary
    ],
)
def test_calibration_event_rejects_invalid_probabilities(pa: float, pb: float) -> None:
    valid = _make_event("btc-updown-15m-1", 1000)
    with pytest.raises(ValueError):
        replace(valid, prob_above=pa, prob_below=pb)


def test_calibration_event_accepts_boundary_probabilities() -> None:
    valid = _make_event("btc-updown-15m-1", 1000)
    replace(valid, prob_above=1.0, prob_below=0.0)
    replace(valid, prob_above=0.0, prob_below=1.0)
    # sum drift of 0.049 stays inside the 0.05 tolerance
    replace(valid, prob_above=0.524, prob_below=0.525)


def test_broker_tie_breaks_same_ready_time_deterministically(tmp_path: Path) -> None:
    broker = HybridCalibrationBroker(
        results_dir=tmp_path,
        enable_local_worker=False,
        # Synthetic 1970-era timestamps; disable skew check so tests focus on
        # ordering semantics. Real-clock skew tests live in their own cases.
        producer_skew_tolerance_ms=10**18,
    )
    broker.update_market_context(
        MarketContext(
            market_id="btc-updown-15m-2",
            barrier=80000.0,
            market_end_time_iso="2026-04-23T00:15:00+00:00",
        )
    )
    first = _make_event("btc-updown-15m-2", 5000, suffix="aaa", fit_s=7.0)
    second = _make_event("btc-updown-15m-2", 5000, suffix="zzz", fit_s=2.0)

    assert broker.publish(first) is True
    assert broker.publish(second) is True
    latest = broker.latest_for_market("btc-updown-15m-2")
    assert latest is not None
    assert latest.event_id == second.event_id


def test_broker_only_applies_active_market_to_terminal_json(tmp_path: Path) -> None:
    broker = HybridCalibrationBroker(
        results_dir=tmp_path,
        enable_local_worker=False,
        # Synthetic 1970-era timestamps; disable skew check so tests focus on
        # ordering semantics. Real-clock skew tests live in their own cases.
        producer_skew_tolerance_ms=10**18,
    )
    broker.update_market_context(
        MarketContext(
            market_id="btc-updown-15m-active",
            barrier=80000.0,
            market_end_time_iso="2026-04-23T00:15:00+00:00",
        )
    )
    inactive = _make_event("btc-updown-15m-other", 1000, fit_s=5.0)
    active = _make_event("btc-updown-15m-active", 2000, fit_s=1.5)

    assert broker.publish(inactive) is True
    assert not (tmp_path / "terminal_data.json").exists()
    assert broker.publish(active) is True

    terminal = json.loads((tmp_path / "terminal_data.json").read_text(encoding="utf-8"))
    assert terminal["timing"]["surface_fit_s"] == 1.5


def test_broker_pulls_remote_inbox_files(tmp_path: Path) -> None:
    broker = HybridCalibrationBroker(
        results_dir=tmp_path,
        enable_local_worker=False,
        # Synthetic 1970-era timestamps; disable skew check so tests focus on
        # ordering semantics. Real-clock skew tests live in their own cases.
        producer_skew_tolerance_ms=10**18,
    )
    broker.update_market_context(
        MarketContext(
            market_id="btc-updown-15m-3",
            barrier=80000.0,
            market_end_time_iso="2026-04-23T00:15:00+00:00",
        )
    )
    event = _make_event("btc-updown-15m-3", 3000, source="local_offload", fit_s=4.0)
    inbox = tmp_path / "calibration_inbox"
    inbox.mkdir(parents=True, exist_ok=True)
    (inbox / "evt.json").write_text(json.dumps(event.to_dict()), encoding="utf-8")

    broker.poll_inbox()

    latest = broker.latest_for_market("btc-updown-15m-3")
    assert latest is not None
    assert latest.source == "local_offload"
    assert list(inbox.glob("*.json")) == []


# --- hardening: clock-skew sanity -------------------------------------------------


def _make_realtime_event(
    *,
    skew_seconds: float = 0.0,
    market_id: str = "btc-updown-15m-realtime",
    suffix: str = "rt",
) -> CalibrationEvent:
    import time as _t

    now_ms = int(_t.time() * 1000)
    ready_ms = now_ms + int(skew_seconds * 1000)
    return _make_event(market_id, ready_ms, suffix=suffix)


def test_publish_rejects_event_from_far_future(tmp_path: Path) -> None:
    # Skew check is ON by default; producer 15 min ahead gets rejected.
    broker = HybridCalibrationBroker(results_dir=tmp_path, enable_local_worker=False)
    event = _make_realtime_event(skew_seconds=300)
    assert broker.publish(event) is False
    assert event.market_id not in broker._latest_by_market


def test_publish_rejects_event_from_far_past(tmp_path: Path) -> None:
    broker = HybridCalibrationBroker(results_dir=tmp_path, enable_local_worker=False)
    event = _make_realtime_event(skew_seconds=-300)
    assert broker.publish(event) is False
    assert event.market_id not in broker._latest_by_market


def test_publish_accepts_slight_skew(tmp_path: Path) -> None:
    # ±10s is well inside the ±30s tolerance.
    broker = HybridCalibrationBroker(results_dir=tmp_path, enable_local_worker=False)
    event = _make_realtime_event(skew_seconds=-10)
    assert broker.publish(event) is True


def test_publish_rejects_event_ready_at_market_end(tmp_path: Path) -> None:
    broker = HybridCalibrationBroker(
        results_dir=tmp_path,
        enable_local_worker=False,
        producer_skew_tolerance_ms=10**18,
    )
    event = _make_event(
        "btc-updown-15m-boundary",
        900_000,
        market_end_ms=900_000,
    )
    assert broker.publish(event) is False
    assert broker.latest_for_market("btc-updown-15m-boundary") is None


def test_publish_rejects_event_ready_after_market_end(tmp_path: Path) -> None:
    broker = HybridCalibrationBroker(
        results_dir=tmp_path,
        enable_local_worker=False,
        producer_skew_tolerance_ms=10**18,
    )
    event = _make_event(
        "btc-updown-15m-boundary",
        901_000,
        market_end_ms=900_000,
    )
    assert broker.publish(event) is False
    assert broker.latest_for_market("btc-updown-15m-boundary") is None


def test_publish_accepts_event_ready_before_market_end(tmp_path: Path) -> None:
    broker = HybridCalibrationBroker(
        results_dir=tmp_path,
        enable_local_worker=False,
        producer_skew_tolerance_ms=10**18,
    )
    event = _make_event(
        "btc-updown-15m-boundary",
        899_999,
        market_end_ms=900_000,
    )
    assert broker.publish(event) is True
    assert broker.latest_for_market("btc-updown-15m-boundary") is event


def test_publish_stamps_received_at_ms(tmp_path: Path) -> None:
    import time as _t

    broker = HybridCalibrationBroker(results_dir=tmp_path, enable_local_worker=False)
    event = _make_realtime_event()
    assert broker.publish(event) is True
    stamped = broker._received_ms_by_market[event.market_id]
    assert abs(stamped - int(_t.time() * 1000)) < 2000


# --- hardening: TTL on latest_for_market -----------------------------------------


def test_latest_for_market_respects_ttl(tmp_path: Path) -> None:
    import time as _t

    broker = HybridCalibrationBroker(results_dir=tmp_path, enable_local_worker=False)
    event = _make_realtime_event(market_id="btc-updown-15m-ttl")
    assert broker.publish(event) is True
    # Without max_age_s: returned unconditionally.
    assert broker.latest_for_market("btc-updown-15m-ttl") is event
    # With generous TTL: still returned.
    assert broker.latest_for_market("btc-updown-15m-ttl", max_age_s=60.0) is event
    # Sleep past a tiny TTL: None.
    _t.sleep(0.1)
    assert broker.latest_for_market("btc-updown-15m-ttl", max_age_s=0.05) is None


def test_latest_for_market_unknown_returns_none_with_ttl(tmp_path: Path) -> None:
    broker = HybridCalibrationBroker(results_dir=tmp_path, enable_local_worker=False)
    assert broker.latest_for_market("never-published", max_age_s=60.0) is None


# --- hardening: meta payload contains received_at_ms ------------------------------


def test_meta_payload_includes_received_at_ms(tmp_path: Path) -> None:
    broker = HybridCalibrationBroker(results_dir=tmp_path, enable_local_worker=False)
    event = _make_realtime_event(market_id="btc-updown-15m-meta")
    broker.update_market_context(
        MarketContext(
            market_id=event.market_id,
            barrier=event.barrier,
            market_end_time_iso=event.market_end_time_iso,
        )
    )
    broker.publish(event)
    meta = json.loads((tmp_path / ".hybrid_calibration_meta.json").read_text())
    assert meta["received_at_ms"] is not None
    assert meta["received_at_ms"] > 0


# --- hardening: worker-loop survival ---------------------------------------------


def test_worker_loop_survives_script_exception(tmp_path: Path) -> None:
    """run_terminal_script raising must not kill the broker thread."""
    import time as _t
    from unittest.mock import patch

    broker = HybridCalibrationBroker(
        results_dir=tmp_path,
        enable_local_worker=True,
        enable_remote_inbox=False,
        worker_delay_min_seconds=0.0,
        worker_delay_max_seconds=0.0,
    )
    now = datetime.now(timezone.utc)
    from datetime import timedelta as _td

    context = MarketContext(
        market_id="btc-updown-15m-survive",
        barrier=80000.0,
        market_end_time_iso=(now + _td(minutes=10)).isoformat(),
    )
    calls = {"n": 0}

    def _boom(**_kwargs):
        calls["n"] += 1
        raise RuntimeError("simulated outage")

    with patch(
        "scripts.trader.hybrid_calibration.run_terminal_script",
        side_effect=_boom,
    ):
        broker.start()
        broker.update_market_context(context)
        deadline = _t.time() + 3.0
        while _t.time() < deadline and calls["n"] < 1:
            _t.sleep(0.05)
        broker.stop()

    assert calls["n"] >= 1, "worker should have attempted at least one iteration"
    # Thread joined cleanly (no hang after stop()).
    assert (
        broker._worker_thread is None
        or not broker._worker_thread.is_alive()
    )


# --- per-consumer inbox routing (shared-calc topology) ---------------------------


def test_per_consumer_inbox_dirs_isolate_events(tmp_path: Path) -> None:
    """When two brokers point at sibling inbox dirs, each only consumes its own.

    Required for the shared-calc daemon: it fans out the same event to N
    inboxes (one per consumer). poll_inbox is destructive (line 281 in
    hybrid_calibration.py — path.unlink), so consumers must not share an inbox.
    """
    grid_inbox = tmp_path / "calibration_inbox_grid"
    live_inbox = tmp_path / "calibration_inbox_live"
    grid_inbox.mkdir()
    live_inbox.mkdir()

    broker_grid = HybridCalibrationBroker(
        results_dir=tmp_path,
        enable_local_worker=False,
        inbox_dir=grid_inbox,
        producer_skew_tolerance_ms=10**18,
    )
    broker_live = HybridCalibrationBroker(
        results_dir=tmp_path,
        enable_local_worker=False,
        inbox_dir=live_inbox,
        producer_skew_tolerance_ms=10**18,
    )
    for broker in (broker_grid, broker_live):
        broker.update_market_context(
            MarketContext(
                market_id="btc-updown-15m-isolated",
                barrier=80000.0,
                market_end_time_iso="2026-04-29T23:59:00+00:00",
            )
        )

    # Daemon-style fan-out: write the SAME event into both dirs.
    event = _make_event("btc-updown-15m-isolated", 1000, source="shared_calc", fit_s=2.0)
    payload = json.dumps(event.to_dict())
    (grid_inbox / "evt.json").write_text(payload, encoding="utf-8")
    (live_inbox / "evt.json").write_text(payload, encoding="utf-8")

    broker_grid.poll_inbox()
    broker_live.poll_inbox()

    # Both brokers see the event — they read independent copies.
    grid_latest = broker_grid.latest_for_market("btc-updown-15m-isolated")
    live_latest = broker_live.latest_for_market("btc-updown-15m-isolated")
    assert grid_latest is not None and grid_latest.event_id == event.event_id
    assert live_latest is not None and live_latest.event_id == event.event_id

    # Each broker only unlinks its own dir.
    assert list(grid_inbox.glob("*.json")) == []
    assert list(live_inbox.glob("*.json")) == []

    # Output paths must ALSO be per-consumer. If both brokers wrote the same
    # terminal_data.json / .hybrid_calibration_meta.json, atomic-replace
    # would race on host-mounted Windows volumes (caught in the wild via
    # the shutdown_log breadcrumb on 2026-04-30 — PermissionError EACCES).
    assert broker_grid.terminal_path != broker_live.terminal_path
    assert broker_grid.meta_path != broker_live.meta_path
    assert broker_grid.terminal_path.name == "terminal_data.grid.json"
    assert broker_live.terminal_path.name == "terminal_data.live.json"
    assert broker_grid.meta_path.name == ".hybrid_calibration_meta.grid.json"
    assert broker_live.meta_path.name == ".hybrid_calibration_meta.live.json"


def test_publish_keeps_grid_calibration_when_terminal_snapshot_write_fails(
    tmp_path: Path,
    monkeypatch,
) -> None:
    grid_inbox = tmp_path / "calibration_inbox_grid"
    grid_inbox.mkdir()
    broker = HybridCalibrationBroker(
        results_dir=tmp_path,
        enable_local_worker=False,
        inbox_dir=grid_inbox,
        producer_skew_tolerance_ms=10**18,
    )
    market_id = "btc-updown-15m-snapshot-fail"
    broker.update_market_context(
        MarketContext(
            market_id=market_id,
            barrier=80_000.0,
            market_end_time_iso="2026-04-30T12:00:00+00:00",
        )
    )

    original_write = hybrid_calibration_module._atomic_write_json

    def flaky_write(path: Path, payload: dict) -> None:
        if path.name == "terminal_data.grid.json":
            raise PermissionError("simulated locked bind-mount file")
        original_write(path, payload)

    monkeypatch.setattr(
        hybrid_calibration_module,
        "_atomic_write_json",
        flaky_write,
    )

    event = _make_event_with_barrier(market_id, 1000, barrier=80_000.0)
    assert broker.publish(event) is True
    assert broker.latest_for_market(market_id) is event
    assert not (tmp_path / "terminal_data.grid.json").exists()
    assert (tmp_path / ".hybrid_calibration_meta.grid.json").exists()


def test_update_market_context_keeps_stored_event_when_snapshot_write_fails(
    tmp_path: Path,
    monkeypatch,
) -> None:
    grid_inbox = tmp_path / "calibration_inbox_grid"
    grid_inbox.mkdir()
    broker = HybridCalibrationBroker(
        results_dir=tmp_path,
        enable_local_worker=False,
        inbox_dir=grid_inbox,
        producer_skew_tolerance_ms=10**18,
    )
    market_id = "btc-updown-15m-context-snapshot-fail"
    event = _make_event_with_barrier(market_id, 1000, barrier=80_000.0)
    assert broker.publish(event) is True

    original_write = hybrid_calibration_module._atomic_write_json

    def flaky_write(path: Path, payload: dict) -> None:
        if path.name == "terminal_data.grid.json":
            raise PermissionError("simulated locked bind-mount file")
        original_write(path, payload)

    monkeypatch.setattr(
        hybrid_calibration_module,
        "_atomic_write_json",
        flaky_write,
    )

    broker.update_market_context(
        MarketContext(
            market_id=market_id,
            barrier=80_000.0,
            market_end_time_iso="2026-04-30T12:00:00+00:00",
        )
    )

    assert broker.latest_for_market(market_id) is event
    assert not (tmp_path / "terminal_data.grid.json").exists()
    assert (tmp_path / ".hybrid_calibration_meta.grid.json").exists()


def test_live_consumer_mirrors_writes_to_bare_terminal_data_for_dashboard(tmp_path: Path) -> None:
    """The 15m streaming dashboard reads results/terminal_data.json mtime as
    its `bot_live` freshness signal. After the per-consumer split, only the
    LIVE consumer mirrors its writes to the bare path so the dashboard
    keeps working without changes — and only live writes the bare path,
    so no rename race is reintroduced."""
    live_inbox = tmp_path / "calibration_inbox_live"
    live_inbox.mkdir()
    grid_inbox = tmp_path / "calibration_inbox_grid"
    grid_inbox.mkdir()

    broker_live = HybridCalibrationBroker(
        results_dir=tmp_path,
        enable_local_worker=False,
        inbox_dir=live_inbox,
        producer_skew_tolerance_ms=10**18,
    )
    broker_grid = HybridCalibrationBroker(
        results_dir=tmp_path,
        enable_local_worker=False,
        inbox_dir=grid_inbox,
        producer_skew_tolerance_ms=10**18,
    )

    # Live mirrors; grid does not.
    assert broker_live.legacy_terminal_path == tmp_path / "terminal_data.json"
    assert broker_live.legacy_meta_path == tmp_path / ".hybrid_calibration_meta.json"
    assert broker_grid.legacy_terminal_path is None
    assert broker_grid.legacy_meta_path is None

    market_id = "btc-updown-15m-mirror"
    for broker in (broker_live, broker_grid):
        broker.update_market_context(
            MarketContext(
                market_id=market_id,
                barrier=80_000.0,
                market_end_time_iso="2026-04-30T12:00:00+00:00",
            )
        )
    event = _make_event_with_barrier(market_id, 1000, barrier=80_000.0)
    assert broker_live.publish(event) is True

    # Live wrote both per-consumer AND bare paths.
    assert (tmp_path / "terminal_data.live.json").exists()
    assert (tmp_path / "terminal_data.json").exists()
    assert (tmp_path / ".hybrid_calibration_meta.live.json").exists()
    assert (tmp_path / ".hybrid_calibration_meta.json").exists()

    # Now grid publishes — must NOT touch the bare paths (those should still
    # reflect live's write only). Capture live's bare-mtime before grid acts.
    bare_terminal = tmp_path / "terminal_data.json"
    bare_meta = tmp_path / ".hybrid_calibration_meta.json"
    live_bare_terminal_mtime = bare_terminal.stat().st_mtime_ns
    live_bare_meta_mtime = bare_meta.stat().st_mtime_ns

    grid_event = _make_event_with_barrier(market_id, 2000, barrier=80_000.0)
    assert broker_grid.publish(grid_event) is True

    # Grid wrote its own per-consumer file.
    assert (tmp_path / "terminal_data.grid.json").exists()
    assert (tmp_path / ".hybrid_calibration_meta.grid.json").exists()
    # The bare paths must NOT have been touched by grid.
    assert bare_terminal.stat().st_mtime_ns == live_bare_terminal_mtime
    assert bare_meta.stat().st_mtime_ns == live_bare_meta_mtime


def test_legacy_single_consumer_inbox_keeps_bare_filenames(tmp_path: Path) -> None:
    """When inbox_dir is the legacy ``calibration_inbox`` (single-consumer)
    or unset, the broker must still write to the bare ``terminal_data.json``
    / ``.hybrid_calibration_meta.json`` so operator tooling that cats those
    paths and the existing meta-payload test keep working."""
    # Default (no inbox_dir).
    broker_default = HybridCalibrationBroker(
        results_dir=tmp_path,
        enable_local_worker=False,
        producer_skew_tolerance_ms=10**18,
    )
    assert broker_default.inbox_dir.name == "calibration_inbox"
    assert broker_default.terminal_path.name == "terminal_data.json"
    assert broker_default.meta_path.name == ".hybrid_calibration_meta.json"

    # Explicit legacy inbox_dir.
    legacy_inbox = tmp_path / "calibration_inbox"
    legacy_inbox.mkdir(exist_ok=True)
    broker_legacy = HybridCalibrationBroker(
        results_dir=tmp_path,
        enable_local_worker=False,
        inbox_dir=legacy_inbox,
        producer_skew_tolerance_ms=10**18,
    )
    assert broker_legacy.terminal_path.name == "terminal_data.json"
    assert broker_legacy.meta_path.name == ".hybrid_calibration_meta.json"


def test_inbox_dir_default_is_results_dir_subpath(tmp_path: Path) -> None:
    """Default inbox_dir keeps the historical layout for callers that don't
    pass an override (single-consumer / VPS topology)."""
    broker = HybridCalibrationBroker(
        results_dir=tmp_path,
        enable_local_worker=False,
    )
    assert broker.inbox_dir == tmp_path / "calibration_inbox"


# --- barrier-divergence guard ---------------------------------------------


def _make_event_with_barrier(
    market_id: str,
    ready_ms: int,
    *,
    barrier: float,
    market_end_ms: int | None = None,
) -> CalibrationEvent:
    base = _make_event(market_id, ready_ms, market_end_ms=market_end_ms)
    return CalibrationEvent(
        schema_version=base.schema_version,
        event_id=base.event_id,
        source=base.source,
        market_id=base.market_id,
        barrier=barrier,
        market_end_time_iso=base.market_end_time_iso,
        ready_at_iso=base.ready_at_iso,
        ready_at_unix_ms=base.ready_at_unix_ms,
        prob_above=base.prob_above,
        prob_below=base.prob_below,
        model_used=base.model_used,
        calibration_seconds=base.calibration_seconds,
        terminal_payload=base.terminal_payload,
    )


def test_publish_rejects_barrier_diverging_from_consumer(tmp_path: Path) -> None:
    """When the consumer's MarketContext sets a barrier and the producer's
    event.barrier differs by >1%, the broker refuses the event."""
    broker = HybridCalibrationBroker(
        results_dir=tmp_path,
        enable_local_worker=False,
        producer_skew_tolerance_ms=10**18,
    )
    market_id = "btc-updown-15m-divergent"
    broker.update_market_context(
        MarketContext(
            market_id=market_id,
            barrier=80_000.0,
            market_end_time_iso="2026-04-30T00:15:00+00:00",
        )
    )
    # 0.5% diff: accepted.
    accepted = _make_event_with_barrier(market_id, 1000, barrier=80_400.0)
    assert broker.publish(accepted) is True
    # 2% diff: rejected.
    rejected = _make_event_with_barrier(market_id, 2000, barrier=82_000.0)
    assert broker.publish(rejected) is False
    # The accepted event remains the broker's latest for the market.
    latest = broker.latest_for_market(market_id)
    assert latest is not None and latest.event_id == accepted.event_id


def test_publish_accepts_when_consumer_has_no_context(tmp_path: Path) -> None:
    """No consumer context → cannot validate barrier → accept."""
    broker = HybridCalibrationBroker(
        results_dir=tmp_path,
        enable_local_worker=False,
        producer_skew_tolerance_ms=10**18,
    )
    event = _make_event_with_barrier("btc-updown-15m-nocontext", 1000, barrier=99_999.0)
    assert broker.publish(event) is True


def test_publish_accepts_when_consumer_barrier_is_zero(tmp_path: Path) -> None:
    """Consumer hasn't yet captured a real barrier (0) → don't gate."""
    broker = HybridCalibrationBroker(
        results_dir=tmp_path,
        enable_local_worker=False,
        producer_skew_tolerance_ms=10**18,
    )
    market_id = "btc-updown-15m-zerocontext"
    broker.update_market_context(
        MarketContext(
            market_id=market_id,
            barrier=0.0,
            market_end_time_iso="2026-04-30T00:15:00+00:00",
        )
    )
    event = _make_event_with_barrier(market_id, 1000, barrier=80_000.0)
    assert broker.publish(event) is True


def test_update_market_context_evicts_stale_event_on_barrier_refine(tmp_path: Path) -> None:
    """Sequence: consumer publishes ctx with barrier=80_000. Producer event
    A passes the divergence check at publish() and gets stored. Consumer's
    barrier later refines (binance → chainlink) to a value >1% off the
    stored event's barrier. update_market_context must evict the stale
    event rather than blindly reapply it — otherwise latest_for_market()
    keeps serving probabilities calibrated against the old barrier."""
    broker = HybridCalibrationBroker(
        results_dir=tmp_path,
        enable_local_worker=False,
        producer_skew_tolerance_ms=10**18,
    )
    market_id = "btc-updown-15m-context-refine"
    broker.update_market_context(
        MarketContext(
            market_id=market_id,
            barrier=80_000.0,
            market_end_time_iso="2026-04-30T00:15:00+00:00",
        )
    )
    accepted = _make_event_with_barrier(market_id, 1000, barrier=80_000.0)
    assert broker.publish(accepted) is True

    # Refine consumer barrier to 82_000 (2.5% off stored event's 80_000).
    broker.update_market_context(
        MarketContext(
            market_id=market_id,
            barrier=82_000.0,
            market_end_time_iso="2026-04-30T00:15:00+00:00",
        )
    )

    assert broker.latest_for_market(market_id) is None, (
        "Stored event must be evicted when consumer's refined barrier "
        "diverges from event.barrier by more than the threshold"
    )


def test_update_market_context_keeps_event_when_barrier_within_tolerance(tmp_path: Path) -> None:
    """Same setup, but the refined barrier is 0.5% off — within the 1%
    threshold. The stored event remains and gets reapplied."""
    broker = HybridCalibrationBroker(
        results_dir=tmp_path,
        enable_local_worker=False,
        producer_skew_tolerance_ms=10**18,
    )
    market_id = "btc-updown-15m-context-refine-ok"
    broker.update_market_context(
        MarketContext(
            market_id=market_id,
            barrier=80_000.0,
            market_end_time_iso="2026-04-30T00:15:00+00:00",
        )
    )
    accepted = _make_event_with_barrier(market_id, 1000, barrier=80_000.0)
    assert broker.publish(accepted) is True

    broker.update_market_context(
        MarketContext(
            market_id=market_id,
            barrier=80_400.0,  # 0.5% off
            market_end_time_iso="2026-04-30T00:15:00+00:00",
        )
    )

    latest = broker.latest_for_market(market_id)
    assert latest is not None and latest.event_id == accepted.event_id


def test_update_market_context_with_zero_barrier_does_not_evict(tmp_path: Path) -> None:
    """When the refreshed context barrier is 0 (consumer hasn't captured a
    real barrier yet), divergence cannot be computed. Don't evict — leave
    the stored event alone so future ticks with a real barrier can validate."""
    broker = HybridCalibrationBroker(
        results_dir=tmp_path,
        enable_local_worker=False,
        producer_skew_tolerance_ms=10**18,
    )
    market_id = "btc-updown-15m-context-zero"
    broker.update_market_context(
        MarketContext(
            market_id=market_id,
            barrier=80_000.0,
            market_end_time_iso="2026-04-30T00:15:00+00:00",
        )
    )
    accepted = _make_event_with_barrier(market_id, 1000, barrier=80_000.0)
    assert broker.publish(accepted) is True

    broker.update_market_context(
        MarketContext(
            market_id=market_id,
            barrier=0.0,
            market_end_time_iso="2026-04-30T00:15:00+00:00",
        )
    )

    latest = broker.latest_for_market(market_id)
    assert latest is not None and latest.event_id == accepted.event_id


# ---------------------------------------------------------------------------
# v3 schema: barrier_source / barrier_offset_s plus entry_signals on the event payload
# ---------------------------------------------------------------------------


def test_calibration_event_v3_round_trip_preserves_barrier_source_and_offset() -> None:
    """v3 event built via from_parts and round-tripped through to_dict /
    from_dict must preserve the new barrier_source / barrier_offset_s
    fields exactly."""
    ctx = MarketContext(
        market_id="btc-updown-15m-x",
        barrier=70_376.82,
        market_end_time_iso="2026-04-30T16:15:00+00:00",
        barrier_source="chainlink",
        barrier_offset_s=1.5,
    )
    ready_at = datetime(2026, 4, 30, 16, 10, 5, tzinfo=timezone.utc)
    event = CalibrationEvent.from_parts(
        source="shared_calc",
        context=ctx,
        ready_at=ready_at,
        model_data={
            "prob_above": 0.6,
            "prob_below": 0.4,
            "model_used": "ssvi_surface",
            "calibration_seconds": 1.2,
        },
        terminal_payload={"timing": {"surface_fit_s": 1.0, "mc_s": 0.5}},
    )

    # from_parts stamps the CURRENT producer schema (v4 adds the optional
    # terminal-return distribution); the v3 fields must still round-trip.
    assert event.schema_version == 5
    assert event.barrier_source == "chainlink"
    assert event.barrier_offset_s == 1.5

    round_tripped = CalibrationEvent.from_dict(event.to_dict())
    assert round_tripped.barrier_source == "chainlink"
    assert round_tripped.barrier_offset_s == 1.5
    assert round_tripped.barrier == 70_376.82


def test_calibration_event_legacy_v1_dict_decodes_with_unknown_source() -> None:
    """A v1 payload missing barrier_source / barrier_offset_s must decode
    with safe defaults (source='unknown', offset=None) so consumers running
    new code don't crash on old inbox files left over during a deploy."""
    legacy_payload = {
        "schema_version": 1,
        "event_id": "12345-legacy",
        "source": "shared_calc",
        "market_id": "btc-updown-15m-legacy",
        "barrier": 70_000.0,
        "market_end_time_iso": "2026-04-30T16:15:00+00:00",
        "ready_at_iso": "2026-04-30T16:10:05+00:00",
        "ready_at_unix_ms": 1777_565_405_000,
        "prob_above": 0.5,
        "prob_below": 0.5,
        "model_used": "ssvi_surface",
        "calibration_seconds": 1.0,
        "terminal_payload": {},
    }
    decoded = CalibrationEvent.from_dict(legacy_payload)
    assert decoded.schema_version == 1
    assert decoded.barrier_source == "unknown"
    assert decoded.barrier_offset_s is None


def test_broker_latest_barrier_for_market_returns_source_from_event(tmp_path: Path) -> None:
    """The new helper used by SharedDataFeed to consume calc's barrier:
    returns (barrier, source, offset_s) for the freshest event."""
    broker = HybridCalibrationBroker(
        results_dir=tmp_path,
        enable_local_worker=False,
        producer_skew_tolerance_ms=10**18,
    )
    broker.update_market_context(
        MarketContext(
            market_id="btc-updown-15m-1",
            barrier=80_000.0,
            market_end_time_iso="2026-04-23T00:15:00+00:00",
        )
    )
    base = _make_event("btc-updown-15m-1", 1000)
    event = CalibrationEvent(
        schema_version=2,
        event_id=base.event_id,
        source=base.source,
        market_id=base.market_id,
        barrier=base.barrier,
        market_end_time_iso=base.market_end_time_iso,
        ready_at_iso=base.ready_at_iso,
        ready_at_unix_ms=base.ready_at_unix_ms,
        prob_above=base.prob_above,
        prob_below=base.prob_below,
        model_used=base.model_used,
        calibration_seconds=base.calibration_seconds,
        terminal_payload=base.terminal_payload,
        barrier_source="chainlink",
        barrier_offset_s=2.5,
    )
    assert broker.publish(event) is True

    out = broker.latest_barrier_for_market("btc-updown-15m-1", max_age_s=10**9)
    assert out is not None
    barrier, source, offset = out
    assert barrier == 80_000.0
    assert source == "chainlink"
    assert offset == 2.5


def test_broker_latest_barrier_for_market_returns_none_when_no_event(tmp_path: Path) -> None:
    broker = HybridCalibrationBroker(
        results_dir=tmp_path,
        enable_local_worker=False,
        producer_skew_tolerance_ms=10**18,
    )
    assert broker.latest_barrier_for_market("never-published") is None
