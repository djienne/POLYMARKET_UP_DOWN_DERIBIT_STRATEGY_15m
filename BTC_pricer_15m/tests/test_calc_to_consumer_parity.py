"""End-to-end parity test: one calc daemon iteration → two consumer brokers.

The whole point of the shared-calculator refactor is that grid and live
*never* see different model_probs for the same market window. This test
stitches the daemon's output side to two broker readers' input sides and
confirms the events come through bit-identical.

If this test ever fails, the per-consumer inbox fan-out has drifted and the
4.5pp `model_prob` divergence we observed in production can come back.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from scripts.trader.calc_daemon import CalcConfig, CalcDaemon
from scripts.trader.hybrid_calibration import (
    HybridCalibrationBroker,
    MarketContext,
)
from scripts.trader.spot_ipc import SpotTick


def _terminal_payload() -> dict:
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "timing": {"surface_fit_s": 1.5, "mc_s": 0.8},
        "avg_prob_above": 0.65,
        "avg_prob_below": 0.35,
    }


def _model_data() -> dict:
    return {
        "prob_above": 0.65,
        "prob_below": 0.35,
        "model_used": "ssvi_surface",
        "calibration_seconds": 2.3,
    }


class _StaticSpotFeed:
    def latest(self, *, max_age_s: float = 10.0) -> SpotTick:
        return SpotTick.create(
            price=82_540.0,
            source="polymarket_rtds",
            sequence=7,
            stale_after_seconds=max_age_s,
            details={"test": True},
        )


def test_calc_to_grid_and_live_brokers_emit_identical_events(tmp_path: Path) -> None:
    grid_inbox = tmp_path / "calibration_inbox_grid"
    live_inbox = tmp_path / "calibration_inbox_live"

    cfg = CalcConfig(
        inbox_targets=[grid_inbox, live_inbox],
        snapshot_path=tmp_path / "terminal_data.shared.json",
        delay_min_seconds=0.0,
        delay_max_seconds=0.0,
        inbox_ttl_seconds=300.0,
        source_label="shared_calc",
        market_scan_interval_seconds=30.0,
    )
    daemon = CalcDaemon(cfg, terminal_path=tmp_path / "worker_terminal.json")
    daemon._spot_feed = _StaticSpotFeed()

    end_time = datetime.now(timezone.utc) + timedelta(minutes=4)
    context = MarketContext(
        market_id="btc-updown-15m-parity",
        barrier=82500.0,
        market_end_time_iso=end_time.isoformat(),
    )

    payload = _terminal_payload()

    def fake_run_terminal_script(**kwargs):
        json_path = kwargs.get("json_path")
        if json_path is not None:
            json_path.parent.mkdir(parents=True, exist_ok=True)
            json_path.write_text(json.dumps(payload), encoding="utf-8")
        return _model_data()

    with patch(
        "scripts.trader.calc_daemon._resolve_active_market_context",
        return_value=context,
    ):
        with patch(
            "scripts.trader.calc_daemon.run_terminal_script",
            side_effect=fake_run_terminal_script,
        ):
            event = daemon.run_once()

    assert event is not None

    # Build two brokers with their own inbox dirs, mirroring grid and live.
    broker_grid = HybridCalibrationBroker(
        results_dir=tmp_path / "grid_results",
        enable_local_worker=False,
        inbox_dir=grid_inbox,
        producer_skew_tolerance_ms=10**18,
    )
    broker_live = HybridCalibrationBroker(
        results_dir=tmp_path / "live_results",
        enable_local_worker=False,
        inbox_dir=live_inbox,
        producer_skew_tolerance_ms=10**18,
    )
    for broker in (broker_grid, broker_live):
        broker.update_market_context(context)
        broker.poll_inbox()

    grid_event = broker_grid.latest_for_market(context.market_id)
    live_event = broker_live.latest_for_market(context.market_id)
    assert grid_event is not None and live_event is not None

    # Field-by-field equality on every public attribute.
    for field in (
        "event_id",
        "source",
        "market_id",
        "barrier",
        "market_end_time_iso",
        "ready_at_iso",
        "ready_at_unix_ms",
        "prob_above",
        "prob_below",
        "model_used",
        "calibration_seconds",
        "terminal_payload",
        "spot_price",
        "spot_source",
        "spot_source_label",
        "spot_sequence",
    ):
        g = getattr(grid_event, field)
        v = getattr(live_event, field)
        assert g == v, (
            f"divergence at field {field!r}: "
            f"grid={g!r} live={v!r}"
        )

    # Each broker has consumed and unlinked its own copy.
    assert list(grid_inbox.glob("*.json")) == []
    assert list(live_inbox.glob("*.json")) == []
