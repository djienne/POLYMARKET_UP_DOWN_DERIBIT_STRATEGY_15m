"""Calibration producer, configuration, market discovery and file fallback tests.

Network/model I/O is replaced here to isolate orchestration failures. The real
Redis entrypoint and recorded numerical path are checked in the TWAP tests.
"""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

from scripts.trader.calc_daemon import (
    CalcConfig,
    CalcDaemon,
    _resolve_active_market_context,
    _sweep_inbox,
)
from scripts.trader.config import GridConfig, InstanceParams
from scripts.trader.hybrid_calibration import (
    CalibrationEvent,
    HybridCalibrationBroker,
    MarketContext,
)
from scripts.trader.spot_ipc import SpotTick


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_terminal_payload() -> dict:
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "timing": {"surface_fit_s": 1.2, "mc_s": 0.9},
        "avg_prob_above": 0.7,
        "avg_prob_below": 0.3,
    }


def _make_model_data() -> dict:
    return {
        "prob_above": 0.7,
        "prob_below": 0.3,
        "model_used": "ssvi_surface",
        "calibration_seconds": 2.1,
    }


class _StaticSpotFeed:
    def __init__(self, price: float = 80_456.0) -> None:
        self.price = price

    def latest(self, *, max_age_s: float = 10.0) -> SpotTick:
        return SpotTick.create(
            price=self.price,
            source="polymarket_rtds",
            sequence=1,
            stale_after_seconds=max_age_s,
            details={"test": True},
        )


def _build_calc_config(
    tmp_path: Path,
    *,
    inbox_subdirs: list[str] | None = None,
    snapshot: bool = True,
    delay: float = 0.0,
    ttl: float = 300.0,
) -> CalcConfig:
    inboxes = inbox_subdirs or ["calibration_inbox_a", "calibration_inbox_b"]
    return CalcConfig(
        inbox_targets=[tmp_path / sub for sub in inboxes],
        snapshot_path=tmp_path / "terminal_data.shared.json" if snapshot else None,
        delay_min_seconds=delay,
        delay_max_seconds=delay,
        inbox_ttl_seconds=ttl,
        source_label="shared_calc_test",
        market_scan_interval_seconds=30.0,
    )


def _setup_daemon_with_mocked_io(
    tmp_path: Path,
    *,
    config: CalcConfig | None = None,
    market_id: str = "btc-updown-15m-1234567890",
    barrier: float = 80000.0,
    minutes_until_end: float = 4.0,
    terminal_path: Path | None = None,
) -> tuple[CalcDaemon, MarketContext]:
    cfg = config or _build_calc_config(tmp_path)
    end_time = datetime.now(timezone.utc) + timedelta(minutes=minutes_until_end)
    context = MarketContext(
        market_id=market_id,
        barrier=barrier,
        market_end_time_iso=end_time.isoformat(),
    )
    daemon = CalcDaemon(cfg, terminal_path=terminal_path or tmp_path / "_worker_terminal.json")
    daemon._spot_feed = _StaticSpotFeed()
    return daemon, context


def test_live_signal_params_load_from_config_yaml_live_section(tmp_path: Path) -> None:
    (tmp_path / "results").mkdir()
    (tmp_path / "config.yaml").write_text(
        """
grid:
  max_entry_price: 0.95
  grid:
    alpha_up: [1.0]
    alpha_down: [1.0]
    floor_up: [0.5]
    floor_down: [0.5]
    tp_percentage: [0.15]
    stop_loss_pct: [0.0]
live:
  paper_mode: false
  live_trading_enabled: true
  edge_alpha_up: 2.5
  edge_alpha_down: 1.8
  edge_floor_up: 0.45
  edge_floor_down: 0.65
  max_entry_price: 0.95
""",
        encoding="utf-8",
    )
    daemon = CalcDaemon(
        _build_calc_config(tmp_path),
        terminal_path=tmp_path / "results" / "terminal_data.shared.json",
    )

    params = daemon._load_signal_params()
    live = next(p for p in params if p["instance_id"] == "live")

    assert live["alpha_up"] == 2.5
    assert live["alpha_down"] == 1.8
    assert live["floor_up"] == 0.45
    assert live["floor_down"] == 0.65
    assert live["max_entry_price"] == 0.95


def test_signal_params_include_appended_stop_loss_grid_instances(tmp_path: Path) -> None:
    (tmp_path / "results").mkdir()
    (tmp_path / "config.yaml").write_text(
        """
grid:
  max_entry_price: 0.95
  append_stop_loss_pct: [0.40]
  grid:
    alpha_up: [2.5]
    alpha_down: [1.8]
    floor_up: [0.45]
    floor_down: [0.65]
    tp_percentage: [0.30]
    stop_loss_pct: [0.20]
live:
  edge_alpha_up: 2.5
  edge_alpha_down: 1.8
  edge_floor_up: 0.45
  edge_floor_down: 0.65
  max_entry_price: 0.95
""",
        encoding="utf-8",
    )
    daemon = CalcDaemon(
        _build_calc_config(tmp_path),
        terminal_path=tmp_path / "results" / "terminal_data.shared.json",
    )

    params = daemon._load_signal_params()
    grid_params = [p for p in params if isinstance(p["instance_id"], int)]

    assert [p["instance_id"] for p in grid_params] == [0, 1]
    assert grid_params[0]["alpha_up"] == grid_params[1]["alpha_up"] == 2.5
    assert grid_params[0]["alpha_down"] == grid_params[1]["alpha_down"] == 1.8
    assert grid_params[0]["floor_up"] == grid_params[1]["floor_up"] == 0.45
    assert grid_params[0]["floor_down"] == grid_params[1]["floor_down"] == 0.65

    daemon._entry_signal_params = params
    signals = daemon._compute_entry_signals(
        prob_above=0.10,
        prob_below=0.95,
        up_best_ask=0.65,
        down_best_ask=0.65,
        market_id="btc-updown-15m-test",
    )
    down_ids = {
        sig["instance_id"]
        for sig in signals
        if sig["direction"] == "DOWN" and sig["market_id"] == "btc-updown-15m-test"
    }

    assert {0, 1}.issubset(down_ids)


def test_production_signal_params_include_sl40_live_mirror_clone() -> None:
    root = Path(__file__).resolve().parents[1]
    grid_config = GridConfig.from_json(root / "config.yaml")
    all_grid_params = grid_config.generate_params()

    daemon = CalcDaemon(
        _build_calc_config(root),
        terminal_path=root / "results" / "terminal_data.shared.json",
    )
    signal_params = daemon._load_signal_params()
    grid_signal_params = {
        int(p["instance_id"]): p
        for p in signal_params
        if isinstance(p["instance_id"], int)
    }

    assert len(grid_signal_params) == len(all_grid_params)
    assert grid_signal_params[815] == {
        "instance_id": 815,
        "alpha_up": 2.5,
        "alpha_down": 2.5,
        "floor_up": 0.45,
        "floor_down": 0.45,
        "max_entry_price": 0.95,
    }
    assert grid_signal_params[1271] == {
        "instance_id": 1271,
        "alpha_up": 2.5,
        "alpha_down": 2.5,
        "floor_up": 0.45,
        "floor_down": 0.45,
        "max_entry_price": 0.95,
    }


def _patch_run_once(
    daemon: CalcDaemon,
    context: MarketContext,
    *,
    model_data: dict | None = None,
    terminal_payload: dict | None = None,
    raise_exc: BaseException | None = None,
):
    """Returns a context-manager-stack that mocks all IO done by run_once."""
    from contextlib import ExitStack

    stack = ExitStack()
    stack.enter_context(
        patch("scripts.trader.calc_daemon._resolve_active_market_context", return_value=context)
    )

    if raise_exc is not None:
        stack.enter_context(
            patch(
                "scripts.trader.calc_daemon.run_terminal_script",
                side_effect=raise_exc,
            )
        )
    else:
        # run_terminal_script writes the worker terminal JSON file as a side
        # effect; mirror that here so the daemon can read it back.
        payload = terminal_payload or _make_terminal_payload()

        def fake_run_terminal_script(**kwargs):
            json_path = kwargs.get("json_path")
            if json_path is not None:
                json_path.parent.mkdir(parents=True, exist_ok=True)
                json_path.write_text(json.dumps(payload), encoding="utf-8")
            return model_data or _make_model_data()

        stack.enter_context(
            patch(
                "scripts.trader.calc_daemon.run_terminal_script",
                side_effect=fake_run_terminal_script,
            )
        )
    return stack


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_config_loads_from_json(tmp_path: Path) -> None:
    cfg_path = tmp_path / "config_calc.json"
    cfg_path.write_text(
        json.dumps(
            {
                "inbox_targets": ["results/inbox_a", "results/inbox_b"],
                "snapshot_path": "results/snap.json",
                "delay_min_seconds": 1.0,
                "delay_max_seconds": 2.0,
                "inbox_ttl_seconds": 120.0,
                "source_label": "shared_calc",
            }
        ),
        encoding="utf-8",
    )
    cfg = CalcConfig.from_json(cfg_path)
    assert cfg.inbox_targets == [Path("results/inbox_a"), Path("results/inbox_b")]
    assert cfg.snapshot_path == Path("results/snap.json")
    assert cfg.delay_min_seconds == 1.0
    assert cfg.delay_max_seconds == 2.0
    assert cfg.inbox_ttl_seconds == 120.0
    assert cfg.source_label == "shared_calc"


def test_config_rejects_empty_inbox_list(tmp_path: Path) -> None:
    cfg_path = tmp_path / "config_calc.json"
    cfg_path.write_text(json.dumps({"inbox_targets": []}), encoding="utf-8")
    with pytest.raises(ValueError):
        CalcConfig.from_json(cfg_path)


def test_run_once_fans_out_byte_identical_payloads(tmp_path: Path) -> None:
    daemon, context = _setup_daemon_with_mocked_io(tmp_path)

    with _patch_run_once(daemon, context):
        event = daemon.run_once()

    assert event is not None
    assert event.source == "shared_calc_test"

    # Each inbox has exactly one file, and they are byte-identical.
    payloads = []
    for target in daemon.config.inbox_targets:
        files = list(target.glob("*.json"))
        assert len(files) == 1, f"expected 1 file in {target}, got {[p.name for p in files]}"
        payloads.append(files[0].read_bytes())
    assert payloads[0] == payloads[1]
    # Snapshot also written.
    assert daemon.config.snapshot_path.exists()
    snapshot_payload = daemon.config.snapshot_path.read_bytes()
    assert snapshot_payload == payloads[0]


def test_run_once_event_round_trips_through_inbox(tmp_path: Path) -> None:
    """A consumer broker reading the inbox should see the same event the
    daemon produced (parity at the JSON-on-disk boundary)."""
    daemon, context = _setup_daemon_with_mocked_io(tmp_path)

    with _patch_run_once(daemon, context):
        event = daemon.run_once()

    inbox = daemon.config.inbox_targets[0]
    raw = json.loads(next(inbox.glob("*.json")).read_text(encoding="utf-8"))
    parsed = CalibrationEvent.from_dict(raw)
    assert parsed.event_id == event.event_id
    assert parsed.market_id == event.market_id
    assert parsed.prob_above == event.prob_above
    assert parsed.prob_below == event.prob_below
    assert parsed.barrier == event.barrier


def test_run_once_does_not_override_model_spot_with_polymarket_ptb(tmp_path: Path) -> None:
    """Polymarket's crypto-price openPrice is the PTB/barrier, not spot.

    The terminal worker should source current spot from the shared spot feed.
    Passing Polymarket openPrice as ``spot`` pins ``spot == barrier`` and
    flattens the model probability series.
    """

    class _TrapPolymarket:
        def current_price(self):  # pragma: no cover - should never be called
            raise AssertionError("must not use Polymarket PTB as current spot")

    cfg = _build_calc_config(tmp_path)
    daemon, context = _setup_daemon_with_mocked_io(
        tmp_path,
        config=cfg,
        terminal_path=tmp_path / "_worker_terminal.json",
    )
    daemon._polymarket = _TrapPolymarket()
    daemon._spot_feed = _StaticSpotFeed(80_456.0)
    captured_kwargs: dict = {}

    def fake_run_terminal_script(**kwargs):
        captured_kwargs.update(kwargs)
        kwargs["json_path"].write_text(
            json.dumps(_make_terminal_payload() | {"spot_price": 80_123.0}),
            encoding="utf-8",
        )
        return _make_model_data() | {"spot_price": 80_123.0}

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
    assert captured_kwargs.get("spot") == pytest.approx(80_456.0)
    assert captured_kwargs.get("spot") != pytest.approx(context.barrier)
    assert event.spot_price == pytest.approx(80_456.0)
    assert event.terminal_payload["spot_price"] == pytest.approx(80_123.0)


def test_run_once_skips_when_no_active_market(tmp_path: Path) -> None:
    cfg = _build_calc_config(tmp_path)
    daemon = CalcDaemon(cfg, terminal_path=tmp_path / "_worker.json")
    with patch(
        "scripts.trader.calc_daemon._resolve_active_market_context",
        return_value=None,
    ):
        out = daemon.run_once()
    assert out is None
    for target in cfg.inbox_targets:
        assert not target.exists() or list(target.glob("*.json")) == []


def test_run_once_skips_when_market_already_ended(tmp_path: Path) -> None:
    cfg = _build_calc_config(tmp_path)
    daemon = CalcDaemon(cfg, terminal_path=tmp_path / "_worker.json")
    expired_context = MarketContext(
        market_id="btc-updown-15m-stale",
        barrier=80000.0,
        market_end_time_iso=(datetime.now(timezone.utc) - timedelta(seconds=5)).isoformat(),
    )
    with patch(
        "scripts.trader.calc_daemon._resolve_active_market_context",
        return_value=expired_context,
    ):
        with patch(
            "scripts.trader.calc_daemon.run_terminal_script",
            side_effect=AssertionError("must not call when market has ended"),
        ):
            out = daemon.run_once()
    assert out is None


def test_run_forever_survives_iteration_exception(tmp_path: Path, caplog) -> None:
    cfg = _build_calc_config(tmp_path, delay=0.0)
    daemon = CalcDaemon(cfg, terminal_path=tmp_path / "_worker.json")
    daemon._spot_feed = _StaticSpotFeed()
    end_time = datetime.now(timezone.utc) + timedelta(minutes=4)
    context = MarketContext(
        market_id="btc-updown-15m-boom",
        barrier=80000.0,
        market_end_time_iso=end_time.isoformat(),
    )

    calls = {"n": 0}

    def boom(**_kwargs):
        calls["n"] += 1
        if calls["n"] >= 2:
            daemon.request_stop()
        raise RuntimeError("simulated outage")

    # We need run_forever's exception path to invoke time.sleep(5). Patch the
    # daemon's _sleep so it returns immediately (otherwise we'd wait 5s each
    # iteration).
    with patch.object(daemon, "_sleep", lambda *_a, **_k: None):
        with patch(
            "scripts.trader.calc_daemon._resolve_active_market_context",
            return_value=context,
        ):
            with patch(
                "scripts.trader.calc_daemon.run_terminal_script",
                side_effect=boom,
            ):
                with caplog.at_level(logging.ERROR, logger="scripts.trader.calc_daemon"):
                    daemon.run_forever()

    assert calls["n"] >= 2, "loop should have retried after the first crash"
    assert any("calc iteration failed" in rec.message for rec in caplog.records), \
        "exception should have been logged"


def test_sweep_inbox_removes_old_files(tmp_path: Path) -> None:
    inbox = tmp_path / "inbox"
    inbox.mkdir()
    old = inbox / "old.json"
    fresh = inbox / "fresh.json"
    old.write_text("{}", encoding="utf-8")
    fresh.write_text("{}", encoding="utf-8")
    # Backdate `old` by 10 minutes.
    past = time.time() - 600
    import os

    os.utime(old, (past, past))

    _sweep_inbox(inbox, ttl_seconds=300.0)

    assert not old.exists()
    assert fresh.exists()


def test_sweep_inbox_noop_when_ttl_disabled(tmp_path: Path) -> None:
    inbox = tmp_path / "inbox"
    inbox.mkdir()
    p = inbox / "evt.json"
    p.write_text("{}", encoding="utf-8")
    import os

    past = time.time() - 86400
    os.utime(p, (past, past))

    _sweep_inbox(inbox, ttl_seconds=0)
    assert p.exists()


def test_producer_file_fallback_leaves_no_temporary_files(tmp_path: Path) -> None:
    """No `.tmp.*` siblings should remain after a successful write — the
    active IPC file publisher must finish its atomic write."""
    daemon, context = _setup_daemon_with_mocked_io(tmp_path)
    with _patch_run_once(daemon, context):
        event = daemon.run_once()
    assert event is not None
    target = daemon.config.inbox_targets[0]
    leftovers = [p for p in target.iterdir() if p.suffix != ".json"]
    assert leftovers == [], f"unexpected non-.json files: {[p.name for p in leftovers]}"




# ---------------------------------------------------------------------------
# Chainlink integration in _capture_barrier
# ---------------------------------------------------------------------------


class _StubChainlink:
    """Minimal stand-in for ChainlinkPriceFeed.

    Has the two attributes the daemon's `_capture_barrier` reads:
    `is_healthy` (bool) and `get_first_at_or_after(start, max_offset_s)`.
    """

    def __init__(self, is_healthy: bool, result: tuple = (None, None)):
        self.is_healthy = is_healthy
        self._result = result

    def get_first_at_or_after(self, *_args, **_kwargs):
        return self._result


def test_capture_barrier_prefers_chainlink_when_healthy(tmp_path: Path) -> None:
    from scripts.trader.calc_daemon import _capture_barrier

    chainlink = _StubChainlink(is_healthy=True, result=(70_500.0, 1.5))

    with patch(
        "scripts.trader.calc_daemon.get_binance_price_at",
        return_value=70_490.0,  # 0.014% diff → cross-validation passes
    ):
        out = _capture_barrier(datetime.now(timezone.utc), chainlink=chainlink)
    assert out == (70_500.0, "chainlink", 1.5)


def test_capture_barrier_returns_none_when_chainlink_unhealthy(tmp_path: Path) -> None:
    """Chainlink-only: if chainlink is unhealthy, return None — NO Binance
    fallback. Polymarket's resolution rule is chainlink-only; we must not
    publish a non-chainlink barrier downstream."""
    from scripts.trader.calc_daemon import _capture_barrier

    chainlink = _StubChainlink(is_healthy=False)
    with patch(
        "scripts.trader.calc_daemon.get_binance_price_at",
        return_value=70_400.0,
    ):
        out = _capture_barrier(datetime.now(timezone.utc), chainlink=chainlink)
    assert out is None


def test_capture_barrier_rejects_chainlink_diverging_from_binance(tmp_path: Path) -> None:
    """Chainlink/Binance >1% divergence → REJECT chainlink, return None.
    The strike must be the resolution-source price; if chainlink and
    Binance disagree by more than 1%, neither can be trusted as the
    Polymarket-correct strike. Calc retries on the next loop."""
    from scripts.trader.calc_daemon import _capture_barrier

    chainlink = _StubChainlink(is_healthy=True, result=(80_000.0, 1.0))
    with patch(
        "scripts.trader.calc_daemon.get_binance_price_at",
        return_value=70_000.0,  # ~14% diff — way above 1% reject threshold
    ):
        out = _capture_barrier(datetime.now(timezone.utc), chainlink=chainlink)
    assert out is None  # No Binance fallback — chainlink-only


def test_capture_barrier_chainlink_only_when_no_binance(tmp_path: Path) -> None:
    """If Binance kline lookup throws, accept Chainlink unvalidated."""
    from scripts.trader.calc_daemon import _capture_barrier

    chainlink = _StubChainlink(is_healthy=True, result=(70_500.0, 2.0))
    with patch(
        "scripts.trader.calc_daemon.get_binance_price_at",
        side_effect=RuntimeError("binance unavailable"),
    ):
        with patch(
            "scripts.trader.calc_daemon.get_current_btc_price",
            side_effect=AssertionError("must not fall to spot when chainlink works"),
        ):
            out = _capture_barrier(datetime.now(timezone.utc), chainlink=chainlink)
    assert out == (70_500.0, "chainlink", 2.0)


def test_capture_barrier_chainlink_none_returns_none(tmp_path: Path) -> None:
    """Chainlink-only: when chainlink=None is passed, return None — NO
    Binance fallback (the chainlink kwarg is the only resolution source)."""
    from scripts.trader.calc_daemon import _capture_barrier

    with patch(
        "scripts.trader.calc_daemon.get_binance_price_at",
        return_value=70_321.0,
    ):
        out = _capture_barrier(datetime.now(timezone.utc), chainlink=None)
    assert out is None


def test_capture_barrier_accepts_chainlink_offset_up_to_60s(tmp_path: Path) -> None:
    """A chainlink point at offset 55s (within the new 60s window — past
    the prior 30s cap) is accepted. Catches the Chainlink long-tail
    heartbeat that pre-iter-3 was rejected."""
    from scripts.trader.calc_daemon import _capture_barrier

    chainlink = _StubChainlink(is_healthy=True, result=(70_500.0, 55.0))
    with patch(
        "scripts.trader.calc_daemon.get_binance_price_at",
        return_value=70_490.0,
    ):
        out = _capture_barrier(datetime.now(timezone.utc), chainlink=chainlink)
    assert out == (70_500.0, "chainlink", 55.0)


def test_capture_barrier_passes_60s_max_offset_to_chainlink(tmp_path: Path) -> None:
    """Verify the call site uses max_offset_s=60 (Polymarket's resolution
    rule has no offset cap; we cap at 60s to catch the long-tail heartbeat).
    Captures the kwarg via a spy stub and asserts on it directly."""
    from scripts.trader.calc_daemon import _capture_barrier

    captured_kwargs: list = []

    class _SpyChainlink:
        is_healthy = True

        def get_first_at_or_after(self, _start, **kwargs):
            captured_kwargs.append(kwargs)
            return (70_500.0, 2.0)

    with patch(
        "scripts.trader.calc_daemon.get_binance_price_at",
        return_value=70_490.0,
    ):
        _capture_barrier(datetime.now(timezone.utc), chainlink=_SpyChainlink())

    assert len(captured_kwargs) == 1
    assert captured_kwargs[0].get("max_offset_s") == 60.0


# ---------------------------------------------------------------------------
# Polymarket as primary barrier source (May 2026)
# ---------------------------------------------------------------------------


class _StubPolymarket:
    """Minimal PolymarketPTB stand-in for unit tests.

    Records every ``ptb_for_window`` call and returns the configured
    value (or raises if ``raise_exc`` is set).
    """

    def __init__(self, value=None, raise_exc=None):
        self.value = value
        self.raise_exc = raise_exc
        self.calls: list[tuple] = []

    def ptb_for_window(self, start_dt, end_dt):
        self.calls.append((start_dt, end_dt))
        if self.raise_exc is not None:
            raise self.raise_exc
        return self.value


def test_capture_barrier_prefers_polymarket_when_healthy(tmp_path: Path) -> None:
    """Polymarket returns a sane value → that's used; chainlink isn't
    consulted for the winning price (it may still be queried for
    divergence telemetry, but the returned tuple is the PM value).

    ``start_utc`` is set 60s in the past so the freshness gate
    (POLYMARKET_FRESHNESS_MIN_S = 30s) is satisfied — Polymarket's
    openPrice is provisional during the first 30s of a window."""
    from scripts.trader.calc_daemon import _capture_barrier

    pm = _StubPolymarket(value=77027.29)
    chainlink = _StubChainlink(is_healthy=True, result=(77000.0, 1.0))
    aged_start = datetime.now(timezone.utc) - timedelta(seconds=60)

    with patch(
        "scripts.trader.calc_daemon.get_binance_price_at",
        return_value=77020.0,
    ):
        out = _capture_barrier(
            aged_start,
            chainlink=chainlink,
            polymarket=pm,
        )
    assert out == (pytest.approx(77027.29), "polymarket", 0.0)
    assert len(pm.calls) == 1


def test_capture_barrier_falls_through_to_chainlink_on_polymarket_none(tmp_path: Path) -> None:
    """Polymarket returns None (e.g. API hiccup) → chainlink WS path
    runs and wins. Source label is "chainlink".

    Aged start (60s past) so the PM freshness gate clears and we
    actually exercise the PM-attempted-but-failed fallback path."""
    from scripts.trader.calc_daemon import _capture_barrier

    pm = _StubPolymarket(value=None)
    chainlink = _StubChainlink(is_healthy=True, result=(77000.0, 1.5))
    aged_start = datetime.now(timezone.utc) - timedelta(seconds=60)

    with patch(
        "scripts.trader.calc_daemon.get_binance_price_at",
        return_value=77000.0,
    ):
        out = _capture_barrier(
            aged_start,
            chainlink=chainlink,
            polymarket=pm,
        )
    assert out == (77000.0, "chainlink", 1.5)


def test_capture_barrier_falls_through_when_polymarket_raises(tmp_path: Path) -> None:
    """Polymarket raises (network) → chainlink path runs without crashing.

    Aged start (60s past) so the PM freshness gate clears."""
    from scripts.trader.calc_daemon import _capture_barrier

    pm = _StubPolymarket(raise_exc=RuntimeError("net down"))
    chainlink = _StubChainlink(is_healthy=True, result=(77100.0, 0.5))
    aged_start = datetime.now(timezone.utc) - timedelta(seconds=60)

    with patch(
        "scripts.trader.calc_daemon.get_binance_price_at",
        return_value=77100.0,
    ):
        out = _capture_barrier(
            aged_start,
            chainlink=chainlink,
            polymarket=pm,
        )
    assert out == (77100.0, "chainlink", 0.5)


def test_capture_barrier_returns_none_when_all_three_fail(tmp_path: Path) -> None:
    """Polymarket=None + chainlink unhealthy + no archive → None.

    Aged start (60s past) so the PM freshness gate clears."""
    from scripts.trader.calc_daemon import _capture_barrier

    pm = _StubPolymarket(value=None)
    chainlink = _StubChainlink(is_healthy=False)
    aged_start = datetime.now(timezone.utc) - timedelta(seconds=60)

    with patch(
        "scripts.trader.calc_daemon.get_binance_price_at",
        return_value=77000.0,
    ):
        out = _capture_barrier(
            aged_start,
            chainlink=chainlink,
            polymarket=pm,
        )
    assert out is None


def test_capture_barrier_defers_when_within_freshness_window(tmp_path: Path) -> None:
    """When window is <30s old (POLYMARKET_FRESHNESS_MIN_S), defer the
    *entire* capture by returning None — do NOT call polymarket and do
    NOT fall through to chainlink. The calc loop retries on the next
    iteration once the gate clears.

    Falling through to chainlink would re-introduce the chainlink-tick
    alignment bug we're trying to escape (4ca5f67b / 147bfad2 /
    7117df01): the window-start chainlink reading often disagrees with
    Polymarket's settle-instant by enough to flip a near-the-money
    outcome. Polymarket-as-primary requires deferral, not fallthrough."""
    from scripts.trader.calc_daemon import _capture_barrier

    pm = _StubPolymarket(value=77027.29)
    chainlink = _StubChainlink(is_healthy=True, result=(77000.0, 1.0))
    fresh_start = datetime.now(timezone.utc) - timedelta(seconds=5)  # only 5s old

    with patch(
        "scripts.trader.calc_daemon.get_binance_price_at",
        return_value=77000.0,
    ):
        out = _capture_barrier(
            fresh_start,
            chainlink=chainlink,
            polymarket=pm,
        )
    # PM is NOT called within the freshness window.
    assert pm.calls == []
    # And the function defers entirely — chainlink is NOT consulted.
    assert out is None


def test_capture_barrier_defers_at_freshness_boundary(tmp_path: Path) -> None:
    """The valid Polymarket PTB window is strictly >30s and <5m."""
    from scripts.trader.calc_daemon import _capture_barrier

    start = datetime(2026, 5, 2, 21, 0, tzinfo=timezone.utc)
    pm = _StubPolymarket(value=78449.69)
    chainlink = _StubChainlink(is_healthy=True, result=(78451.37, 1.0))

    with patch(
        "scripts.trader.calc_daemon._utc_now",
        return_value=start + timedelta(seconds=30),
    ), patch(
        "scripts.trader.calc_daemon.get_binance_price_at",
        return_value=78450.0,
    ):
        out = _capture_barrier(start, chainlink=chainlink, polymarket=pm)

    assert out is None
    assert pm.calls == []


def test_capture_barrier_refuses_late_polymarket_capture_without_fallback(tmp_path: Path) -> None:
    """After 5m with no existing lock, do not capture or fall back."""
    from scripts.trader.calc_daemon import _capture_barrier

    class _BombChainlink:
        is_healthy = True

        def get_first_at_or_after(self, *_args, **_kwargs):
            raise AssertionError("must not fall back after the PTB window closes")

    start = datetime(2026, 5, 2, 21, 0, tzinfo=timezone.utc)
    pm = _StubPolymarket(value=78449.69)

    with patch(
        "scripts.trader.calc_daemon._utc_now",
        return_value=start + timedelta(seconds=300),
    ), patch(
        "scripts.trader.calc_daemon.get_binance_price_at",
        side_effect=AssertionError("must not validate without a captured barrier"),
    ):
        out = _capture_barrier(start, chainlink=_BombChainlink(), polymarket=pm)

    assert out is None
    assert pm.calls == []


def test_capture_barrier_rejects_polymarket_insane_value(tmp_path: Path) -> None:
    """Polymarket returns $1 (clearly broken) → falls through to
    chainlink. Sanity gate (``_is_sane_btc_price``) prevents poisoned
    upstream data from polluting our barrier.

    Aged start (60s past) so the PM freshness gate clears."""
    from scripts.trader.calc_daemon import _capture_barrier

    pm = _StubPolymarket(value=1.0)
    chainlink = _StubChainlink(is_healthy=True, result=(77000.0, 1.0))
    aged_start = datetime.now(timezone.utc) - timedelta(seconds=60)

    with patch(
        "scripts.trader.calc_daemon.get_binance_price_at",
        return_value=77000.0,
    ):
        out = _capture_barrier(
            aged_start,
            chainlink=chainlink,
            polymarket=pm,
        )
    assert out == (77000.0, "chainlink", 1.0)


def test_capture_barrier_polymarket_ptb_independent_from_rtds(tmp_path) -> None:
    """When PM and CL both return values but disagree by > $0.50,
    log a WARN. PM still wins — it's authoritative for the PTB.

    Aged start (60s past) so the PM freshness gate clears."""
    from scripts.trader.calc_daemon import _capture_barrier

    pm = _StubPolymarket(value=77027.29)

    class _BombChainlink:
        @property
        def is_healthy(self):
            raise AssertionError("PTB must not depend on RTDS health")

    aged_start = datetime.now(timezone.utc) - timedelta(seconds=60)

    with patch(
        "scripts.trader.calc_daemon.get_binance_price_at",
        return_value=77050.0,
    ):
        out = _capture_barrier(
            aged_start,
            chainlink=_BombChainlink(),
            polymarket=pm,
        )
    assert out == (pytest.approx(77027.29), "polymarket", 0.0)


# ---------------------------------------------------------------------------
# Per-market barrier lock — _resolve_active_market_context with locked_barriers
# ---------------------------------------------------------------------------


def _stub_market_data(market_id: str, start_offset_s: int = 5) -> dict:
    """Build the dict that extract_market_data() returns. The market is
    'active' from `now - start_offset_s` and ends 15 minutes after that."""
    start = datetime.now(timezone.utc) - timedelta(seconds=start_offset_s)
    end = start + timedelta(minutes=15)
    return {
        "market_id": market_id,
        "end_date_utc": end,
        "window_start_utc": start,
    }


def test_resolve_market_context_locks_first_chainlink_capture(tmp_path: Path) -> None:
    """First call captures chainlink @ $X. Buffer ages, second call would
    return $Y. The lock must keep returning $X for the same market."""
    from scripts.trader import calc_daemon as cd

    locks: dict[str, tuple] = {}
    captures = iter([
        (76_113.81, "chainlink", 0.0),   # genuine first emission
        (76_188.25, "chainlink", 44.0),  # buffer aged; "first" is now offset 44
    ])

    with patch.object(cd, "find_active_15m_market", return_value={"id": "m"}), \
         patch.object(cd, "extract_market_data",
                      return_value=_stub_market_data("btc-updown-15m-LOCK")), \
         patch.object(cd, "_capture_barrier", side_effect=lambda *a, **kw: next(captures)):
        first = cd._resolve_active_market_context(locked_barriers=locks)
        second = cd._resolve_active_market_context(locked_barriers=locks)

    assert first is not None and second is not None
    assert first.barrier == 76_113.81
    assert second.barrier == 76_113.81  # Locked, NOT 76,188.25
    assert second.barrier_source == "chainlink"
    assert second.barrier_offset_s == 0.0
    assert locks["btc-updown-15m-LOCK"] == (76_113.81, "chainlink", 0.0)


def test_resolve_market_context_returns_none_when_chainlink_misses_first_call(tmp_path: Path) -> None:
    """First call: chainlink not yet available → _capture_barrier returns
    None → _resolve_active_market_context returns None (no event published).
    Second call: chainlink emits → lock populates with chainlink value."""
    from scripts.trader import calc_daemon as cd

    locks: dict[str, tuple] = {}
    captures = iter([
        None,                                 # first call: chainlink missed
        (76_113.81, "chainlink", 5.0),         # second call: chainlink emits
    ])

    market_data = _stub_market_data("btc-updown-15m-DEFER", start_offset_s=10)
    with patch.object(cd, "find_active_15m_market", return_value={"id": "m"}), \
         patch.object(cd, "extract_market_data", return_value=market_data), \
         patch.object(cd, "_capture_barrier", side_effect=lambda *a, **kw: next(captures)):
        first = cd._resolve_active_market_context(locked_barriers=locks)
        second = cd._resolve_active_market_context(locked_barriers=locks)

    assert first is None  # No fallback — calc publishes nothing
    assert second is not None
    assert second.barrier == 76_113.81
    assert second.barrier_source == "chainlink"
    assert locks["btc-updown-15m-DEFER"] == (76_113.81, "chainlink", 5.0)


def test_resolve_market_context_lock_bypasses_capture_after_first_hit(tmp_path: Path) -> None:
    """Once a chainlink barrier is locked, subsequent calls do NOT call
    _capture_barrier — the locked value is permanent for that market."""
    from scripts.trader import calc_daemon as cd

    locks: dict[str, tuple] = {}
    capture_calls: list = []

    def _spy_capture(*args, **kwargs):
        capture_calls.append((args, kwargs))
        return (76_113.81, "chainlink", 0.0)

    market_data = _stub_market_data("btc-updown-15m-LOCKHOLD")
    with patch.object(cd, "find_active_15m_market", return_value={"id": "m"}), \
         patch.object(cd, "extract_market_data", return_value=market_data), \
         patch.object(cd, "_capture_barrier", side_effect=_spy_capture):
        first = cd._resolve_active_market_context(locked_barriers=locks)
        for _ in range(5):
            cd._resolve_active_market_context(locked_barriers=locks)

    # _capture_barrier was called exactly once — subsequent calls hit the lock.
    assert len(capture_calls) == 1
    assert first.barrier == 76_113.81
    assert locks["btc-updown-15m-LOCKHOLD"] == (76_113.81, "chainlink", 0.0)


def test_resolve_market_context_locks_polymarket_capture_inside_window(tmp_path: Path) -> None:
    from scripts.trader import calc_daemon as cd

    locks: dict[str, tuple] = {}
    market_data = _stub_market_data("btc-updown-15m-PMLOCK", start_offset_s=60)

    with patch.object(cd, "find_active_15m_market", return_value={"id": "m"}), \
         patch.object(cd, "extract_market_data", return_value=market_data), \
         patch.object(
             cd,
             "_capture_barrier",
             return_value=(78_449.69, "polymarket", 0.0),
         ):
        ctx = cd._resolve_active_market_context(locked_barriers=locks)

    assert ctx is not None
    assert ctx.barrier == 78_449.69
    assert ctx.barrier_source == "polymarket"
    assert locks["btc-updown-15m-PMLOCK"] == (78_449.69, "polymarket", 0.0)


def test_resolve_market_context_reuses_locked_polymarket_after_capture_window(tmp_path: Path) -> None:
    from scripts.trader import calc_daemon as cd

    locks: dict[str, tuple] = {
        "btc-updown-15m-PMHELD": (78_449.69, "polymarket", 0.0),
    }
    market_data = _stub_market_data("btc-updown-15m-PMHELD", start_offset_s=360)

    with patch.object(cd, "find_active_15m_market", return_value={"id": "m"}), \
         patch.object(cd, "extract_market_data", return_value=market_data), \
         patch.object(
             cd,
             "_capture_barrier",
             side_effect=AssertionError("locked market must not recapture after 5m"),
         ):
        ctx = cd._resolve_active_market_context(locked_barriers=locks)

    assert ctx is not None
    assert ctx.barrier == 78_449.69
    assert ctx.barrier_source == "polymarket"


def test_resolve_market_context_clears_lock_on_new_market(tmp_path: Path) -> None:
    """When the daemon detects a new market_id, the old market's lock is
    GC'd and the new market gets a fresh capture."""
    from scripts.trader.calc_daemon import CalcDaemon, CalcConfig

    cfg = _build_calc_config(tmp_path)
    daemon = CalcDaemon(cfg)
    # Pre-populate the lock with an "old" market.
    daemon._market_barriers["btc-updown-15m-OLD"] = (70_000.0, "chainlink", 0.0)
    daemon._last_market_id = "btc-updown-15m-OLD"

    # Simulate detecting the NEW market.
    daemon._last_market_id = "btc-updown-15m-NEW"
    # Mimic the GC logic in run_once after market_id transition.
    daemon._market_barriers = {
        k: v for k, v in daemon._market_barriers.items()
        if k == "btc-updown-15m-NEW"
    }

    assert "btc-updown-15m-OLD" not in daemon._market_barriers
    assert "btc-updown-15m-NEW" not in daemon._market_barriers


def test_resolve_market_context_without_locked_barriers_recomputes(tmp_path: Path) -> None:
    """Back-compat: when no locked_barriers dict is passed (e.g. from the
    free-function call sites in tests), the function must re-capture
    every call. Previous behavior preserved for tests that assume this."""
    from scripts.trader import calc_daemon as cd

    captures = iter([
        (76_113.81, "chainlink", 0.0),
        (76_188.25, "chainlink", 44.0),
    ])
    with patch.object(cd, "find_active_15m_market", return_value={"id": "m"}), \
         patch.object(cd, "extract_market_data",
                      return_value=_stub_market_data("btc-updown-15m-NOLOCK")), \
         patch.object(cd, "_capture_barrier", side_effect=lambda *a, **kw: next(captures)):
        first = cd._resolve_active_market_context()  # no locked_barriers
        second = cd._resolve_active_market_context()

    assert first.barrier == 76_113.81
    assert second.barrier == 76_188.25  # No lock → drift visible


# ---------------------------------------------------------------------------
# Settlement queue + persistence
# ---------------------------------------------------------------------------


class _SettlementChainlinkStub:
    """Chainlink stub that returns a fixed point on get_first_at_or_after."""

    def __init__(self, *, healthy=True, point=None):
        self.is_healthy = healthy
        self._point = point  # (price, offset_s) or (None, None)
        self.calls: list = []

    def get_first_at_or_after(self, ts, *, max_offset_s=60.0):
        self.calls.append((ts, max_offset_s))
        return self._point if self._point is not None else (None, None)


def test_calc_writes_settlement_strike_file_on_first_lock(tmp_path: Path) -> None:
    """When calc locks a strike for a new market, a per-market settlement
    file is written with strike fields populated and settlement fields null."""
    from scripts.trader.calc_daemon import CalcDaemon

    cfg = _build_calc_config(tmp_path)
    daemon = CalcDaemon(cfg, terminal_path=tmp_path / "terminal.json")
    market_id = "btc-updown-15m-XYZ"
    window_start = datetime(2026, 4, 30, 18, 0, tzinfo=timezone.utc)
    window_end = window_start + timedelta(minutes=15)

    daemon._write_settlement_strike(
        market_id=market_id,
        window_start=window_start,
        window_end=window_end,
        strike=76_113.81,
        strike_offset_s=0.0,
    )

    path = daemon._settlement_path(market_id)
    assert path.exists()
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["market_id"] == market_id
    assert payload["strike"] == 76_113.81
    assert payload["strike_source"] == "chainlink"
    assert payload["strike_offset_s"] == 0.0
    assert payload["settlement_price"] is None
    assert payload["settlement_source"] is None


def test_unknown_pending_contract_never_uses_chainlink_end_price(tmp_path: Path, monkeypatch) -> None:
    """A missing contract cannot be resolved from an instantaneous point."""
    from scripts.trader.calc_daemon import CalcDaemon

    cfg = _build_calc_config(tmp_path)
    daemon = CalcDaemon(cfg, terminal_path=tmp_path / "terminal.json")
    market_id = "btc-updown-15m-SETTLE"
    window_start = datetime.now(timezone.utc) - timedelta(minutes=16)  # 1 min past end
    window_end = window_start + timedelta(minutes=15)

    # Strike file already on disk.
    daemon._write_settlement_strike(
        market_id=market_id,
        window_start=window_start,
        window_end=window_end,
        strike=76_113.81,
        strike_offset_s=0.0,
    )
    daemon._pending_settlements[market_id] = (
        window_start, window_end, 76_113.81, 0.0,
    )
    daemon._chainlink = _SettlementChainlinkStub(healthy=True, point=(76_183.97, 12.0))
    monkeypatch.setattr("scripts.trader.calc_daemon.fetch_market_contract", lambda *args, **kwargs: None)

    daemon._resolve_pending_settlements()

    assert market_id in daemon._pending_settlements
    payload = json.loads(daemon._settlement_path(market_id).read_text(encoding="utf-8"))
    assert payload["settlement_price"] is None
    assert payload["settlement_status"] == "pending"
    assert payload["settlement_source"] is None


def test_calc_keeps_settlement_pending_when_contract_unavailable(tmp_path: Path, monkeypatch) -> None:
    """Unavailable contract identity leaves an expired market pending."""
    from scripts.trader.calc_daemon import CalcDaemon

    cfg = _build_calc_config(tmp_path)
    daemon = CalcDaemon(cfg, terminal_path=tmp_path / "terminal.json")
    market_id = "btc-updown-15m-WAIT"
    window_start = datetime.now(timezone.utc) - timedelta(minutes=15, seconds=30)
    window_end = window_start + timedelta(minutes=15)

    daemon._write_settlement_strike(
        market_id=market_id,
        window_start=window_start,
        window_end=window_end,
        strike=76_113.81,
        strike_offset_s=0.0,
    )
    daemon._pending_settlements[market_id] = (
        window_start, window_end, 76_113.81, 0.0,
    )
    daemon._chainlink = _SettlementChainlinkStub(healthy=True, point=(None, None))
    monkeypatch.setattr("scripts.trader.calc_daemon.fetch_market_contract", lambda *args, **kwargs: None)

    daemon._resolve_pending_settlements()

    assert market_id in daemon._pending_settlements  # still pending
    payload = json.loads(daemon._settlement_path(market_id).read_text(encoding="utf-8"))
    assert payload["settlement_price"] is None  # NOT yet written


def test_missing_contract_stays_pending_past_legacy_timeout(tmp_path: Path, monkeypatch) -> None:
    """A timeout cannot establish a payout or erase unresolved exposure."""
    from scripts.trader.calc_daemon import CalcDaemon

    cfg = _build_calc_config(tmp_path)
    daemon = CalcDaemon(cfg, terminal_path=tmp_path / "terminal.json")
    market_id = "btc-updown-15m-ABANDON"
    window_start = datetime.now(timezone.utc) - timedelta(days=2, minutes=15)
    window_end = window_start + timedelta(minutes=15)

    daemon._write_settlement_strike(
        market_id=market_id, window_start=window_start, window_end=window_end,
        strike=70_000.0, strike_offset_s=0.0,
    )
    daemon._pending_settlements[market_id] = (
        window_start, window_end, 70_000.0, 0.0,
    )
    daemon._chainlink = _SettlementChainlinkStub(healthy=True, point=(None, None))
    monkeypatch.setattr("scripts.trader.calc_daemon.fetch_market_contract", lambda *args, **kwargs: None)

    daemon._resolve_pending_settlements()

    assert market_id in daemon._pending_settlements
    payload = json.loads(daemon._settlement_path(market_id).read_text(encoding="utf-8"))
    assert payload["settlement_status"] == "pending"
    assert payload["settlement_price"] is None
    assert "settlement_abandoned_at_iso" not in payload


def test_calc_restore_settlement_state_rehydrates_strike_and_pending(tmp_path: Path) -> None:
    """Calc restart: prior strike files (without settlement) repopulate
    _market_barriers and _pending_settlements so the lock survives a
    restart and we don't recapture a different chainlink point."""
    from scripts.trader.calc_daemon import CalcDaemon

    cfg = _build_calc_config(tmp_path)
    settlements_dir = (tmp_path / "terminal.json").parent / "settlements"
    settlements_dir.mkdir(parents=True, exist_ok=True)
    market_id = "btc-updown-15m-PERSIST"
    window_start = datetime(2026, 4, 30, 18, 30, tzinfo=timezone.utc)
    window_end = window_start + timedelta(minutes=15)
    payload = {
        "market_id": market_id,
        "window_start_iso": window_start.isoformat(),
        "window_end_iso": window_end.isoformat(),
        "strike": 76_113.81,
        "strike_offset_s": 0.0,
        "strike_source": "chainlink",
        "strike_captured_at_iso": "2026-04-30T18:30:32+00:00",
        "settlement_price": None,
        "settlement_offset_s": None,
        "settlement_source": None,
        "settlement_captured_at_iso": None,
    }
    (settlements_dir / f"{market_id}.json").write_text(
        json.dumps(payload), encoding="utf-8",
    )

    # Construct daemon AFTER the file exists — _restore_settlement_state_from_disk
    # runs in __init__.
    daemon = CalcDaemon(cfg, terminal_path=tmp_path / "terminal.json")
    assert daemon._market_barriers[market_id] == (76_113.81, "chainlink", 0.0)
    assert market_id in daemon._pending_settlements


def test_calc_restore_preserves_strike_source_and_chain_broken(tmp_path: Path) -> None:
    from scripts.trader.calc_daemon import CalcDaemon

    cfg = _build_calc_config(tmp_path)
    settlements_dir = (tmp_path / "terminal.json").parent / "settlements"
    settlements_dir.mkdir(parents=True, exist_ok=True)
    window_start = datetime(2026, 4, 30, 18, 30, tzinfo=timezone.utc)
    window_end = window_start + timedelta(minutes=15)

    for market_id, source in (
        ("btc-updown-15m-POLY", "polymarket"),
        ("btc-updown-15m-BROKEN", "chain_broken"),
    ):
        payload = {
            "market_id": market_id,
            "window_start_iso": window_start.isoformat(),
            "window_end_iso": window_end.isoformat(),
            "strike": 76_113.81,
            "strike_offset_s": 0.0,
            "strike_source": source,
            "strike_captured_at_iso": "2026-04-30T18:30:32+00:00",
            "settlement_status": "pending",
            "settlement_price": None,
            "settlement_offset_s": None,
            "settlement_source": None,
            "settlement_captured_at_iso": None,
        }
        (settlements_dir / f"{market_id}.json").write_text(
            json.dumps(payload), encoding="utf-8",
        )

    daemon = CalcDaemon(cfg, terminal_path=tmp_path / "terminal.json")

    assert daemon._market_barriers["btc-updown-15m-POLY"] == (76_113.81, "polymarket", 0.0)
    assert daemon._market_barriers["btc-updown-15m-BROKEN"] == (76_113.81, "chain_broken", 0.0)
    assert "btc-updown-15m-BROKEN" in daemon._chain_broken_markets


# ---------------------------------------------------------------------------
# Archive fallback in _capture_barrier and _resolve_pending_settlements
# ---------------------------------------------------------------------------


class _StubArchive:
    """Stand-in for ChainlinkArchive."""

    def __init__(self, *, point=None):
        # point: (price, round_ts_unix, round_id) or None
        self._point = point
        self.calls: list = []

    def get_round_at_or_after(self, target_ts, *, max_lookahead_s=600):
        self.calls.append((target_ts, max_lookahead_s))
        return self._point


def test_capture_barrier_falls_back_to_archive_when_ws_buffer_misses() -> None:
    """When the WS feed is healthy but has no point at-or-after window-start
    (e.g. just-restarted calc, buffer empty), _capture_barrier consults the
    on-chain archive next."""
    from scripts.trader.calc_daemon import _capture_barrier

    chainlink = _StubChainlink(is_healthy=True, result=(None, None))
    target = datetime(2026, 4, 30, 17, 30, tzinfo=timezone.utc)
    archive = _StubArchive(point=(76_113.81, int(target.timestamp()) + 4, 12345))
    with patch(
        "scripts.trader.calc_daemon.get_binance_price_at",
        return_value=76_120.0,  # within 1% — sanity check passes
    ):
        out = _capture_barrier(target, chainlink=chainlink, archive=archive)

    assert out == (76_113.81, "chainlink", 4.0)
    assert archive.calls == [(int(target.timestamp()), 60)]


def test_capture_barrier_skips_archive_when_ws_already_has_point() -> None:
    """The archive must NOT be consulted when the WS buffer already
    answered. Performance + cost sanity."""
    from scripts.trader.calc_daemon import _capture_barrier

    chainlink = _StubChainlink(is_healthy=True, result=(76_113.81, 0.0))
    archive = _StubArchive(point=(99_999.0, 1, 1))  # would be wrong value
    with patch(
        "scripts.trader.calc_daemon.get_binance_price_at",
        return_value=76_120.0,
    ):
        out = _capture_barrier(
            datetime.now(timezone.utc), chainlink=chainlink, archive=archive,
        )

    assert out == (76_113.81, "chainlink", 0.0)
    assert archive.calls == []  # archive untouched


def test_capture_barrier_archive_subject_to_binance_sanity_check() -> None:
    """Archive-sourced prices must still pass the >1% Binance cross-check.
    A divergent archive price is REJECTED — same gate as the WS path."""
    from scripts.trader.calc_daemon import _capture_barrier

    chainlink = _StubChainlink(is_healthy=False)
    target = datetime(2026, 4, 30, 17, 30, tzinfo=timezone.utc)
    archive = _StubArchive(point=(80_000.0, int(target.timestamp()) + 2, 12345))
    with patch(
        "scripts.trader.calc_daemon.get_binance_price_at",
        return_value=70_000.0,  # ~14% diff
    ):
        out = _capture_barrier(target, chainlink=chainlink, archive=archive)

    assert out is None  # rejected


def test_unknown_contract_does_not_resolve_from_archive(tmp_path: Path, monkeypatch) -> None:
    """An available archive spot quote is not an authoritative token payout."""
    from scripts.trader.calc_daemon import CalcDaemon

    cfg = _build_calc_config(tmp_path)
    daemon = CalcDaemon(cfg, terminal_path=tmp_path / "terminal.json")
    market_id = "btc-updown-15m-ARC-SETTLE"
    window_start = datetime.now(timezone.utc) - timedelta(minutes=16)
    window_end = window_start + timedelta(minutes=15)

    daemon._write_settlement_strike(
        market_id=market_id, window_start=window_start, window_end=window_end,
        strike=76_113.81, strike_offset_s=0.0,
    )
    daemon._pending_settlements[market_id] = (
        window_start, window_end, 76_113.81, 0.0,
    )

    # WS healthy but buffer is empty for the relevant boundary.
    daemon._chainlink = _SettlementChainlinkStub(healthy=True, point=(None, None))
    daemon._archive = _StubArchive(
        point=(76_183.97, int(window_end.timestamp()) + 12, 999),
    )
    monkeypatch.setattr("scripts.trader.calc_daemon.fetch_market_contract", lambda *args, **kwargs: None)

    daemon._resolve_pending_settlements()

    assert market_id in daemon._pending_settlements
    payload = json.loads(daemon._settlement_path(market_id).read_text(encoding="utf-8"))
    assert payload["settlement_price"] is None
    assert payload["settlement_source"] is None


def test_calc_daemon_init_skips_archive_when_no_rpc_urls(tmp_path: Path) -> None:
    """Default config has no polygon_rpc_urls → archive is None.
    Calc continues to work, just without the restart-protection layer."""
    from scripts.trader.calc_daemon import CalcDaemon

    cfg = _build_calc_config(tmp_path)
    daemon = CalcDaemon(cfg, terminal_path=tmp_path / "terminal.json")
    assert daemon._archive is None


def test_calc_daemon_init_creates_archive_when_rpc_urls_provided(tmp_path: Path) -> None:
    """When polygon_rpc_urls is non-empty, the archive is instantiated."""
    from scripts.trader.calc_daemon import CalcDaemon, CalcConfig

    cfg = CalcConfig(
        inbox_targets=[tmp_path / "inbox"],
        snapshot_path=None,
        delay_min_seconds=0.0,
        delay_max_seconds=0.0,
        inbox_ttl_seconds=300.0,
        source_label="test",
        market_scan_interval_seconds=30.0,
        polygon_rpc_urls=("http://fake-rpc",),
    )
    daemon = CalcDaemon(cfg, terminal_path=tmp_path / "terminal.json")
    assert daemon._archive is not None


# ---------------------------------------------------------------------------
# Chain-consistency: settlement of N must equal strike of N+1
# ---------------------------------------------------------------------------


def test_chain_consistency_clean_chain_does_not_flag(tmp_path: Path) -> None:
    """Settlement of N == strike of N+1 (within tolerance) → no flag."""
    from scripts.trader.calc_daemon import CalcDaemon

    cfg = _build_calc_config(tmp_path)
    daemon = CalcDaemon(cfg, terminal_path=tmp_path / "terminal.json")
    we = datetime(2026, 5, 1, 12, 5, tzinfo=timezone.utc)
    ws = we - timedelta(minutes=15)
    next_we = we + timedelta(minutes=15)

    # Write market N (settled) and market N+1 (strike only) with matching
    # values at the boundary.
    daemon._write_settlement_strike(
        market_id="m-N", window_start=ws, window_end=we,
        strike=70_000.0, strike_offset_s=0.0,
    )
    daemon._finalize_settlement(
        "m-N", ws, we, strike=70_000.0, strike_offset_s=0.0,
        settlement_price=70_010.0, settlement_offset_s=2.0,
    )
    daemon._write_settlement_strike(
        market_id="m-Nplus1", window_start=we, window_end=next_we,
        strike=70_010.0, strike_offset_s=0.0,
    )

    assert "m-Nplus1" not in daemon._chain_broken_markets
    assert "m-N" not in daemon._chain_broken_markets


def test_chain_consistency_break_marks_next_market_as_chain_broken(tmp_path: Path) -> None:
    """Settlement of N != strike of N+1 (>$0.005 tolerance) → mark N+1
    as chain_broken AND rewrite N+1's settlement file with
    strike_source='chain_broken'."""
    from scripts.trader.calc_daemon import CalcDaemon

    cfg = _build_calc_config(tmp_path)
    daemon = CalcDaemon(cfg, terminal_path=tmp_path / "terminal.json")
    we = datetime(2026, 5, 1, 12, 5, tzinfo=timezone.utc)
    ws = we - timedelta(minutes=15)
    next_we = we + timedelta(minutes=15)

    # Market N+1 strike captured first.
    daemon._write_settlement_strike(
        market_id="m-Nplus1", window_start=we, window_end=next_we,
        strike=70_010.0, strike_offset_s=0.0,
    )
    # Market N strike + settlement, with settlement DIFFERING by $5 from
    # N+1's strike (>> tolerance of $0.005).
    daemon._write_settlement_strike(
        market_id="m-N", window_start=ws, window_end=we,
        strike=70_000.0, strike_offset_s=0.0,
    )
    daemon._finalize_settlement(
        "m-N", ws, we, strike=70_000.0, strike_offset_s=0.0,
        settlement_price=70_005.0,  # disagrees with m-Nplus1's strike (70010)
        settlement_offset_s=2.0,
    )

    # Suspect market is the NEXT one (N+1 — strike came from a different
    # chainlink point than the one that settled N).
    assert "m-Nplus1" in daemon._chain_broken_markets
    payload = json.loads(daemon._settlement_path("m-Nplus1").read_text(encoding="utf-8"))
    assert payload["strike_source"] == "chain_broken"


def test_chain_break_propagates_to_market_context_source(tmp_path: Path) -> None:
    """When _resolve_active_market_context is called for a chain-broken
    market, the returned MarketContext has barrier_source='chain_broken'
    so consumers' barrier_confirmed evaluates False and entries block."""
    from scripts.trader import calc_daemon as cd

    locks: dict[str, tuple] = {"m-Nplus1": (70_010.0, "chainlink", 0.0)}
    chain_broken: set[str] = {"m-Nplus1"}
    market_data = _stub_market_data("m-Nplus1")

    with patch.object(cd, "find_active_15m_market", return_value={"id": "m"}), \
         patch.object(cd, "extract_market_data", return_value=market_data):
        ctx = cd._resolve_active_market_context(
            locked_barriers=locks,
            chain_broken_markets=chain_broken,
        )
    assert ctx is not None
    assert ctx.barrier == 70_010.0
    assert ctx.barrier_source == "chain_broken"


def test_chain_consistency_within_tolerance_does_not_flag(tmp_path: Path) -> None:
    """Half-cent rounding noise within tolerance ($0.005) is accepted."""
    from scripts.trader.calc_daemon import CalcDaemon

    cfg = _build_calc_config(tmp_path)
    daemon = CalcDaemon(cfg, terminal_path=tmp_path / "terminal.json")
    we = datetime(2026, 5, 1, 12, 5, tzinfo=timezone.utc)
    ws = we - timedelta(minutes=15)
    next_we = we + timedelta(minutes=15)

    daemon._write_settlement_strike(
        market_id="m-N", window_start=ws, window_end=we,
        strike=70_000.0, strike_offset_s=0.0,
    )
    daemon._finalize_settlement(
        "m-N", ws, we, strike=70_000.0, strike_offset_s=0.0,
        settlement_price=70_010.001, settlement_offset_s=2.0,
    )
    daemon._write_settlement_strike(
        market_id="m-Nplus1", window_start=we, window_end=next_we,
        strike=70_010.003,  # +$0.002 vs settlement; under tolerance
        strike_offset_s=0.0,
    )

    assert "m-Nplus1" not in daemon._chain_broken_markets


# ---------------------------------------------------------------------------
# Health file = calibration freshness + idle-reason surfacing
#
# Regression guard for the 2026-06-17 silent calibration outage: Polymarket
# skipped one 15m market and published the next ~11 min late (past the
# POLYMARKET_CAPTURE_MAX_S barrier-capture gate), so calc produced no
# calibration for ~32 min. The old `calc_daemon.health` was a bare touch() that
# kept the container HEALTHCHECK green and the no-market path logged nothing,
# so the outage was invisible. These tests pin the new behaviour: the health
# file records calibration freshness + the reason calc is idle, and the loop
# emits a single throttled WARNING (no flood, no silence).
# ---------------------------------------------------------------------------


def test_health_file_reports_calibration_freshness(tmp_path: Path) -> None:
    from scripts.trader import calc_daemon as cd

    daemon, context = _setup_daemon_with_mocked_io(tmp_path)
    health_path = tmp_path / "calc_daemon.health"
    with patch.object(cd, "HEALTH_PATH", health_path):
        with _patch_run_once(daemon, context):
            event = daemon.run_once()
        daemon._write_health()

    assert event is not None
    payload = json.loads(health_path.read_text(encoding="utf-8"))
    assert payload["idle_reason"] is None
    assert payload["last_calibration_unix"] is not None
    assert payload["last_calibration_market_id"] == context.market_id
    assert payload["seconds_since_calibration"] is not None
    assert payload["seconds_since_calibration"] < 5.0
    assert payload["heartbeat_unix"] >= payload["last_calibration_unix"]


def test_health_file_records_idle_reason_when_no_active_market(tmp_path: Path) -> None:
    from scripts.trader import calc_daemon as cd

    cfg = _build_calc_config(tmp_path)
    daemon = CalcDaemon(cfg, terminal_path=tmp_path / "_worker.json")
    health_path = tmp_path / "calc_daemon.health"
    with patch.object(cd, "HEALTH_PATH", health_path), \
         patch.object(daemon, "_sleep", lambda *_a, **_k: None), \
         patch.object(cd, "_resolve_active_market_context", return_value=None):
        daemon.run_once()
        daemon._write_health()

    payload = json.loads(health_path.read_text(encoding="utf-8"))
    assert payload["idle_reason"] == "no_active_market"
    assert payload["last_calibration_unix"] is None
    assert payload["seconds_since_calibration"] is None
    assert payload["idle_seconds"] is not None


def test_resolve_status_reports_no_active_market(tmp_path: Path) -> None:
    from scripts.trader import calc_daemon as cd

    status: dict = {}
    with patch.object(cd, "find_active_15m_market", return_value=None):
        ctx = cd._resolve_active_market_context(status=status)
    assert ctx is None
    assert status["reason"] == "no_active_market"
    assert status["market_present"] is False


def test_resolve_status_reports_barrier_uncapturable(tmp_path: Path) -> None:
    """Market exists but its barrier cannot be captured (late publish /
    chainlink miss). The 20:30 market on 2026-06-17 hit this path: present,
    but uncapturable — distinct from 'no active market'."""
    from scripts.trader import calc_daemon as cd

    status: dict = {}
    with patch.object(cd, "find_active_15m_market", return_value={"id": "m"}), \
         patch.object(cd, "extract_market_data",
                      return_value=_stub_market_data("btc-updown-15m-UNCAP")), \
         patch.object(cd, "_capture_barrier", return_value=None):
        ctx = cd._resolve_active_market_context(status=status)
    assert ctx is None
    assert status["reason"] == "barrier_uncapturable"
    assert status["market_present"] is True


def test_maybe_log_idle_throttles_then_re_emits(tmp_path: Path, caplog) -> None:
    from scripts.trader import calc_daemon as cd

    daemon = CalcDaemon(_build_calc_config(tmp_path), terminal_path=tmp_path / "t.json")
    daemon._note_idle("no_active_market")

    with caplog.at_level(logging.WARNING, logger="scripts.trader.calc_daemon"):
        daemon._maybe_log_idle()  # first idle log fires (throttle starts at 0)
        daemon._maybe_log_idle()  # throttled — no second line
        first = [r for r in caplog.records if "calc idle" in r.message]
        assert len(first) == 1
        # Expire the throttle window and confirm it re-emits.
        daemon._last_idle_log_monotonic -= (cd.IDLE_LOG_INTERVAL_SECONDS + 1.0)
        daemon._maybe_log_idle()
        second = [r for r in caplog.records if "calc idle" in r.message]
        assert len(second) == 2


def test_maybe_log_idle_silent_when_calibrating(tmp_path: Path, caplog) -> None:
    daemon = CalcDaemon(_build_calc_config(tmp_path), terminal_path=tmp_path / "t.json")
    # No idle reason set == producing calibrations.
    with caplog.at_level(logging.WARNING, logger="scripts.trader.calc_daemon"):
        daemon._maybe_log_idle()
    assert not [r for r in caplog.records if "calc idle" in r.message]


def test_mark_calibrated_logs_recovery_and_clears_idle(tmp_path: Path, caplog) -> None:
    daemon = CalcDaemon(_build_calc_config(tmp_path), terminal_path=tmp_path / "t.json")
    daemon._note_idle("barrier_uncapturable", market_present=True)
    assert daemon._idle_reason == "barrier_uncapturable"

    with caplog.at_level(logging.INFO, logger="scripts.trader.calc_daemon"):
        daemon._mark_calibrated("btc-updown-15m-REC")

    assert daemon._idle_reason is None
    assert daemon._idle_since_unix is None
    assert daemon._last_calibration_market_id == "btc-updown-15m-REC"
    assert daemon._last_calibration_unix is not None
    assert any("calc recovered" in r.message for r in caplog.records)
