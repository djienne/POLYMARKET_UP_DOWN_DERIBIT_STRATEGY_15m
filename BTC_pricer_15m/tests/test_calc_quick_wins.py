"""Calibration input reuse, publication fallback and market-scan ordering.

A1: calibrate_to_expiry skips the blocking Binance fetch when the caller
    supplies the spot.
A4: serialize-once publishing + fallback-only file inbox.
A5: market-scan probe order tries the active window (offset 0) first.
"""

import json
from pathlib import Path
from types import SimpleNamespace

import pytest


# ---------------------------------------------------------------------------
# A1 — spot_override skips fetch_spot_with_fallback
# ---------------------------------------------------------------------------

class _Opt:
    def __init__(self, spot=63000.0, tte=2.0 / 365.25):
        self.spot_price = spot
        self.time_to_expiry = tte


def test_calibrate_to_expiry_skips_spot_fetch_with_override(monkeypatch):
    import btc_pricer.cli.common as common

    def _boom(*args, **kwargs):
        raise AssertionError("fetch_spot_with_fallback must not be called with spot_override")

    monkeypatch.setattr(common, "fetch_spot_with_fallback", _boom)

    # Minimal surface prep: one expiry that resolves cleanly.
    monkeypatch.setattr(
        common,
        "_prepare_expiry_surface",
        lambda expiry, options, data_filter, config, logger: (expiry, 63010.0, 2.0 / 365.25, None, None),
    )
    monkeypatch.setattr(common, "DataFilter", lambda filters: object())

    config = SimpleNamespace(
        filters=object(),
        model=SimpleNamespace(min_calibration_ttm_days=1.0),
    )
    client = SimpleNamespace()
    options_by_expiry = {"10JUL26": [_Opt()]}

    heston_data, ssvi_data, spot, expiry, forward, ttm = common.calibrate_to_expiry(
        client,
        config,
        return_both=True,
        skip_heston=True,
        options_by_expiry=options_by_expiry,
        spot_override=63123.45,
    )
    assert spot == pytest.approx(63123.45)
    assert heston_data is None
    assert forward == pytest.approx(63010.0)


def test_calibrate_to_expiry_fetches_spot_without_override(monkeypatch):
    import btc_pricer.cli.common as common

    calls = {"n": 0}

    def _fake_fetch(deribit_spot):
        calls["n"] += 1
        return 64000.0, "binance"

    monkeypatch.setattr(common, "fetch_spot_with_fallback", _fake_fetch)
    monkeypatch.setattr(
        common,
        "_prepare_expiry_surface",
        lambda expiry, options, data_filter, config, logger: (expiry, 63010.0, 2.0 / 365.25, None, None),
    )
    monkeypatch.setattr(common, "DataFilter", lambda filters: object())

    config = SimpleNamespace(
        filters=object(),
        model=SimpleNamespace(min_calibration_ttm_days=1.0),
    )
    result = common.calibrate_to_expiry(
        SimpleNamespace(),
        config,
        return_both=True,
        skip_heston=True,
        options_by_expiry={"10JUL26": [_Opt()]},
    )
    assert calls["n"] == 1
    assert result[2] == pytest.approx(64000.0)


# ---------------------------------------------------------------------------
# A4 — serialize once, file inbox only on Redis failure
# ---------------------------------------------------------------------------

def _make_event(**overrides):
    from scripts.trader.hybrid_calibration import CalibrationEvent

    base = dict(
        schema_version=3,
        event_id="1700000000000-abc",
        source="shared_calc",
        market_id="btc-updown-15m-1700000000",
        barrier=63000.0,
        market_end_time_iso="2026-07-10T12:15:00+00:00",
        ready_at_iso="2026-07-10T12:01:00+00:00",
        ready_at_unix_ms=1_700_000_000_000,
        prob_above=0.55,
        prob_below=0.45,
        model_used="ssvi_surface",
        calibration_seconds=1.0,
        terminal_payload={"timing": {"mc_s": 0.05}},
    )
    base.update(overrides)
    return CalibrationEvent(**base)


class _FailingPublisher:
    def __init__(self):
        self.calls = 0

    def publish(self, event, payload_json=None):
        self.calls += 1
        raise RuntimeError("redis down")


class _RecordingPublisher:
    def __init__(self):
        self.events = []
        self.payloads = []

    def publish(self, event, payload_json=None):
        self.events.append(event)
        self.payloads.append(payload_json)


def test_fallback_publisher_skips_fallback_on_primary_success():
    from scripts.trader.ipc import FallbackCalibrationPublisher

    primary = _RecordingPublisher()
    fallback = _RecordingPublisher()
    pub = FallbackCalibrationPublisher(primary, fallback)
    pub.publish(_make_event())
    assert len(primary.events) == 1
    assert len(fallback.events) == 0


def test_fallback_publisher_writes_fallback_on_primary_failure():
    from scripts.trader.ipc import FallbackCalibrationPublisher

    primary = _FailingPublisher()
    fallback = _RecordingPublisher()
    pub = FallbackCalibrationPublisher(primary, fallback)
    pub.publish(_make_event())
    assert primary.calls == 1
    assert len(fallback.events) == 1
    assert fallback.payloads[0] is not None


def test_file_publisher_writes_compact_payload(tmp_path):
    from scripts.trader.ipc import FileCalibrationPublisher, serialize_calibration_event
    from scripts.trader.hybrid_calibration import CalibrationEvent

    event = _make_event()
    target = tmp_path / "inbox"
    FileCalibrationPublisher((target,)).publish(event)

    out = target / f"{event.event_id}.json"
    raw = out.read_text(encoding="utf-8")
    # Compact (no indentation) and byte-identical to the canonical serialization.
    assert "\n  " not in raw
    assert raw == serialize_calibration_event(event)
    # Round-trips through the consumer path.
    parsed = CalibrationEvent.from_dict(json.loads(raw))
    assert parsed.prob_above == pytest.approx(0.55)


def test_build_publisher_returns_fallback_for_redis_with_file_fallback(monkeypatch, tmp_path):
    import scripts.trader.ipc as ipc

    class _FakeRedisPublisher:
        def __init__(self, **kwargs):
            pass

    monkeypatch.setattr(ipc, "RedisStreamCalibrationPublisher", _FakeRedisPublisher)
    pub = ipc.build_calibration_publisher(
        backend="redis",
        inbox_targets=[tmp_path / "inbox"],
        redis_url="redis://localhost:6379/0",
        file_fallback=True,
    )
    assert isinstance(pub, ipc.FallbackCalibrationPublisher)


# ---------------------------------------------------------------------------
# A5 — market-scan probe order
# ---------------------------------------------------------------------------

def test_find_active_market_probes_current_window_first(monkeypatch):
    import scripts.polymarket_btc_15m as pm

    probed = []

    def _fake_fetch(slug):
        probed.append(slug)
        return None

    monkeypatch.setattr(pm, "fetch_event_by_slug", _fake_fetch)
    assert pm.find_active_15m_market() is None
    assert len(probed) == 6

    from datetime import datetime, timezone

    base = pm._quarter_hour_floor(datetime.now(timezone.utc))
    assert probed[0] == pm.slug_for_timestamp(base)
