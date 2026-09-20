"""Legacy terminal-payoff tests: local probability re-evaluation from the event's MC
terminal-return distribution (schema v4)."""

from datetime import datetime, timedelta, timezone

import numpy as np
import pytest

from scripts.trader.hybrid_calibration import CalibrationEvent
from scripts.trader.local_eval import (
    LocalProb,
    compute_entry_signals,
    evaluate_prob_from_event,
)

SPOT_CALC = 63291.51
BARRIER = 63308.08
READY_AT = datetime(2026, 7, 10, 12, 1, 0, tzinfo=timezone.utc)
MARKET_END = datetime(2026, 7, 10, 12, 12, 24, tzinfo=timezone.utc)
TTM_DAYS = (MARKET_END - READY_AT).total_seconds() / 86400.0

_QUANTILE_PROBS = np.linspace(0.0, 1.0, 1001)


def _mc_sample(seed=7, n=400_000, vol_15m=0.0024):
    rng = np.random.default_rng(seed)
    return rng.standard_normal(n) * vol_15m - 0.5 * vol_15m ** 2


def _make_event(returns=None, **overrides):
    quantiles = None
    prob_above = 0.435
    if returns is not None:
        quantiles = tuple(
            round(float(q), 8) for q in np.quantile(returns, _QUANTILE_PROBS)
        )
        prob_above = float(np.mean(SPOT_CALC * np.exp(returns) >= BARRIER))
    base = dict(
        schema_version=4,
        event_id="1783638000000-test",
        source="shared_calc",
        market_id="btc-updown-15m-1783638000",
        barrier=BARRIER,
        market_end_time_iso=MARKET_END.isoformat(),
        ready_at_iso=READY_AT.isoformat(),
        ready_at_unix_ms=int(READY_AT.timestamp() * 1000),
        prob_above=prob_above,
        prob_below=1.0 - prob_above,
        model_used="ssvi_surface",
        calibration_seconds=1.0,
        terminal_payload={},
        spot_price=SPOT_CALC,
        terminal_return_quantiles=quantiles,
        terminal_return_ttm_days=TTM_DAYS,
        prob_model="mc",
    )
    base.update(overrides)
    return CalibrationEvent(**base)


class TestEvaluateProb:
    def test_round_trip_identity_at_calc_spot_and_time(self):
        returns = _mc_sample()
        event = _make_event(returns)
        local = evaluate_prob_from_event(event, spot=SPOT_CALC, now=READY_AT)
        assert local is not None
        # Quantile-grid interpolation error bound ~0.05pp + tie handling.
        assert local.prob_above == pytest.approx(event.prob_above, abs=2e-3)
        assert local.prob_above + local.prob_below == pytest.approx(1.0)

    def test_spot_move_shifts_probability_correctly(self):
        returns = _mc_sample()
        event = _make_event(returns)
        base = evaluate_prob_from_event(event, spot=SPOT_CALC, now=READY_AT)
        up = evaluate_prob_from_event(event, spot=SPOT_CALC * 1.001, now=READY_AT)
        down = evaluate_prob_from_event(event, spot=SPOT_CALC * 0.999, now=READY_AT)
        assert up.prob_above > base.prob_above
        assert down.prob_above < base.prob_above
        # A 0.1% spot move over an 11-minute window is a large probability move.
        assert up.prob_above - base.prob_above > 0.05

    def test_spot_move_matches_direct_mc_rethreshold(self):
        """Local eval must reproduce re-thresholding the raw MC sample."""
        returns = _mc_sample()
        event = _make_event(returns)
        for bump in (1.0005, 0.9992):
            spot_now = SPOT_CALC * bump
            local = evaluate_prob_from_event(event, spot=spot_now, now=READY_AT)
            direct = float(np.mean(spot_now * np.exp(returns) >= BARRIER))
            assert local.prob_above == pytest.approx(direct, abs=2e-3)

    def test_ttm_decay_sharpens_distribution(self):
        returns = _mc_sample()
        event = _make_event(returns)
        # Spot above barrier + less time left => probability of staying above rises.
        spot_above = BARRIER * 1.0005
        early = evaluate_prob_from_event(event, spot=spot_above, now=READY_AT)
        late = evaluate_prob_from_event(
            event, spot=spot_above, now=MARKET_END - timedelta(seconds=60)
        )
        assert late.ttm_scale < early.ttm_scale
        assert late.prob_above > early.prob_above

    def test_legacy_event_without_quantiles_returns_none(self):
        event = _make_event(returns=None, schema_version=3)
        assert evaluate_prob_from_event(event, spot=SPOT_CALC, now=READY_AT) is None

    def test_market_ended_returns_none(self):
        returns = _mc_sample()
        event = _make_event(returns)
        assert (
            evaluate_prob_from_event(
                event, spot=SPOT_CALC, now=MARKET_END + timedelta(seconds=1)
            )
            is None
        )

    def test_degenerate_inputs_return_none(self):
        returns = _mc_sample()
        event = _make_event(returns)
        assert evaluate_prob_from_event(event, spot=0.0, now=READY_AT) is None
        short = _make_event(returns)
        object.__setattr__(short, "terminal_return_quantiles", tuple(range(5)))
        assert evaluate_prob_from_event(short, spot=SPOT_CALC, now=READY_AT) is None


class TestSchemaV4:
    def test_v4_round_trip(self):
        returns = _mc_sample(n=50_000)
        event = _make_event(returns)
        raw = event.to_dict()
        parsed = CalibrationEvent.from_dict(raw)
        assert parsed.schema_version == 4
        assert parsed.terminal_return_quantiles == event.terminal_return_quantiles
        assert parsed.terminal_return_ttm_days == pytest.approx(TTM_DAYS)
        assert parsed.prob_model == "mc"

    def test_v3_event_parses_under_v4_consumer(self):
        event = _make_event(returns=None, schema_version=3)
        raw = event.to_dict()
        for key in ("terminal_return_quantiles", "terminal_return_ttm_days", "prob_model"):
            raw.pop(key, None)
        parsed = CalibrationEvent.from_dict(raw)
        assert parsed.terminal_return_quantiles is None
        assert parsed.prob_model == ""

    def test_v4_extras_ignored_by_field_reader(self):
        """A consumer that reads only known keys must tolerate v4 extras."""
        returns = _mc_sample(n=50_000)
        raw = _make_event(returns).to_dict()
        raw["some_future_field"] = {"x": 1}
        parsed = CalibrationEvent.from_dict(raw)
        assert parsed.prob_above == pytest.approx(raw["prob_above"])


class TestSignalDelegation:
    def test_calc_daemon_delegates_to_shared_implementation(self):
        from scripts.trader import calc_daemon, local_eval

        assert calc_daemon.compute_entry_signals is local_eval.compute_entry_signals
        assert calc_daemon.load_signal_params is local_eval.load_signal_params

    def test_compute_entry_signals_shape(self):
        params = [{
            "instance_id": 0,
            "alpha_up": 1.5,
            "alpha_down": 1.5,
            "floor_up": 0.55,
            "floor_down": 0.55,
            "max_entry_price": 0.90,
        }]
        signals = compute_entry_signals(0.80, 0.20, 0.60, 0.35, params, "mkt")
        assert len(signals) == 1
        sig = signals[0]
        assert sig["direction"] == "UP"
        assert sig["instance_id"] == 0
        assert sig["market_id"] == "mkt"
        assert sig["model_prob"] == pytest.approx(0.80)
