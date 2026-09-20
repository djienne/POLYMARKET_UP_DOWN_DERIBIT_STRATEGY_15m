"""Tests for the closed-form smile digital (btc_pricer/models/digital.py).

The cf digital is a DIAGNOSTIC in production (probability_mode stays "mc"):
at production micro-TTM phi (400-1400) the static -dC/dK and the
sticky-moneyness MC diverge by 1.0-2.5pp near ATM — they are different
models. These tests therefore verify the cf math against an independent
finite-difference of Black prices (exactness of the calculus), its limits,
and its wiring, NOT equivalence with the MC.
"""

import math

import numpy as np
import pytest

from btc_pricer.models.digital import (
    ssvi_digital_prob_above,
    ssvi_digital_prob_pair,
    ssvi_dw_dk,
    ssvi_total_variance,
)
from btc_pricer.models.ssvi import SSVIParams

DAYS_PER_YEAR = 365.25
TTM_15M = 15.0 / (24 * 60) / DAYS_PER_YEAR


def _black_call(center: float, strike: float, total_var: float) -> float:
    """Undiscounted Black call with distribution center ``center``."""
    if total_var <= 0:
        return max(0.0, center - strike)
    s = math.sqrt(total_var)
    d1 = math.log(center / strike) / s + 0.5 * s
    d2 = d1 - s
    n = lambda x: 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))
    return center * n(d1) - strike * n(d2)


def _fd_digital(params: SSVIParams, forward: float, strike: float, spot: float, eps: float = 0.01) -> float:
    """Finite-difference -dC/dK with the smile's strike dependence."""
    def call_at(k_strike: float) -> float:
        k = math.log(k_strike / forward)
        w = ssvi_total_variance(params, k)
        return _black_call(spot, k_strike, w)

    return -(call_at(strike + eps) - call_at(strike - eps)) / (2 * eps)


PROD_PARAMS = SSVIParams(theta=1.7444e-06, rho=-0.26623, phi=468.77, ttm=11.4 / (24 * 60) / DAYS_PER_YEAR)


class TestClosedFormMatchesFiniteDifference:
    """The analytic formula must equal -dC/dK of Black-with-smile exactly."""

    @pytest.mark.parametrize("off_pct", [-0.2, -0.05, 0.0, 0.05, 0.2])
    def test_production_params(self, off_pct):
        spot = 63291.51
        strike = spot * (1 + off_pct / 100)
        p_cf = ssvi_digital_prob_above(PROD_PARAMS, spot, strike, spot=spot)
        p_fd = _fd_digital(PROD_PARAMS, spot, strike, spot)
        assert p_cf == pytest.approx(p_fd, abs=5e-6)

    def test_spot_off_forward(self):
        spot = 63291.51
        forward = spot * 1.0003
        strike = spot * 1.0005
        p_cf = ssvi_digital_prob_above(PROD_PARAMS, forward, strike, spot=spot)
        p_fd = _fd_digital(PROD_PARAMS, forward, strike, spot)
        assert p_cf == pytest.approx(p_fd, abs=5e-6)

    def test_daily_scale_params(self):
        params = SSVIParams(theta=0.45 ** 2 / DAYS_PER_YEAR, rho=-0.2, phi=2.0, ttm=1.0 / DAYS_PER_YEAR)
        spot = 100_000.0
        for strike in (98_000.0, 100_000.0, 102_000.0):
            p_cf = ssvi_digital_prob_above(params, spot, strike, spot=spot)
            p_fd = _fd_digital(params, spot, strike, spot, eps=0.5)
            assert p_cf == pytest.approx(p_fd, abs=1e-6)


class TestDwDk:
    def test_matches_numeric_derivative(self):
        eps = 1e-7
        for k in (-0.002, 0.0, 0.0015):
            numeric = (
                ssvi_total_variance(PROD_PARAMS, k + eps)
                - ssvi_total_variance(PROD_PARAMS, k - eps)
            ) / (2 * eps)
            assert ssvi_dw_dk(PROD_PARAMS, k) == pytest.approx(numeric, rel=1e-5)


class TestLimitsAndSanity:
    def test_pair_sums_to_one(self):
        above, below = ssvi_digital_prob_pair(PROD_PARAMS, 63291.51, 63308.08, spot=63291.51)
        assert above + below == pytest.approx(1.0)
        assert 0.0 <= above <= 1.0

    def test_monotone_in_strike(self):
        spot = 63291.51
        strikes = [spot * (1 + o / 100) for o in np.linspace(-0.4, 0.4, 41)]
        probs = [ssvi_digital_prob_above(PROD_PARAMS, spot, k, spot=spot) for k in strikes]
        diffs = np.diff(probs)
        # Butterfly-satisfying smile => valid CDF => monotone decreasing.
        assert (diffs <= 1e-12).all()

    def test_degenerate_ttm_is_indicator(self):
        params = SSVIParams(theta=1e-30, rho=-0.2, phi=100.0, ttm=1e-12)
        spot = 63000.0
        assert ssvi_digital_prob_above(params, spot, spot - 10, spot=spot) == 1.0
        assert ssvi_digital_prob_above(params, spot, spot + 10, spot=spot) == 0.0

    def test_deep_strikes_saturate(self):
        spot = 63291.51
        assert ssvi_digital_prob_above(PROD_PARAMS, spot, spot * 0.97, spot=spot) > 0.999
        assert ssvi_digital_prob_above(PROD_PARAMS, spot, spot * 1.03, spot=spot) < 0.001

    def test_rejects_nonpositive_inputs(self):
        with pytest.raises(ValueError):
            ssvi_digital_prob_above(PROD_PARAMS, -1.0, 63000.0)
        with pytest.raises(ValueError):
            ssvi_digital_prob_above(PROD_PARAMS, 63000.0, 0.0)


class TestCfModeWiring:
    def test_cf_mode_selects_closed_form(self):
        from cli_terminal import build_json_output

        output = build_json_output(
            None,
            100_000.0,
            101_000.0,
            "up",
            1.0 / 365.0,
            surface_mc_above=0.60,
            surface_mc_below=0.40,
            surface_cf_above=0.63,
            surface_cf_below=0.37,
            trading_model="ssvi_surface",
            probability_mode="cf",
        )
        assert output["preferred_model"] == "ssvi_surface"
        assert output["probability_source_label"] == "SSVI Surface CF digital"
        assert output["selected_prob_above"] == pytest.approx(0.63)
        assert output["ssvi_surface"]["cf_prob_above"] == pytest.approx(0.63)
        assert output["cf_mc_divergence"] == pytest.approx(0.03)

    def test_cf_mode_falls_back_to_mc_when_cf_missing(self):
        from cli_terminal import build_json_output

        output = build_json_output(
            None,
            100_000.0,
            101_000.0,
            "up",
            1.0 / 365.0,
            surface_mc_above=0.60,
            surface_mc_below=0.40,
            trading_model="ssvi_surface",
            probability_mode="cf",
        )
        assert output["probability_source_label"] == "SSVI Surface MC"
        assert output["selected_prob_above"] == pytest.approx(0.60)

    def test_mc_mode_records_cf_diagnostic_without_selecting_it(self):
        from cli_terminal import build_json_output

        output = build_json_output(
            None,
            100_000.0,
            101_000.0,
            "up",
            1.0 / 365.0,
            surface_mc_above=0.60,
            surface_mc_below=0.40,
            surface_cf_above=0.62,
            surface_cf_below=0.38,
            trading_model="ssvi_surface",
            probability_mode="mc",
        )
        assert output["probability_source_label"] == "SSVI Surface MC"
        assert output["selected_prob_above"] == pytest.approx(0.60)
        assert output["ssvi_surface"]["cf_prob_above"] == pytest.approx(0.62)
        assert output["cf_mc_divergence"] == pytest.approx(0.02)

    def test_cf_aliases_normalize(self):
        from cli_terminal import normalize_probability_mode

        assert normalize_probability_mode("cf") == "cf"
        assert normalize_probability_mode("closed_form") == "cf"
        assert normalize_probability_mode("digital") == "cf"
