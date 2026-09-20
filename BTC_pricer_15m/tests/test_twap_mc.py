"""Independent stochastic-integral anchors for the TWAP payoff approximation."""
import math

import numpy as np
import pytest
from scipy.stats import norm

from btc_pricer.constants import DAYS_PER_YEAR
from btc_pricer.models.ssvi import SSVIParams
from btc_pricer.models.terminal_probability import TerminalProbabilityCalculator

YEAR_SECONDS = 86400.0 * DAYS_PER_YEAR


def flat_params(horizon=300.0, sigma=0.2):
    ttm = horizon / YEAR_SECONDS
    return SSVIParams(theta=sigma * sigma * ttm, rho=0.0, phi=1e-9, ttm=ttm)


@pytest.mark.parametrize("horizon,window", [(300.0, 60.0), (60.0, 60.0)])
def test_low_vol_average_matches_linear_gaussian_integral(horizon, window):
    # For dS=sigma*S0*dW and a future final-w average, Var(A)=
    # (sigma*S0)^2*(h-2*w/3). This covariance integral is independent of MC.
    spot, sigma = 80_000.0, 0.2
    calc = TerminalProbabilityCalculator(n_simulations=120_000, seed=71)
    samples = calc.simulate_twap_prices_compact(flat_params(horizon, sigma), spot, spot,
                                               averaging_window_seconds=window)
    variance = (spot * sigma) ** 2 * (horizon - 2 * window / 3) / YEAR_SECONDS
    assert samples.var(ddof=1) == pytest.approx(variance, rel=0.025)
    assert samples.mean() == pytest.approx(spot, abs=0.02)
    barrier = spot + 0.4 * math.sqrt(variance)
    probability, se = calc._calculate_terminal_probability(samples, barrier, "up")
    assert abs(probability - norm.cdf(-0.4)) < 5 * se + 2e-4


def test_vanishing_window_matches_black_scholes_terminal_limit():
    spot, sigma, horizon = 80_000.0, 0.2, 300.0
    params = flat_params(horizon, sigma)
    barrier = spot * math.exp(0.3 * math.sqrt(params.theta))
    calc = TerminalProbabilityCalculator(n_simulations=120_000, seed=81)
    samples = calc.simulate_twap_prices_compact(params, spot, spot, averaging_window_seconds=0.001)
    probability, se = calc._calculate_terminal_probability(samples, barrier, "up")
    expected = norm.cdf(math.log(spot / barrier) / math.sqrt(params.theta) - 0.5 * math.sqrt(params.theta))
    assert abs(probability - expected) < 5 * se + 1e-4


def test_production_smile_time_refinement_and_path_uncertainty():
    params = SSVIParams(theta=5.88e-7, rho=0.03, phi=1237.0, ttm=300 / YEAR_SECONDS)
    estimates = []
    for step, seed in ((5.0, 101), (2.5, 202), (1.25, 303)):
        calc = TerminalProbabilityCalculator(n_simulations=160_000, seed=seed)
        samples = calc.simulate_twap_prices_compact(params, 80_000.0, 80_000.0, max_step_seconds=step)
        estimates.append(calc._calculate_terminal_probability(samples, 80_030.0, "up"))
    for (coarse, se_c), (fine, se_f) in zip(estimates, estimates[1:]):
        assert abs(coarse - fine) < 5 * math.hypot(se_c, se_f)


def test_pair_average_uncertainty_and_probability_complements():
    calc = TerminalProbabilityCalculator(n_simulations=4, use_antithetic=True)
    paired = np.array([0.9, 1.1, 1.1, 0.9])
    p, se = calc._calculate_terminal_probability(paired, 1.0, "up")
    assert p == 0.5
    assert se == 0.0  # each independent pair contributes exactly 1/2
    assert calc._calculate_terminal_probability(paired, 1.0, "down")[0] == 1 - p


def test_missing_historical_window_and_nonfinite_inputs_are_rejected():
    calc = TerminalProbabilityCalculator(n_simulations=100)
    with pytest.raises(ValueError, match="historical"):
        calc.simulate_twap_prices_compact(flat_params(30), 80_000, 80_000)
    with pytest.raises(ValueError, match="finite"):
        calc.simulate_twap_prices_compact(flat_params(), float("nan"), 80_000)


def test_constant_price_is_preserved_and_equality_resolves_up():
    params = SSVIParams(theta=1e-40, rho=0.0, phi=1e-9, ttm=300/YEAR_SECONDS)
    calc = TerminalProbabilityCalculator(n_simulations=100, seed=1)
    averages = calc.simulate_twap_prices_compact(params, 80_000, 80_000)
    assert np.all(averages == 80_000)
    assert calc._calculate_terminal_probability(averages, 80_000, "up") == (1.0, 0.0)
    assert calc._calculate_terminal_probability(averages, 80_000, "down") == (0.0, 0.0)


def test_numpy_integral_path_is_numerically_consistent(monkeypatch):
    import btc_pricer.models.terminal_probability as module
    monkeypatch.setattr(module, "NUMBA_AVAILABLE", False)
    spot, sigma, h = 80_000.0, 0.2, 300.0
    calc = TerminalProbabilityCalculator(n_simulations=60_000, seed=105)
    samples = calc.simulate_twap_prices_compact(flat_params(h, sigma), spot, spot)
    expected = (spot * sigma) ** 2 * (h - 40) / YEAR_SECONDS
    assert samples.var() == pytest.approx(expected, rel=0.035)
