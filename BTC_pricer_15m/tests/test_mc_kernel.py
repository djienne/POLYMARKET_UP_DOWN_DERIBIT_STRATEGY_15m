"""Tests for the rewritten MC kernels (block seeding + true antithetic).

The old kernels seeded a Mersenne Twister once PER PATH (measured 2.6-3.2x
overhead) and the "antithetic" flag ran two independent halves (no variance
reduction). The rewrite seeds once per 256-path block and drives real
(+Z, -Z) pairs inside one kernel launch.
"""

import math

import numpy as np
import pytest

from btc_pricer.models.ssvi import SSVIParams
from btc_pricer.models.heston import HestonParams
from btc_pricer.models.terminal_probability import TerminalProbabilityCalculator

DAYS_PER_YEAR = 365.25
TTM_15M = 15.0 / (24 * 60) / DAYS_PER_YEAR

SSVI_PROD = SSVIParams(theta=1.7444e-06, rho=-0.26623, phi=468.77, ttm=TTM_15M)
SPOT = 63291.51
STRIKE = SPOT * 1.0003

N_PATHS = 40_000


def _calc(seed, antithetic=True, n=N_PATHS):
    return TerminalProbabilityCalculator(
        n_simulations=n, n_steps_per_day=1440, use_antithetic=antithetic, seed=seed
    )


class TestDeterminism:
    def test_same_seed_same_result(self):
        p1 = _calc(123).terminal_probability_ssvi(SSVI_PROD, SPOT, SPOT, STRIKE, "up")
        p2 = _calc(123).terminal_probability_ssvi(SSVI_PROD, SPOT, SPOT, STRIKE, "up")
        assert p1.terminal_probability == p2.terminal_probability

    def test_different_seed_different_result(self):
        p1 = _calc(123).terminal_probability_ssvi(SSVI_PROD, SPOT, SPOT, STRIKE, "up")
        p2 = _calc(456).terminal_probability_ssvi(SSVI_PROD, SPOT, SPOT, STRIKE, "up")
        assert p1.terminal_probability != p2.terminal_probability


class TestAntithetic:
    def test_path_count_preserved(self):
        res = _calc(1).terminal_probability_ssvi(SSVI_PROD, SPOT, SPOT, STRIKE, "up")
        assert res.n_simulations == N_PATHS

    def test_pairs_are_mirrored(self):
        """Leg j and leg half+j must be driven by +Z / -Z: their log-returns
        should be strongly anticorrelated (near -1 over a 15-min horizon)."""
        calc = _calc(7)
        final = calc.simulate_local_vol_paths_compact(
            SSVI_PROD, SPOT, SPOT, 20_000, 15, antithetic=True
        )
        half = len(final) // 2
        r_a = np.log(final[:half] / SPOT)
        r_b = np.log(final[half:] / SPOT)
        corr = np.corrcoef(r_a, r_b)[0, 1]
        assert corr < -0.99

    def test_variance_reduction_vs_binomial(self):
        """True antithetic must beat the plain binomial SE for a near-ATM digital."""
        np.random.seed(11)
        calc = TerminalProbabilityCalculator(
            n_simulations=30_000, n_steps_per_day=1440, use_antithetic=True
        )
        probs = np.array([
            calc.terminal_probability_ssvi(SSVI_PROD, SPOT, SPOT, STRIKE, "up").terminal_probability
            for _ in range(16)
        ])
        p = probs.mean()
        binom_se = math.sqrt(p * (1 - p) / 30_000)
        assert probs.std() < binom_se

    def test_antithetic_and_plain_agree_in_mean(self):
        np.random.seed(3)
        calc_a = TerminalProbabilityCalculator(
            n_simulations=400_000, n_steps_per_day=1440, use_antithetic=True
        )
        calc_p = TerminalProbabilityCalculator(
            n_simulations=400_000, n_steps_per_day=1440, use_antithetic=False
        )
        pa = calc_a.terminal_probability_ssvi(SSVI_PROD, SPOT, SPOT, STRIKE, "up").terminal_probability
        pp = calc_p.terminal_probability_ssvi(SSVI_PROD, SPOT, SPOT, STRIKE, "up").terminal_probability
        # 5 sigma of the plain run's SE
        se = math.sqrt(pp * (1 - pp) / 400_000)
        assert abs(pa - pp) < 5 * se


class TestHestonKernel:
    HESTON = HestonParams(v0=0.20, kappa=2.0, theta=0.20, xi=1.0, rho=-0.5, ttm=TTM_15M)

    def test_deterministic_and_paired(self):
        calc = _calc(21)
        final = calc.simulate_heston_paths_compact(self.HESTON, SPOT, 10_000, 15, antithetic=True)
        assert len(final) == 10_000
        half = len(final) // 2
        r_a = np.log(final[:half] / SPOT)
        r_b = np.log(final[half:] / SPOT)
        assert np.corrcoef(r_a, r_b)[0, 1] < -0.9

    def test_heston_mean_is_martingale(self):
        calc = _calc(22)
        final = calc.simulate_heston_paths_compact(self.HESTON, SPOT, 200_000, 15, antithetic=True)
        # E[S_T] = S_0 for the driftless log-Euler scheme, within MC noise.
        assert final.mean() == pytest.approx(SPOT, rel=2e-3)


class TestNumpyFallbackParity:
    """The no-numba fallback must implement the same estimator."""

    def test_ssvi_fallback_agrees(self, monkeypatch):
        import btc_pricer.models.terminal_probability as tp

        np.random.seed(5)
        calc = TerminalProbabilityCalculator(
            n_simulations=150_000, n_steps_per_day=1440, use_antithetic=True
        )
        p_numba = calc.terminal_probability_ssvi(SSVI_PROD, SPOT, SPOT, STRIKE, "up").terminal_probability

        monkeypatch.setattr(tp, "NUMBA_AVAILABLE", False)
        np.random.seed(5)
        p_numpy = calc.terminal_probability_ssvi(SSVI_PROD, SPOT, SPOT, STRIKE, "up").terminal_probability

        se = math.sqrt(p_numba * (1 - p_numba) / 150_000)
        assert abs(p_numba - p_numpy) < 6 * se
