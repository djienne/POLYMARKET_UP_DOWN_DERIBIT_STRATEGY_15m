"""SSVI objective consistency and fit-cache scheduling tests.

C4: the vectorized _SSVISurfaceObjective must match the original per-slice
    loop implementation to float-summation noise (<=1e-9 relative).
C2: deferred warm-start-first skips per-slice fits when accepted, falls
    through when rejected, and forces a periodic full re-fit.
C6: per-slice results are memoized across retry attempts.
C1: fingerprint is stable under sub-1e-5 mark_iv jitter.
"""

import numpy as np
import pytest

from btc_pricer.models.ssvi import (
    SSVIFitter,
    SSVISliceData,
    SSVISurfaceFitter,
    SSVISurfaceParams,
    _SSVISurfaceObjective,
)


class _ReferenceSurfaceObjective:
    """The original per-slice loop implementation, kept for equivalence tests."""

    def __init__(self, slices_data, use_relative_error):
        self.n_slices = len(slices_data)
        self.log_moneyness = [s.log_moneyness for s in slices_data]
        self.market_iv = [s.market_iv for s in slices_data]
        self.ttms = [s.ttm for s in slices_data]
        self.weights = [
            s.weights if s.weights is not None else np.ones(len(s.market_iv))
            for s in slices_data
        ]
        self.use_relative_error = use_relative_error

    def __call__(self, x):
        rho, eta, lam = x[0], x[1], x[2]
        thetas = np.cumsum(x[3:] ** 2)
        total_error = 0.0
        penalty = 0.0
        for i in range(self.n_slices):
            theta_i = thetas[i]
            if theta_i <= 0:
                return 1e10
            phi_i = eta * theta_i ** (-lam)
            k = self.log_moneyness[i]
            term = phi_i * k + rho
            sqrt_term = np.sqrt(np.maximum(term ** 2 + 1 - rho ** 2, 1e-10))
            model_var = 0.5 * theta_i * (1 + rho * phi_i * k + sqrt_term)
            model_iv = np.sqrt(np.maximum(model_var / self.ttms[i], 1e-10))
            if self.use_relative_error:
                rel = (model_iv - self.market_iv[i]) / self.market_iv[i]
                total_error += np.sum(self.weights[i] * rel ** 2)
            else:
                total_error += np.sum(self.weights[i] * (model_iv - self.market_iv[i]) ** 2)
            factor_i = theta_i * (1 + abs(rho))
            b1 = factor_i * phi_i
            b2 = factor_i * phi_i ** 2
            if b1 > 4:
                penalty += 1000.0 * (b1 - 4) ** 2
            if b2 > 4:
                penalty += 1000.0 * (b2 - 4) ** 2
        return total_error + penalty


def _random_slices(rng, n_slices=4):
    slices = []
    ttm = 0.001
    for i in range(n_slices):
        n_pts = rng.integers(6, 25)
        k = np.sort(rng.uniform(-0.15, 0.15, n_pts))
        iv = rng.uniform(0.3, 0.9, n_pts)
        slices.append(SSVISliceData(
            expiry_name=f"E{i}",
            ttm=ttm,
            log_moneyness=k,
            market_iv=iv,
            forward=100.0,
        ))
        ttm *= 2.7
    return slices


class TestVectorizedObjectiveEquivalence:
    @pytest.mark.parametrize("use_relative_error", [True, False])
    def test_matches_reference_over_random_vectors(self, use_relative_error):
        rng = np.random.default_rng(42)
        for _ in range(5):
            slices = _random_slices(rng)
            new_obj = _SSVISurfaceObjective(slices, use_relative_error)
            ref_obj = _ReferenceSurfaceObjective(slices, use_relative_error)
            n = len(slices)
            for _ in range(100):
                x = np.concatenate([
                    [rng.uniform(-0.9, 0.9), rng.uniform(0.05, 8.0), rng.uniform(0.0, 0.5)],
                    rng.uniform(0.001, 1.0, n),
                ])
                v_new = new_obj(x)
                v_ref = ref_obj(x)
                assert v_new == pytest.approx(v_ref, rel=1e-9, abs=1e-12)

    def test_butterfly_penalty_region(self):
        rng = np.random.default_rng(7)
        slices = _random_slices(rng, 3)
        new_obj = _SSVISurfaceObjective(slices, True)
        ref_obj = _ReferenceSurfaceObjective(slices, True)
        # Large deltas + eta drive the butterfly penalty branch
        x = np.array([0.5, 8.0, 0.0, 1.5, 1.5, 1.5])
        assert new_obj(x) == pytest.approx(ref_obj(x), rel=1e-9)


def _make_synthetic_slices(surface: SSVISurfaceParams, n_pts=15, noise=0.0, seed=0):
    """Slices generated FROM a surface — exact recovery is possible."""
    rng = np.random.default_rng(seed)
    slices = []
    for name, ttm, params in zip(
        surface.expiry_names, surface.ttms, surface.to_per_slice_params()
    ):
        k = np.linspace(-0.1, 0.1, n_pts)
        from btc_pricer.models.ssvi import SSVIModel

        iv = SSVIModel(params).implied_volatility_array(k)
        if noise:
            iv = iv * (1 + rng.normal(0, noise, n_pts))
        slices.append(SSVISliceData(
            expiry_name=name, ttm=ttm, log_moneyness=k,
            market_iv=iv, forward=100.0,
        ))
    return slices


def _test_surface():
    return SSVISurfaceParams(
        rho=-0.25,
        eta=1.3,
        lam=0.4,
        thetas=[8.4e-05, 3.2e-04, 4.6e-04, 7.0e-04],
        ttms=[0.00105, 0.00379, 0.00652, 0.00926],
        expiry_names=["E0", "E1", "E2", "E3"],
    )


class _CountingFitter(SSVIFitter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fit_calls = 0

    def fit(self, *args, **kwargs):
        self.fit_calls += 1
        return super().fit(*args, **kwargs)


def _make_surface_fitter(**kwargs):
    per_slice = _CountingFitter(
        use_multi_start=True,
        n_starts=3,
        use_global_optimizer=False,
        very_short_dated_phi_bounds=(0.001, 5000.0),
        early_stop_r2=0.95,
        local_ftol=1e-7,
        local_maxiter=200,
    )
    return SSVISurfaceFitter(
        per_slice_fitter=per_slice,
        eta_bounds=(0.01, 10.0),
        maxiter=60,
        **kwargs,
    ), per_slice


class TestDeferredWarmStart:
    def test_accepted_warm_fit_skips_per_slice(self):
        surface = _test_surface()
        slices = _make_synthetic_slices(surface, noise=0.001)
        fitter, per_slice = _make_surface_fitter(defer_per_slice_fits=True)
        result = fitter.fit(slices, warm_start_surface_params=surface)
        assert result.success
        assert result.fit_method == "warm_start_deferred"
        assert per_slice.fit_calls == 0
        assert result.aggregate_r_squared > 0.95

    def test_no_warm_params_falls_through_to_full_path(self):
        surface = _test_surface()
        slices = _make_synthetic_slices(surface, noise=0.001)
        fitter, per_slice = _make_surface_fitter(defer_per_slice_fits=True)
        result = fitter.fit(slices, warm_start_surface_params=None)
        assert per_slice.fit_calls == len(slices)
        assert result.success

    def test_mismatched_expiries_fall_through(self):
        surface = _test_surface()
        slices = _make_synthetic_slices(surface, noise=0.001)
        stale = SSVISurfaceParams(
            rho=surface.rho, eta=surface.eta, lam=surface.lam,
            thetas=surface.thetas[:3], ttms=surface.ttms[:3],
            expiry_names=["OTHER0", "OTHER1", "OTHER2"],
        )
        fitter, per_slice = _make_surface_fitter(defer_per_slice_fits=True)
        result = fitter.fit(slices, warm_start_surface_params=stale)
        assert per_slice.fit_calls == len(slices)
        assert result.fit_method != "warm_start_deferred"

    def test_periodic_full_refit_forced(self):
        surface = _test_surface()
        slices = _make_synthetic_slices(surface, noise=0.001)
        fitter, per_slice = _make_surface_fitter(
            defer_per_slice_fits=True, full_refit_every_n=2
        )
        r1 = fitter.fit(slices, warm_start_surface_params=surface)
        r2 = fitter.fit(slices, warm_start_surface_params=surface)
        assert r1.fit_method == "warm_start_deferred"
        assert r2.fit_method == "warm_start_deferred"
        assert per_slice.fit_calls == 0
        r3 = fitter.fit(slices, warm_start_surface_params=surface)
        assert per_slice.fit_calls == len(slices)
        assert r3.success

    def test_degraded_warm_params_rejected(self):
        surface = _test_surface()
        slices = _make_synthetic_slices(surface, noise=0.001)
        # A surface that badly misfits the data (wrong sign skew, huge eta shift)
        bad = SSVISurfaceParams(
            rho=0.9, eta=8.0, lam=0.01,
            thetas=[t * 30 for t in surface.thetas],
            ttms=surface.ttms,
            expiry_names=surface.expiry_names,
        )
        fitter, per_slice = _make_surface_fitter(defer_per_slice_fits=True)
        result = fitter.fit(slices, warm_start_surface_params=bad)
        # The warm candidate may recover inside its centered box or be
        # rejected — but if the deferred path accepted, its R² must clear the
        # absolute floor; otherwise the full path must have run.
        if result.fit_method == "warm_start_deferred":
            assert result.aggregate_r_squared >= fitter.min_acceptable_surface_r2
        else:
            assert per_slice.fit_calls == len(slices)


class TestPerSliceCache:
    def test_cache_shared_across_attempts(self):
        surface = _test_surface()
        slices = _make_synthetic_slices(surface, noise=0.001)
        fitter, per_slice = _make_surface_fitter()
        cache = {}
        fitter.fit(slices, per_slice_cache=cache)
        assert per_slice.fit_calls == len(slices)
        # Second attempt on a subset re-uses every cached slice fit.
        fitter.fit(slices[1:], per_slice_cache=cache)
        assert per_slice.fit_calls == len(slices)


class TestFingerprintStability:
    def test_iv_jitter_below_precision_keeps_fingerprint(self):
        from btc_pricer.cli.common import _fingerprint_float, _FINGERPRINT_IV_DIGITS

        base = 0.5123456789
        jittered = base + 4e-7  # below the 5-decimal rounding step / 2
        assert (
            _fingerprint_float(base, _FINGERPRINT_IV_DIGITS)
            == _fingerprint_float(jittered, _FINGERPRINT_IV_DIGITS)
        )

    def test_iv_move_above_precision_changes_fingerprint(self):
        from btc_pricer.cli.common import _fingerprint_float, _FINGERPRINT_IV_DIGITS

        base = 0.51234
        moved = base + 2e-5
        assert (
            _fingerprint_float(base, _FINGERPRINT_IV_DIGITS)
            != _fingerprint_float(moved, _FINGERPRINT_IV_DIGITS)
        )
