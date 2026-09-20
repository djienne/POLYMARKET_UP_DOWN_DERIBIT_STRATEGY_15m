"""Tests for SSVI model fitting."""

import json
import time
import pytest
import numpy as np
import math
from btc_pricer.models.ssvi import (
    SSVIParams, SSVIModel, SSVIFitter, SSVIFitResult,
    SSVISliceData, SSVISurfaceParams, SSVISurfaceFitter,
)


class TestSSVIParams:
    """Test SSVI parameter validation."""

    def test_valid_params(self):
        """Test creating valid parameters."""
        params = SSVIParams(theta=0.04, rho=-0.3, phi=0.5, ttm=0.25)
        assert params.theta == 0.04
        assert params.rho == -0.3
        assert params.phi == 0.5

    def test_invalid_theta(self):
        """Test that negative theta raises error."""
        with pytest.raises(ValueError):
            SSVIParams(theta=-0.04, rho=-0.3, phi=0.5, ttm=0.25)

    def test_invalid_rho(self):
        """Test that rho outside (-1, 1) raises error."""
        with pytest.raises(ValueError):
            SSVIParams(theta=0.04, rho=1.0, phi=0.5, ttm=0.25)

        with pytest.raises(ValueError):
            SSVIParams(theta=0.04, rho=-1.0, phi=0.5, ttm=0.25)

    def test_invalid_phi(self):
        """Test that negative phi raises error."""
        with pytest.raises(ValueError):
            SSVIParams(theta=0.04, rho=-0.3, phi=-0.5, ttm=0.25)

    def test_butterfly_condition(self):
        """Test butterfly arbitrage condition."""
        # Satisfies condition
        params = SSVIParams(theta=0.04, rho=-0.3, phi=0.5, ttm=0.25)
        assert params.butterfly_condition()

        # Violates condition (extreme values)
        params_bad = SSVIParams(theta=3.0, rho=0.9, phi=2.0, ttm=0.25)
        assert not params_bad.butterfly_condition()

    def test_butterfly_condition_both_conditions(self):
        """Test that butterfly checks both Gatheral-Jacquier conditions.

        Condition 1: θφ(1+|ρ|) ≤ 4
        Condition 2: θφ²(1+|ρ|) ≤ 4
        """
        # Case: passes cond1, fails cond2 (φ > 1 makes cond2 stricter)
        # θ=0.5, φ=3.0, ρ=0 → factor=0.5, cond1=1.5, cond2=4.5
        params = SSVIParams(theta=0.5, rho=0.0, phi=3.0, ttm=0.25)
        factor = params.theta * (1 + abs(params.rho))
        assert factor * params.phi <= 4, "cond1 should pass"
        assert factor * params.phi ** 2 > 4, "cond2 should fail"
        assert not params.butterfly_condition()

        # Case: both pass
        params_ok = SSVIParams(theta=0.04, rho=-0.3, phi=0.5, ttm=0.25)
        assert params_ok.butterfly_condition()

        # Case: both fail
        params_bad = SSVIParams(theta=3.0, rho=0.9, phi=2.0, ttm=0.25)
        assert not params_bad.butterfly_condition()

        # Case: φ < 1 → cond1 is more restrictive than cond2
        params_low_phi = SSVIParams(theta=0.5, rho=0.0, phi=0.5, ttm=0.25)
        factor_lp = params_low_phi.theta * (1 + abs(params_low_phi.rho))
        cond1_lp = factor_lp * params_low_phi.phi
        cond2_lp = factor_lp * params_low_phi.phi ** 2
        assert cond1_lp > cond2_lp, "With φ<1, cond1 should be more restrictive"
        assert params_low_phi.butterfly_condition()

        # Case: φ = 1 → both conditions equal
        params_phi1 = SSVIParams(theta=0.5, rho=0.0, phi=1.0, ttm=0.25)
        factor_1 = params_phi1.theta * (1 + abs(params_phi1.rho))
        assert abs(factor_1 * params_phi1.phi - factor_1 * params_phi1.phi ** 2) < 1e-10

    def test_to_dict_includes_both_conditions(self):
        """Test that to_dict includes both butterfly condition values."""
        params = SSVIParams(theta=0.5, rho=0.0, phi=3.0, ttm=0.25)
        d = params.to_dict()
        assert "butterfly_cond1" in d
        assert "butterfly_cond2" in d
        assert d["butterfly_cond1"] == pytest.approx(1.5)
        assert d["butterfly_cond2"] == pytest.approx(4.5)
        assert d["butterfly_satisfied"] is False


class TestSSVIModel:
    """Test SSVI model calculations."""

    def setup_method(self):
        """Set up test fixtures."""
        self.params = SSVIParams(theta=0.04, rho=-0.3, phi=0.5, ttm=0.25)
        self.model = SSVIModel(self.params)

    def test_total_variance_atm(self):
        """Test total variance at ATM."""
        # At k=0, w(0) = θ/2 * (1 + ρφ*0 + sqrt((φ*0+ρ)² + 1-ρ²))
        # = θ/2 * (1 + sqrt(ρ² + 1-ρ²)) = θ/2 * (1 + 1) = θ
        w_atm = self.model.total_variance(0)
        expected = self.params.theta
        assert abs(w_atm - expected) < 1e-10

    def test_implied_volatility_atm(self):
        """Test implied volatility at ATM."""
        iv_atm = self.model.implied_volatility(0)
        w_atm = self.model.total_variance(0)
        expected = math.sqrt(w_atm / self.params.ttm)
        assert abs(iv_atm - expected) < 1e-10

    def test_smile_shape(self):
        """Test that SSVI produces correct smile shape."""
        k_values = np.linspace(-0.3, 0.3, 100)
        ivs = self.model.implied_volatility_array(k_values)

        # With negative rho, should have downward slope on left
        # and potentially upturn on right (typical BTC smile)
        atm_idx = len(k_values) // 2

        # IV should be higher on the left (negative k) for negative rho
        assert ivs[0] > ivs[atm_idx]

    def test_array_vs_scalar(self):
        """Test that array and scalar methods give same results."""
        k_values = np.array([-0.2, -0.1, 0, 0.1, 0.2])

        # Scalar
        scalar_ivs = [self.model.implied_volatility(k) for k in k_values]

        # Array
        array_ivs = self.model.implied_volatility_array(k_values)

        np.testing.assert_allclose(scalar_ivs, array_ivs, rtol=1e-10)

    def test_strike_method(self):
        """Test implied volatility by strike."""
        forward = 100000
        strike = 95000

        k = math.log(strike / forward)
        iv_by_k = self.model.implied_volatility(k)
        iv_by_strike = self.model.implied_volatility_strike(strike, forward)

        assert abs(iv_by_k - iv_by_strike) < 1e-10


class TestSSVIFitter:
    """Test SSVI fitting."""

    def setup_method(self):
        """Set up test fixtures."""
        self.fitter = SSVIFitter()

    def test_fit_synthetic_data(self):
        """Test fitting to synthetic SSVI data."""
        # True parameters
        true_theta = 0.04
        true_rho = -0.25
        true_phi = 0.4
        ttm = 0.25

        true_params = SSVIParams(
            theta=true_theta, rho=true_rho, phi=true_phi, ttm=ttm
        )
        true_model = SSVIModel(true_params)

        # Generate synthetic data
        log_moneyness = np.linspace(-0.3, 0.3, 20)
        market_iv = true_model.implied_volatility_array(log_moneyness)

        # Fit
        result = self.fitter.fit(log_moneyness, market_iv, ttm)

        assert result.success
        assert result.params is not None
        assert result.r_squared > 0.97  # Should be near-perfect fit (with regularization)

        # Check recovered parameters (within tolerance)
        # Note: relative error objective with regularization may trade off
        # exact parameter recovery for better fit on non-synthetic data
        assert abs(result.params.theta - true_theta) < 0.01
        assert abs(result.params.rho - true_rho) < 0.15
        assert abs(result.params.phi - true_phi) < 0.35

    def test_fit_with_noise(self):
        """Test fitting with noisy data."""
        true_params = SSVIParams(theta=0.05, rho=-0.3, phi=0.5, ttm=0.5)
        true_model = SSVIModel(true_params)

        log_moneyness = np.linspace(-0.4, 0.4, 30)
        market_iv = true_model.implied_volatility_array(log_moneyness)

        # Add smaller noise for more reliable fit
        rng = np.random.default_rng(1)
        noise = rng.normal(0, 0.005, len(market_iv))
        noisy_iv = market_iv + noise

        result = self.fitter.fit(log_moneyness, noisy_iv, 0.5)

        assert result.success
        assert result.r_squared > 0.70  # Acceptable fit with noise

    def test_fit_insufficient_data(self):
        """Test that fitting fails with insufficient data."""
        log_moneyness = np.array([0])
        market_iv = np.array([0.8])

        result = self.fitter.fit(log_moneyness, market_iv, 0.25)

        assert not result.success
        assert "at least 3" in result.message.lower()

    def test_butterfly_constraint(self):
        """Test that fit respects butterfly constraint."""
        # Create data that might lead to butterfly violation without constraint
        log_moneyness = np.linspace(-0.5, 0.5, 15)
        # Extreme smile
        market_iv = 0.5 + 0.8 * np.abs(log_moneyness)

        result = self.fitter.fit(log_moneyness, market_iv, 0.25)

        if result.params is not None:
            assert result.params.butterfly_condition()


class TestSSVIIntegration:
    """Integration tests for SSVI."""

    def test_realistic_btc_smile(self):
        """Test fitting a realistic BTC-like IV smile."""
        # Typical BTC smile characteristics:
        # - Negative skew (puts more expensive)
        # - Moderate curvature
        # - ATM vol around 60-80%

        log_moneyness = np.array([
            -0.30, -0.25, -0.20, -0.15, -0.10, -0.05,
            0.00, 0.05, 0.10, 0.15, 0.20, 0.25, 0.30
        ])

        # Simulated market IVs (typical BTC smile)
        market_iv = np.array([
            0.95, 0.88, 0.82, 0.76, 0.72, 0.69,
            0.68, 0.68, 0.70, 0.73, 0.77, 0.82, 0.88
        ])

        ttm = 0.25  # 3 months

        fitter = SSVIFitter()
        result = fitter.fit(log_moneyness, market_iv, ttm)

        assert result.success
        assert result.r_squared > 0.75  # SSVI may not perfectly fit all shapes
        assert result.params is not None

        # Check that fitted params are reasonable
        # Negative rho for typical skew (or close to it)
        assert result.params.rho < 0.2

        # Positive phi for curvature
        assert result.params.phi > 0


class TestSSVISurfaceParams:
    """Test SSVISurfaceParams."""

    def test_phi_function(self):
        """Test φ(θ) = η·θ^(-λ) computed correctly."""
        params = SSVISurfaceParams(
            rho=-0.3, eta=2.0, lam=0.25,
            thetas=[0.01, 0.02, 0.04],
            ttms=[0.01, 0.02, 0.04],
            expiry_names=["A", "B", "C"],
        )
        # φ(0.04) = 2.0 * 0.04^(-0.25)
        expected = 2.0 * 0.04 ** (-0.25)
        assert abs(params.phi(0.04) - expected) < 1e-10

    def test_monotonicity_validation(self):
        """Test that constructor rejects non-monotone θ."""
        with pytest.raises(ValueError, match="monotone"):
            SSVISurfaceParams(
                rho=-0.3, eta=1.0, lam=0.25,
                thetas=[0.04, 0.02, 0.06],  # Not monotone
                ttms=[0.01, 0.02, 0.04],
                expiry_names=["A", "B", "C"],
            )

    def test_invalid_rho(self):
        """Test that rho outside (-1,1) raises error."""
        with pytest.raises(ValueError):
            SSVISurfaceParams(
                rho=1.0, eta=1.0, lam=0.25,
                thetas=[0.01, 0.02], ttms=[0.01, 0.02],
                expiry_names=["A", "B"],
            )

    def test_invalid_eta(self):
        """Test that non-positive eta raises error."""
        with pytest.raises(ValueError):
            SSVISurfaceParams(
                rho=-0.3, eta=0.0, lam=0.25,
                thetas=[0.01, 0.02], ttms=[0.01, 0.02],
                expiry_names=["A", "B"],
            )

    def test_invalid_lam(self):
        """Test that lam outside [0, 0.5] raises error."""
        with pytest.raises(ValueError):
            SSVISurfaceParams(
                rho=-0.3, eta=1.0, lam=0.6,
                thetas=[0.01, 0.02], ttms=[0.01, 0.02],
                expiry_names=["A", "B"],
            )

    def test_to_per_slice_params(self):
        """Test decomposition to per-slice SSVIParams."""
        params = SSVISurfaceParams(
            rho=-0.3, eta=2.0, lam=0.25,
            thetas=[0.01, 0.04],
            ttms=[0.01, 0.04],
            expiry_names=["A", "B"],
        )
        per_slice = params.to_per_slice_params()
        assert len(per_slice) == 2
        for sp in per_slice:
            assert isinstance(sp, SSVIParams)
            assert sp.rho == -0.3
            assert sp.theta > 0
            assert sp.phi > 0
        # All slices share ρ
        assert per_slice[0].rho == per_slice[1].rho

    def test_get_params_for_ttm(self):
        """Test interpolated params for arbitrary TTM."""
        params = SSVISurfaceParams(
            rho=-0.3, eta=2.0, lam=0.25,
            thetas=[0.01, 0.04],
            ttms=[0.01, 0.04],
            expiry_names=["A", "B"],
        )
        # Interpolation at midpoint
        mid_params = params.get_params_for_ttm(0.02)
        assert isinstance(mid_params, SSVIParams)
        assert mid_params.rho == -0.3
        assert 0.01 < mid_params.theta < 0.04
        # Check butterfly condition holds for interpolated params
        assert mid_params.butterfly_condition()

    def test_interpolate_theta_boundary(self):
        """Test interpolation boundary behavior."""
        params = SSVISurfaceParams(
            rho=-0.3, eta=2.0, lam=0.25,
            thetas=[0.01, 0.04],
            ttms=[0.01, 0.04],
            expiry_names=["A", "B"],
        )
        # Below range: short-end scales linearly with ttm
        assert params.interpolate_theta(0.005) == pytest.approx(0.005)
        # Above range: flat extrapolation
        assert params.interpolate_theta(0.1) == 0.04

    def test_interpolate_theta_single_slice_short_end(self):
        """Test short-end scaling for one-slice surfaces."""
        params = SSVISurfaceParams(
            rho=-0.3, eta=2.0, lam=0.25,
            thetas=[0.02],
            ttms=[0.02],
            expiry_names=["A"],
        )
        # Keep nearest ATM variance: theta scales with ttm
        assert params.interpolate_theta(0.005) == pytest.approx(0.005)

    def test_to_dict(self):
        """Test serialization to dict."""
        params = SSVISurfaceParams(
            rho=-0.3, eta=2.0, lam=0.25,
            thetas=[0.01, 0.04],
            ttms=[0.01, 0.04],
            expiry_names=["A", "B"],
        )
        d = params.to_dict()
        assert d["rho"] == -0.3
        assert d["eta"] == 2.0
        assert d["lam"] == 0.25
        assert len(d["per_slice"]) == 2


class TestSSVISurfaceFitter:
    """Test SSVI surface joint fitting."""

    @staticmethod
    def _generate_synthetic_slices(rho, eta, lam, thetas, ttms, n_points=15):
        """Generate synthetic market data from known surface params."""
        slices = []
        for theta, ttm in zip(thetas, ttms):
            phi = eta * theta ** (-lam)
            k = np.linspace(-0.2, 0.2, n_points)
            # SSVI total variance
            term = phi * k + rho
            sqrt_term = np.sqrt(term ** 2 + 1 - rho ** 2)
            w = 0.5 * theta * (1 + rho * phi * k + sqrt_term)
            iv = np.sqrt(w / ttm)
            slices.append(SSVISliceData(
                expiry_name=f"SLICE_{ttm:.4f}",
                ttm=ttm,
                log_moneyness=k,
                market_iv=iv,
                forward=100000.0,
            ))
        return slices

    @staticmethod
    def _fake_per_slice_fitter(params):
        class _FakeFitter:
            def fit(self, log_moneyness, market_iv, ttm, weights=None):
                index = self.calls
                self.calls += 1
                return SSVIFitResult(
                    params=params[index],
                    success=True,
                    r_squared=1.0,
                    rmse=0.0,
                    max_residual=0.0,
                    n_points=len(log_moneyness),
                    message="ok",
                )

            calls = 0

        return _FakeFitter()

    def test_fit_synthetic_surface(self):
        """Test recovery of known surface parameters."""
        true_rho = -0.3
        true_eta = 1.5
        true_lam = 0.2
        true_thetas = [0.005, 0.010, 0.020]
        true_ttms = [0.005, 0.010, 0.020]

        slices = self._generate_synthetic_slices(
            true_rho, true_eta, true_lam, true_thetas, true_ttms
        )

        fitter = SSVISurfaceFitter(
            per_slice_fitter=SSVIFitter(),
            maxiter=200,
            workers=1,  # Deterministic for test
        )
        result = fitter.fit(slices)

        assert result.success
        assert result.params is not None
        assert result.aggregate_r_squared > 0.90
        # Check parameter recovery (with tolerance)
        assert abs(result.params.rho - true_rho) < 0.3
        assert result.params.eta > 0
        assert 0 <= result.params.lam <= 0.5

    def test_warm_start_narrows_first_optimizer_bounds(self):
        """Previous surface params center the first DE search box."""
        previous = SSVISurfaceParams(
            rho=-0.25,
            eta=1.4,
            lam=0.20,
            thetas=[0.005, 0.010, 0.020],
            ttms=[0.005, 0.010, 0.020],
            expiry_names=["SLICE_0.0050", "SLICE_0.0100", "SLICE_0.0200"],
        )
        slices = self._generate_synthetic_slices(
            previous.rho, previous.eta, previous.lam, previous.thetas, previous.ttms
        )
        calls = []

        fitter = SSVISurfaceFitter(
            per_slice_fitter=self._fake_per_slice_fitter(previous.to_per_slice_params()),
            maxiter=200,
            workers=1,
        )

        def fake_run_surface_de(objective, bounds, x0, *, maxiter, popsize=15):
            calls.append({"bounds": bounds, "x0": x0, "maxiter": maxiter, "popsize": popsize})
            return type("Result", (), {"x": x0, "message": "ok"})()

        fitter._run_surface_de = fake_run_surface_de

        result = fitter.fit(slices, warm_start_surface_params=previous)

        assert result.success
        assert result.fit_method == "warm_start"
        assert result.warm_start_used is True
        assert len(calls) == 1
        warm_bounds = calls[0]["bounds"]
        assert warm_bounds[0] == pytest.approx((-0.45, -0.05))
        assert warm_bounds[1] == pytest.approx((0.7, 2.8))
        assert warm_bounds[2] == pytest.approx((0.05, 0.35))
        assert calls[0]["maxiter"] == 50
        assert calls[0]["popsize"] == 8

    def test_warm_start_skipped_when_expiries_mismatch(self):
        """A previous fit with different expiry ordering does not warm-start."""
        previous = SSVISurfaceParams(
            rho=-0.25,
            eta=1.4,
            lam=0.20,
            thetas=[0.005, 0.010, 0.020],
            ttms=[0.005, 0.010, 0.020],
            expiry_names=["OTHER_A", "OTHER_B", "OTHER_C"],
        )
        true_surface = SSVISurfaceParams(
            rho=-0.25,
            eta=1.4,
            lam=0.20,
            thetas=[0.005, 0.010, 0.020],
            ttms=[0.005, 0.010, 0.020],
            expiry_names=["SLICE_0.0050", "SLICE_0.0100", "SLICE_0.0200"],
        )
        slices = self._generate_synthetic_slices(
            true_surface.rho, true_surface.eta, true_surface.lam,
            true_surface.thetas, true_surface.ttms
        )
        calls = []
        full_x0 = np.concatenate([
            [true_surface.rho, true_surface.eta, true_surface.lam],
            np.sqrt(np.diff(np.concatenate([[0.0], true_surface.thetas]))),
        ])

        fitter = SSVISurfaceFitter(
            per_slice_fitter=self._fake_per_slice_fitter(true_surface.to_per_slice_params()),
            maxiter=200,
            workers=1,
        )

        def fake_run_surface_de(objective, bounds, x0, *, maxiter, popsize=15):
            calls.append({"bounds": bounds, "x0": x0, "maxiter": maxiter, "popsize": popsize})
            return type("Result", (), {"x": full_x0, "message": "ok"})()

        fitter._run_surface_de = fake_run_surface_de

        result = fitter.fit(slices, warm_start_surface_params=previous)

        assert result.success
        assert result.fit_method == "full_de"
        assert result.warm_start_used is False
        assert len(calls) == 1
        assert calls[0]["maxiter"] == 200
        assert calls[0]["popsize"] == 15

    def test_warm_start_falls_back_to_full_de_when_quality_guard_fails(self):
        """A low-quality warm-start candidate is rejected before full DE."""
        good_surface = SSVISurfaceParams(
            rho=-0.25,
            eta=1.4,
            lam=0.20,
            thetas=[0.005, 0.010, 0.020],
            ttms=[0.005, 0.010, 0.020],
            expiry_names=["SLICE_0.0050", "SLICE_0.0100", "SLICE_0.0200"],
        )
        bad_surface = SSVISurfaceParams(
            rho=0.15,
            eta=0.5,
            lam=0.05,
            thetas=[0.002, 0.004, 0.006],
            ttms=good_surface.ttms,
            expiry_names=good_surface.expiry_names,
        )
        slices = self._generate_synthetic_slices(
            good_surface.rho, good_surface.eta, good_surface.lam,
            good_surface.thetas, good_surface.ttms
        )
        good_x = np.concatenate([
            [good_surface.rho, good_surface.eta, good_surface.lam],
            np.sqrt(np.diff(np.concatenate([[0.0], good_surface.thetas]))),
        ])
        calls = []

        fitter = SSVISurfaceFitter(
            per_slice_fitter=self._fake_per_slice_fitter(good_surface.to_per_slice_params()),
            maxiter=200,
            workers=1,
        )

        def fake_run_surface_de(objective, bounds, x0, *, maxiter, popsize=15):
            calls.append({"maxiter": maxiter, "popsize": popsize})
            if len(calls) == 1:
                return type("Result", (), {"x": x0, "message": "warm"})()
            return type("Result", (), {"x": good_x, "message": "full"})()

        fitter._run_surface_de = fake_run_surface_de

        result = fitter.fit(slices, warm_start_surface_params=bad_surface)

        assert result.success
        assert result.fit_method == "full_de"
        assert result.warm_start_used is False
        assert len(calls) == 2
        assert calls[0]["popsize"] == 8
        assert calls[1]["popsize"] == 15

    def test_disk_warm_start_cache_written_and_loaded_by_new_fitter(self, tmp_path):
        """Successful fits write disk cache that a fresh fitter can use."""
        surface = SSVISurfaceParams(
            rho=-0.25,
            eta=1.4,
            lam=0.20,
            thetas=[0.005, 0.010, 0.020],
            ttms=[0.005, 0.010, 0.020],
            expiry_names=["SLICE_0.0050", "SLICE_0.0100", "SLICE_0.0200"],
        )
        slices = self._generate_synthetic_slices(
            surface.rho, surface.eta, surface.lam, surface.thetas, surface.ttms
        )
        cache_path = tmp_path / "ssvi_surface_warm.json"

        fitter1 = SSVISurfaceFitter(
            per_slice_fitter=self._fake_per_slice_fitter(surface.to_per_slice_params()),
            maxiter=200,
            workers=1,
            per_slice_parallel=1,
            warm_start_path=cache_path,
        )
        fitter1._run_surface_de = (
            lambda objective, bounds, x0, *, maxiter, popsize=15:
            type("Result", (), {"x": x0, "message": "ok"})()
        )

        result1 = fitter1.fit(slices)

        assert result1.success
        assert cache_path.exists()
        payload = json.loads(cache_path.read_text(encoding="utf-8"))
        assert payload["rho"] == pytest.approx(result1.params.rho)
        assert payload["eta"] == pytest.approx(result1.params.eta)
        assert payload["lam"] == pytest.approx(result1.params.lam)
        assert payload["timestamp"] <= time.time()

        calls = []
        fitter2 = SSVISurfaceFitter(
            per_slice_fitter=self._fake_per_slice_fitter(surface.to_per_slice_params()),
            maxiter=200,
            workers=1,
            per_slice_parallel=1,
            warm_start_path=cache_path,
        )

        def fake_run_surface_de(objective, bounds, x0, *, maxiter, popsize=15):
            calls.append(x0.copy())
            return type("Result", (), {"x": x0, "message": "ok"})()

        fitter2._run_surface_de = fake_run_surface_de
        result2 = fitter2.fit(slices)

        assert result2.success
        assert calls[0][0] == pytest.approx(payload["rho"])
        assert calls[0][1] == pytest.approx(payload["eta"])
        assert calls[0][2] == pytest.approx(payload["lam"])
        assert result2.params.rho == pytest.approx(result1.params.rho, abs=1e-9)
        assert result2.params.eta == pytest.approx(result1.params.eta, abs=1e-9)
        assert result2.params.lam == pytest.approx(result1.params.lam, abs=1e-9)

    def test_disk_warm_start_stale_or_invalid_cache_ignored(self, tmp_path):
        """Stale and out-of-bounds disk cache entries do not seed the optimizer."""
        surface = SSVISurfaceParams(
            rho=-0.25,
            eta=1.4,
            lam=0.20,
            thetas=[0.005, 0.010, 0.020],
            ttms=[0.005, 0.010, 0.020],
            expiry_names=["SLICE_0.0050", "SLICE_0.0100", "SLICE_0.0200"],
        )
        slices = self._generate_synthetic_slices(
            surface.rho, surface.eta, surface.lam, surface.thetas, surface.ttms
        )
        bad_payloads = [
            {"rho": 0.70, "eta": 2.0, "lam": 0.10, "timestamp": 0.0},
            {"rho": 2.00, "eta": 2.0, "lam": 0.10, "timestamp": time.time()},
        ]

        for idx, payload in enumerate(bad_payloads):
            cache_path = tmp_path / f"ssvi_surface_warm_{idx}.json"
            cache_path.write_text(json.dumps(payload), encoding="utf-8")
            calls = []
            fitter = SSVISurfaceFitter(
                per_slice_fitter=self._fake_per_slice_fitter(surface.to_per_slice_params()),
                maxiter=200,
                workers=1,
                per_slice_parallel=1,
                warm_start_path=cache_path,
            )

            def fake_run_surface_de(objective, bounds, x0, *, maxiter, popsize=15):
                calls.append(x0.copy())
                return type("Result", (), {"x": x0, "message": "ok"})()

            fitter._run_surface_de = fake_run_surface_de
            result = fitter.fit(slices)

            assert result.success
            assert calls[0][0] == pytest.approx(surface.rho)
            assert calls[0][0] != pytest.approx(payload["rho"])

    def test_memory_warm_start_takes_precedence_over_disk_cache(self, tmp_path):
        """Explicit in-memory params remain primary over disk warm-start cache."""
        previous = SSVISurfaceParams(
            rho=-0.25,
            eta=1.4,
            lam=0.20,
            thetas=[0.005, 0.010, 0.020],
            ttms=[0.005, 0.010, 0.020],
            expiry_names=["SLICE_0.0050", "SLICE_0.0100", "SLICE_0.0200"],
        )
        slices = self._generate_synthetic_slices(
            previous.rho, previous.eta, previous.lam, previous.thetas, previous.ttms
        )
        cache_path = tmp_path / "ssvi_surface_warm.json"
        cache_path.write_text(json.dumps({
            "rho": 0.70,
            "eta": 2.0,
            "lam": 0.10,
            "timestamp": time.time(),
        }), encoding="utf-8")
        calls = []

        fitter = SSVISurfaceFitter(
            per_slice_fitter=self._fake_per_slice_fitter(previous.to_per_slice_params()),
            maxiter=200,
            workers=1,
            per_slice_parallel=1,
            warm_start_path=cache_path,
        )

        def fake_run_surface_de(objective, bounds, x0, *, maxiter, popsize=15):
            calls.append({"x0": x0.copy(), "popsize": popsize})
            return type("Result", (), {"x": x0, "message": "ok"})()

        fitter._run_surface_de = fake_run_surface_de
        result = fitter.fit(slices, warm_start_surface_params=previous)

        assert result.success
        assert result.fit_method == "warm_start"
        assert result.warm_start_used is True
        assert len(calls) == 1
        assert calls[0]["popsize"] == 8
        assert calls[0]["x0"][0] == pytest.approx(previous.rho)
        assert calls[0]["x0"][0] != pytest.approx(0.70)

    def test_monotonicity_enforced(self):
        """Test that surface enforces monotone θ even when per-slice fits don't."""
        # Generate slices where per-slice fits might swap θ ordering
        slices = self._generate_synthetic_slices(
            rho=-0.3, eta=1.5, lam=0.2,
            thetas=[0.008, 0.010, 0.015],
            ttms=[0.005, 0.010, 0.015],
        )

        fitter = SSVISurfaceFitter(
            per_slice_fitter=SSVIFitter(),
            maxiter=200,
            workers=1,
        )
        result = fitter.fit(slices)

        assert result.success, result.message
        assert result.params is not None
        if True:
            # θ values must be monotone increasing
            for i in range(1, len(result.params.thetas)):
                assert result.params.thetas[i] >= result.params.thetas[i - 1]

    def test_butterfly_per_slice(self):
        """Test that all decomposed slices satisfy butterfly condition."""
        slices = self._generate_synthetic_slices(
            rho=-0.3, eta=1.5, lam=0.2,
            thetas=[0.005, 0.010, 0.020],
            ttms=[0.005, 0.010, 0.020],
        )

        fitter = SSVISurfaceFitter(
            per_slice_fitter=SSVIFitter(),
            maxiter=200,
            workers=1,
        )
        result = fitter.fit(slices)

        assert result.success, result.message
        assert result.per_slice_params
        if True:
            for sp in result.per_slice_params:
                factor = sp.theta * (1 + abs(sp.rho))
                assert sp.butterfly_condition(), (
                    f"Butterfly violated: θ={sp.theta:.6f}, φ={sp.phi:.3f}, "
                    f"ρ={sp.rho:.3f}, cond1={factor * sp.phi:.4f}, "
                    f"cond2={factor * sp.phi ** 2:.4f}"
                )

    def test_decomposition_to_per_slice(self):
        """Test that to_per_slice_params produces valid SSVIParams usable downstream."""
        slices = self._generate_synthetic_slices(
            rho=-0.3, eta=1.5, lam=0.2,
            thetas=[0.005, 0.010],
            ttms=[0.005, 0.010],
        )

        fitter = SSVISurfaceFitter(
            per_slice_fitter=SSVIFitter(),
            maxiter=200,
            workers=1,
        )
        result = fitter.fit(slices)

        assert result.success, result.message
        assert result.params is not None
        if True:
            per_slice = result.params.to_per_slice_params()
            assert len(per_slice) == 2
            for sp in per_slice:
                # Each SSVIParams should work with SSVIModel
                model = SSVIModel(sp)
                iv = model.implied_volatility(0.0)
                assert iv > 0
                # And produce valid arrays
                k = np.linspace(-0.1, 0.1, 10)
                ivs = model.implied_volatility_array(k)
                assert np.all(ivs > 0)

    def test_min_expiries_guard(self):
        """Test that fitter refuses with < 2 slices."""
        slices = self._generate_synthetic_slices(
            rho=-0.3, eta=1.5, lam=0.2,
            thetas=[0.01], ttms=[0.01],
        )

        fitter = SSVISurfaceFitter(
            per_slice_fitter=SSVIFitter(),
            workers=1,
        )
        result = fitter.fit(slices)

        assert not result.success
        assert "at least 2" in result.message.lower()

    def test_fallback_on_failure(self, monkeypatch):
        """A failed joint solve retains scientifically usable independent slices."""
        fitter = SSVISurfaceFitter(
            per_slice_fitter=SSVIFitter(),
            maxiter=5,
            workers=1,
            fallback_to_independent=True,
            local_first=False,
        )

        # Use data that's hard to fit jointly
        slices = self._generate_synthetic_slices(
            rho=-0.3, eta=1.5, lam=0.2,
            thetas=[0.005, 0.010, 0.020],
            ttms=[0.005, 0.010, 0.020],
        )

        def failed_joint(*args, **kwargs):
            raise RuntimeError("injected joint optimizer failure")
        monkeypatch.setattr(fitter, "_run_surface_de", failed_joint)
        result = fitter.fit(slices)
        assert not result.success and result.params is None
        assert "Joint optimization failed" in result.message
        assert len(result.per_slice_params) == len(slices)
        assert result.aggregate_r_squared > 0.95
        assert all(params.butterfly_condition() for params in result.per_slice_params)

    def test_interpolate_theta(self):
        """Test theta interpolation for arbitrary TTM."""
        slices = self._generate_synthetic_slices(
            rho=-0.3, eta=1.5, lam=0.2,
            thetas=[0.005, 0.010, 0.020],
            ttms=[0.005, 0.010, 0.020],
        )

        fitter = SSVISurfaceFitter(
            per_slice_fitter=SSVIFitter(),
            maxiter=200,
            workers=1,
        )
        result = fitter.fit(slices)

        assert result.success, result.message
        assert result.params is not None
        if True:
            # Interpolation at an intermediate TTM
            theta_mid = result.params.interpolate_theta(0.015)
            # Should be between the two surrounding thetas
            theta_10 = result.params.interpolate_theta(0.010)
            theta_20 = result.params.interpolate_theta(0.020)
            assert theta_10 <= theta_mid <= theta_20


    def test_interpolate_theta_pchip_monotonicity(self):
        """Test that PCHIP interpolation preserves monotonicity with many expiries."""
        # Monotonically increasing thetas (total variance must grow with TTM)
        thetas = [0.003, 0.008, 0.015, 0.025, 0.040]
        ttms = [0.005, 0.010, 0.020, 0.040, 0.080]

        params = SSVISurfaceParams(
            rho=-0.3, eta=1.5, lam=0.2,
            thetas=thetas, ttms=ttms,
            expiry_names=[f"EXP{i}" for i in range(len(ttms))],
        )

        # Sample many intermediate points and verify monotonicity
        test_ttms = np.linspace(ttms[0], ttms[-1], 200)
        interpolated = [params.interpolate_theta(t) for t in test_ttms]

        for i in range(1, len(interpolated)):
            assert interpolated[i] >= interpolated[i - 1], (
                f"Monotonicity violated at ttm={test_ttms[i]:.6f}: "
                f"{interpolated[i]} < {interpolated[i-1]}"
            )


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
