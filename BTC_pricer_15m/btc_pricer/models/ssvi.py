"""SSVI (Surface SVI) model fitting.

Implements the SSVI parametrization from Gatheral & Jacquier (2014):
"Arbitrage-free SVI volatility surfaces"

The SSVI formula for total implied variance is:
w(k, θ) = θ/2 * [1 + ρφk + sqrt((φk + ρ)² + 1 - ρ²)]

Where:
- k = log(K/F) is log-moneyness
- θ = ATM total variance (σ²T at k=0)
- ρ ∈ (-1, 1) controls skew
- φ > 0 controls curvature
"""

import json
import logging
import math
import os
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, List, Optional, Tuple
import numpy as np
from scipy.interpolate import PchipInterpolator
from scipy.optimize import minimize, differential_evolution, NonlinearConstraint

from ..constants import DAYS_PER_YEAR
from ..utils.fit_stats import calculate_fit_stats, FitStats

logger = logging.getLogger(__name__)


@dataclass
class SSVIParams:
    """SSVI model parameters."""
    theta: float  # ATM total variance
    rho: float    # Skew parameter
    phi: float    # Curvature parameter
    ttm: float    # Time to maturity (for converting to IV)

    def __post_init__(self):
        """Validate parameters."""
        if self.theta <= 0:
            raise ValueError(f"theta must be positive, got {self.theta}")
        if not -1 < self.rho < 1:
            raise ValueError(f"rho must be in (-1, 1), got {self.rho}")
        if self.phi <= 0:
            raise ValueError(f"phi must be positive, got {self.phi}")
        if self.ttm <= 0:
            raise ValueError(f"ttm must be positive, got {self.ttm}")

    def butterfly_condition(self) -> bool:
        """Check if butterfly arbitrage conditions are satisfied.

        Gatheral-Jacquier (2014) Theorem 4.2 sufficient conditions:
            Condition 1: θφ(1 + |ρ|) ≤ 4
            Condition 2: θφ²(1 + |ρ|) ≤ 4

        Returns:
            True if both conditions are satisfied (no butterfly arbitrage).
        """
        factor = self.theta * (1 + abs(self.rho))
        cond1 = factor * self.phi <= 4 + 1e-9
        cond2 = factor * self.phi ** 2 <= 4 + 1e-9
        return cond1 and cond2

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        factor = self.theta * (1 + abs(self.rho))
        return {
            "theta": float(self.theta),
            "rho": float(self.rho),
            "phi": float(self.phi),
            "ttm": float(self.ttm),
            "butterfly_satisfied": bool(self.butterfly_condition()),
            "butterfly_cond1": float(factor * self.phi),
            "butterfly_cond2": float(factor * self.phi ** 2),
        }


class SSVIModel:
    """SSVI volatility surface model."""

    def __init__(self, params: SSVIParams):
        """Initialize the SSVI model.

        Args:
            params: SSVI parameters.
        """
        self.params = params

    @property
    def ttm(self) -> float:
        """Time to maturity (for VolatilitySurface protocol)."""
        return self.params.ttm

    def total_variance(self, k: float) -> float:
        """Calculate total implied variance w(k).

        w(k) = θ/2 * [1 + ρφk + sqrt((φk + ρ)² + 1 - ρ²)]

        Args:
            k: Log-moneyness ln(K/F).

        Returns:
            Total implied variance.
        """
        theta = self.params.theta
        rho = self.params.rho
        phi = self.params.phi

        term = phi * k + rho
        sqrt_term = math.sqrt(term * term + 1 - rho * rho)

        return 0.5 * theta * (1 + rho * phi * k + sqrt_term)

    def implied_variance(self, k: float) -> float:
        """Calculate implied variance σ²(k).

        σ²(k) = w(k) / T

        Args:
            k: Log-moneyness.

        Returns:
            Implied variance.
        """
        return self.total_variance(k) / self.params.ttm

    def implied_volatility(self, k: float) -> float:
        """Calculate implied volatility σ(k).

        Args:
            k: Log-moneyness.

        Returns:
            Implied volatility.
        """
        var = self.implied_variance(k)
        return math.sqrt(max(0, var))

    def implied_volatility_strike(self, strike: float, forward: float) -> float:
        """Calculate implied volatility for a given strike.

        Args:
            strike: Strike price.
            forward: Forward price.

        Returns:
            Implied volatility.
        """
        k = math.log(strike / forward)
        return self.implied_volatility(k)

    def total_variance_array(self, k_array: np.ndarray) -> np.ndarray:
        """Calculate total variance for array of log-moneyness values.

        Args:
            k_array: Array of log-moneyness values.

        Returns:
            Array of total variances.
        """
        theta = self.params.theta
        rho = self.params.rho
        phi = self.params.phi

        term = phi * k_array + rho
        sqrt_term = np.sqrt(term * term + 1 - rho * rho)

        return 0.5 * theta * (1 + rho * phi * k_array + sqrt_term)

    def implied_volatility_array(self, k_array: np.ndarray) -> np.ndarray:
        """Calculate implied volatility for array of log-moneyness values.

        Args:
            k_array: Array of log-moneyness values.

        Returns:
            Array of implied volatilities.
        """
        var = self.total_variance_array(k_array) / self.params.ttm
        return np.sqrt(np.maximum(0, var))


@dataclass
class SSVIFitResult:
    """Result of SSVI fitting."""
    params: SSVIParams
    success: bool
    r_squared: float
    rmse: float
    max_residual: float
    n_points: int
    message: str


class _SSVIObjective:
    """Picklable SSVI objective for parallel differential_evolution (workers=4).

    Encapsulates all data as instance attributes so pickle can serialize it
    across process boundaries.
    """

    def __init__(self, log_moneyness, market_iv, ttm, weights,
                 phi_bounds, use_relative_error, regularization_lambda):
        self.log_moneyness = log_moneyness
        self.market_iv = market_iv
        self.ttm = ttm
        self.weights = weights
        self.market_total_var = market_iv ** 2 * ttm
        self.phi_center = (phi_bounds[0] + phi_bounds[1]) / 4
        self.phi_max_sq = phi_bounds[1] ** 2
        self.use_relative_error = use_relative_error
        self.regularization_lambda = regularization_lambda

    def __call__(self, params):
        theta, rho, phi = params
        term = phi * self.log_moneyness + rho
        sqrt_term = np.sqrt(np.maximum(term ** 2 + 1 - rho ** 2, 1e-10))
        model_var = 0.5 * theta * (1 + rho * phi * self.log_moneyness + sqrt_term)

        if self.use_relative_error:
            model_iv = np.sqrt(np.maximum(model_var / self.ttm, 1e-10))
            rel_errors = (model_iv - self.market_iv) / self.market_iv
            sse = np.sum(self.weights * rel_errors ** 2)
            reg = (self.regularization_lambda
                   * (phi - self.phi_center) ** 2 / self.phi_max_sq)
            return sse + reg
        else:
            residuals = (model_var - self.market_total_var) ** 2
            return np.sum(self.weights * residuals)


class SSVIFitter:
    """Fit SSVI model to market implied volatilities."""

    def __init__(
        self,
        rho_bounds: Tuple[float, float] = (-0.99, 0.99),
        phi_bounds: Tuple[float, float] = (0.001, 5.0),
        theta_bounds: Tuple[float, float] = (0.001, 10.0),
        optimizer: str = "L-BFGS-B",
        short_dated_ttm_threshold: float = 0.10,
        short_dated_phi_bounds: Tuple[float, float] = (0.001, 20.0),
        very_short_dated_ttm_threshold: float = 0.02,
        very_short_dated_phi_bounds: Tuple[float, float] = (0.001, 200.0),
        use_multi_start: bool = True,
        n_starts: int = 5,
        use_global_optimizer: bool = True,
        global_optimizer: str = "differential_evolution",
        use_relative_error: bool = True,
        regularization_lambda: float = 0.001,
        workers: int = 1,
        local_ftol: float = 1e-10,
        local_maxiter: int = 500,
        early_stop_r2: float = 0.0,
    ):
        """Initialize the SSVI fitter.

        Args:
            rho_bounds: Bounds for rho parameter.
            phi_bounds: Bounds for phi parameter.
            theta_bounds: Bounds for theta parameter.
            optimizer: Scipy optimizer to use.
            short_dated_ttm_threshold: TTM threshold for short-dated options (~36 days).
            short_dated_phi_bounds: Wider phi bounds for short-dated options.
            very_short_dated_ttm_threshold: TTM threshold for very short-dated options (~7 days).
            very_short_dated_phi_bounds: Even wider phi bounds for very short-dated options.
            use_multi_start: Whether to use multi-start optimization.
            n_starts: Number of starting points for multi-start optimization.
            use_global_optimizer: Whether to fall back to global optimizer if needed.
            global_optimizer: Global optimizer to use (differential_evolution).
            use_relative_error: Whether to use relative error in IV space.
            regularization_lambda: Regularization strength for extreme phi values.
            workers: DE worker count. Defaults to 1 — multiprocessing-pool
                overhead dominates the 3-param objective cost.
            local_ftol: SLSQP ftol for the local multi-start passes. Market
                IVs are noisy at ~1e-2 relative, so production loosens this.
            local_maxiter: SLSQP maxiter for the local multi-start passes.
            early_stop_r2: Stop iterating multi-starts once a start reaches
                this R² (0 disables the early stop).
        """
        self.rho_bounds = rho_bounds
        self.phi_bounds = phi_bounds
        self.theta_bounds = theta_bounds
        self.optimizer = optimizer
        self.short_dated_ttm_threshold = short_dated_ttm_threshold
        self.short_dated_phi_bounds = short_dated_phi_bounds
        self.very_short_dated_ttm_threshold = very_short_dated_ttm_threshold
        self.very_short_dated_phi_bounds = very_short_dated_phi_bounds
        self.use_multi_start = use_multi_start
        self.n_starts = n_starts
        self.use_global_optimizer = use_global_optimizer
        self.global_optimizer = global_optimizer
        self.use_relative_error = use_relative_error
        self.regularization_lambda = regularization_lambda
        self.workers = min(max(1, int(workers)), 4)
        self.local_ftol = float(local_ftol)
        self.local_maxiter = int(local_maxiter)
        self.early_stop_r2 = float(early_stop_r2)

    def _generate_initial_guesses(
        self,
        log_moneyness: np.ndarray,
        market_iv: np.ndarray,
        ttm: float,
        phi_bounds: Tuple[float, float]
    ) -> List[np.ndarray]:
        """Generate multiple starting points for optimization.

        Args:
            log_moneyness: Array of log-moneyness values.
            market_iv: Array of market implied volatilities.
            ttm: Time to maturity.
            phi_bounds: Bounds for phi parameter.

        Returns:
            List of initial parameter arrays [theta, rho, phi].
        """
        market_total_var = market_iv ** 2 * ttm

        # Data-driven theta estimate from ATM variance
        atm_idx = np.argmin(np.abs(log_moneyness))
        theta_atm = market_total_var[atm_idx]

        # Estimate rho from left/right wing IV difference
        left_mask = log_moneyness < -0.05
        right_mask = log_moneyness > 0.05
        if np.any(left_mask) and np.any(right_mask):
            left_iv = np.mean(market_iv[left_mask])
            right_iv = np.mean(market_iv[right_mask])
            # Higher left IV -> more negative rho
            iv_diff = (left_iv - right_iv) / (left_iv + right_iv)
            rho_est = np.clip(-iv_diff * 2, -0.8, 0.3)
        else:
            rho_est = -0.3

        # Estimate phi from curvature (quadratic fit)
        if len(log_moneyness) >= 5:
            try:
                coeffs = np.polyfit(log_moneyness, market_iv, 2)
                curvature = coeffs[0]
                atm_iv = market_iv[atm_idx]
                # phi relates to curvature: higher curvature -> higher phi
                phi_est = np.clip(abs(curvature) / atm_iv * 10, phi_bounds[0], phi_bounds[1] / 2)
            except (np.linalg.LinAlgError, ValueError):
                phi_est = 1.0
        else:
            phi_est = 1.0

        # Generate variations. For the very-short-dated regime the aggressive
        # high-phi guesses are ordered right after the data-driven start so a
        # small n_starts still keeps them (steep smiles need high phi).
        if phi_bounds[1] > 20:
            guesses = [
                np.array([theta_atm, rho_est, phi_est]),  # Data-driven
                np.array([theta_atm, rho_est, phi_bounds[1] / 2]),  # Very high phi
                np.array([theta_atm, -0.2, phi_bounds[1] / 3]),  # High phi, moderate skew
                np.array([theta_atm * 1.2, rho_est, phi_bounds[1] / 2]),  # Higher theta + high phi
                np.array([theta_atm, rho_est, phi_bounds[1] / 4]),  # Higher phi
                np.array([theta_atm * 0.8, rho_est, phi_est * 2]),  # Lower theta, higher phi
                np.array([theta_atm, -0.5, 2.0]),  # Higher skew/curvature
                np.array([theta_atm, -0.3, 0.5]),  # Conservative
            ]
            # Historically forced to 8 starts; honor a configured n_starts >= 8
            # but rely on early_stop_r2 to cut the tail in the common case.
            return guesses[:max(self.n_starts, 8)]

        guesses = [
            np.array([theta_atm, rho_est, phi_est]),  # Data-driven
            np.array([theta_atm, -0.3, 0.5]),  # Conservative
            np.array([theta_atm, -0.5, 2.0]),  # Higher skew/curvature
            np.array([theta_atm, rho_est, phi_bounds[1] / 4]),  # Higher phi
            np.array([theta_atm * 0.8, rho_est, phi_est * 2]),  # Lower theta, higher phi
        ]
        return guesses[:max(self.n_starts, 1)]

    def _compute_model_iv(
        self,
        params: np.ndarray,
        log_moneyness: np.ndarray,
        ttm: float
    ) -> np.ndarray:
        """Compute model implied volatility from parameters.

        Args:
            params: Array [theta, rho, phi].
            log_moneyness: Array of log-moneyness values.
            ttm: Time to maturity.

        Returns:
            Array of model implied volatilities.
        """
        theta, rho, phi = params
        term = phi * log_moneyness + rho
        sqrt_term = np.sqrt(np.maximum(term ** 2 + 1 - rho ** 2, 1e-10))
        model_var = 0.5 * theta * (1 + rho * phi * log_moneyness + sqrt_term)
        return np.sqrt(np.maximum(model_var / ttm, 1e-10))

    def _create_objective(
        self,
        log_moneyness: np.ndarray,
        market_iv: np.ndarray,
        ttm: float,
        weights: np.ndarray,
        phi_bounds: Tuple[float, float] = (0.001, 5.0)
    ) -> Callable[[np.ndarray], float]:
        """Create the objective function.

        Returns an _SSVIObjective instance which is picklable, allowing
        differential_evolution to use workers=-1 for parallel population
        evaluation.

        Args:
            log_moneyness: Array of log-moneyness values.
            market_iv: Array of market implied volatilities.
            ttm: Time to maturity.
            weights: Fitting weights.
            phi_bounds: Phi bounds (used to set regularization center).

        Returns:
            Picklable callable objective.
        """
        return _SSVIObjective(
            log_moneyness, market_iv, ttm, weights,
            phi_bounds, self.use_relative_error, self.regularization_lambda
        )

    def _run_local_optimization(
        self,
        objective,
        x0: np.ndarray,
        bounds: List[Tuple[float, float]],
        butterfly_constraint
    ) -> Tuple[np.ndarray, float, bool]:
        """Run local optimization from a starting point.

        Args:
            objective: Objective function.
            x0: Initial guess.
            bounds: Parameter bounds.
            butterfly_constraint: Butterfly constraint function.

        Returns:
            Tuple of (best_params, best_value, success).
        """
        constraints = [{"type": "ineq", "fun": butterfly_constraint}]

        result = minimize(
            objective,
            x0,
            method="SLSQP",
            bounds=bounds,
            constraints=constraints,
            options={"maxiter": self.local_maxiter, "ftol": self.local_ftol}
        )

        if not result.success:
            # Try without constraints as fallback
            result = minimize(
                objective,
                x0,
                method=self.optimizer,
                bounds=bounds,
                options={"maxiter": self.local_maxiter}
            )

        return result.x, result.fun, result.success

    def _run_global_optimization(
        self,
        objective,
        bounds: List[Tuple[float, float]],
        butterfly_constraint
    ) -> Tuple[np.ndarray, float, bool]:
        """Run global optimization using differential evolution.

        Args:
            objective: Objective function.
            bounds: Parameter bounds.
            butterfly_constraint: Butterfly constraint function.

        Returns:
            Tuple of (best_params, best_value, success).
        """
        # Create nonlinear constraint for both butterfly conditions
        nlc = NonlinearConstraint(
            lambda x: np.array([
                4 - x[0] * (1 + abs(x[1])) * x[2],
                4 - x[0] * (1 + abs(x[1])) * x[2] ** 2,
            ]),
            0, np.inf
        )

        result = differential_evolution(
            objective,
            bounds=bounds,
            constraints=nlc,
            maxiter=200,
            polish=True,
            seed=42,
            workers=self.workers,
            updating='deferred'
        )

        return result.x, result.fun, result.success

    def _calculate_fit_stats(
        self,
        params: SSVIParams,
        log_moneyness: np.ndarray,
        market_iv: np.ndarray
    ) -> FitStats:
        """Calculate fit statistics.

        Args:
            params: Fitted SSVI parameters.
            log_moneyness: Array of log-moneyness values.
            market_iv: Array of market implied volatilities.

        Returns:
            FitStats with r_squared, rmse, and max_residual.
        """
        model = SSVIModel(params)
        model_iv = model.implied_volatility_array(log_moneyness)
        return calculate_fit_stats(model_iv, market_iv)

    def fit(
        self,
        log_moneyness: np.ndarray,
        market_iv: np.ndarray,
        ttm: float,
        weights: Optional[np.ndarray] = None
    ) -> SSVIFitResult:
        """Fit SSVI model to market data.

        Args:
            log_moneyness: Array of log-moneyness values ln(K/F).
            market_iv: Array of market implied volatilities.
            ttm: Time to maturity in years.
            weights: Optional weights for fitting.

        Returns:
            SSVIFitResult with fitted parameters and diagnostics.
        """
        if len(log_moneyness) != len(market_iv):
            raise ValueError("log_moneyness and market_iv must have same length")

        if len(log_moneyness) < 3:
            return SSVIFitResult(
                params=None,
                success=False,
                r_squared=0.0,
                rmse=float('inf'),
                max_residual=float('inf'),
                n_points=len(log_moneyness),
                message="Need at least 3 data points for fitting"
            )

        if weights is None:
            weights = np.ones(len(log_moneyness))

        # Select phi bounds based on TTM (tiered for short-dated options)
        # Very short-dated options need extremely high phi to capture steep smiles
        ttm_days = ttm * DAYS_PER_YEAR
        if ttm < self.very_short_dated_ttm_threshold:
            phi_bounds = self.very_short_dated_phi_bounds
            logger.info(f"SSVI fit: very short-dated regime (TTM={ttm_days:.1f}d), phi_bounds={phi_bounds}")
        elif ttm < self.short_dated_ttm_threshold:
            phi_bounds = self.short_dated_phi_bounds
            logger.info(f"SSVI fit: short-dated regime (TTM={ttm_days:.1f}d), phi_bounds={phi_bounds}")
        else:
            phi_bounds = self.phi_bounds
            logger.info(f"SSVI fit: standard regime (TTM={ttm_days:.1f}d), phi_bounds={phi_bounds}")

        # Scale theta bounds by TTM relative to a reference (3 months)
        # This ensures short-dated options can have appropriately small theta values
        # θ (total variance) = σ² × T, so expected θ scales linearly with TTM
        reference_ttm = 0.25  # 3 months reference
        ttm_scale = max(ttm / reference_ttm, 0.01)  # Floor to prevent numerical issues
        theta_lower = self.theta_bounds[0] * ttm_scale
        theta_upper = self.theta_bounds[1] * ttm_scale
        scaled_theta_bounds = (theta_lower, theta_upper)

        # Set up bounds
        bounds = [
            scaled_theta_bounds,
            self.rho_bounds,
            phi_bounds
        ]

        # Butterfly constraint (min of both Gatheral-Jacquier conditions)
        def butterfly_constraint(params: np.ndarray) -> float:
            theta, rho, phi = params
            factor = theta * (1 + abs(rho))
            return min(4 - factor * phi, 4 - factor * phi ** 2)

        # Create objective function (pass phi_bounds for adaptive regularization)
        objective = self._create_objective(log_moneyness, market_iv, ttm, weights, phi_bounds)

        # Generate initial guesses
        initial_guesses = self._generate_initial_guesses(
            log_moneyness, market_iv, ttm, phi_bounds
        )

        best_params = None
        best_value = float('inf')
        best_success = False

        # Multi-start local optimization
        if self.use_multi_start:
            for x0 in initial_guesses:
                # Clip initial guess to scaled bounds
                x0[0] = np.clip(x0[0], scaled_theta_bounds[0], scaled_theta_bounds[1])
                x0[1] = np.clip(x0[1], self.rho_bounds[0], self.rho_bounds[1])
                x0[2] = np.clip(x0[2], phi_bounds[0], phi_bounds[1])

                try:
                    params_opt, value, success = self._run_local_optimization(
                        objective, x0, bounds, butterfly_constraint
                    )
                    if value < best_value:
                        best_params = params_opt
                        best_value = value
                        best_success = success
                except (ValueError, RuntimeError, np.linalg.LinAlgError) as e:
                    logger.debug(f"Local optimization failed for starting point: {e}")
                    continue

                # Early stop: once a start fits well enough, further starts
                # only re-discover the same basin. Guarded by config.
                if self.early_stop_r2 > 0 and best_params is not None:
                    try:
                        stats = self._calculate_fit_stats(
                            SSVIParams(
                                theta=best_params[0],
                                rho=best_params[1],
                                phi=best_params[2],
                                ttm=ttm,
                            ),
                            log_moneyness,
                            market_iv,
                        )
                        if stats.r_squared >= self.early_stop_r2:
                            break
                    except ValueError:
                        pass
        else:
            # Single start
            x0 = initial_guesses[0]
            x0[0] = np.clip(x0[0], scaled_theta_bounds[0], scaled_theta_bounds[1])
            x0[1] = np.clip(x0[1], self.rho_bounds[0], self.rho_bounds[1])
            x0[2] = np.clip(x0[2], phi_bounds[0], phi_bounds[1])

            best_params, best_value, best_success = self._run_local_optimization(
                objective, x0, bounds, butterfly_constraint
            )

        # Check if we need global optimization fallback
        need_global = False
        if best_params is not None:
            try:
                temp_params = SSVIParams(
                    theta=best_params[0],
                    rho=best_params[1],
                    phi=best_params[2],
                    ttm=ttm
                )
                fit_stats = self._calculate_fit_stats(
                    temp_params, log_moneyness, market_iv
                )
                need_global = fit_stats.r_squared < 0.85
            except ValueError:
                need_global = True
        else:
            need_global = True

        # Global optimizer fallback
        if need_global and self.use_global_optimizer:
            try:
                global_params, global_value, global_success = self._run_global_optimization(
                    objective, bounds, butterfly_constraint
                )
                if global_value < best_value:
                    best_params = global_params
                    best_value = global_value
                    best_success = global_success
            except (ValueError, RuntimeError) as e:
                logger.debug(f"Global optimization failed, keeping best local result: {e}")

        if best_params is None:
            return SSVIFitResult(
                params=None,
                success=False,
                r_squared=0.0,
                rmse=float('inf'),
                max_residual=float('inf'),
                n_points=len(log_moneyness),
                message="Optimization failed for all starting points"
            )

        theta_fit, rho_fit, phi_fit = best_params

        # Create parameters object
        try:
            params = SSVIParams(
                theta=theta_fit,
                rho=rho_fit,
                phi=phi_fit,
                ttm=ttm
            )
        except ValueError as e:
            return SSVIFitResult(
                params=None,
                success=False,
                r_squared=0.0,
                rmse=float('inf'),
                max_residual=float('inf'),
                n_points=len(log_moneyness),
                message=f"Invalid parameters: {e}"
            )

        # Calculate fit statistics
        fit_stats = self._calculate_fit_stats(params, log_moneyness, market_iv)

        # Post-fit butterfly validation: reject arbitrageable params
        butterfly_ok = params.butterfly_condition()
        if not butterfly_ok:
            factor = params.theta * (1 + abs(params.rho))
            logger.warning(
                f"SSVI fit rejected: butterfly violated "
                f"(cond1={factor * params.phi:.4f}, cond2={factor * params.phi ** 2:.4f}, limit=4)"
            )

        success = (best_success or fit_stats.r_squared > 0.8) and butterfly_ok

        return SSVIFitResult(
            params=params,
            success=success,
            r_squared=fit_stats.r_squared,
            rmse=fit_stats.rmse,
            max_residual=fit_stats.max_residual,
            n_points=len(log_moneyness),
            message="Optimization completed" + (" (global optimizer used)" if need_global and self.use_global_optimizer else "")
                + ("" if butterfly_ok else " [butterfly violated]")
        )


# =============================================================================
# SSVI Surface Fit (Gatheral & Jacquier 2014)
# =============================================================================
# Proper SSVI surface parametrization with:
# - Global ρ (skew) shared across all slices
# - φ(θ) = η·θ^(-λ) power-law curvature (global η, λ)
# - Monotone increasing θ(T) ATM total variance term structure
# - Per-slice butterfly constraints: θ·φ(θ)·(1+|ρ|) ≤ 4 AND θ·φ(θ)²·(1+|ρ|) ≤ 4


@dataclass
class SSVISliceData:
    """Market data for one expiry slice."""
    expiry_name: str
    ttm: float
    log_moneyness: np.ndarray
    market_iv: np.ndarray
    forward: float
    weights: Optional[np.ndarray] = None


@dataclass
class SSVISurfaceParams:
    """Global SSVI surface parameters (Gatheral & Jacquier 2014).

    The surface formula:
        w(k, T) = θ(T)/2 · [1 + ρ·φ(θ)·k + √((φ(θ)·k + ρ)² + 1 - ρ²)]
    where φ(θ) = η·θ^(-λ), 0 ≤ λ ≤ 0.5, η > 0.
    """
    rho: float                # Global skew
    eta: float                # Power-law scale: φ(θ) = η·θ^(-λ)
    lam: float                # Power-law exponent (0 ≤ λ ≤ 0.5)
    thetas: List[float]       # Per-expiry θ values, monotone increasing
    ttms: List[float]         # Corresponding TTMs, sorted ascending
    expiry_names: List[str]   # Labels

    def __post_init__(self):
        """Validate parameters."""
        if not -1 < self.rho < 1:
            raise ValueError(f"rho must be in (-1, 1), got {self.rho}")
        if self.eta <= 0:
            raise ValueError(f"eta must be positive, got {self.eta}")
        if not 0 <= self.lam <= 0.5:
            raise ValueError(f"lam must be in [0, 0.5], got {self.lam}")
        if len(self.thetas) != len(self.ttms):
            raise ValueError("thetas and ttms must have same length")
        if len(self.thetas) != len(self.expiry_names):
            raise ValueError("thetas and expiry_names must have same length")
        # Validate monotonicity
        for i in range(1, len(self.thetas)):
            if self.thetas[i] < self.thetas[i - 1]:
                raise ValueError(
                    f"thetas must be monotone increasing, but "
                    f"theta[{i}]={self.thetas[i]:.6f} < theta[{i-1}]={self.thetas[i-1]:.6f}"
                )

    def phi(self, theta: float) -> float:
        """Compute curvature φ(θ) = η·θ^(-λ)."""
        return self.eta * theta ** (-self.lam)

    def to_per_slice_params(self) -> List[SSVIParams]:
        """Decompose to per-slice SSVIParams for downstream compatibility."""
        result = []
        for theta, ttm in zip(self.thetas, self.ttms):
            result.append(SSVIParams(
                theta=theta,
                rho=self.rho,
                phi=self.phi(theta),
                ttm=ttm,
            ))
        return result

    def interpolate_theta(self, ttm: float) -> float:
        """PCHIP interpolation in log-TTM space for arbitrary TTM.

        Uses monotone cubic Hermite splines (C^1 smooth, no overshoot).
        Extrapolation policy:
        - Short end (ttm below fitted range): theta scales linearly with ttm
          using the nearest fitted ATM variance (theta/ttm). This ensures
          theta -> 0 as ttm -> 0 and avoids overstating near-expiry variance.
        - Long end (ttm above fitted range): flat extrapolation.
        """
        if ttm <= 0:
            raise ValueError(f"ttm must be positive, got {ttm}")

        if len(self.ttms) == 1:
            theta0 = self.thetas[0]
            ttm0 = self.ttms[0]
            if ttm <= ttm0:
                return theta0 * max(ttm / ttm0, 1e-12)
            return theta0

        log_ttms = np.log(self.ttms)
        log_target = np.log(ttm)

        # Short-end extrapolation: keep nearest ATM variance and scale theta with time
        if log_target <= log_ttms[0]:
            return self.thetas[0] * max(ttm / self.ttms[0], 1e-12)

        # Long-end extrapolation: flat
        if log_target >= log_ttms[-1]:
            return self.thetas[-1]

        pchip = PchipInterpolator(log_ttms, self.thetas, extrapolate=False)
        return float(pchip(log_target))

    def get_params_for_ttm(self, ttm: float) -> SSVIParams:
        """Get SSVIParams for an arbitrary TTM via interpolation."""
        theta = self.interpolate_theta(ttm)
        return SSVIParams(theta=theta, rho=self.rho, phi=self.phi(theta), ttm=ttm)

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "rho": float(self.rho),
            "eta": float(self.eta),
            "lam": float(self.lam),
            "thetas": [float(t) for t in self.thetas],
            "ttms": [float(t) for t in self.ttms],
            "expiry_names": list(self.expiry_names),
            "per_slice": [p.to_dict() for p in self.to_per_slice_params()],
        }


@dataclass
class SSVISurfaceFitResult:
    """Result of SSVI surface fitting."""
    params: Optional[SSVISurfaceParams]
    per_slice_params: List[SSVIParams]  # Decomposed for downstream use
    success: bool
    aggregate_r_squared: float
    per_slice_r_squared: List[float]
    message: str
    fit_method: str = "full_de"
    warm_start_used: bool = False


class _SSVISurfaceObjective:
    """Picklable objective for SSVI surface joint fitting.

    Optimizes x = [ρ, η, λ, δ₁, ..., δₙ] where θᵢ = Σⱼ₌₁ⁱ δⱼ².

    The evaluation is fully vectorized over the concatenated point set: the
    differential-evolution passes call this 10-30k times per fit, and the old
    per-slice Python loop spent most of that in numpy dispatch overhead. Same
    math — per-slice error sums are accumulated in ascending slice order, so
    results match the loop implementation to float summation noise (≤1e-9
    relative; see tests/test_ssvi_surface_objective.py).
    """

    def __init__(self, slices_data, use_relative_error):
        # Store per-slice data as plain arrays for pickle
        self.n_slices = len(slices_data)
        self.log_moneyness = [s.log_moneyness for s in slices_data]
        self.market_iv = [s.market_iv for s in slices_data]
        self.ttms = [s.ttm for s in slices_data]
        self.weights = [
            s.weights if s.weights is not None else np.ones(len(s.market_iv))
            for s in slices_data
        ]
        self.use_relative_error = use_relative_error

        # Precomputed concatenated arrays for the vectorized evaluation.
        self._k_all = np.concatenate([np.asarray(k, dtype=float) for k in self.log_moneyness])
        self._iv_all = np.concatenate([np.asarray(v, dtype=float) for v in self.market_iv])
        self._w_all = np.concatenate([np.asarray(w, dtype=float) for w in self.weights])
        self._inv_ttm_all = np.concatenate([
            np.full(len(k), 1.0 / ttm)
            for k, ttm in zip(self.log_moneyness, self.ttms)
        ])
        self._slice_ids = np.concatenate([
            np.full(len(k), i, dtype=np.int64)
            for i, k in enumerate(self.log_moneyness)
        ])

    def _unpack(self, x):
        """Unpack optimization vector into (rho, eta, lam, thetas)."""
        rho = x[0]
        eta = x[1]
        lam = x[2]
        deltas = x[3:]
        # Monotone reparametrization: θᵢ = Σⱼ₌₁ⁱ δⱼ²
        thetas = np.cumsum(deltas ** 2)
        return rho, eta, lam, thetas

    def __call__(self, x):
        rho, eta, lam, thetas = self._unpack(x)

        if np.any(thetas <= 0):
            return 1e10

        phis = eta * thetas ** (-lam)
        theta_pt = thetas[self._slice_ids]
        phi_pt = phis[self._slice_ids]

        # SSVI total variance, all points at once
        term = phi_pt * self._k_all + rho
        sqrt_term = np.sqrt(np.maximum(term ** 2 + 1 - rho ** 2, 1e-10))
        model_var = 0.5 * theta_pt * (1 + rho * phi_pt * self._k_all + sqrt_term)
        model_iv = np.sqrt(np.maximum(model_var * self._inv_ttm_all, 1e-10))

        if self.use_relative_error:
            rel_errors = (model_iv - self._iv_all) / self._iv_all
            point_terms = self._w_all * rel_errors ** 2
        else:
            point_terms = self._w_all * (model_iv - self._iv_all) ** 2
        # Per-slice partial sums in ascending slice order (points are stored
        # slice-contiguous), matching the loop implementation's accumulation.
        total_error = float(np.sum(np.add.reduceat(
            point_terms, np.searchsorted(self._slice_ids, np.arange(self.n_slices))
        )))

        # Butterfly penalty per slice (both conditions), vectorized
        factor = thetas * (1 + abs(rho))
        b1_excess = np.maximum(factor * phis - 4, 0.0)
        b2_excess = np.maximum(factor * phis ** 2 - 4, 0.0)
        penalty = 1000.0 * (np.sum(b1_excess ** 2) + np.sum(b2_excess ** 2))

        return total_error + penalty


class SSVISurfaceFitter:
    """Joint SSVI surface fitter (Gatheral & Jacquier 2014).

    Fits global (ρ, η, λ) and monotone θ(T) across multiple expiry slices.
    Uses per-slice SSVIFitter for warm-starting, then joint differential_evolution.
    Falls back to independent per-slice fits if joint fit degrades quality.
    """

    WARM_START_MAX_AGE_SEC = 600  # Discard cached surface params older than 10 minutes

    def __init__(
        self,
        per_slice_fitter: SSVIFitter,
        rho_bounds: Tuple[float, float] = (-0.99, 0.99),
        eta_bounds: Tuple[float, float] = (0.01, 10.0),
        lam_bounds: Tuple[float, float] = (0.0, 0.5),
        maxiter: int = 300,
        workers: int = 1,
        use_relative_error: bool = True,
        fallback_to_independent: bool = True,
        min_acceptable_surface_r2: float = 0.85,
        per_slice_parallel: int = 4,
        warm_start_path: Optional[Path] = None,
        defer_per_slice_fits: bool = False,
        full_refit_every_n: int = 50,
        local_first: bool = False,
    ):
        self.per_slice_fitter = per_slice_fitter
        self.rho_bounds = rho_bounds
        self.eta_bounds = eta_bounds
        self.lam_bounds = lam_bounds
        self.maxiter = maxiter
        self.workers = min(workers, 4)  # Cap at 4 per CLAUDE.md
        self.use_relative_error = use_relative_error
        self.fallback_to_independent = fallback_to_independent
        self.min_acceptable_surface_r2 = min_acceptable_surface_r2
        self.per_slice_parallel = min(max(1, int(per_slice_parallel)), 4)
        self.warm_start_path = Path(warm_start_path) if warm_start_path else None
        # Warm-start-first: try the previous surface (locally re-optimized)
        # BEFORE spending the per-slice multi-start fits; run them only when
        # the warm candidate is rejected. Guarded by an absolute R² floor and
        # a periodic forced full re-fit so the surface can't drift on its own
        # baseline.
        self.defer_per_slice_fits = bool(defer_per_slice_fits)
        self.full_refit_every_n = max(0, int(full_refit_every_n))
        self.local_first = bool(local_first)
        self._deferred_accept_count = 0

    def _load_warm_start(self) -> Optional[Tuple[float, float, float]]:
        """Load cached (rho, eta, lambda) if fresh and inside current bounds."""
        if not self.warm_start_path or not self.warm_start_path.exists():
            return None
        try:
            payload = json.loads(self.warm_start_path.read_text(encoding="utf-8"))
            rho = float(payload["rho"])
            eta = float(payload["eta"])
            lam = float(payload["lam"])
            ts = float(payload.get("timestamp", 0.0))
        except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError) as exc:
            logger.debug(f"SSVI warm-start unreadable, ignoring: {exc}")
            return None

        if time.time() - ts > self.WARM_START_MAX_AGE_SEC:
            logger.debug("SSVI warm-start stale, ignoring")
            return None

        in_bounds = (
            self.rho_bounds[0] <= rho <= self.rho_bounds[1]
            and self.eta_bounds[0] <= eta <= self.eta_bounds[1]
            and self.lam_bounds[0] <= lam <= self.lam_bounds[1]
        )
        if not in_bounds:
            logger.debug("SSVI warm-start outside current bounds, ignoring")
            return None
        return rho, eta, lam

    def _save_warm_start(self, rho: float, eta: float, lam: float) -> None:
        """Atomically persist (rho, eta, lambda) to the warm-start cache."""
        if not self.warm_start_path:
            return
        payload = {
            "rho": float(rho),
            "eta": float(eta),
            "lam": float(lam),
            "timestamp": time.time(),
        }
        try:
            self.warm_start_path.parent.mkdir(parents=True, exist_ok=True)
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                dir=str(self.warm_start_path.parent),
                delete=False,
            ) as tmp:
                json.dump(payload, tmp)
                tmp_name = tmp.name
            os.replace(tmp_name, self.warm_start_path)
        except OSError as exc:
            logger.warning(f"Could not write SSVI warm-start cache: {exc}")

    def _compute_slice_r_squared(
        self,
        params: SSVISurfaceParams,
        slices: List[SSVISliceData],
    ) -> List[float]:
        """Compute per-slice R² for surface params."""
        r_squareds = []
        per_slice = params.to_per_slice_params()
        for sp, sl in zip(per_slice, slices):
            model = SSVIModel(sp)
            model_iv = model.implied_volatility_array(sl.log_moneyness)
            ss_res = np.sum((model_iv - sl.market_iv) ** 2)
            ss_tot = np.sum((sl.market_iv - np.mean(sl.market_iv)) ** 2)
            r2 = 1 - ss_res / ss_tot if ss_tot > 0 else 0.0
            r_squareds.append(r2)
        return r_squareds

    def _aggregate_r_squared(
        self,
        per_slice_r_squared: List[float],
        point_counts: List[int],
    ) -> float:
        """Compute point-count weighted aggregate R-squared."""
        total_points = sum(point_counts)
        if total_points <= 0:
            return 0.0
        return sum(
            r2 * n for r2, n in zip(per_slice_r_squared, point_counts)
        ) / total_points

    def _surface_vector_to_params(
        self,
        x: np.ndarray,
        slices: List[SSVISliceData],
    ) -> SSVISurfaceParams:
        """Convert optimizer vector [rho, eta, lam, deltas...] to surface params."""
        thetas = list(np.cumsum(x[3:] ** 2))
        return SSVISurfaceParams(
            rho=float(x[0]),
            eta=float(x[1]),
            lam=float(x[2]),
            thetas=thetas,
            ttms=[s.ttm for s in slices],
            expiry_names=[s.expiry_name for s in slices],
        )

    def _surface_params_to_vector(
        self,
        params: SSVISurfaceParams,
        slices: List[SSVISliceData],
    ) -> Optional[np.ndarray]:
        """Convert previous surface params to optimizer vector if slices match."""
        expiry_names = [s.expiry_name for s in slices]
        if list(params.expiry_names) != expiry_names:
            return None
        if len(params.thetas) != len(slices):
            return None

        thetas = np.array(params.thetas, dtype=float)
        if np.any(thetas <= 0):
            return None
        increments = np.diff(np.concatenate([[0.0], thetas]))
        if np.any(increments < 0):
            return None
        deltas = np.sqrt(np.maximum(increments, 1e-12))
        return np.concatenate([[params.rho, params.eta, params.lam], deltas])

    def _centered_bounds(
        self,
        x_center: np.ndarray,
        full_bounds: List[Tuple[float, float]],
    ) -> Optional[List[Tuple[float, float]]]:
        """Build bounded search box centered on previous surface parameters."""
        if len(x_center) != len(full_bounds):
            return None

        def _clip_interval(lo, hi, base_lo, base_hi):
            lo = max(base_lo, lo)
            hi = min(base_hi, hi)
            if lo >= hi:
                return None
            return (float(lo), float(hi))

        centered = [
            _clip_interval(x_center[0] - 0.20, x_center[0] + 0.20, *full_bounds[0]),
            _clip_interval(x_center[1] * 0.5, x_center[1] * 2.0, *full_bounds[1]),
            _clip_interval(x_center[2] - 0.15, x_center[2] + 0.15, *full_bounds[2]),
        ]
        if any(bounds is None for bounds in centered):
            return None

        for i, delta in enumerate(x_center[3:], start=3):
            bounds = _clip_interval(
                max(1e-6, delta * 0.5),
                max(1e-6, delta * 1.5),
                *full_bounds[i],
            )
            if bounds is None:
                return None
            centered.append(bounds)

        return centered

    def _run_surface_de(
        self,
        objective,
        bounds: List[Tuple[float, float]],
        x0: np.ndarray,
        *,
        maxiter: int,
        popsize: int = 15,
    ):
        """Run differential evolution for the joint surface objective."""
        return differential_evolution(
            objective,
            bounds=bounds,
            x0=x0,
            maxiter=maxiter,
            popsize=popsize,
            polish=True,
            seed=42,
            workers=self.workers,
            updating='deferred',
        )

    def _evaluate_surface_candidate(
        self,
        x: np.ndarray,
        slices: List[SSVISliceData],
        indep_agg_r2: float,
        indep_r2s: List[float],
        *,
        aggregate_tolerance: float,
        slice_tolerance: Optional[float] = None,
    ) -> Tuple[Optional[SSVISurfaceParams], float, List[float], Optional[str]]:
        """Validate and score an optimizer candidate."""
        try:
            surface_params = self._surface_vector_to_params(x, slices)
        except ValueError as e:
            return None, indep_agg_r2, indep_r2s, f"Invalid surface params: {e}"

        surface_r2s = self._compute_slice_r_squared(surface_params, slices)
        surface_n_points = [len(s.log_moneyness) for s in slices]
        surface_agg_r2 = self._aggregate_r_squared(surface_r2s, surface_n_points)

        if (
            self.fallback_to_independent
            and surface_agg_r2 < indep_agg_r2 - aggregate_tolerance
            and surface_agg_r2 < self.min_acceptable_surface_r2
        ):
            return (
                None,
                surface_agg_r2,
                surface_r2s,
                f"Surface R2={surface_agg_r2:.4f} worse than independent R2={indep_agg_r2:.4f}",
            )

        if slice_tolerance is not None:
            for surface_r2, indep_r2 in zip(surface_r2s, indep_r2s):
                if surface_r2 < indep_r2 - slice_tolerance:
                    return (
                        None,
                        surface_agg_r2,
                        surface_r2s,
                        f"Surface slice R2={surface_r2:.4f} worse than independent R2={indep_r2:.4f}",
                    )

        return surface_params, surface_agg_r2, surface_r2s, None

    def _try_deferred_warm_fit(
        self,
        slices: List[SSVISliceData],
        warm_start_surface_params: SSVISurfaceParams,
    ) -> Optional[SSVISurfaceFitResult]:
        """Warm-start-first pass: accept the re-optimized previous surface
        without running per-slice fits.

        The acceptance baseline is the previous surface's per-slice R² on the
        CURRENT slices (one vectorized pass) with the standard gates
        (aggregate tol 0.005, slice tol 0.03) plus the absolute
        ``min_acceptable_surface_r2`` floor. Returns None on any rejection so
        the caller falls through to the full Step-1 path unchanged.
        """
        if (
            self.full_refit_every_n > 0
            and self._deferred_accept_count >= self.full_refit_every_n
        ):
            self._deferred_accept_count = 0
            logger.info(
                "SSVI surface deferred warm-start: periodic full re-fit due "
                f"(every {self.full_refit_every_n} accepted warm fits)"
            )
            return None

        warm_x0 = self._surface_params_to_vector(warm_start_surface_params, slices)
        if warm_x0 is None:
            return None
        n = len(slices)
        delta_bounds = [(0.001, 5.0)] * n
        bounds = [self.rho_bounds, self.eta_bounds, self.lam_bounds] + delta_bounds
        warm_bounds = self._centered_bounds(warm_x0, bounds)
        if warm_bounds is None:
            return None

        try:
            baseline_r2s = self._compute_slice_r_squared(warm_start_surface_params, slices)
        except (ValueError, ZeroDivisionError) as e:
            logger.debug(f"deferred warm-start baseline failed: {e}")
            return None
        n_points = [len(s.log_moneyness) for s in slices]
        baseline_agg = self._aggregate_r_squared(baseline_r2s, n_points)

        objective = _SSVISurfaceObjective(slices, self.use_relative_error)
        warm_x0 = np.array([
            np.clip(value, low, high)
            for value, (low, high) in zip(warm_x0, warm_bounds)
        ])
        try:
            warm_result = self._run_surface_de(
                objective,
                warm_bounds,
                warm_x0,
                maxiter=max(40, self.maxiter // 4),
                popsize=8,
            )
        except (ValueError, RuntimeError) as e:
            logger.warning(f"SSVI surface deferred warm-start DE failed: {e}")
            return None

        params, agg_r2, r2s, reject_reason = self._evaluate_surface_candidate(
            warm_result.x,
            slices,
            baseline_agg,
            baseline_r2s,
            aggregate_tolerance=0.005,
            slice_tolerance=0.03,
        )
        if params is None:
            logger.info(f"SSVI surface deferred warm-start rejected: {reject_reason}")
            return None
        if agg_r2 < self.min_acceptable_surface_r2:
            logger.info(
                f"SSVI surface deferred warm-start below floor: "
                f"R²={agg_r2:.4f} < {self.min_acceptable_surface_r2}"
            )
            return None

        self._deferred_accept_count += 1
        self._save_warm_start(params.rho, params.eta, params.lam)
        logger.info(
            f"SSVI surface deferred warm fit: ρ={params.rho:.3f}, "
            f"η={params.eta:.3f}, λ={params.lam:.3f}, R²={agg_r2:.4f} "
            f"(baseline {baseline_agg:.4f}, per-slice fits skipped)"
        )
        return SSVISurfaceFitResult(
            params=params,
            per_slice_params=params.to_per_slice_params(),
            success=True,
            aggregate_r_squared=agg_r2,
            per_slice_r_squared=r2s,
            message="Surface fit successful (deferred warm start)",
            fit_method="warm_start_deferred",
            warm_start_used=True,
        )

    def fit(
        self,
        slices: List[SSVISliceData],
        warm_start_surface_params: Optional[SSVISurfaceParams] = None,
        per_slice_cache: Optional[dict] = None,
    ) -> SSVISurfaceFitResult:
        """Fit SSVI surface jointly across expiry slices.

        Step 0 (optional, ``defer_per_slice_fits``): try the warm-start
                candidate first — validated against the previous surface's
                per-slice R² on the CURRENT data — and skip Step 1 entirely
                when accepted.
        Step 1: Per-slice warm start using existing SSVIFitter
                (memoized across retry attempts via ``per_slice_cache``).
        Step 2: Construct initial x0 from per-slice results.
        Step 3: Joint differential_evolution (optionally preceded by a cheap
                L-BFGS-B pass when ``local_first``).
        Step 4: Validate vs per-slice — fall back if aggregate R² drops.
        """
        if len(slices) < 2:
            return SSVISurfaceFitResult(
                params=None,
                per_slice_params=[],
                success=False,
                aggregate_r_squared=0.0,
                per_slice_r_squared=[],
                message=f"Need at least 2 slices, got {len(slices)}",
            )

        # Sort slices by TTM ascending
        slices = sorted(slices, key=lambda s: s.ttm)

        # Step 0: warm-start-first — skip the per-slice fits when the
        # previous surface (re-optimized in a centered box) still explains
        # the current data.
        if self.defer_per_slice_fits and warm_start_surface_params is not None:
            deferred = self._try_deferred_warm_fit(slices, warm_start_surface_params)
            if deferred is not None:
                return deferred

        # Step 1: Per-slice warm start (parallel — per-slice DE is serial, so threads give real speedup)
        n_slices = len(slices)
        pool_size = min(self.per_slice_parallel, n_slices)
        per_slice_results = [None] * n_slices
        if per_slice_cache is None:
            per_slice_cache = {}
        pending = [
            i for i, sl in enumerate(slices)
            if sl.expiry_name not in per_slice_cache
        ]
        for i, sl in enumerate(slices):
            if sl.expiry_name in per_slice_cache:
                per_slice_results[i] = per_slice_cache[sl.expiry_name]
        if pending and min(pool_size, len(pending)) > 1:
            def _fit_one(i: int):
                sl = slices[i]
                return i, self.per_slice_fitter.fit(
                    sl.log_moneyness, sl.market_iv, sl.ttm, sl.weights
                )
            with ThreadPoolExecutor(max_workers=min(pool_size, len(pending))) as pool:
                for i, result in pool.map(_fit_one, pending):
                    per_slice_results[i] = result
                    per_slice_cache[slices[i].expiry_name] = result
        else:
            for i in pending:
                sl = slices[i]
                result = self.per_slice_fitter.fit(
                    sl.log_moneyness, sl.market_iv, sl.ttm, sl.weights
                )
                per_slice_results[i] = result
                per_slice_cache[sl.expiry_name] = result
        per_slice_params_indep = [
            r.params if r.success and r.params is not None else None
            for r in per_slice_results
        ]

        # Need at least 2 successful per-slice fits to warm start
        successful = [p for p in per_slice_params_indep if p is not None]
        if len(successful) < 2:
            return SSVISurfaceFitResult(
                params=None,
                per_slice_params=[p for p in per_slice_params_indep if p is not None],
                success=False,
                aggregate_r_squared=0.0,
                per_slice_r_squared=[
                    r.r_squared if r.success else 0.0 for r in per_slice_results
                ],
                message="Fewer than 2 per-slice fits succeeded",
            )

        # Compute independent per-slice aggregate R² for fallback comparison
        indep_r2s = [r.r_squared if r.success else 0.0 for r in per_slice_results]
        indep_n_points = [r.n_points for r in per_slice_results]
        indep_agg_r2 = self._aggregate_r_squared(indep_r2s, indep_n_points)

        # Step 2: Construct initial guess
        rho_init, eta_init, lam_init, delta_init = self._construct_initial_guess(
            slices, per_slice_params_indep
        )

        # Disk warm-start is a fallback for process restarts/subprocess mode.
        # The in-memory warm_start_surface_params path below remains primary.
        if warm_start_surface_params is None:
            cached = self._load_warm_start()
            if cached is not None:
                rho_init, eta_init, lam_init = cached
                logger.debug(
                    "SSVI surface warm-start from disk cache: "
                    f"rho={rho_init:.3f}, eta={eta_init:.3f}, lam={lam_init:.3f}"
                )

        # Step 3: Joint optimization
        n = len(slices)
        # δ bounds: small positive lower bound to avoid zero thetas
        delta_bounds = [(0.001, 5.0)] * n
        bounds = [self.rho_bounds, self.eta_bounds, self.lam_bounds] + delta_bounds

        objective = _SSVISurfaceObjective(slices, self.use_relative_error)

        x0 = np.concatenate([[rho_init, eta_init, lam_init], delta_init])

        opt_x = None
        opt_msg = None
        fit_method = "full_de"
        warm_start_used = False

        warm_start_x0 = (
            self._surface_params_to_vector(warm_start_surface_params, slices)
            if warm_start_surface_params is not None else None
        )
        warm_start_bounds = (
            self._centered_bounds(warm_start_x0, bounds)
            if warm_start_x0 is not None else None
        )

        if warm_start_bounds is not None:
            try:
                warm_start_x0 = np.array([
                    np.clip(value, low, high)
                    for value, (low, high) in zip(warm_start_x0, warm_start_bounds)
                ])
                warm_result = self._run_surface_de(
                    objective,
                    warm_start_bounds,
                    warm_start_x0,
                    maxiter=max(40, self.maxiter // 4),
                    popsize=8,
                )
                warm_params, warm_agg_r2, warm_r2s, reject_reason = (
                    self._evaluate_surface_candidate(
                        warm_result.x,
                        slices,
                        indep_agg_r2,
                        indep_r2s,
                        aggregate_tolerance=0.005,
                        slice_tolerance=0.03,
                    )
                )
                if warm_params is not None:
                    surface_params = warm_params
                    surface_agg_r2 = warm_agg_r2
                    surface_r2s = warm_r2s
                    opt_x = warm_result.x
                    opt_msg = warm_result.message
                    fit_method = "warm_start"
                    warm_start_used = True
                else:
                    logger.info(f"SSVI surface warm-start rejected: {reject_reason}")
            except (ValueError, RuntimeError) as e:
                logger.warning(f"SSVI surface warm-start differential_evolution failed: {e}")

        # Cheap local pass before the full DE: x0 comes from converged
        # per-slice fits, so L-BFGS-B usually lands in the right basin in
        # a few hundred evaluations. Validated with the same gates; DE
        # remains the fallback whenever the local result is rejected.
        if opt_x is None and self.local_first:
            try:
                local_result = minimize(
                    objective,
                    x0,
                    method="L-BFGS-B",
                    bounds=bounds,
                    options={"maxiter": 300},
                )
                local_params, local_agg_r2, _local_r2s, local_reject = (
                    self._evaluate_surface_candidate(
                        local_result.x,
                        slices,
                        indep_agg_r2,
                        indep_r2s,
                        aggregate_tolerance=0.005,
                        slice_tolerance=0.03,
                    )
                )
                if (
                    local_params is not None
                    and local_agg_r2 >= self.min_acceptable_surface_r2
                ):
                    opt_x = local_result.x
                    opt_msg = "local_first L-BFGS-B accepted"
                    fit_method = "local_first"
                else:
                    logger.info(
                        "SSVI surface local-first pass rejected "
                        f"({local_reject or f'R²={local_agg_r2:.4f} below floor'}); "
                        "escalating to differential evolution"
                    )
            except (ValueError, RuntimeError) as e:
                logger.debug(f"SSVI surface local-first pass failed: {e}")

        if opt_x is None:
            try:
                result = self._run_surface_de(
                    objective,
                    bounds,
                    x0,
                    maxiter=self.maxiter,
                )
                opt_x = result.x
                opt_msg = result.message
            except (ValueError, RuntimeError) as e:
                logger.warning(f"SSVI surface differential_evolution failed: {e}")
                opt_x = None
                opt_msg = str(e)

        if opt_x is None:
            # Fall back to independent fits
            return SSVISurfaceFitResult(
                params=None,
                per_slice_params=[p for p in per_slice_params_indep if p is not None],
                success=False,
                aggregate_r_squared=indep_agg_r2,
                per_slice_r_squared=indep_r2s,
                message=f"Joint optimization failed: {opt_msg}",
            )

        # Unpack result
        rho_opt = opt_x[0]
        eta_opt = opt_x[1]
        lam_opt = opt_x[2]
        deltas_opt = opt_x[3:]
        thetas_opt = list(np.cumsum(deltas_opt ** 2))

        try:
            surface_params = SSVISurfaceParams(
                rho=float(rho_opt),
                eta=float(eta_opt),
                lam=float(lam_opt),
                thetas=thetas_opt,
                ttms=[s.ttm for s in slices],
                expiry_names=[s.expiry_name for s in slices],
            )
        except ValueError as e:
            logger.warning(f"SSVI surface params validation failed: {e}")
            return SSVISurfaceFitResult(
                params=None,
                per_slice_params=[p for p in per_slice_params_indep if p is not None],
                success=False,
                aggregate_r_squared=indep_agg_r2,
                per_slice_r_squared=indep_r2s,
                message=f"Invalid surface params: {e}",
            )

        # Step 4: Compare surface R² vs independent
        surface_r2s = self._compute_slice_r_squared(surface_params, slices)
        surface_n_points = [len(s.log_moneyness) for s in slices]
        surface_total = sum(surface_n_points)
        surface_agg_r2 = sum(
            r2 * n for r2, n in zip(surface_r2s, surface_n_points)
        ) / surface_total if surface_total > 0 else 0.0

        # Fall back if surface fit is worse AND below the absolute floor
        if (
            self.fallback_to_independent
            and surface_agg_r2 < indep_agg_r2 - 0.02
            and surface_agg_r2 < self.min_acceptable_surface_r2
        ):
            logger.info(
                f"SSVI surface R²={surface_agg_r2:.4f} < independent R²={indep_agg_r2:.4f} "
                f"and below floor {self.min_acceptable_surface_r2}, falling back to independent fits"
            )
            return SSVISurfaceFitResult(
                params=None,
                per_slice_params=[p for p in per_slice_params_indep if p is not None],
                success=False,
                aggregate_r_squared=indep_agg_r2,
                per_slice_r_squared=indep_r2s,
                message=(
                    f"Surface R²={surface_agg_r2:.4f} worse than independent "
                    f"R²={indep_agg_r2:.4f} and below floor {self.min_acceptable_surface_r2}, fell back"
                ),
            )

        decomposed = surface_params.to_per_slice_params()

        if surface_agg_r2 < indep_agg_r2 - 0.02:
            logger.info(
                f"SSVI surface R²={surface_agg_r2:.4f} < independent R²={indep_agg_r2:.4f} "
                f"but accepted (above floor {self.min_acceptable_surface_r2})"
            )

        logger.info(
            f"SSVI surface fit: ρ={rho_opt:.3f}, η={eta_opt:.3f}, λ={lam_opt:.3f}, "
            f"R²={surface_agg_r2:.4f} (independent: {indep_agg_r2:.4f})"
        )
        for i, sl in enumerate(slices):
            logger.info(
                f"  {sl.expiry_name}: θ={thetas_opt[i]:.6f}, "
                f"φ={surface_params.phi(thetas_opt[i]):.3f}, "
                f"R²={surface_r2s[i]:.4f} (indep: {indep_r2s[i]:.4f})"
            )

        self._save_warm_start(rho_opt, eta_opt, lam_opt)

        return SSVISurfaceFitResult(
            params=surface_params,
            per_slice_params=decomposed,
            success=True,
            aggregate_r_squared=surface_agg_r2,
            per_slice_r_squared=surface_r2s,
            message="Surface fit successful",
            fit_method=fit_method,
            warm_start_used=warm_start_used,
        )

    def _construct_initial_guess(
        self,
        slices: List[SSVISliceData],
        per_slice_params: List[Optional[SSVIParams]],
    ) -> Tuple[float, float, float, np.ndarray]:
        """Construct initial guess for joint optimization from per-slice fits.

        Returns:
            (rho_init, eta_init, lam_init, delta_init)
        """
        n = len(slices)

        # Collect successful per-slice (theta, rho, phi) values
        thetas_raw = []
        rhos = []
        phis = []
        for i, p in enumerate(per_slice_params):
            if p is not None:
                thetas_raw.append(p.theta)
                rhos.append(p.rho)
                phis.append(p.phi)
            else:
                # Fill from neighbors or use a default
                thetas_raw.append(None)
                rhos.append(None)
                phis.append(None)

        # ρ₀ = weighted median of per-slice ρ values
        valid_rhos = [r for r in rhos if r is not None]
        rho_init = float(np.median(valid_rhos)) if valid_rhos else -0.3

        # Fill missing thetas with interpolation
        valid_idxs = [i for i, t in enumerate(thetas_raw) if t is not None]
        if len(valid_idxs) < n:
            valid_thetas = [thetas_raw[i] for i in valid_idxs]
            valid_ttms = [slices[i].ttm for i in valid_idxs]
            for i in range(n):
                if thetas_raw[i] is None:
                    # Simple linear interp in TTM space
                    thetas_raw[i] = float(np.interp(
                        slices[i].ttm, valid_ttms, valid_thetas
                    ))

        # Enforce monotonicity
        thetas_mono = list(thetas_raw)
        for i in range(1, n):
            if thetas_mono[i] <= thetas_mono[i - 1]:
                thetas_mono[i] = thetas_mono[i - 1] + 1e-6

        # Compute δᵢ from monotone θ sequence: θ₁ = δ₁², θᵢ = θᵢ₋₁ + δᵢ²
        deltas = np.zeros(n)
        deltas[0] = math.sqrt(thetas_mono[0])
        for i in range(1, n):
            deltas[i] = math.sqrt(thetas_mono[i] - thetas_mono[i - 1])

        # Fit η₀, λ₀ from per-slice (θ, φ) pairs via log-linear regression
        # log(φ) = log(η) - λ·log(θ)
        valid_theta_phi = [
            (thetas_raw[i], phis[i])
            for i in range(n)
            if thetas_raw[i] is not None and phis[i] is not None
                and thetas_raw[i] > 0 and phis[i] > 0
        ]

        if len(valid_theta_phi) >= 2:
            log_thetas = np.array([math.log(tp[0]) for tp in valid_theta_phi])
            log_phis = np.array([math.log(tp[1]) for tp in valid_theta_phi])
            # log(φ) = log(η) - λ·log(θ)
            # Fit: log_phis = a + b * log_thetas, where a = log(η), b = -λ
            coeffs = np.polyfit(log_thetas, log_phis, 1)
            lam_init = float(np.clip(-coeffs[0], 0.0, 0.5))
            eta_init = float(np.clip(math.exp(coeffs[1]), self.eta_bounds[0], self.eta_bounds[1]))
        else:
            eta_init = 1.0
            lam_init = 0.25

        return rho_init, eta_init, lam_init, deltas
