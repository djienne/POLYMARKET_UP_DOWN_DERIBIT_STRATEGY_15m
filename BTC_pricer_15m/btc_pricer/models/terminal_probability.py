"""Terminal Probability Calculator.

Estimates threshold probabilities using calibrated Heston or SSVI models.
Terminal-price methods evaluate P(S_T >= H) or P(S_T < H). The 15-minute
strategy instead uses terminal_probability_twap, which approximates the
trailing 60-second arithmetic-average payoff under the SSVI path model.

Known Limitations:
-----------------
1. SSVI Local Vol Approximation: The SSVI path simulation uses a "sticky-moneyness"
   heuristic where volatility at each step is the SSVI implied vol at the current
   log(S/F). This captures the key smile dynamics (spot down -> vol up) but is not
   a true Dupire local volatility model and is not dynamically arbitrage-consistent.
   Its forecast accuracy must be assessed against observations; a good option
   fit alone does not establish predictive accuracy for the underlying asset.
"""

import logging
import math
from dataclasses import dataclass
from typing import Tuple, Optional

import numpy as np
from scipy.stats import norm

from ..constants import DAYS_PER_YEAR
from .heston import HestonParams
from .ssvi import SSVIParams, SSVIModel

logger = logging.getLogger(__name__)

# Optional Numba acceleration for MC simulation
NUMBA_AVAILABLE = False
try:
    from numba import njit, prange, config as numba_config
    import numba
    NUMBA_AVAILABLE = True
    # Limit parallel Numba kernels to 4 threads to match the local Docker CPU cap.
    numba.set_num_threads(min(numba.config.NUMBA_NUM_THREADS, 4))
    logger.debug("Numba parallel: %d threads", numba.get_num_threads())
except ImportError:
    njit = None
    prange = range


if NUMBA_AVAILABLE:
    # Paths per RNG block. Seeding a Mersenne Twister is a 624-word state
    # init — doing it once per PATH (the old design) cost a measured 2.6-3.2x
    # on 10-15 step kernels. One seed per 256-path block amortizes it away
    # while keeping the kernel deterministic given base_seed and safe under
    # prange (each block owns its RNG stream).
    _MC_PATHS_PER_BLOCK = 256

    @njit(parallel=True, cache=True)
    def _ssvi_twap_mc_numba(spot, forward, theta, rho, phi, ttm,
                           step_dts, average_dts, average_window,
                           n_paths, base_seed, antithetic):
        """Compact trapezoidal path integral; every antithetic leg owns its vol."""
        result = np.empty(n_paths)
        n_sim = n_paths // 2 if antithetic else n_paths
        blocks = (n_sim + _MC_PATHS_PER_BLOCK - 1) // _MC_PATHS_PER_BLOCK
        for block in prange(blocks):
            np.random.seed(base_seed + block)
            for j in range(block * _MC_PATHS_PER_BLOCK, min((block + 1) * _MC_PATHS_PER_BLOCK, n_sim)):
                price_a, price_b = spot, spot
                integral_a, integral_b = 0.0, 0.0
                for i in range(len(step_dts)):
                    dt = step_dts[i]
                    z = np.random.standard_normal()
                    k = math.log(price_a / forward)
                    term = phi * k + rho
                    variance = 0.5 * theta * (1.0 + rho * phi * k + math.sqrt(term * term + 1.0 - rho * rho)) / ttm
                    next_a = price_a * math.exp(-0.5 * variance * dt + math.sqrt(max(variance, 0.0) * dt) * z)
                    integral_a += 0.5 * ((price_a - spot) + (next_a - spot)) * average_dts[i]
                    price_a = next_a
                    if antithetic:
                        k_b = math.log(price_b / forward)
                        term_b = phi * k_b + rho
                        variance_b = 0.5 * theta * (1.0 + rho * phi * k_b + math.sqrt(term_b * term_b + 1.0 - rho * rho)) / ttm
                        next_b = price_b * math.exp(-0.5 * variance_b * dt - math.sqrt(max(variance_b, 0.0) * dt) * z)
                        integral_b += 0.5 * ((price_b - spot) + (next_b - spot)) * average_dts[i]
                        price_b = next_b
                result[j] = spot + integral_a / average_window
                if antithetic:
                    result[n_sim + j] = spot + integral_b / average_window
        return result

    @njit(parallel=True, cache=True)
    def _ssvi_mc_numba(
        spot, forward, theta, rho, phi, ttm,
        dt, sqrt_dt, n_paths, n_steps, base_seed, antithetic
    ):
        """Parallel Numba SSVI local-vol MC kernel.

        Inlines the SSVI implied volatility formula to avoid Python object
        calls inside the hot loop.  Uses prange for thread parallelism with
        one RNG seed per block of paths.

        With ``antithetic=True`` each draw z drives a pair of paths (+z / -z);
        pair j is written to final_prices[j] and final_prices[half + j]. The
        pair legs diverge, so each leg evaluates its own local vol.

        Returns:
            Array of final prices, shape (n_paths,).
        """
        rho_sq_comp = 1.0 - rho * rho
        inv_ttm = 1.0 / ttm
        final_prices = np.empty(n_paths)

        n_sim = n_paths // 2 if antithetic else n_paths
        half = n_sim
        n_blocks = (n_sim + _MC_PATHS_PER_BLOCK - 1) // _MC_PATHS_PER_BLOCK

        for b in prange(n_blocks):
            np.random.seed(base_seed + b)
            j_start = b * _MC_PATHS_PER_BLOCK
            j_end = min(j_start + _MC_PATHS_PER_BLOCK, n_sim)
            for j in range(j_start, j_end):
                if antithetic:
                    price_a = spot
                    price_b = spot
                    for i in range(n_steps):
                        z = np.random.standard_normal()

                        k_a = math.log(price_a / forward)
                        term_a = phi * k_a + rho
                        sqrt_term_a = math.sqrt(term_a * term_a + rho_sq_comp)
                        var_a = 0.5 * theta * (1.0 + rho * phi * k_a + sqrt_term_a) * inv_ttm
                        vol_a = math.sqrt(var_a) if var_a > 0.0 else 0.0
                        price_a = price_a * math.exp(
                            -0.5 * vol_a * vol_a * dt + vol_a * sqrt_dt * z
                        )

                        k_b = math.log(price_b / forward)
                        term_b = phi * k_b + rho
                        sqrt_term_b = math.sqrt(term_b * term_b + rho_sq_comp)
                        var_b = 0.5 * theta * (1.0 + rho * phi * k_b + sqrt_term_b) * inv_ttm
                        vol_b = math.sqrt(var_b) if var_b > 0.0 else 0.0
                        price_b = price_b * math.exp(
                            -0.5 * vol_b * vol_b * dt - vol_b * sqrt_dt * z
                        )

                    final_prices[j] = price_a
                    final_prices[half + j] = price_b
                else:
                    price = spot
                    for i in range(n_steps):
                        k = math.log(price / forward)
                        term = phi * k + rho
                        sqrt_term = math.sqrt(term * term + rho_sq_comp)
                        total_var = 0.5 * theta * (1.0 + rho * phi * k + sqrt_term)
                        var = total_var * inv_ttm
                        vol = math.sqrt(var) if var > 0.0 else 0.0

                        z = np.random.standard_normal()
                        price = price * math.exp(
                            -0.5 * vol * vol * dt + vol * sqrt_dt * z
                        )

                    final_prices[j] = price

        return final_prices

    @njit(parallel=True, cache=True)
    def _heston_mc_numba(
        spot, v0, kappa, theta, xi, rho,
        dt, sqrt_dt, n_paths, n_steps, base_seed, antithetic
    ):
        """Parallel Numba Heston MC kernel with per-block RNG.

        One RNG seed per block of paths (see _MC_PATHS_PER_BLOCK). With
        ``antithetic=True`` each (z1, z2) pair also drives a mirrored
        (-z1, -z2) leg with its own variance path; pair j is written to
        final_prices[j] and final_prices[half + j].

        Returns:
            Array of final prices, shape (n_paths,).
        """
        rho_comp = math.sqrt(1.0 - rho * rho)
        final_prices = np.empty(n_paths)

        n_sim = n_paths // 2 if antithetic else n_paths
        half = n_sim
        n_blocks = (n_sim + _MC_PATHS_PER_BLOCK - 1) // _MC_PATHS_PER_BLOCK

        for b in prange(n_blocks):
            np.random.seed(base_seed + b)
            j_start = b * _MC_PATHS_PER_BLOCK
            j_end = min(j_start + _MC_PATHS_PER_BLOCK, n_sim)
            for j in range(j_start, j_end):
                if antithetic:
                    price_a = spot
                    price_b = spot
                    v_a = v0
                    v_b = v0
                    for i in range(n_steps):
                        z1 = np.random.standard_normal()
                        z2_raw = np.random.standard_normal()
                        z2 = rho * z1 + rho_comp * z2_raw

                        v_a_sqrt = math.sqrt(v_a if v_a > 0.0 else 0.0)
                        v_a = v_a + kappa * (theta - v_a) * dt + xi * v_a_sqrt * sqrt_dt * z2
                        v_a = v_a if v_a > 0.0 else 0.0
                        price_a = price_a * math.exp(
                            -0.5 * v_a * dt + v_a_sqrt * sqrt_dt * z1
                        )

                        v_b_sqrt = math.sqrt(v_b if v_b > 0.0 else 0.0)
                        v_b = v_b + kappa * (theta - v_b) * dt - xi * v_b_sqrt * sqrt_dt * z2
                        v_b = v_b if v_b > 0.0 else 0.0
                        price_b = price_b * math.exp(
                            -0.5 * v_b * dt - v_b_sqrt * sqrt_dt * z1
                        )

                    final_prices[j] = price_a
                    final_prices[half + j] = price_b
                else:
                    price = spot
                    v = v0
                    for i in range(n_steps):
                        z1 = np.random.standard_normal()
                        z2_raw = np.random.standard_normal()
                        z2 = rho * z1 + rho_comp * z2_raw

                        v_pos = v if v > 0.0 else 0.0
                        v_sqrt = math.sqrt(v_pos)

                        # CIR variance update
                        v = v + kappa * (theta - v) * dt + xi * v_sqrt * sqrt_dt * z2
                        v = v if v > 0.0 else 0.0

                        # Log-Euler price update
                        price = price * math.exp(
                            -0.5 * v * dt + v_sqrt * sqrt_dt * z1
                        )

                    final_prices[j] = price

        return final_prices


@dataclass
class MCResult:
    """Result of Monte Carlo terminal probability calculation."""
    target_price: float         # Target price level
    spot: float                 # Current price
    ttm: float                  # Time to maturity in years
    terminal_probability: float # P(ends beyond target)
    confidence_interval: Tuple[float, float]  # 95% CI
    n_simulations: int
    direction: str              # "down" or "up"
    method: str                 # "heston" or "ssvi_local_vol"

    @property
    def target_distance_pct(self) -> float:
        """Distance to target price as percentage of spot."""
        return (self.target_price - self.spot) / self.spot * 100

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "target_price": self.target_price,
            "spot": self.spot,
            "ttm": self.ttm,
            "terminal_probability": self.terminal_probability,
            "confidence_interval": list(self.confidence_interval),
            "n_simulations": self.n_simulations,
            "direction": self.direction,
            "method": self.method,
            "target_distance_pct": self.target_distance_pct,
        }


class TerminalProbabilityCalculator:
    """Calculate terminal probabilities via Monte Carlo simulation.

    Uses Monte Carlo simulation with the Heston stochastic volatility model
    or SSVI local vol to estimate the probability that the price ends above
    or below a target price level at expiry.
    """

    def __init__(
        self,
        n_simulations: int = 200000,
        n_steps_per_day: int = 1440,  # 1-minute steps
        confidence_level: float = 0.95,
        use_antithetic: bool = True,
        seed: Optional[int] = None
    ):
        """Initialize the calculator.

        Args:
            n_simulations: Number of Monte Carlo paths.
            n_steps_per_day: Time steps per day (1440 = 1-min intervals).
            confidence_level: Confidence level for CI (default 0.95).
            use_antithetic: Use antithetic variates for variance reduction.
            seed: Random seed for reproducibility.
        """
        self.n_simulations = n_simulations
        self.n_steps_per_day = n_steps_per_day
        self.confidence_level = confidence_level
        self.use_antithetic = use_antithetic
        self.seed = seed

        if seed is not None:
            np.random.seed(seed)

    def simulate_heston_paths(
        self,
        heston_params: HestonParams,
        spot: float,
        n_paths: int,
        n_steps: int
    ) -> np.ndarray:
        """Simulate Heston price paths using Euler-Maruyama discretization.

        The Heston SDE:
            dS_t = S_t * sqrt(v_t) * dW_1
            dv_t = kappa * (theta - v_t) * dt + xi * sqrt(v_t) * dW_2
            corr(dW_1, dW_2) = rho

        Args:
            heston_params: Calibrated Heston parameters.
            spot: Current spot price.
            n_paths: Number of paths to simulate.
            n_steps: Number of time steps.

        Returns:
            Array of shape (n_paths, n_steps+1) with price paths.
        """
        dt = heston_params.ttm / n_steps
        sqrt_dt = np.sqrt(dt)

        # Extract parameters
        v0 = heston_params.v0
        kappa = heston_params.kappa
        theta = heston_params.theta
        xi = heston_params.xi
        rho = heston_params.rho

        # Initialize arrays
        paths = np.zeros((n_paths, n_steps + 1))
        paths[:, 0] = spot

        # Initialize variance
        V = np.full(n_paths, v0)

        # Correlation factor
        rho_comp = np.sqrt(1 - rho**2)

        for i in range(n_steps):
            # Generate correlated Brownian increments
            Z1 = np.random.standard_normal(n_paths)
            Z2 = rho * Z1 + rho_comp * np.random.standard_normal(n_paths)

            # Ensure variance is non-negative (reflection scheme)
            V_sqrt = np.sqrt(np.maximum(V, 0))

            # Update variance (CIR process)
            V_new = V + kappa * (theta - V) * dt + xi * V_sqrt * sqrt_dt * Z2
            V = np.maximum(V_new, 0)  # Reflection to keep positive

            # Update price (log-Euler for numerical stability)
            # dlog(S) = -0.5*V*dt + sqrt(V)*dW
            paths[:, i + 1] = paths[:, i] * np.exp(
                -0.5 * np.maximum(V, 0) * dt + V_sqrt * sqrt_dt * Z1
            )

        return paths

    def simulate_heston_paths_compact(
        self,
        heston_params: HestonParams,
        spot: float,
        n_paths: int,
        n_steps: int,
        antithetic: bool = False,
    ) -> np.ndarray:
        """Simulate Heston paths, returning only final prices.

        Uses the same Euler-Maruyama discretization as simulate_heston_paths()
        but stores only a single vector of length n_paths instead of the full
        (n_paths, n_steps+1) matrix.

        When Numba is available, delegates to a JIT-compiled kernel for
        significant speedup.

        Args:
            heston_params: Calibrated Heston parameters.
            spot: Current spot price.
            n_paths: Number of paths to simulate (total, including both legs
                     of each antithetic pair; must be even when antithetic).
            n_steps: Number of time steps.
            antithetic: Drive paired paths from (+Z, -Z) for true antithetic
                        variance reduction.

        Returns:
            Array of final prices, shape (n_paths,).
        """
        dt = heston_params.ttm / n_steps
        sqrt_dt = np.sqrt(dt)

        # Extract parameters
        v0 = heston_params.v0
        kappa = heston_params.kappa
        theta = heston_params.theta
        xi = heston_params.xi
        rho = heston_params.rho

        # Fast path: parallel Numba JIT kernel (no pre-allocation needed)
        if NUMBA_AVAILABLE:
            base_seed = np.random.randint(0, 2**31)
            return _heston_mc_numba(
                spot, v0, kappa, theta, xi, rho,
                dt, sqrt_dt, n_paths, n_steps,
                base_seed, antithetic,
            )

        # Fallback: vectorized NumPy — generate randoms per-step
        rho_comp = np.sqrt(1 - rho**2)
        n_sim = n_paths // 2 if antithetic else n_paths
        prices = np.full(n_sim, spot)
        V = np.full(n_sim, v0)
        if antithetic:
            prices_b = np.full(n_sim, spot)
            V_b = np.full(n_sim, v0)

        for i in range(n_steps):
            Z1 = np.random.standard_normal(n_sim)
            Z2 = rho * Z1 + rho_comp * np.random.standard_normal(n_sim)

            V_sqrt = np.sqrt(np.maximum(V, 0))

            V_new = V + kappa * (theta - V) * dt + xi * V_sqrt * sqrt_dt * Z2
            V = np.maximum(V_new, 0)

            prices = prices * np.exp(
                -0.5 * np.maximum(V, 0) * dt + V_sqrt * sqrt_dt * Z1
            )

            if antithetic:
                # Mirror leg: both Brownians flipped, own variance path.
                V_b_sqrt = np.sqrt(np.maximum(V_b, 0))
                V_b_new = V_b + kappa * (theta - V_b) * dt - xi * V_b_sqrt * sqrt_dt * Z2
                V_b = np.maximum(V_b_new, 0)
                prices_b = prices_b * np.exp(
                    -0.5 * np.maximum(V_b, 0) * dt - V_b_sqrt * sqrt_dt * Z1
                )

        if antithetic:
            return np.concatenate([prices, prices_b])
        return prices

    def _calculate_terminal_probability(
        self,
        final_prices: np.ndarray,
        target_price: float,
        direction: str
    ) -> Tuple[float, float]:
        """Calculate terminal probability from final prices.

        Args:
            final_prices: Final prices, shape (n_paths,).
            target_price: Target price level.
            direction: "down" or "up".

        Returns:
            Tuple of (terminal_prob, se).
        """
        n_paths = len(final_prices)

        if direction == "down":
            terminal = final_prices < target_price  # equality resolves UP
        else:
            terminal = final_prices >= target_price

        terminal_prob = np.mean(terminal)
        # Antithetic legs are correlated. The independent observations are
        # pair averages, not the individual digital indicators.
        if self.use_antithetic and n_paths >= 4 and n_paths % 2 == 0:
            half = n_paths // 2
            paired = (terminal[:half].astype(float) + terminal[half:].astype(float)) * 0.5
            se = float(np.std(paired, ddof=1) / np.sqrt(half))
        else:
            se = np.sqrt(terminal_prob * (1 - terminal_prob) / n_paths)

        return terminal_prob, se

    def _validate_target(self, spot: float, target_price: float, direction: str):
        """Log a warning when target price is on the wrong side of spot."""
        if direction == "down" and target_price >= spot:
            logger.warning(
                f"Down target {target_price} >= spot {spot}, "
                "probability will be high"
            )
        elif direction == "up" and target_price <= spot:
            logger.warning(
                f"Up target {target_price} <= spot {spot}, "
                "probability will be high"
            )

    def _simulate_final_prices(self, simulate_fn, ttm: float) -> np.ndarray:
        """Run MC with antithetic handling, return final_prices array.

        Antithetic variance reduction is real (+Z/-Z pairs inside one kernel
        launch) — the old implementation ran two *independent* halves, which
        bought no variance reduction at the cost of a second launch.

        Args:
            simulate_fn: Callable(n_paths, n_steps, antithetic) -> np.ndarray
                         of final prices.
            ttm: Time to maturity in years.

        Returns:
            Array of final prices.
        """
        days = ttm * DAYS_PER_YEAR
        n_steps = max(int(days * self.n_steps_per_day), 10)

        if self.use_antithetic:
            n_paths = self.n_simulations - (self.n_simulations % 2)
            final_prices = simulate_fn(n_paths, n_steps, antithetic=True)
        else:
            final_prices = simulate_fn(self.n_simulations, n_steps)
        # Keep a reference for terminal-distribution export (consumers can
        # re-evaluate the digital at a moved spot without re-running MC).
        self.last_final_prices = final_prices
        return final_prices

    def _build_result(
        self,
        final_prices: np.ndarray,
        target_price: float,
        direction: str,
        spot: float,
        ttm: float,
        method: str
    ) -> MCResult:
        """Calculate terminal prob + CI and return MCResult."""
        terminal_prob, se = self._calculate_terminal_probability(
            final_prices, target_price, direction
        )
        z = norm.ppf(1 - (1 - self.confidence_level) / 2)
        return MCResult(
            target_price=target_price,
            spot=spot,
            ttm=ttm,
            terminal_probability=terminal_prob,
            confidence_interval=(max(0, terminal_prob - z * se), min(1, terminal_prob + z * se)),
            n_simulations=len(final_prices),
            direction=direction,
            method=method,
        )

    def terminal_probability_heston(
        self,
        heston_params: HestonParams,
        spot: float,
        target_price: float,
        direction: str = "down"
    ) -> MCResult:
        """Calculate terminal probability using Heston MC.

        Args:
            heston_params: Calibrated Heston parameters.
            spot: Current spot price.
            target_price: Target price level.
            direction: "down" (target < spot) or "up" (target > spot).

        Returns:
            MCResult with terminal probability and confidence interval.
        """
        self._validate_target(spot, target_price, direction)

        logger.info(
            f"Simulating {self.n_simulations} paths "
            f"({self.n_steps_per_day}/day)..."
        )

        def simulate_fn(n_paths, n_steps, antithetic=False):
            return self.simulate_heston_paths_compact(
                heston_params, spot, n_paths, n_steps, antithetic=antithetic
            )

        final_prices = self._simulate_final_prices(simulate_fn, heston_params.ttm)
        return self._build_result(final_prices, target_price, direction, spot, heston_params.ttm, "heston")

    def terminal_probability_heston_multiple(
        self,
        heston_params: HestonParams,
        spot: float,
        targets: list,
        direction: str = "down"
    ) -> list:
        """Calculate terminal probabilities for multiple target prices efficiently.

        Simulates paths once and evaluates all targets.

        Args:
            heston_params: Calibrated Heston parameters.
            spot: Current spot price.
            targets: List of target price levels.
            direction: "down" or "up".

        Returns:
            List of MCResult objects.
        """
        logger.info(
            f"Simulating {self.n_simulations} paths for {len(targets)} targets..."
        )

        def simulate_fn(n_paths, n_steps, antithetic=False):
            return self.simulate_heston_paths_compact(
                heston_params, spot, n_paths, n_steps, antithetic=antithetic
            )

        final_prices = self._simulate_final_prices(simulate_fn, heston_params.ttm)
        return [
            self._build_result(final_prices, target, direction, spot, heston_params.ttm, "heston")
            for target in targets
        ]

    def simulate_local_vol_paths_compact(
        self,
        ssvi_params: SSVIParams,
        spot: float,
        forward: float,
        n_paths: int,
        n_steps: int,
        antithetic: bool = False,
    ) -> np.ndarray:
        """Simulate SSVI local-vol paths, returning only final prices.

        Uses a "sticky-moneyness" approach where volatility at each step
        depends on current spot's log-moneyness relative to forward.

        Args:
            ssvi_params: SSVI parameters for the vol surface.
            spot: Current spot price.
            forward: Forward price (for log-moneyness calculation).
            n_paths: Number of paths to simulate (total, including both legs
                     of each antithetic pair; must be even when antithetic).
            n_steps: Number of time steps.
            antithetic: Drive paired paths from (+Z, -Z) for true antithetic
                        variance reduction.

        Returns:
            Array of final prices, shape (n_paths,).
        """
        dt = ssvi_params.ttm / n_steps
        sqrt_dt = np.sqrt(dt)

        # Fast path: Numba JIT with parallel prange
        if NUMBA_AVAILABLE:
            base_seed = np.random.randint(0, 2**31)
            return _ssvi_mc_numba(
                spot, forward,
                ssvi_params.theta, ssvi_params.rho, ssvi_params.phi,
                ssvi_params.ttm,
                dt, sqrt_dt, n_paths, n_steps, base_seed, antithetic
            )

        # Fallback: vectorized NumPy (no JIT)
        ssvi_model = SSVIModel(ssvi_params)
        n_sim = n_paths // 2 if antithetic else n_paths
        prices = np.full(n_sim, spot)
        if antithetic:
            prices_b = np.full(n_sim, spot)

        for i in range(n_steps):
            log_moneyness = np.log(prices / forward)
            vol = ssvi_model.implied_volatility_array(log_moneyness)
            Z = np.random.standard_normal(n_sim)
            prices = prices * np.exp(
                -0.5 * vol**2 * dt + vol * sqrt_dt * Z
            )
            if antithetic:
                vol_b = ssvi_model.implied_volatility_array(np.log(prices_b / forward))
                prices_b = prices_b * np.exp(
                    -0.5 * vol_b**2 * dt - vol_b * sqrt_dt * Z
                )

        if antithetic:
            return np.concatenate([prices, prices_b])
        return prices

    def terminal_probability_ssvi(
        self,
        ssvi_params: SSVIParams,
        spot: float,
        forward: float,
        target_price: float,
        direction: str = "down"
    ) -> MCResult:
        """Calculate terminal probability using SSVI Local Vol MC.

        Unlike constant vol, this captures the vol smile:
        - When spot drops, IV increases (negative skew)
        - When spot rises, IV decreases
        This gives more realistic terminal probabilities.

        Args:
            ssvi_params: Calibrated SSVI parameters.
            spot: Current spot price.
            forward: Forward price (for moneyness calculation).
            target_price: Target price level.
            direction: "down" (target < spot) or "up" (target > spot).

        Returns:
            MCResult with terminal probability and confidence interval.
        """
        self._validate_target(spot, target_price, direction)

        logger.info(
            f"Simulating {self.n_simulations} Local Vol paths "
            f"({self.n_steps_per_day}/day)..."
        )

        def simulate_fn(n_paths, n_steps, antithetic=False):
            return self.simulate_local_vol_paths_compact(
                ssvi_params, spot, forward, n_paths, n_steps, antithetic=antithetic
            )

        final_prices = self._simulate_final_prices(simulate_fn, ssvi_params.ttm)
        return self._build_result(final_prices, target_price, direction, spot, ssvi_params.ttm, "ssvi_local_vol")

    def simulate_twap_prices_compact(
        self, ssvi_params: SSVIParams, spot: float, forward: float,
        *, averaging_window_seconds: float = 60.0, max_step_seconds: float = 5.0,
    ) -> np.ndarray:
        """Approximate the trailing arithmetic average under the SSVI SDE.

        Only future averaging windows are supported: entries are already
        blocked in the final 180 seconds. This is a continuous-price model,
        not an implementation of Chainlink's unpublished measurement rule.
        Times are converted from seconds to years once at the kernel boundary.
        """
        seconds_per_year = DAYS_PER_YEAR * 86400.0
        horizon = ssvi_params.ttm * seconds_per_year
        window = float(averaging_window_seconds)
        step = float(max_step_seconds)
        if not all(math.isfinite(v) and v > 0 for v in (spot, forward, horizon, window, step)):
            raise ValueError("TWAP inputs must be finite and positive")
        if horizon + 1e-8 < window:
            raise ValueError("TWAP averaging window contains unobserved historical prices")
        if self.n_simulations < 4:
            raise ValueError("at least four Monte Carlo paths are required")
        before = max(0.0, horizon - window)
        before_steps = max(1, int(math.ceil(before / step))) if before > 0 else 0
        average_steps = max(1, int(math.ceil(window / step)))
        step_dts = np.concatenate((
            np.full(before_steps, before / before_steps if before_steps else 0.0),
            np.full(average_steps, window / average_steps),
        )) / seconds_per_year
        average_dts = step_dts.copy()
        average_dts[:before_steps] = 0.0
        window_years = window / seconds_per_year
        n_paths = self.n_simulations - self.n_simulations % 2 if self.use_antithetic else self.n_simulations
        if NUMBA_AVAILABLE:
            averages = _ssvi_twap_mc_numba(
                spot, forward, ssvi_params.theta, ssvi_params.rho, ssvi_params.phi,
                ssvi_params.ttm, step_dts, average_dts, window_years,
                n_paths, np.random.randint(0, 2**31), self.use_antithetic,
            )
        else:
            n_independent = n_paths // 2 if self.use_antithetic else n_paths
            prices = np.full(n_paths, float(spot))
            integral = np.zeros(n_paths)
            model = SSVIModel(ssvi_params)
            for dt, weight in zip(step_dts, average_dts):
                z = np.random.standard_normal(n_independent)
                if self.use_antithetic:
                    z = np.concatenate((z, -z))
                vol = model.implied_volatility_array(np.log(prices / forward))
                next_prices = prices * np.exp(-0.5 * vol * vol * dt + vol * np.sqrt(dt) * z)
                integral += 0.5 * ((prices - spot) + (next_prices - spot)) * weight
                prices = next_prices
            averages = spot + integral / window_years
        if not np.all(np.isfinite(averages)) or np.any(averages <= 0):
            raise ValueError("non-finite or non-positive TWAP simulation")
        self.last_average_prices = averages
        return averages

    def terminal_probability_twap(
        self, ssvi_params: SSVIParams, spot: float, forward: float,
        target_price: float, direction: str = "up", *,
        averaging_window_seconds: float = 60.0, max_step_seconds: float = 5.0,
    ) -> MCResult:
        averages = self.simulate_twap_prices_compact(
            ssvi_params, spot, forward,
            averaging_window_seconds=averaging_window_seconds,
            max_step_seconds=max_step_seconds,
        )
        return self._build_result(averages, target_price, direction, spot, ssvi_params.ttm, "ssvi_twap60_mc_approx")

    def terminal_probability_ssvi_multiple(
        self,
        ssvi_params: SSVIParams,
        spot: float,
        forward: float,
        targets: list,
        direction: str = "down"
    ) -> list:
        """Calculate terminal probabilities for multiple targets using SSVI Local Vol.

        Simulates paths once and evaluates all targets.

        Args:
            ssvi_params: Calibrated SSVI parameters.
            spot: Current spot price.
            forward: Forward price.
            targets: List of target price levels.
            direction: "down" or "up".

        Returns:
            List of MCResult objects.
        """
        logger.info(
            f"Simulating {self.n_simulations} Local Vol paths for {len(targets)} targets..."
        )

        def simulate_fn(n_paths, n_steps, antithetic=False):
            return self.simulate_local_vol_paths_compact(
                ssvi_params, spot, forward, n_paths, n_steps, antithetic=antithetic
            )

        final_prices = self._simulate_final_prices(simulate_fn, ssvi_params.ttm)
        return [
            self._build_result(final_prices, target, direction, spot, ssvi_params.ttm, "ssvi_local_vol")
            for target in targets
        ]
