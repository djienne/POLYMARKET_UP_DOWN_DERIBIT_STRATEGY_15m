"""Closed-form smile digital: P(S_T >= K) under a calibrated SSVI slice.

For a terminal Up/Down digital the full Monte Carlo is redundant: the
risk-neutral terminal distribution implied by the smile gives the digital
price analytically as -dC/dK evaluated with the smile's strike dependence,

    P(S_T >= K) = N(d2) - S * n(d1) * w'(k) / (2 * K * sqrt(w))

where w(k) is the SSVI total variance at log-moneyness k = ln(K/F_smile),
w'(k) its closed-form derivative, and d1/d2 are the Black terms with total
volatility sqrt(w). This static smile digital differs from the
sticky-moneyness MC in ``terminal_probability.py``; neither is a digital
on a trailing time average. Tests compare this derivative with independent
finite differences of Black call prices, not with a different payoff.

Center vs. smile forward: the MC starts paths at ``spot`` with martingale
drift (E[S_T] = spot) while evaluating the vol lookup at ln(S/F_smile).
Mirroring that, ``spot`` is the distribution center in d1/d2 and ``forward``
(the calibration expiry's Deribit forward) only positions the smile lookup.
For a 15-minute horizon the true forward is spot to within fractions of a
basis point, so this is also the economically correct centering.

Pure functions over SSVIParams, retained for terminal-payoff diagnostics.
"""

from __future__ import annotations

import math
from typing import Optional, Tuple

from .ssvi import SSVIParams

__all__ = [
    "ssvi_total_variance",
    "ssvi_dw_dk",
    "ssvi_digital_prob_above",
    "ssvi_digital_prob_pair",
]

_SQRT2 = math.sqrt(2.0)
_INV_SQRT_2PI = 1.0 / math.sqrt(2.0 * math.pi)

# Below this total variance the distribution is effectively a point mass;
# fall back to the indicator limit instead of dividing by ~0.
_DEGENERATE_TOTAL_VAR = 1e-16


def _norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / _SQRT2))


def _norm_pdf(x: float) -> float:
    return math.exp(-0.5 * x * x) * _INV_SQRT_2PI


def ssvi_total_variance(params: SSVIParams, k: float) -> float:
    """SSVI total variance w(k) = θ/2·[1 + ρφk + √((φk+ρ)² + 1−ρ²)]."""
    term = params.phi * k + params.rho
    sqrt_term = math.sqrt(term * term + 1.0 - params.rho * params.rho)
    return 0.5 * params.theta * (1.0 + params.rho * params.phi * k + sqrt_term)


def ssvi_dw_dk(params: SSVIParams, k: float) -> float:
    """Closed-form smile slope w'(k) = θ/2·[ρφ + φ(φk+ρ)/√((φk+ρ)² + 1−ρ²)]."""
    term = params.phi * k + params.rho
    sqrt_term = math.sqrt(term * term + 1.0 - params.rho * params.rho)
    return 0.5 * params.theta * params.phi * (params.rho + term / sqrt_term)


def ssvi_digital_prob_above(
    params: SSVIParams,
    forward: float,
    strike: float,
    spot: Optional[float] = None,
) -> float:
    """P(S_T >= strike) as the smile-consistent digital -dC/dK.

    Args:
        params: SSVI slice parameters interpolated to the target TTM.
        forward: Smile-centering forward (the calibration expiry's forward;
                 the fitted w(k) lives in k = ln(K/forward) coordinates).
        strike: Digital strike (the Polymarket barrier).
        spot: Distribution center (current spot). Defaults to ``forward``,
              which gives the pure risk-neutral digital for the calibration
              expiry. The 15m pipeline passes the live spot to mirror the
              MC's spot-centered martingale dynamics.

    Returns:
        Probability in [0, 1].
    """
    if strike <= 0 or forward <= 0:
        raise ValueError(f"strike and forward must be positive, got {strike}, {forward}")
    center = float(spot) if spot is not None else float(forward)
    if center <= 0:
        raise ValueError(f"spot must be positive, got {spot}")

    k = math.log(strike / forward)
    w = ssvi_total_variance(params, k)

    if w <= _DEGENERATE_TOTAL_VAR:
        # Point-mass limit: everything ends at the center.
        if center > strike:
            return 1.0
        if center < strike:
            return 0.0
        return 0.5

    sqrt_w = math.sqrt(w)
    d1 = math.log(center / strike) / sqrt_w + 0.5 * sqrt_w
    d2 = d1 - sqrt_w

    # -dC/dK = N(d2) - vega·dσ/dK with vega = S·n(d1)·√T and
    # dσ/dK = w'(k) / (2·σ·T·K)  ⇒  correction = S·n(d1)·w'(k)/(2·K·√w).
    skew_correction = center * _norm_pdf(d1) * ssvi_dw_dk(params, k) / (2.0 * strike * sqrt_w)
    prob = _norm_cdf(d2) - skew_correction
    return min(1.0, max(0.0, prob))


def ssvi_digital_prob_pair(
    params: SSVIParams,
    forward: float,
    strike: float,
    spot: Optional[float] = None,
) -> Tuple[float, float]:
    """Return (prob_above, prob_below) for the digital at ``strike``."""
    prob_above = ssvi_digital_prob_above(params, forward, strike, spot=spot)
    return prob_above, 1.0 - prob_above
