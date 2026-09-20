#!/usr/bin/env python3
"""
BTC Terminal Probability Calculator (consolidated CLI)

Modes:
  terminal (default) — P(S_T >= K at expiry) using MC + B-L
  barrier            — P(S_t >= K at any t <= T) using MC first-passage
  --bl-only          — B-L extraction only, skip MC and surface fitting (~5s)
  --intraday         — ATM IV sqrt(T) scaling forecasts for short horizons
"""

import argparse
import itertools
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Tuple

import numpy as np

from btc_pricer.config import Config
from btc_pricer.api.deribit import DeribitClient, DeribitAPIError
from btc_pricer.api.binance import fetch_spot_with_fallback
from btc_pricer.constants import DAYS_PER_YEAR
from btc_pricer.models.heston import HestonParams
from btc_pricer.models.terminal_probability import TerminalProbabilityCalculator, MCResult
from btc_pricer.models.breeden_litzenberger import BreedenLitzenberger
from btc_pricer.models.digital import ssvi_digital_prob_pair
from btc_pricer.cli.common import (
    setup_logging,
    load_config,
    handle_cli_exceptions,
    add_common_arguments,
    find_closest_expiry_after,
    calibrate_to_expiry,
    extract_surface_data,
    format_current_time_multizone,
    parse_expiry_date,
    fit_ssvi_surface_for_ttm,
    fingerprint_filtered_ssvi_surface,
    prepare_ssvi_surface_inputs,
    SurfaceFitResult,
    get_atm_iv_from_nearest_expiry,
)
from btc_pricer.utils.time_parser import (
    parse_datetime_with_timezone,
    calculate_ttm_to_target,
    format_target_time,
    TimeParseError,
)
from scripts.trader._io import atomic_write_json


VALID_PROBABILITY_MODES = {"mc", "avg", "bl", "cf"}
PROBABILITY_MODE_ALIASES = {
    "monte_carlo": "mc",
    "monte-carlo": "mc",
    "average": "avg",
    "bl_only": "bl",
    "bl-only": "bl",
    "closed_form": "cf",
    "closed-form": "cf",
    "digital": "cf",
}

# Process-lifetime tick counter driving the cf-mode shadow MC cadence. The
# long-lived TerminalCalibrationWorker keeps this module alive across
# calibrations, so "every Nth tick" is a real wall-clock cadence in production.
_CF_SHADOW_TICK_COUNTER = itertools.count()

# Probability grid for the exported MC terminal-return quantiles (schema v4
# consumers re-evaluate P(up) at the live spot from this). 1001 points bounds
# the CDF interpolation error at ~0.05pp — the MC's own noise floor.
_TERMINAL_RETURN_QUANTILE_POINTS = 1001
_TERMINAL_RETURN_QUANTILE_PROBS = np.linspace(0.0, 1.0, _TERMINAL_RETURN_QUANTILE_POINTS)


def normalize_probability_mode(mode: Optional[str]) -> str:
    """Normalize the configured trading probability selector."""
    normalized = (mode or "mc").strip().lower()
    normalized = PROBABILITY_MODE_ALIASES.get(normalized, normalized)
    if normalized not in VALID_PROBABILITY_MODES:
        allowed = ", ".join(sorted(VALID_PROBABILITY_MODES))
        raise ValueError(f"Invalid model.probability_mode={mode!r}; expected one of: {allowed}")
    return normalized


def _select_probability_pair(
    mode: str,
    *,
    mc_above: Optional[float],
    mc_below: Optional[float],
    bl_above: Optional[float],
    bl_below: Optional[float],
    cf_above: Optional[float] = None,
    cf_below: Optional[float] = None,
) -> tuple[Optional[float], Optional[float], Optional[str]]:
    """Return the configured trading probability pair and source label."""
    has_mc = mc_above is not None and mc_below is not None
    has_bl = bl_above is not None and bl_below is not None
    has_cf = cf_above is not None and cf_below is not None

    if mode == "cf":
        if has_cf:
            return cf_above, cf_below, "SSVI Surface CF digital"
        # Surface fitted but cf failed numerically — fall back to MC if a
        # shadow run happened this tick rather than publishing nothing.
        if has_mc:
            return mc_above, mc_below, "SSVI Surface MC"
        return None, None, None

    if mode == "mc":
        if has_mc:
            return mc_above, mc_below, "SSVI Surface MC"
        return None, None, None

    if mode == "bl":
        if has_bl:
            return bl_above, bl_below, "SSVI Surface B-L"
        return None, None, None

    if has_mc and has_bl:
        return (mc_above + bl_above) / 2, (mc_below + bl_below) / 2, "SSVI Surface MC + B-L average"
    if has_mc:
        return mc_above, mc_below, "SSVI Surface MC"
    if has_bl:
        return bl_above, bl_below, "SSVI Surface B-L"
    return None, None, None


def _write_json_output(path: Path, payload: dict) -> None:
    atomic_write_json(path, payload)


def decompose_probs(result: MCResult) -> Tuple[float, float]:
    """Return (prob_above, prob_below) regardless of direction."""
    if result.direction == "down":
        return 1 - result.terminal_probability, result.terminal_probability
    return result.terminal_probability, 1 - result.terminal_probability


def format_terminal_result(
    result: MCResult,
    params,
    r_squared: float,
    model_type: str = "heston",
    expiry_str: Optional[str] = None,
    target_utc: Optional[datetime] = None,
    target_tz_display: Optional[str] = None
) -> str:
    """Format terminal probability result for display."""
    ci_low, ci_high = result.confidence_interval
    ci_width = (ci_high - ci_low) / 2 * 100

    model_display = model_type.upper()
    if model_type == "ssvi":
        model_display = "SSVI Local Vol MC"

    # Format target time if provided (--until was used)
    target_display = ""
    if target_utc is not None:
        target_str = format_target_time(target_utc, target_tz_display or "UTC")
        target_display = f" Target Time:       {target_str}"

    # Format calibration expiry time
    expiry_display = ""
    if expiry_str:
        try:
            expiry_dt = datetime.strptime(expiry_str, "%d%b%y").replace(hour=8)  # 08:00 UTC
            label = "Calibration Expiry:" if target_utc else "Expiry:            "
            expiry_display = f" {label} {expiry_dt.strftime('%d %b %Y')} 08:00 UTC"
        except ValueError:
            label = "Calibration Expiry:" if target_utc else "Expiry:            "
            expiry_display = f" {label} {expiry_str}"

    # Current time in multiple timezones
    current_time_str = format_current_time_multizone()

    # Calculate terminal probabilities for both directions
    term_prob_above, term_prob_below = decompose_probs(result)

    lines = [
        "",
        "=" * 65,
        "          TERMINAL PROBABILITY ANALYSIS",
        "=" * 65,
        f" Current Time:      {current_time_str}",
        f" Spot Price:        ${result.spot:,.2f} (Binance)",
        f" Target Price:      ${result.target_price:,.0f} ({result.direction.upper()}, {result.target_distance_pct:+.1f}%)",
        f" Time Horizon:      {result.ttm * DAYS_PER_YEAR:.3f} days ({result.ttm * DAYS_PER_YEAR * 24:.2f} hours)",
    ]
    if target_display:
        lines.append(target_display)
    if expiry_display:
        lines.append(expiry_display)
    lines.extend([
        "-" * 65,
        f" P(>${result.target_price:,.0f}): {term_prob_above * 100:.1f}% +/- {ci_width:.1f}%",
        f" P(<${result.target_price:,.0f}): {term_prob_below * 100:.1f}%",
        "-" * 65,
        f" Model: {model_display} (R²={r_squared:.3f})",
    ])

    if model_type == "heston":
        lines.append(
            f" Parameters: v0={params.v0:.3f}, kappa={params.kappa:.2f}, "
            f"theta={params.theta:.3f}, xi={params.xi:.2f}, rho={params.rho:.2f}"
        )
    else:  # ssvi
        lines.append(
            f" Parameters: theta={params.theta:.4f}, rho={params.rho:.3f}, phi={params.phi:.3f}"
        )

    lines.extend([
        f" Simulations: {result.n_simulations:,}",
        "=" * 65,
        "",
    ])
    return "\n".join(lines)


def format_comparison_result(
    heston_result: Optional[MCResult],
    ssvi_result: Optional[MCResult],
    heston_params,
    ssvi_params,
    heston_r2: Optional[float],
    ssvi_r2: Optional[float],
    spot: float,
    target_price: float,
    direction: str,
    ttm: float,
    expiry_str: Optional[str] = None,
    target_utc: Optional[datetime] = None,
    target_tz_display: Optional[str] = None,
    surface_bl_above: Optional[float] = None,
    surface_bl_below: Optional[float] = None,
    surface_r2: Optional[float] = None,
    surface_info: Optional[str] = None,
    ssvi_is_surface: bool = False,
) -> str:
    """Format comparison of Heston, SSVI, and optionally SSVI Surface results."""
    target_dist = (target_price - spot) / spot * 100

    # Format target time if provided
    target_display = ""
    if target_utc is not None:
        target_str = format_target_time(target_utc, target_tz_display or "UTC")
        target_display = f" Target Time:       {target_str}"

    # Format calibration expiry
    expiry_display = ""
    if expiry_str:
        try:
            expiry_dt = datetime.strptime(expiry_str, "%d%b%y").replace(hour=8)
            label = "Calibration Expiry:" if target_utc else "Expiry:            "
            expiry_display = f" {label} {expiry_dt.strftime('%d %b %Y')} 08:00 UTC"
        except ValueError:
            label = "Calibration Expiry:" if target_utc else "Expiry:            "
            expiry_display = f" {label} {expiry_str}"

    # Current time in multiple timezones
    current_time_str = format_current_time_multizone()

    lines = [
        "",
        "=" * 70,
        "               TERMINAL PROBABILITY COMPARISON",
        "=" * 70,
        f" Current Time:      {current_time_str}",
        f" Spot Price:        ${spot:,.2f} (Binance)",
        f" Target Price:      ${target_price:,.0f} ({direction.upper()}, {target_dist:+.1f}%)",
        f" Time Horizon:      {ttm * DAYS_PER_YEAR:.3f} days ({ttm * DAYS_PER_YEAR * 24:.2f} hours)",
    ]

    if target_display:
        lines.append(target_display)
    if expiry_display:
        lines.append(expiry_display)

    lines.append("-" * 70)
    lines.append(f" {'Model':<20} {'R²':>8} {'P(>target)':>16} {'P(<target)':>16}")
    lines.append("-" * 70)

    # Heston row
    if heston_result is not None:
        ci_low, ci_high = heston_result.confidence_interval
        ci_width = (ci_high - ci_low) / 2 * 100
        p_above, p_below = decompose_probs(heston_result)
        above_str = f"{p_above*100:.1f}% ±{ci_width:.1f}%"
        lines.append(f" {'Heston MC':<20} {heston_r2:>7.3f} {above_str:>16} {p_below*100:>15.1f}%")
    else:
        lines.append(f" {'Heston MC':<20} {'failed':>8} {'-':>16} {'-':>16}")

    # SSVI row (MC uses surface params when available)
    ssvi_mc_label = "SSVI Surface MC" if ssvi_is_surface else "SSVI Local Vol MC"
    if ssvi_result is not None:
        ci_low, ci_high = ssvi_result.confidence_interval
        ci_width = (ci_high - ci_low) / 2 * 100
        p_above, p_below = decompose_probs(ssvi_result)
        above_str = f"{p_above*100:.1f}% ±{ci_width:.1f}%"
        lines.append(f" {ssvi_mc_label:<20} {ssvi_r2:>7.3f} {above_str:>16} {p_below*100:>15.1f}%")
    else:
        lines.append(f" {ssvi_mc_label:<20} {'failed':>8} {'-':>16} {'-':>16}")

    # SSVI Surface B-L row
    if surface_bl_above is not None:
        above_str = f"{surface_bl_above*100:.1f}%"
        lines.append(f" {'SSVI Surface B-L':<20} {surface_r2:>7.3f} {above_str:>16} {surface_bl_below*100:>15.1f}%")

    lines.append("-" * 70)

    # Show parameters for each model
    if heston_params is not None:
        lines.append(
            f" Heston: v0={heston_params.v0:.3f}, kappa={heston_params.kappa:.2f}, "
            f"theta={heston_params.theta:.3f}, xi={heston_params.xi:.2f}, rho={heston_params.rho:.2f}"
        )
    if ssvi_params is not None:
        lines.append(
            f" SSVI:   theta={ssvi_params.theta:.4f}, rho={ssvi_params.rho:.3f}, phi={ssvi_params.phi:.3f}"
        )
    if surface_info:
        lines.append(f" {surface_info}")

    n_sims = heston_result.n_simulations if heston_result else (ssvi_result.n_simulations if ssvi_result else 0)
    lines.append(f" Simulations: {n_sims:,}")
    lines.append("=" * 70)
    lines.append("")

    return "\n".join(lines)


def build_json_output(
    heston_r2: Optional[float],
    spot: float,
    target_price: float,
    direction: str,
    ttm: float,
    heston_params=None,
    bl_prob_above: Optional[float] = None,
    bl_prob_below: Optional[float] = None,
    surface_bl_above: Optional[float] = None,
    surface_bl_below: Optional[float] = None,
    surface_mc_above: Optional[float] = None,
    surface_mc_below: Optional[float] = None,
    surface_cf_above: Optional[float] = None,
    surface_cf_below: Optional[float] = None,
    surface_r2: Optional[float] = None,
    surface_params_at_ttm=None,
    surface_params_obj=None,
    timing: Optional[dict] = None,
    trading_model: str = "ssvi_surface",
    probability_mode: str = "mc",
) -> dict:
    """Build JSON output structure for terminal probability results."""
    probability_mode = normalize_probability_mode(probability_mode)
    output = {
        "spot_price": spot,
        "target_price": target_price,
        "direction": direction,
        "ttm_days": ttm * DAYS_PER_YEAR,
        "heston": None,
        "preferred_model": None,
        "probability_mode": probability_mode,
        "probability_source_label": None,
        "selected_prob_above": None,
        "selected_prob_below": None,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    # Add Heston results (B-L only -- MC is skipped)
    if heston_params is not None:
        output["heston"] = {
            "prob_above": bl_prob_above,
            "prob_below": bl_prob_below,
            "r_squared": heston_r2,
            "params": heston_params.to_dict() if heston_params else None,
        }

    # Add SSVI Surface data. MC must remain visible even when B-L is disabled.
    if (
        surface_bl_above is not None
        or surface_bl_below is not None
        or surface_mc_above is not None
        or surface_mc_below is not None
        or surface_cf_above is not None
        or surface_params_at_ttm is not None
        or surface_params_obj is not None
    ):
        output["ssvi_surface"] = {
            "prob_above": surface_bl_above,
            "prob_below": surface_bl_below,
            "r_squared": surface_r2,
            "params": surface_params_at_ttm.to_dict() if surface_params_at_ttm else None,
            "surface_params": surface_params_obj.to_dict() if surface_params_obj else None,
        }
        if surface_mc_above is not None:
            output["ssvi_surface"]["mc_prob_above"] = surface_mc_above
            output["ssvi_surface"]["mc_prob_below"] = surface_mc_below
        if surface_cf_above is not None:
            output["ssvi_surface"]["cf_prob_above"] = surface_cf_above
            output["ssvi_surface"]["cf_prob_below"] = surface_cf_below

    # Include timing data for downstream consumers
    output["timing"] = timing or {}

    surface_has_selected_input = (
        (probability_mode == "mc" and surface_mc_above is not None and surface_mc_below is not None)
        or (probability_mode == "bl" and surface_bl_above is not None and surface_bl_below is not None)
        or (
            probability_mode == "cf"
            and (
                (surface_cf_above is not None and surface_cf_below is not None)
                or (surface_mc_above is not None and surface_mc_below is not None)
            )
        )
        or (
            probability_mode == "avg"
            and (
                (surface_mc_above is not None and surface_mc_below is not None)
                or (surface_bl_above is not None and surface_bl_below is not None)
            )
        )
    )

    # Determine preferred model from config (trading_model).
    # selected_prob_* carries the configured trading probability; avg_prob_* is
    # retained below as a legacy alias for existing consumers.
    if trading_model == "heston" and bl_prob_above is not None:
        output["preferred_model"] = "heston"
    elif trading_model == "ssvi_surface" and surface_has_selected_input:
        output["preferred_model"] = "ssvi_surface"
    elif bl_prob_above is not None:
        output["preferred_model"] = "heston"
    elif surface_mc_above is not None or surface_bl_above is not None:
        output["preferred_model"] = "ssvi_surface"

    # Add Breeden-Litzenberger terminal probabilities (Heston B-L)
    output["bl_prob_above"] = bl_prob_above
    output["bl_prob_below"] = bl_prob_below

    # Select the configured trading probability.
    preferred = output["preferred_model"]
    if preferred == "ssvi_surface":
        selected_above, selected_below, source_label = _select_probability_pair(
            probability_mode,
            mc_above=surface_mc_above,
            mc_below=surface_mc_below,
            bl_above=surface_bl_above,
            bl_below=surface_bl_below,
            cf_above=surface_cf_above,
            cf_below=surface_cf_below,
        )
    elif preferred == "heston":
        # Heston B-L only (MC skipped for Heston due to Feller violations)
        selected_above, selected_below, source_label = bl_prob_above, bl_prob_below, "Heston B-L"
    else:
        selected_above, selected_below, source_label = None, None, None

    output["selected_prob_above"] = selected_above
    output["selected_prob_below"] = selected_below
    output["probability_source_label"] = source_label
    output["avg_prob_above"] = selected_above
    output["avg_prob_below"] = selected_below

    # Track divergence for diagnostics
    if surface_mc_above is not None and surface_bl_above is not None:
        output["bl_mc_divergence"] = abs(surface_mc_above - surface_bl_above)
    else:
        output["bl_mc_divergence"] = None
    if surface_mc_above is not None and surface_cf_above is not None:
        output["cf_mc_divergence"] = abs(surface_mc_above - surface_cf_above)
    else:
        output["cf_mc_divergence"] = None

    return output


# =========================================================================
# Intraday mode
# =========================================================================

def run_intraday(args, config, logger):
    """Run intraday forecast mode."""
    from btc_pricer.models.intraday_forecast import (
        IntradayForecaster,
        format_intraday_forecast,
        format_intraday_table,
    )

    client = DeribitClient(config.api, config.validation)

    # Get ATM IV (from market or override)
    if args.atm_iv is not None and args.spot is not None:
        atm_iv = args.atm_iv
        spot_price = args.spot
        source_expiry = "manual_override"
        logger.info(f"Using manual ATM IV: {atm_iv*100:.1f}%")
        logger.info(f"Using manual spot: ${spot_price:,.0f}")
    else:
        atm_iv, deribit_spot, source_expiry, _ = get_atm_iv_from_nearest_expiry(
            client, config
        )
        spot_price, _ = fetch_spot_with_fallback(deribit_spot)

        if args.atm_iv is not None:
            atm_iv = args.atm_iv
            logger.info(f"Overriding ATM IV to: {atm_iv*100:.1f}%")
        if args.spot is not None:
            spot_price = args.spot
            logger.info(f"Overriding spot to: ${spot_price:,.0f}")

    forecaster = IntradayForecaster(
        use_drift=config.intraday.use_drift,
        annual_drift=config.intraday.annual_drift
    )

    if args.hours:
        series = forecaster.forecast_series(
            spot_price, atm_iv, args.hours, source_expiry
        )
    else:
        series = forecaster.forecast_series(
            spot_price, atm_iv, config.intraday.standard_horizons, source_expiry
        )

    print("\n" + format_intraday_table(series))

    print("\n" + "=" * 60)
    print("DETAILED FORECASTS")
    print("=" * 60)
    for forecast in series.forecasts[:3]:
        print("\n" + format_intraday_forecast(forecast))

    if args.output:
        output_data = {
            "timestamp": datetime.now().isoformat(),
            "spot_price": spot_price,
            "atm_iv_annual": atm_iv,
            "atm_iv_annual_pct": atm_iv * 100,
            "source_expiry": source_expiry,
            "forecasts": series.to_dict()["forecasts"]
        }
        args.output.parent.mkdir(parents=True, exist_ok=True)
        with open(args.output, "w") as f:
            json.dump(output_data, f, indent=2)
        logger.info(f"Results saved to {args.output}")


# =========================================================================
# BL-only mode
# =========================================================================

def run_bl_only(args, config, logger):
    """Run BL-only fast mode (no MC, no surface fitting)."""
    client = DeribitClient(config.api, config.validation)

    targets = args.targets if args.targets else [args.target]

    # Calibrate single model (faster)
    params, spot_price, calibration_expiry, forward, r_squared, ttm, model_type = calibrate_to_expiry(
        client, config, args.expiry
    )

    if args.spot is not None:
        spot_price = args.spot
        logger.info(f"Overriding spot price to: ${spot_price:,.2f}")

    ttm_days = ttm * DAYS_PER_YEAR

    # Extract RND using Breeden-Litzenberger
    logger.info("Extracting Risk-Neutral Density...")
    bl = BreedenLitzenberger(
        strike_grid_points=500, strike_range_std=4.0, use_log_strikes=True
    )

    if model_type == "heston":
        rnd = bl.extract_from_heston(params, forward, use_quantlib=config.heston.use_quantlib)
    else:
        rnd = bl.extract_from_ssvi(params, forward)

    if not rnd.is_valid:
        logger.warning(f"RND extraction warnings: {rnd.warnings}")

    # Calculate probabilities
    results = []
    for target in targets:
        prob_above = bl.probability_above(rnd, target)
        prob_below = bl.probability_below(rnd, target)
        distance_pct = (target - spot_price) / spot_price * 100
        results.append({
            "target_price": target,
            "distance_pct": distance_pct,
            "prob_above": prob_above,
            "prob_below": prob_below,
        })

    # Display
    model_display = model_type.upper()

    lines = [
        "",
        "=" * 75,
        "                    TERMINAL PROBABILITY (B-L ONLY)",
        "=" * 75,
        f" Spot Price: ${spot_price:,.2f}",
        f" Expiry: {calibration_expiry} ({ttm_days:.1f} days) | Model: {model_display} (R²={r_squared:.3f})",
        "-" * 75,
        f" {'Target Price':>14} {'Distance':>10} {'P(Above)':>12} {'P(Below)':>12}",
        "-" * 75,
    ]

    for r in sorted(results, key=lambda x: x["target_price"]):
        lines.append(
            f" ${r['target_price']:>13,.2f} {r['distance_pct']:>+9.1f}% "
            f"{r['prob_above']*100:>11.1f}% {r['prob_below']*100:>11.1f}%"
        )
    lines.extend(["-" * 75, ""])
    print("\n".join(lines))

    # Save to JSON if requested
    if args.output:
        output_data = {
            "timestamp": datetime.now().isoformat(),
            "spot_price": spot_price,
            "forward_price": forward,
            "calibration_expiry": calibration_expiry,
            "ttm_days": ttm_days,
            "model_type": model_type,
            "model_params": params.to_dict(),
            "r_squared": r_squared,
            "rnd_statistics": rnd.to_dict(),
            "probabilities": results,
        }
        args.output.parent.mkdir(parents=True, exist_ok=True)
        with open(args.output, "w") as f:
            json.dump(output_data, f, indent=2, default=str)
        logger.info(f"Results saved to {args.output}")


# =========================================================================
# Barrier mode
# =========================================================================

def run_barrier(args, config, logger):
    """Run barrier (first-passage) probability mode."""
    from btc_pricer.models.barrier_probability import BarrierProbabilityCalculator

    import time as _time

    targets = args.targets if args.targets else [args.target]

    n_sims = args.sims
    steps_per_day = args.steps_per_day
    if hasattr(config, 'monte_carlo'):
        if n_sims is None:
            n_sims = getattr(config.monte_carlo, 'n_simulations', 200000)
        if steps_per_day is None:
            steps_per_day = getattr(config.monte_carlo, 'n_steps_per_day', 288)
    else:
        if n_sims is None:
            n_sims = 200000
        if steps_per_day is None:
            steps_per_day = 288

    client = DeribitClient(config.api, config.validation)

    # Calibrate
    _t0_cal = _time.time()
    heston_data, ssvi_data, spot_price, calibration_expiry, forward, ttm = calibrate_to_expiry(
        client, config, args.expiry, return_both=True
    )
    _calibration_elapsed = _time.time() - _t0_cal

    heston_params, heston_r2 = heston_data if heston_data else (None, None)

    if args.ttm is not None:
        ttm = args.ttm / DAYS_PER_YEAR
    if args.spot is not None:
        spot_price = args.spot

    # SSVI surface fit
    options_by_expiry = client.fetch_all_options("BTC")
    surface_result = fit_ssvi_surface_for_ttm(client, config, ttm, options_by_expiry)

    surface_params_at_ttm = surface_result.params_at_ttm if surface_result else None
    surface_params_obj = surface_result.surface_params if surface_result else None
    surface_r2 = surface_result.r2 if surface_result else None
    surface_info = surface_result.info if surface_result else None

    if heston_params is not None:
        heston_params = HestonParams(
            v0=heston_params.v0, kappa=heston_params.kappa,
            theta=heston_params.theta, xi=heston_params.xi,
            rho=heston_params.rho, ttm=ttm
        )

    calculator = BarrierProbabilityCalculator(
        n_simulations=n_sims,
        n_steps_per_day=steps_per_day,
        use_antithetic=True
    )

    for target in targets:
        barrier_type = "down" if target < spot_price else "up"

        # Run SSVI Surface barrier MC
        barrier_result = None
        if surface_params_at_ttm is not None:
            logger.info(f"Running barrier MC for ${target:,.0f}...")
            _t0 = _time.time()
            barrier_result = calculator.touch_probability_ssvi(
                surface_params_at_ttm, spot_price, forward, target, barrier_type
            )
            _mc_elapsed = _time.time() - _t0
            logger.info(f"Barrier MC completed in {_mc_elapsed:.1f}s")

        # B-L probabilities
        bl = BreedenLitzenberger(
            strike_grid_points=500, strike_range_std=4.0, use_log_strikes=True
        )
        bl_prob_above = None
        bl_prob_below = None
        surface_bl_above = None
        surface_bl_below = None

        if heston_params is not None:
            try:
                rnd = bl.extract_from_heston(heston_params, forward, use_quantlib=True)
                if rnd and rnd.is_valid:
                    bl_prob_above = float(bl.probability_above(rnd, target))
                    bl_prob_below = float(bl.probability_below(rnd, target))
            except Exception as e:
                logger.warning(f"Heston B-L failed: {e}")

        if surface_params_at_ttm is not None:
            try:
                rnd = bl.extract_from_ssvi(surface_params_at_ttm, forward)
                if rnd and rnd.is_valid:
                    surface_bl_above = float(bl.probability_above(rnd, target))
                    surface_bl_below = float(bl.probability_below(rnd, target))
            except Exception as e:
                logger.warning(f"SSVI Surface B-L failed: {e}")

        # Display barrier result
        barrier_dist = (target - spot_price) / spot_price * 100
        current_time_str = format_current_time_multizone()
        lines = [
            "",
            "=" * 70,
            "               BARRIER (FIRST-PASSAGE) PROBABILITY",
            "=" * 70,
            f" Current Time:      {current_time_str}",
            f" Spot Price:        ${spot_price:,.2f} (Binance)",
            f" Barrier:           ${target:,.0f} ({barrier_type.upper()}, {barrier_dist:+.1f}%)",
            f" Time Horizon:      {ttm * DAYS_PER_YEAR:.3f} days ({ttm * DAYS_PER_YEAR * 24:.2f} hours)",
            f" Expiry:            {calibration_expiry}",
            "-" * 70,
        ]

        if barrier_result is not None:
            ci_low, ci_high = barrier_result.confidence_interval
            ci_width = (ci_high - ci_low) / 2 * 100
            lines.append(f" P(touch ${target:,.0f}): {barrier_result.touch_probability * 100:.1f}% +/- {ci_width:.1f}%")
            lines.append(f" P(terminal):        {barrier_result.terminal_probability * 100:.1f}%")
            lines.append(f" Touch/Terminal:      {barrier_result.touch_to_terminal_ratio:.2f}x")
        if surface_bl_above is not None:
            lines.append(f" P(terminal B-L):    >{surface_bl_above*100:.1f}%  <{surface_bl_below*100:.1f}%")

        if surface_info:
            lines.append(f" {surface_info}")
        lines.append(f" Simulations: {n_sims:,}")
        lines.append("=" * 70)
        lines.append("")
        print("\n".join(lines))

    # JSON output
    if args.output and targets:
        target = targets[-1]
        barrier_type = "down" if target < spot_price else "up"
        output = {
            "mode": "barrier",
            "spot_price": spot_price,
            "barrier": target,
            "barrier_type": barrier_type,
            "ttm_days": ttm * DAYS_PER_YEAR,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        if barrier_result is not None:
            output["touch_probability"] = barrier_result.touch_probability
            output["terminal_probability"] = barrier_result.terminal_probability
        if surface_bl_above is not None:
            output["surface_bl_above"] = surface_bl_above
            output["surface_bl_below"] = surface_bl_below
        _write_json_output(args.output, output)
        logger.info(f"JSON output written to {args.output}")


# =========================================================================
# Terminal mode (default — existing behavior)
# =========================================================================

def run_terminal(
    args,
    config,
    logger,
    client=None,
    warm_start_surface_params=None,
    cached_surface_result=None,
    spot_provider=None,
):
    """Run terminal probability mode (default).

    ``spot_provider``: optional zero-arg callable returning the freshest BTC
    spot (or None). Called once after the surface fit so the pricing step uses
    a spot ~1-2s fresher than the one sampled at cycle start. The surface fit
    itself does not depend on spot, so this is a pure freshness win.

    Returns the JSON output payload dict (also written to ``args.output``
    when set), or None when no payload was produced.
    """
    import time as _time
    _calibration_elapsed = None
    _surface_elapsed = None
    _mc_elapsed = None
    _bl_elapsed = None
    _sbl_elapsed = None

    targets = args.targets if args.targets else [args.target]

    n_sims = args.sims
    steps_per_day = args.steps_per_day
    if hasattr(config, 'monte_carlo'):
        if n_sims is None:
            n_sims = getattr(config.monte_carlo, 'n_simulations', 200000)
        if steps_per_day is None:
            steps_per_day = getattr(config.monte_carlo, 'n_steps_per_day', 288)
    else:
        if n_sims is None:
            n_sims = 200000
        if steps_per_day is None:
            steps_per_day = 288

    if client is None:
        client = DeribitClient(config.api, config.validation)

    target_utc = None
    target_tz_display = None
    options_by_expiry = None
    deribit_surface_fingerprint = None
    precomputed_surface_slices = None

    if args.until:
        try:
            target_utc, target_tz_display = parse_datetime_with_timezone(args.until)
            ttm_from_until = calculate_ttm_to_target(target_utc)
            logger.info(f"Target time: {target_utc.strftime('%Y-%m-%d %H:%M UTC')} ({args.until})")
            logger.info(f"TTM to target: {ttm_from_until * DAYS_PER_YEAR:.2f} days")
        except TimeParseError as e:
            raise SystemExit(str(e))

        logger.info("Fetching options data to find appropriate expiry...")
        options_by_expiry = client.fetch_all_options("BTC")
        if not options_by_expiry:
            raise DeribitAPIError("No options data received")
        # One filter pass produces both the fingerprint and the surface
        # slices (the slices feed fit_ssvi_surface_for_ttm below, skipping
        # its duplicate pass).
        deribit_surface_fingerprint, precomputed_surface_slices = (
            prepare_ssvi_surface_inputs(options_by_expiry, config)
        )

        auto_expiry = find_closest_expiry_after(options_by_expiry, target_utc)
        logger.info(f"Auto-selected expiry: {auto_expiry}")

        _t0_cal = _time.time()
        skip_heston = getattr(args, 'skip_heston', False)
        heston_data, ssvi_data, spot_price, calibration_expiry, forward, _ = calibrate_to_expiry(
            client, config, auto_expiry, return_both=True, skip_heston=skip_heston,
            options_by_expiry=options_by_expiry,
            spot_override=args.spot,
        )
        _calibration_elapsed = _time.time() - _t0_cal
        ttm = ttm_from_until
    else:
        _t0_cal = _time.time()
        skip_heston = getattr(args, 'skip_heston', False)
        heston_data, ssvi_data, spot_price, calibration_expiry, forward, ttm = calibrate_to_expiry(
            client, config, args.expiry, return_both=True, skip_heston=skip_heston,
            spot_override=args.spot,
        )
        _calibration_elapsed = _time.time() - _t0_cal

        if args.ttm is not None:
            ttm = args.ttm / DAYS_PER_YEAR
            logger.info(f"Using custom TTM: {args.ttm:.1f} days")

    heston_params, heston_r2 = heston_data if heston_data else (None, None)

    # SSVI Surface fit using extracted helper
    skip_ssvi_surface = getattr(args, 'skip_ssvi_surface', False)
    _fingerprint_hit = (
        cached_surface_result is not None
        and deribit_surface_fingerprint is not None
        and getattr(cached_surface_result, "deribit_surface_fingerprint", None) == deribit_surface_fingerprint
        and getattr(cached_surface_result, "surface_params", None) is not None
    )
    # Max-age reuse: the surface moves in the 3rd decimal over ~10s, so an
    # only-just-stale fit is still far inside its own residual. Disabled by
    # default (reuse_max_age_seconds: 0); the fingerprint match stays exact.
    _age_hit = False
    if not _fingerprint_hit and cached_surface_result is not None:
        _reuse_max_age = float(getattr(config.ssvi_surface, "reuse_max_age_seconds", 0.0) or 0.0)
        _fitted_at = getattr(cached_surface_result, "fitted_at_monotonic", None)
        _age_hit = (
            _reuse_max_age > 0
            and _fitted_at is not None
            and (_time.monotonic() - _fitted_at) < _reuse_max_age
            and getattr(cached_surface_result, "surface_params", None) is not None
        )
    if skip_ssvi_surface:
        surface_result = None
        logger.info("Skipping SSVI Surface calibration (--skip-ssvi-surface)")
    elif _fingerprint_hit or _age_hit:
        cached_params = cached_surface_result.surface_params
        if _fingerprint_hit:
            logger.info("Deribit surface unchanged; reusing previous SSVI surface fit")
        else:
            logger.info("Reusing previous SSVI surface fit inside max-age window")
        surface_result = SurfaceFitResult(
            cached_params.get_params_for_ttm(ttm),
            cached_params,
            cached_surface_result.r2,
            cached_surface_result.info,
            0.0,
            "cached_surface" if _fingerprint_hit else "cached_surface_age",
            cached_surface_result.warm_start_used,
            deribit_surface_fingerprint if _fingerprint_hit else
            getattr(cached_surface_result, "deribit_surface_fingerprint", None),
            True,
            fitted_at_monotonic=getattr(cached_surface_result, "fitted_at_monotonic", None),
        )
    else:
        surface_result = fit_ssvi_surface_for_ttm(
            client,
            config,
            ttm,
            options_by_expiry,
            warm_start_surface_params=warm_start_surface_params,
            precomputed_fingerprint=deribit_surface_fingerprint,
            precomputed_slices=precomputed_surface_slices,
        )
        if surface_result is not None and deribit_surface_fingerprint is not None:
            surface_result.deribit_surface_fingerprint = deribit_surface_fingerprint

    surface_params_at_ttm = None
    surface_params_obj = None
    surface_r2 = None
    surface_info = None
    surface_fit_method = None
    surface_warm_start_used = False
    surface_fit_reused = False

    if surface_result is not None:
        surface_params_at_ttm = surface_result.params_at_ttm
        surface_params_obj = surface_result.surface_params
        surface_r2 = surface_result.r2
        surface_info = surface_result.info
        surface_fit_method = surface_result.fit_method
        surface_warm_start_used = surface_result.warm_start_used
        deribit_surface_fingerprint = surface_result.deribit_surface_fingerprint
        surface_fit_reused = surface_result.surface_fit_reused
        _surface_elapsed = surface_result.elapsed

    # Update params with custom TTM
    if heston_params is not None:
        heston_params = HestonParams(
            v0=heston_params.v0, kappa=heston_params.kappa,
            theta=heston_params.theta, xi=heston_params.xi,
            rho=heston_params.rho, ttm=ttm
        )

    if args.spot is not None:
        spot_price = args.spot
        logger.info(f"Overriding spot price to: ${spot_price:,.0f}")

    # Re-read the freshest spot after the (spot-independent) surface fit so
    # the pricing step doesn't price a cycle-start spot that is already
    # seconds old. Takes precedence over args.spot — it is strictly newer.
    _spot_refresh_used = False
    if spot_provider is not None:
        try:
            _fresh_spot = spot_provider()
        except Exception as exc:  # noqa: BLE001
            if getattr(args, "payoff_kind", "terminal_spot") == "chainlink_twap_60s":
                raise ValueError(f"TWAP spot refresh failed: {exc}") from exc
            _fresh_spot = None
            logger.debug("spot refresh provider failed: %s", exc)
        if _fresh_spot is not None and _fresh_spot > 0:
            _fresh_spot = float(_fresh_spot)
            if spot_price and _fresh_spot != spot_price:
                logger.info(
                    f"Refreshed spot post-fit: ${spot_price:,.2f} -> ${_fresh_spot:,.2f}"
                )
            spot_price = _fresh_spot
            _spot_refresh_used = True
        elif getattr(args, "payoff_kind", "terminal_spot") == "chainlink_twap_60s":
            raise ValueError("TWAP spot refresh unavailable")

    # Surface calibration can outlive the horizon sampled at cycle start.
    # Re-evaluate the maturity at the same instant as the pricing spot.
    payoff_kind = getattr(args, "payoff_kind", "terminal_spot")
    twap_payoff = payoff_kind == "chainlink_twap_60s"
    probability_evaluated_at = datetime.now(timezone.utc)
    if target_utc is not None and twap_payoff:
        ttm = max(0.0, (target_utc - probability_evaluated_at).total_seconds()) / (86400.0 * DAYS_PER_YEAR)
        if ttm <= 0:
            raise ValueError("market ended before probability evaluation")
        if surface_params_obj is not None:
            surface_params_at_ttm = surface_params_obj.get_params_for_ttm(ttm)

    if payoff_kind not in ("terminal_spot", "chainlink_twap_60s"):
        raise ValueError(f"unsupported payoff: {payoff_kind}")

    # Auto-detect direction
    directions = {}
    for target in targets:
        detected_dir = "down" if target < spot_price else "up"
        if args.direction != detected_dir:
            logger.info(
                f"Auto-correcting direction for ${target:,.0f}: "
                f"'{args.direction}' -> '{detected_dir}' (target {'<' if target < spot_price else '>'} spot)"
            )
        directions[target] = detected_dir

    calculator = TerminalProbabilityCalculator(
        n_simulations=n_sims, n_steps_per_day=steps_per_day, use_antithetic=True
    )
    probability_mode = normalize_probability_mode(getattr(config.model, "probability_mode", "mc"))
    if twap_payoff:
        probability_mode = "mc"  # a terminal smile digital is not an average payoff
    run_bl = probability_mode in {"avg", "bl"}
    # cf mode: the closed-form digital replaces the MC; the MC still runs as
    # a shadow every Nth tick purely to track cf/MC divergence.
    run_mc = True
    if probability_mode == "cf":
        cf_shadow_every = int(getattr(config.model, "cf_shadow_mc_every_n_ticks", 5) or 0)
        run_mc = (
            cf_shadow_every > 0
            and next(_CF_SHADOW_TICK_COUNTER) % cf_shadow_every == 0
        )

    json_output = None

    logger.info(
        "Calculating terminal probabilities (trading_model=%s, probability_mode=%s)...",
        config.model.trading_model,
        probability_mode,
    )
    if not run_bl:
        logger.info(
            "B-L terminal probability extraction disabled by probability_mode=%s",
            probability_mode,
        )

    for target in targets:
        heston_result = None
        ssvi_result = None
        cf_above = None
        cf_below = None
        _cf_elapsed = None

        if surface_params_at_ttm is not None and not twap_payoff:
            _t0 = _time.time()
            try:
                cf_above, cf_below = ssvi_digital_prob_pair(
                    surface_params_at_ttm, forward, target, spot=spot_price
                )
            except (ValueError, ZeroDivisionError, OverflowError) as exc:
                logger.warning(f"CF digital computation failed: {exc}")
            _cf_elapsed = _time.time() - _t0

        terminal_return_quantiles = None
        if surface_params_at_ttm is not None and run_mc:
            logger.info(f"Running SSVI Surface Local Vol MC for target ${target:,.0f}...")
            _t0 = _time.time()
            if twap_payoff:
                ssvi_result = calculator.terminal_probability_twap(
                    surface_params_at_ttm, spot_price, forward, target, directions[target],
                    averaging_window_seconds=getattr(args, "averaging_window_seconds", 60.0),
                    max_step_seconds=5.0,
                )
            else:
                ssvi_result = calculator.terminal_probability_ssvi(
                    surface_params_at_ttm, spot_price, forward, target, directions[target]
                )
            _mc_elapsed = _time.time() - _t0
            logger.info(f"SSVI Surface MC completed in {_mc_elapsed:.1f}s")

            # Export the MC terminal-return distribution so consumers can
            # re-evaluate the digital at a moved spot without waiting for the
            # next calibration (schema v4 local evaluation).
            final_prices = getattr(calculator, "last_final_prices", None)
            if not twap_payoff and final_prices is not None and len(final_prices) > 0 and spot_price > 0:
                try:
                    log_returns = np.log(np.maximum(final_prices, 1e-12) / spot_price)
                    quantiles = np.quantile(log_returns, _TERMINAL_RETURN_QUANTILE_PROBS)
                    terminal_return_quantiles = [round(float(q), 8) for q in quantiles]
                except (ValueError, FloatingPointError) as exc:
                    logger.warning(f"terminal-return quantile export failed: {exc}")

        bl_prob_above = None
        bl_prob_below = None
        surface_bl_above = None
        surface_bl_below = None

        if run_bl:
            bl_cfg = config.breeden_litzenberger
            bl = BreedenLitzenberger(
                strike_grid_points=bl_cfg.strike_grid_points,
                strike_range_std=bl_cfg.strike_range_std,
                use_log_strikes=bl_cfg.use_log_strikes,
            )

            if heston_params is not None:
                try:
                    # Use P2 = -dC/dK via native characteristic function.
                    # More stable than B-L density extraction at all TTMs, and
                    # correct for sub-day TTMs (QuantLib rounds to min 1 day).
                    _t0 = _time.time()
                    from btc_pricer.models.heston import HestonModel
                    heston_model = HestonModel(heston_params, use_quantlib=False)
                    eps = 1.0
                    c_up = heston_model.call_price(forward, target + eps)
                    c_dn = heston_model.call_price(forward, target - eps)
                    bl_prob_above = max(0.0, min(1.0, -(c_up - c_dn) / (2 * eps)))
                    bl_prob_below = 1.0 - bl_prob_above
                    _bl_elapsed = _time.time() - _t0
                    logger.info(
                        f"Heston P2 (dC/dK) in {_bl_elapsed:.1f}s: "
                        f"P(>{target:,.0f})={bl_prob_above:.4f}, "
                        f"P(<{target:,.0f})={bl_prob_below:.4f}"
                    )
                except Exception as e:
                    logger.warning(f"Heston dC/dK failed: {e}")

            if surface_params_at_ttm is not None:
                try:
                    _t0 = _time.time()
                    rnd = bl.extract_from_ssvi(surface_params_at_ttm, forward)
                    _sbl_elapsed = _time.time() - _t0
                    if rnd and rnd.is_valid:
                        surface_bl_above = float(bl.probability_above(rnd, target))
                        surface_bl_below = float(bl.probability_below(rnd, target))
                        logger.info(
                            f"SSVI Surface B-L in {_sbl_elapsed:.1f}s: P(>{target:,.0f})={surface_bl_above:.4f}, "
                            f"P(<{target:,.0f})={surface_bl_below:.4f}"
                        )
                    else:
                        logger.warning("SSVI Surface B-L RND extraction returned invalid result")
                except Exception as e:
                    logger.warning(f"SSVI Surface B-L failed: {e}")

        if heston_result is not None and ssvi_result is not None:
            print(format_comparison_result(
                heston_result, ssvi_result,
                heston_params, surface_params_at_ttm,
                heston_r2, surface_r2,
                spot_price, target, directions[target], ttm,
                calibration_expiry, target_utc, target_tz_display,
                surface_bl_above=surface_bl_above, surface_bl_below=surface_bl_below,
                surface_r2=surface_r2, surface_info=surface_info, ssvi_is_surface=True,
            ))
        elif heston_result is not None:
            print(format_terminal_result(
                heston_result, heston_params, heston_r2, "heston",
                calibration_expiry, target_utc, target_tz_display
            ))
        elif ssvi_result is not None:
            print(format_terminal_result(
                ssvi_result, surface_params_at_ttm, surface_r2, "ssvi",
                calibration_expiry, target_utc, target_tz_display
            ))

        s_mc_above, s_mc_below = None, None
        if surface_params_at_ttm is not None and ssvi_result is not None:
            s_mc_above, s_mc_below = decompose_probs(ssvi_result)

        # cf/MC divergence watch. Only alert in cf mode, where cf IS the
        # trading number — in mc mode the divergence is expected (the static
        # -dC/dK and the sticky-moneyness MC are different models, 1-2.5pp
        # apart near ATM at production micro-TTM phi) and is recorded in the
        # payload (cf_mc_divergence) without log noise.
        if probability_mode == "cf" and cf_above is not None and s_mc_above is not None:
            _cf_mc_div = abs(cf_above - s_mc_above)
            _cf_alert = float(getattr(config.model, "cf_mc_divergence_alert", 0.02) or 0.02)
            if _cf_mc_div > _cf_alert:
                logger.warning(
                    "cf/MC divergence %.4f exceeds alert threshold %.4f "
                    "(cf=%.4f mc=%.4f target=%.2f)",
                    _cf_mc_div, _cf_alert, cf_above, s_mc_above, target,
                )

        if args.output:
            timing = {
                "calibration_s": _calibration_elapsed,
                "surface_fit_s": _surface_elapsed,
                "surface_fit_method": surface_fit_method,
                "surface_warm_start_used": surface_warm_start_used,
                "surface_fit_reused": surface_fit_reused,
                "deribit_surface_fingerprint": deribit_surface_fingerprint,
                "mc_s": _mc_elapsed,
                "cf_s": _cf_elapsed,
                "bl_s": _bl_elapsed,
                "surface_bl_s": _sbl_elapsed,
                "spot_refresh_used": _spot_refresh_used,
            }
            json_output = build_json_output(
                heston_r2, spot_price, target, directions[target], ttm,
                heston_params=heston_params,
                bl_prob_above=bl_prob_above, bl_prob_below=bl_prob_below,
                surface_bl_above=surface_bl_above, surface_bl_below=surface_bl_below,
                surface_mc_above=s_mc_above, surface_mc_below=s_mc_below,
                surface_cf_above=cf_above, surface_cf_below=cf_below,
                surface_r2=surface_r2,
                surface_params_at_ttm=surface_params_at_ttm,
                surface_params_obj=surface_params_obj,
                timing=timing,
                trading_model=config.model.trading_model,
                probability_mode=probability_mode,
            )
            if terminal_return_quantiles is not None:
                json_output["terminal_return_quantiles"] = terminal_return_quantiles
                json_output["terminal_return_ttm_days"] = ttm * DAYS_PER_YEAR
            json_output["payoff_kind"] = payoff_kind
            json_output["smile_forward"] = forward
            json_output["averaging_window_seconds"] = 60 if twap_payoff else 0
            json_output["probability_evaluated_at"] = probability_evaluated_at.isoformat()
            if twap_payoff:
                json_output["probability_source_label"] = "SSVI 60s average MC (approximate)"
                json_output["model_assumption"] = "continuous arithmetic trailing average; Chainlink measurement details are not reproduced"

    if args.output and json_output:
        _write_json_output(args.output, json_output)
        logger.info(f"JSON output written to {args.output}")

    return json_output


# =========================================================================
# Main entry point
# =========================================================================

@handle_cli_exceptions
def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="BTC Probability Calculator — terminal, barrier, BL-only, or intraday modes"
    )
    add_common_arguments(parser)
    parser.add_argument("--payoff-kind", choices=("terminal_spot", "chainlink_twap_60s"), default="terminal_spot")
    parser.add_argument("--averaging-window-seconds", type=float, default=60.0)

    # Mode selection
    parser.add_argument(
        "--mode",
        choices=["terminal", "barrier"],
        default="terminal",
        help="Probability mode: terminal (default) or barrier (first-passage)"
    )
    parser.add_argument(
        "--bl-only",
        action="store_true",
        help="BL-only fast mode: skip MC and surface fitting (~5s)"
    )
    parser.add_argument(
        "--intraday",
        action="store_true",
        help="Intraday ATM IV sqrt(T) scaling forecasts"
    )
    parser.add_argument(
        "--skip-heston",
        action="store_true",
        help="Skip Heston calibration (saves ~3 min). SSVI Surface still runs."
    )
    parser.add_argument(
        "--skip-ssvi-surface",
        action="store_true",
        help="Skip SSVI Surface calibration and MC. Heston B-L still runs."
    )

    # Target arguments
    parser.add_argument(
        "--target",
        type=float,
        help="Single target/barrier price level (e.g., 85000)"
    )
    parser.add_argument(
        "--targets",
        type=float,
        nargs="+",
        help="Multiple target/barrier price levels (e.g., 80000 85000 90000 95000)"
    )
    parser.add_argument(
        "--expiry",
        type=str,
        help="Target expiry date (e.g., 31JAN26). Uses nearest liquid expiry if not specified."
    )
    parser.add_argument(
        "--direction",
        choices=["down", "up"],
        default="down",
        help="Direction: 'down' for price dropping to target, 'up' for rising (default: down)"
    )
    parser.add_argument(
        "--sims",
        type=int,
        default=None,
        help="Number of Monte Carlo simulations (default: from config)"
    )
    parser.add_argument(
        "--steps-per-day",
        type=int,
        default=None,
        help="Time steps per day for simulation (default: from config)"
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output JSON file path"
    )
    parser.add_argument(
        "--spot",
        type=float,
        default=None,
        help="Override spot price"
    )
    parser.add_argument(
        "--ttm",
        type=float,
        default=None,
        help="Override time to maturity in days (for custom horizons)"
    )
    parser.add_argument(
        "--until",
        type=str,
        default=None,
        help="Target end time with timezone (e.g., '11:59 PM ET', '2026-01-30 18:00 PST'). "
             "Auto-selects closest expiry after target."
    )

    # Intraday-specific arguments
    parser.add_argument(
        "--hours",
        type=float,
        nargs="+",
        default=None,
        help="Forecast horizons in hours for --intraday mode (e.g., 1 4 12 24)"
    )
    parser.add_argument(
        "--atm-iv",
        type=float,
        default=None,
        help="Override ATM IV for --intraday mode (as decimal, e.g., 0.65 for 65%%)"
    )

    args = parser.parse_args()

    # Validate mutual exclusivity
    exclusive_count = sum([args.intraday, args.bl_only, args.mode == "barrier"])
    if exclusive_count > 1:
        parser.error("--intraday, --bl-only, and --mode barrier are mutually exclusive")

    # Validate intraday doesn't mix with target-based args
    if args.intraday:
        if args.target or args.targets:
            parser.error("--intraday is mutually exclusive with --target/--targets")
    else:
        # All other modes require a target
        if not args.target and not args.targets:
            parser.error("Must specify either --target or --targets (or use --intraday)")

    # Validate --until exclusivity
    if args.until:
        if args.expiry:
            parser.error("--until and --expiry are mutually exclusive")
        if args.ttm:
            parser.error("--until and --ttm are mutually exclusive")

    setup_logging(args.verbose)
    logger = logging.getLogger(__name__)

    config = load_config(args.config, logger)

    # Dispatch to appropriate mode
    if args.intraday:
        run_intraday(args, config, logger)
    elif args.bl_only:
        run_bl_only(args, config, logger)
    elif args.mode == "barrier":
        run_barrier(args, config, logger)
    else:
        run_terminal(args, config, logger)


if __name__ == "__main__":
    main()
