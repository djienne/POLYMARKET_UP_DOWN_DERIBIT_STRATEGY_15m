"""Shared entry-signal math and legacy terminal-distribution re-evaluation.

Schema-v4 terminal-price events can carry quantiles of ln(S_T / spot_at_calc).
Legacy consumers re-threshold that CDF at the current spot, scaling log returns
by sqrt(ttm_now / ttm_at_calc). This holds the smile shape fixed and is an
approximation; interpolation agreement does not establish physical accuracy.

Current schema-v5 TWAP events use the producer's average-payoff scalar and its
original observation/evaluation times. A terminal marginal cannot determine an
average payoff, so evaluate_prob_from_event deliberately refuses TWAP events.
Grid and live share signal formulas but run separate loops; timestamps and
execution checks, rather than shared code alone, establish usable parity.
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

from btc_pricer.edge import has_edge, required_model_prob

log = logging.getLogger(__name__)

# Smallest usable quantile grid. Below this the CDF interpolation error
# rivals the edge thresholds and local evaluation refuses to run (fail back
# to the event's scalar probability).
_MIN_QUANTILE_POINTS = 51


@dataclass(frozen=True)
class LocalProb:
    """A locally re-evaluated probability with its provenance."""

    prob_above: float
    prob_below: float
    spot_used: float
    ttm_scale: float
    source: str = "local_mc_cdf"


def evaluate_prob_from_event(
    event,
    *,
    spot: float,
    now: datetime,
) -> Optional[LocalProb]:
    """Re-evaluate P(S_T >= barrier) at the live ``spot`` from a v4 event.

    Returns None when the event carries no usable distribution (legacy
    schema, degenerate inputs, market already ended) — callers must fall
    back to the event's scalar probability.
    """
    # A terminal marginal does not identify the distribution of a path
    # average. TWAP events use a fresh producer-owned v5 scalar only.
    if getattr(event, "payoff_kind", "legacy") == "chainlink_twap_60s":
        return None
    quantiles = getattr(event, "terminal_return_quantiles", None)
    if not quantiles or len(quantiles) < _MIN_QUANTILE_POINTS:
        return None
    spot_calc = getattr(event, "spot_price", None)
    barrier = getattr(event, "barrier", None)
    if (
        spot is None or spot <= 0
        or spot_calc is None or spot_calc <= 0
        or barrier is None or barrier <= 0
    ):
        return None

    try:
        ttm_now_s = (event.market_end_time - now).total_seconds()
    except Exception:  # noqa: BLE001
        return None
    if ttm_now_s <= 0:
        return None

    # Brownian time-decay rescaling of the return distribution.
    scale = 1.0
    ttm_calc_days = getattr(event, "terminal_return_ttm_days", None)
    if ttm_calc_days is not None and ttm_calc_days > 0:
        ttm_calc_s = float(ttm_calc_days) * 86400.0
        if ttm_calc_s > 0:
            scale = math.sqrt(max(ttm_now_s, 1e-9) / ttm_calc_s)

    threshold = math.log(barrier / spot) / max(scale, 1e-9)

    n = len(quantiles)
    if threshold <= quantiles[0]:
        cdf = 0.0
    elif threshold >= quantiles[-1]:
        cdf = 1.0
    else:
        # Binary search + linear interpolation on the (return -> prob) grid.
        lo, hi = 0, n - 1
        while hi - lo > 1:
            mid = (lo + hi) // 2
            if quantiles[mid] <= threshold:
                lo = mid
            else:
                hi = mid
        q_lo, q_hi = quantiles[lo], quantiles[hi]
        p_lo = lo / (n - 1)
        p_hi = hi / (n - 1)
        if q_hi > q_lo:
            cdf = p_lo + (p_hi - p_lo) * (threshold - q_lo) / (q_hi - q_lo)
        else:
            cdf = 0.5 * (p_lo + p_hi)

    prob_above = min(1.0, max(0.0, 1.0 - cdf))
    return LocalProb(
        prob_above=prob_above,
        prob_below=1.0 - prob_above,
        spot_used=float(spot),
        ttm_scale=scale,
    )


def compute_entry_signals(
    prob_above: float,
    prob_below: float,
    up_best_ask: float,
    down_best_ask: float,
    signal_params: list[dict],
    market_id: str = "",
) -> tuple:
    """Evaluate edge for all param combos. Returns tuple of signal dicts.

    Single source of truth for signal math — the calc daemon (published
    signals) and local-eval consumers (per-tick signals) both call this.
    """
    signals = []
    for p in signal_params:
        for direction, model_prob, market_prob, alpha_key, floor_key in (
            ("UP", prob_above, up_best_ask, "alpha_up", "floor_up"),
            ("DOWN", prob_below, down_best_ask, "alpha_down", "floor_down"),
        ):
            if market_prob <= 0:
                continue
            if market_prob > p["max_entry_price"]:
                continue
            alpha = p[alpha_key]
            floor = p[floor_key]
            if has_edge(model_prob, market_prob, alpha, floor):
                signals.append({
                    "instance_id": p["instance_id"],
                    "market_id": market_id,
                    "direction": direction,
                    "edge_ratio": round(model_prob / market_prob, 4) if market_prob > 0 else 0,
                    "executable_price": market_prob,
                    "required_prob": round(required_model_prob(market_prob, alpha, floor), 4),
                    "model_prob": round(model_prob, 4),
                })
    return tuple(signals)


def load_signal_params(config_path: Path) -> list[dict]:
    """Load grid + live param combos from config.yaml for signal computation.

    Moved from calc_daemon so producers and consumers resolve the identical
    parameter set from the identical file.
    """
    from scripts.trader.config import GridConfig, TraderConfig

    config_path = Path(config_path)
    if not config_path.exists():
        log.warning("config.yaml not found at %s; entry signals disabled", config_path)
        return []
    try:
        grid_config = GridConfig.from_file(config_path)
    except Exception as exc:  # noqa: BLE001
        log.warning("failed to load GridConfig from %s for signal params: %s", config_path, exc)
        return []

    params_list: list[dict] = []
    grid_params = grid_config.generate_params()
    for idx, params in enumerate(grid_params):
        params_list.append({
            "instance_id": idx,
            "alpha_up": params.alpha_up,
            "alpha_down": params.alpha_down,
            "floor_up": params.floor_up,
            "floor_down": params.floor_down,
            "max_entry_price": grid_config.max_entry_price,
        })

    live_signal_loaded = False
    try:
        live_tc = TraderConfig.from_json(config_path)
        params_list.append({
            "instance_id": "live",
            "alpha_up": live_tc.edge_alpha_up,
            "alpha_down": live_tc.edge_alpha_down,
            "floor_up": live_tc.edge_floor_up,
            "floor_down": live_tc.edge_floor_down,
            "max_entry_price": live_tc.max_entry_price,
        })
        live_signal_loaded = True
    except Exception as exc:  # noqa: BLE001
        log.warning("failed to load TraderConfig from %s for live signals: %s", config_path, exc)

    log.info(
        "loaded %d entry signal param combos (%d grid + %s live)",
        len(params_list),
        len(params_list) - (1 if live_signal_loaded else 0),
        1 if live_signal_loaded else 0,
    )
    return params_list
