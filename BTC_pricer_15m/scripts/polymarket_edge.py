#!/usr/bin/env python3
"""Find edge between Polymarket prices and model probabilities."""

import argparse
import contextlib
import io
import json
import logging
import os
import subprocess
import re
import sys
import threading
import time
from pathlib import Path
from types import SimpleNamespace

# Add project root to path for btc_pricer imports
sys.path.insert(0, str(Path(__file__).parent.parent))
from btc_pricer.edge import has_edge as _has_edge, required_model_prob
from scripts.trader._platform import subprocess_kwargs as _subprocess_kwargs

# Default JSON file paths
DEFAULT_POLYMARKET_JSON = Path(__file__).parent.parent / "results" / "polymarket_data.json"
DEFAULT_TERMINAL_JSON = Path(__file__).parent.parent / "results" / "terminal_data.json"
DEFAULT_CONFIG_PATH = Path("config.yaml")
log = logging.getLogger(__name__)


class TerminalCalibrationWorker:
    """Long-lived in-process runner for cli_terminal terminal calibrations.

    The old path launched a fresh Python process for every calibration. This
    worker keeps imports, config, a Deribit HTTP session, and JIT-compiled code
    alive across calls while preserving the terminal JSON contract.
    """

    def __init__(self, config_path: Path = DEFAULT_CONFIG_PATH):
        self.config_path = Path(config_path)
        self._lock = threading.Lock()
        self._config = None
        self._client = None
        self._run_terminal = None
        self._last_surface_params = None
        self._last_surface_fit_result = None
        self._last_surface_cache_context = None
        self._logger = logging.getLogger("terminal_calibration_worker")

    def _ensure_ready(self):
        if self._config is None:
            from btc_pricer.cli.common import load_config

            self._config = load_config(self.config_path, self._logger)

        if self._client is None:
            from btc_pricer.api.deribit import DeribitClient

            self._client = DeribitClient(self._config.api, self._config.validation)

        if self._run_terminal is None:
            import cli_terminal

            self._run_terminal = cli_terminal.run_terminal

    def _reset_client(self):
        self._client = None

    def run(
        self,
        *,
        barrier: float,
        hours: float,
        json_file: Path,
        market_end_time: str = None,
        skip_heston: bool = False,
        skip_ssvi_surface: bool = False,
        verbose: bool = False,
        spot: float | None = None,
        spot_provider=None,
        payoff_kind: str = "terminal_spot",
        averaging_window_seconds: float = 60.0,
    ) -> dict:
        args = SimpleNamespace(
            targets=None,
            target=barrier,
            sims=None,
            steps_per_day=None,
            until=market_end_time,
            expiry=None,
            ttm=None if market_end_time else hours / 24.0,
            spot=spot,
            direction="down",
            skip_heston=skip_heston,
            skip_ssvi_surface=skip_ssvi_surface,
            output=Path(json_file),
            payoff_kind=payoff_kind,
            averaging_window_seconds=averaging_window_seconds,
        )

        with self._lock:
            self._ensure_ready()
            stdout = io.StringIO()
            started = time.time()
            cache_context = (
                float(barrier),
                str(market_end_time or f"hours:{float(hours):.8f}"),
                bool(skip_heston),
                bool(skip_ssvi_surface),
            )
            cached_surface_result = (
                self._last_surface_fit_result
                if self._last_surface_cache_context == cache_context
                else None
            )
            try:
                with contextlib.redirect_stdout(stdout):
                    payload = self._run_terminal(
                        args,
                        self._config,
                        self._logger,
                        client=self._client,
                        warm_start_surface_params=self._last_surface_params,
                        cached_surface_result=cached_surface_result,
                        spot_provider=spot_provider,
                    )
            except Exception:
                # A broken requests session should not poison later in-process
                # attempts. The caller may still fall back to subprocess.
                self._reset_client()
                raise

            output = stdout.getvalue()
            result = None
            if isinstance(payload, dict):
                # Fast path: consume the payload in-memory instead of
                # re-reading the JSON file the worker just wrote.
                result = parse_terminal_json(payload)
                if result.get("prob_above") is not None:
                    result["terminal_payload"] = payload
                else:
                    result = None
            if result is None:
                result = _parse_terminal_result_from_json_or_stdout(
                    Path(json_file), output, verbose=verbose
                )
            surface_params = _surface_params_from_result(result)
            if surface_params is not None:
                self._last_surface_params = surface_params
            surface_fit_result = _surface_fit_result_from_result(result)
            if surface_fit_result is not None:
                # Stamp the fit time for the max-age reuse window. A cache
                # reuse inherits the ORIGINAL fit's timestamp so chained
                # reuses can't extend the window indefinitely.
                if result.get("surface_fit_reused"):
                    prev = self._last_surface_fit_result
                    surface_fit_result.fitted_at_monotonic = (
                        getattr(prev, "fitted_at_monotonic", None) if prev is not None else None
                    )
                else:
                    surface_fit_result.fitted_at_monotonic = time.monotonic()
                self._last_surface_fit_result = surface_fit_result
                self._last_surface_cache_context = cache_context
            result["raw_output"] = output
            result["calibration_seconds"] = time.time() - started
            return result


_terminal_worker = None
_terminal_worker_lock = threading.Lock()


def _get_terminal_worker() -> TerminalCalibrationWorker:
    global _terminal_worker
    with _terminal_worker_lock:
        if _terminal_worker is None:
            _terminal_worker = TerminalCalibrationWorker()
        return _terminal_worker


def parse_terminal_json(data: dict) -> dict:
    """Parse raw terminal_data JSON into the dict format expected by the trader.

    This is extracted from run_terminal_script() so it can be reused
    for shared calibration cache reads without running a subprocess.
    """
    preferred = data.get("preferred_model", "ssvi_surface")
    model_data = data.get(preferred) or {}
    selected_above = data.get("selected_prob_above")
    selected_below = data.get("selected_prob_below")
    legacy_selected_above = data.get("avg_prob_above")
    legacy_selected_below = data.get("avg_prob_below")
    if (
        not model_data
        and selected_above is None
        and legacy_selected_above is None
    ):
        return {"prob_above": None, "prob_below": None}

    used_model = preferred
    alt_model = "heston" if preferred == "ssvi_surface" else "ssvi_surface"
    alt_data = data.get(alt_model) or data.get("heston") or {}
    surface_data = data.get("ssvi_surface") or {}
    heston_data = data.get("heston") or {}
    if selected_above is None:
        selected_above = legacy_selected_above
        selected_below = legacy_selected_below
    if selected_above is None:
        selected_above = model_data.get("prob_above")
        selected_below = model_data.get("prob_below")

    result_data = {
        "prob_above": selected_above,
        "prob_below": selected_below,
        "spot_price": data.get("spot_price"),
        "model_used": used_model,
        "probability_mode": data.get("probability_mode"),
        "probability_source_label": data.get("probability_source_label"),
        "selected_prob_above": selected_above,
        "selected_prob_below": selected_below,
        "r_squared": model_data.get("r_squared"),
        "model_params": model_data.get("params"),
        "heston_r_squared": heston_data.get("r_squared"),
        "ssvi_r_squared": (data.get("ssvi") or {}).get("r_squared"),
        "alt_model": alt_model if alt_data else None,
        "alt_prob_above": alt_data.get("prob_above"),
        "alt_prob_below": alt_data.get("prob_below"),
        "alt_r_squared": alt_data.get("r_squared"),
        "alt_params": alt_data.get("params"),
        "bl_prob_above": data.get("bl_prob_above"),
        "bl_prob_below": data.get("bl_prob_below"),
        "bl_mc_divergence": data.get("bl_mc_divergence"),
        "cf_mc_divergence": data.get("cf_mc_divergence"),
        "surface_cf_above": surface_data.get("cf_prob_above"),
        "surface_cf_below": surface_data.get("cf_prob_below"),
        "surface_mc_above": surface_data.get("mc_prob_above"),
        "surface_mc_below": surface_data.get("mc_prob_below"),
        "surface_bl_above": surface_data.get("prob_above"),
        "surface_bl_below": surface_data.get("prob_below"),
        "surface_r_squared": surface_data.get("r_squared"),
        "heston_bl_above": heston_data.get("prob_above"),
        "heston_bl_below": heston_data.get("prob_below"),
        "surface_params": surface_data.get("surface_params"),
        "terminal_return_quantiles": data.get("terminal_return_quantiles"),
        "terminal_return_ttm_days": data.get("terminal_return_ttm_days"),
        "payoff_kind": data.get("payoff_kind", "terminal_spot"),
        "averaging_window_seconds": data.get("averaging_window_seconds", 0),
        "probability_evaluated_at": data.get("probability_evaluated_at"),
        "surface_fit_method": (data.get("timing") or {}).get("surface_fit_method"),
        "surface_warm_start_used": (data.get("timing") or {}).get("surface_warm_start_used"),
        "surface_fit_reused": (data.get("timing") or {}).get("surface_fit_reused"),
        "surface_fit_s": (data.get("timing") or {}).get("surface_fit_s"),
        "deribit_surface_fingerprint": (data.get("timing") or {}).get("deribit_surface_fingerprint"),
    }
    result_data["mc_prob_above"] = result_data["surface_mc_above"]
    result_data["mc_prob_below"] = result_data["surface_mc_below"]

    return result_data


def _surface_fit_result_from_result(result: dict):
    """Rehydrate the last successful SSVI surface fit for unchanged-surface reuse."""
    params = _surface_params_from_result(result)
    fingerprint = result.get("deribit_surface_fingerprint")
    if params is None or not fingerprint:
        return None
    try:
        from btc_pricer.cli.common import SurfaceFitResult

        first_ttm = float(params.ttms[0])
        return SurfaceFitResult(
            params.get_params_for_ttm(first_ttm),
            params,
            result.get("surface_r_squared"),
            None,
            float(result.get("surface_fit_s") or 0.0),
            str(result.get("surface_fit_method") or "unknown"),
            bool(result.get("surface_warm_start_used")),
            str(fingerprint),
            bool(result.get("surface_fit_reused")),
        )
    except (AttributeError, IndexError, TypeError, ValueError) as exc:
        logging.getLogger(__name__).warning(
            "Could not restore SSVI surface fit cache: %s", exc
        )
        return None


def _surface_params_from_result(result: dict):
    """Rehydrate successful SSVI surface params from terminal JSON parse output."""
    params = result.get("surface_params")
    if not params:
        return None

    try:
        from btc_pricer.models.ssvi import SSVISurfaceParams

        return SSVISurfaceParams(
            rho=params["rho"],
            eta=params["eta"],
            lam=params["lam"],
            thetas=list(params["thetas"]),
            ttms=list(params["ttms"]),
            expiry_names=list(params["expiry_names"]),
        )
    except (KeyError, TypeError, ValueError) as exc:
        logging.getLogger(__name__).warning(
            "Could not restore SSVI surface warm-start params: %s", exc
        )
        return None


def parse_polymarket_json(data: dict) -> dict:
    """Parse raw polymarket_data JSON into the dict format expected by the trader."""
    return {
        "barrier": data.get("barrier"),
        "hours_remaining": data.get("hours_remaining"),
        "hours": data.get("hours"),
        "minutes": data.get("minutes"),
        "prob_up": data.get("prob_up"),
        "prob_down": data.get("prob_down"),
        "current_price": data.get("current_price"),
    }


def _parse_polymarket_stdout(output: str) -> dict:
    """Parse Polymarket script stdout using regex (fallback method).

    Args:
        output: Raw stdout from polymarket_btc_daily.py

    Returns:
        Dictionary with parsed market data
    """
    # Parse: "Price to beat: $102,500.00"
    barrier_match = re.search(r"Price to beat: \$([0-9,]+\.?\d*)", output)
    barrier = float(barrier_match.group(1).replace(",", "")) if barrier_match else None

    # Parse: "Time remaining: 5h 23m"
    time_match = re.search(r"Time remaining: (\d+)h (\d+)m", output)
    hours = int(time_match.group(1)) if time_match else 0
    minutes = int(time_match.group(2)) if time_match else 0

    # Parse: "UP    65.0% ($0.650)"
    up_match = re.search(r"UP\s+([0-9.]+)%", output)
    prob_up = float(up_match.group(1)) / 100 if up_match else None

    # Parse: "DOWN  35.0% ($0.350)"
    down_match = re.search(r"DOWN\s+([0-9.]+)%", output)
    prob_down = float(down_match.group(1)) / 100 if down_match else None

    # Parse: "Current price: $103,150.00"
    current_match = re.search(r"Current price: \$([0-9,]+\.?\d*)", output)
    current_price = float(current_match.group(1).replace(",", "")) if current_match else None

    return {
        "barrier": barrier,
        "hours_remaining": hours + minutes / 60,
        "hours": hours,
        "minutes": minutes,
        "prob_up": prob_up,
        "prob_down": prob_down,
        "current_price": current_price,
        "raw_output": output
    }


def run_polymarket_script(verbose: bool = False, json_path: Path = None):
    """Run polymarket_btc_daily.py and parse output.

    Args:
        verbose: Print raw script output
        json_path: Path for JSON output file (uses default if None)

    Returns:
        Dictionary with market data
    """
    script_dir = Path(__file__).parent
    script_path = script_dir / "polymarket_btc_daily.py"
    json_file = json_path or DEFAULT_POLYMARKET_JSON

    # Run with --json flag
    result = subprocess.run(
        [sys.executable, str(script_path), "--json", str(json_file)],
        **_subprocess_kwargs(capture_output=True, text=True),
    )
    output = result.stdout

    # Check for subprocess failure
    if result.returncode != 0:
        log.error("polymarket_btc_daily.py failed with code %s", result.returncode)
        if result.stderr:
            log.error("stderr: %s", result.stderr[:500])
        if not output:
            log.error("stdout was empty")
        # Return dict with None values to signal failure
        return {
            "barrier": None,
            "hours_remaining": 0,
            "hours": 0,
            "minutes": 0,
            "prob_up": None,
            "prob_down": None,
            "current_price": None,
            "raw_output": output,
            "error": result.stderr
        }

    if verbose:
        log.debug("polymarket_btc_daily.py completed (returncode=0)")
        print("=== Polymarket Script Output ===")
        print(output)
        print("================================\n")

    # Try to read JSON output first
    try:
        if json_file.exists():
            if verbose:
                log.debug("Reading JSON from %s", json_file)
            with open(json_file, "r") as f:
                data = json.load(f)

            result_data = parse_polymarket_json(data)
            result_data["raw_output"] = output
            if verbose:
                log.debug(
                    "Polymarket JSON parsed: barrier=%s, prob_up=%s, prob_down=%s",
                    result_data["barrier"],
                    result_data["prob_up"],
                    result_data["prob_down"],
                )
            return result_data
        else:
            log.warning("JSON file does not exist: %s", json_file)
    except (json.JSONDecodeError, OSError) as e:
        log.warning("Failed to read JSON (%s), falling back to regex: %s", json_file, e)

    # Fallback to regex parsing
    if verbose:
        log.debug("Using regex fallback for Polymarket parsing")
    fallback_result = _parse_polymarket_stdout(output)
    if fallback_result["barrier"] is None:
        log.error("Regex fallback also failed - could not parse Polymarket data from stdout")
        log.error("stdout preview: %s", output[:500] if output else "EMPTY")
    elif verbose:
        log.debug(
            "Regex parsed: barrier=%s, prob_up=%s, prob_down=%s",
            fallback_result["barrier"],
            fallback_result["prob_up"],
            fallback_result["prob_down"],
        )
    return fallback_result


def _parse_terminal_stdout(output: str) -> dict:
    """Parse terminal script stdout using regex (fallback method).

    Args:
        output: Raw stdout from cli_terminal.py

    Returns:
        Dictionary with parsed model probabilities
    """
    # Parse SSVI row from table format (cli_terminal.py output):
    # " SSVI Surface MC       0.992     98.4% ±0.0%            1.6%"
    # Columns: model, R², P(>target) ±ci, P(<target)
    ssvi_match = re.search(
        r"SSVI (?:Surface|Local Vol) MC\s+[\d.]+\s+([\d.]+)%\s*[±\+\-][\d.]+%\s+([\d.]+)%",
        output
    )
    if ssvi_match:
        prob_above = float(ssvi_match.group(1)) / 100
        prob_below = float(ssvi_match.group(2)) / 100
    else:
        # Fallback to Heston row if SSVI not found
        heston_match = re.search(
            r"Heston MC\s+[\d.]+\s+([\d.]+)%\s*[±\+\-][\d.]+%\s+([\d.]+)%",
            output
        )
        if heston_match:
            prob_above = float(heston_match.group(1)) / 100
            prob_below = float(heston_match.group(2)) / 100
        else:
            prob_above = None
            prob_below = None

    # Parse spot price: "Spot Price:        $103,150.00 (Binance)"
    spot_match = re.search(r"Spot Price:\s+\$([0-9,]+\.?\d*)", output)
    spot_price = float(spot_match.group(1).replace(",", "")) if spot_match else None

    return {
        "prob_above": prob_above,
        "prob_below": prob_below,
        "spot_price": spot_price,
        "raw_output": output
    }


def _parse_terminal_result_from_json_or_stdout(
    json_file: Path,
    output: str,
    *,
    verbose: bool = False,
) -> dict:
    """Parse terminal output using JSON first, then stdout regex fallback."""
    try:
        if json_file.exists():
            if verbose:
                log.debug("Reading terminal JSON from %s", json_file)
            with open(json_file, "r") as f:
                data = json.load(f)

            preferred = data.get("preferred_model", "ssvi_surface")
            result_data = parse_terminal_json(data)
            if result_data.get("prob_above") is not None:
                result_data["raw_output"] = output
                if verbose:
                    log.debug(
                        "Terminal JSON parsed (model=%s): prob_above=%s, prob_below=%s",
                        result_data.get("model_used"),
                        result_data["prob_above"],
                        result_data["prob_below"],
                    )
                return result_data
            else:
                log.warning(
                    "No model data found in JSON (preferred=%s, keys=%s)",
                    preferred,
                    list(data.keys()),
                )
        else:
            log.warning("Terminal JSON file does not exist: %s", json_file)
    except (json.JSONDecodeError, OSError) as e:
        log.warning("Failed to read terminal JSON (%s), falling back to regex: %s", json_file, e)

    if verbose:
        log.debug("Using regex fallback for terminal parsing")
    fallback_result = _parse_terminal_stdout(output)
    if fallback_result["prob_above"] is None:
        log.error("Regex fallback also failed - could not parse model probabilities from stdout")
        log.error("stdout preview: %s", output[:500] if output else "EMPTY")
    elif verbose:
        log.debug(
            "Regex parsed: prob_above=%s, prob_below=%s",
            fallback_result["prob_above"],
            fallback_result["prob_below"],
        )
    return fallback_result


def _build_terminal_command(
    *,
    barrier: float,
    hours: float,
    json_file: Path,
    market_end_time: str = None,
    skip_heston: bool = False,
    skip_ssvi_surface: bool = False,
    payoff_kind: str = "terminal_spot",
    averaging_window_seconds: float = 60.0,
) -> list:
    script_dir = Path(__file__).parent.parent
    script_path = script_dir / "cli_terminal.py"

    cmd = [
        sys.executable,
        str(script_path),
        "--target",
        str(barrier),
        "--output",
        str(json_file),
    ]

    if market_end_time:
        cmd.extend(["--until", market_end_time])
    else:
        # Fallback: raw TTM (calibration may pick wrong Deribit expiry near market end)
        cmd.extend(["--ttm", str(hours / 24.0)])

    if skip_heston:
        cmd.append("--skip-heston")

    if skip_ssvi_surface:
        cmd.append("--skip-ssvi-surface")

    if payoff_kind != "terminal_spot":
        cmd.extend(["--payoff-kind", payoff_kind, "--averaging-window-seconds", str(averaging_window_seconds)])

    return cmd


def _run_terminal_subprocess(
    barrier: float,
    hours: float,
    *,
    verbose: bool = False,
    json_path: Path = None,
    market_end_time: str = None,
    skip_heston: bool = False,
    skip_ssvi_surface: bool = False,
    spot: float | None = None,
    payoff_kind: str = "terminal_spot",
    averaging_window_seconds: float = 60.0,
) -> dict:
    json_file = json_path or DEFAULT_TERMINAL_JSON
    cmd = _build_terminal_command(
        barrier=barrier,
        hours=hours,
        json_file=json_file,
        market_end_time=market_end_time,
        skip_heston=skip_heston,
        skip_ssvi_surface=skip_ssvi_surface,
        payoff_kind=payoff_kind,
        averaging_window_seconds=averaging_window_seconds,
    )
    if spot is not None:
        cmd.extend(["--spot", str(spot)])

    result = subprocess.run(
        cmd,
        **_subprocess_kwargs(capture_output=True, text=True),
    )
    output = result.stdout

    if result.returncode != 0:
        log.error("cli_terminal.py failed with code %s", result.returncode)
        if result.stderr:
            log.error("stderr: %s", result.stderr[:500])
        if not output:
            log.error("stdout was empty")
        return {
            "prob_above": None,
            "prob_below": None,
            "spot_price": None,
            "raw_output": output,
            "error": result.stderr,
        }

    if verbose:
        log.debug("cli_terminal.py completed (returncode=0)")
        print("=== Terminal Script Output ===")
        print(output)
        print("==============================\n")

    return _parse_terminal_result_from_json_or_stdout(
        Path(json_file), output, verbose=verbose
    )


def run_terminal_script(barrier: float, hours: float, verbose: bool = False,
                        json_path: Path = None, market_end_time: str = None,
                        skip_heston: bool = False, skip_ssvi_surface: bool = False,
                        use_inprocess: bool = None,
                        spot: float | None = None,
                        spot_provider=None,
                        payoff_kind: str = "terminal_spot",
                        averaging_window_seconds: float = 60.0):
    """Run cli_terminal.py and parse output.

    Args:
        barrier: Reference price level
        hours: Time remaining in hours
        verbose: Print raw script output
        json_path: Path for JSON output file (uses default if None)
        market_end_time: ISO format market expiry time (e.g. "2026-02-20T18:00:00+00:00").
            When provided, passes --until instead of --ttm so cli_terminal.py picks the
            correct Deribit expiry for calibration.
        skip_heston: If True, pass --skip-heston to skip Heston fitting (~3 min faster).
        spot_provider: Optional zero-arg callable returning the freshest BTC spot;
            called post-fit so the pricing step uses a fresher spot (in-process only).
            For TWAP, a missing/failed refresh aborts that cycle without a
            subprocess fallback or reuse of the cycle-start observation.

    Returns:
        Dictionary with model probabilities
    """
    json_file = json_path or DEFAULT_TERMINAL_JSON

    if use_inprocess is None:
        mode = os.getenv("BTC_PRICER_CALIBRATION_MODE", "inprocess").strip().lower()
        use_inprocess = mode not in {"subprocess", "legacy", "cli"}

    if use_inprocess:
        try:
            return _get_terminal_worker().run(
                barrier=barrier,
                hours=hours,
                verbose=verbose,
                json_file=Path(json_file),
                market_end_time=market_end_time,
                skip_heston=skip_heston,
                skip_ssvi_surface=skip_ssvi_surface,
                spot=spot,
                spot_provider=spot_provider,
                payoff_kind=payoff_kind,
                averaging_window_seconds=averaging_window_seconds,
            )
        except Exception as exc:
            if payoff_kind == "chainlink_twap_60s":
                # A subprocess fallback cannot preserve the sampled spot
                # callback/provenance after an in-process pricing failure.
                # Retry the next producer cycle instead of publishing a
                # newly timestamped forecast from different inputs.
                log.warning("TWAP calibration failed; next cycle will retry: %s", exc)
                return {"prob_above": None, "prob_below": None, "error": str(exc), "payoff_kind": payoff_kind}
            log.warning("In-process calibration failed, falling back to subprocess: %s", exc)

    return _run_terminal_subprocess(
        barrier,
        hours,
        verbose=verbose,
        json_path=Path(json_file),
        market_end_time=market_end_time,
        skip_heston=skip_heston,
        skip_ssvi_surface=skip_ssvi_surface,
        spot=spot,
        payoff_kind=payoff_kind,
        averaging_window_seconds=averaging_window_seconds,
    )


def find_opportunities(poly_data: dict, model_data: dict,
                       alpha_up: float, alpha_down: float,
                       floor_up: float, floor_down: float):
    """Find trading opportunities with sufficient edge.

    Uses the continuous edge function: required = max(floor, 1 - (1 - market_prob)^alpha).
    Alpha/floor are specified per-direction so UP and DOWN can have different curve shapes.
    The edge ratio is still computed for display but the entry decision uses the continuous function.
    """
    opportunities = []

    # Check UP opportunity (model says price ends above barrier)
    if poly_data["prob_up"] and model_data["prob_above"]:
        market_p = poly_data["prob_up"]
        model_p = model_data["prob_above"]
        edge_up = model_p / market_p if market_p > 0 else 0
        req = required_model_prob(market_p, alpha_up, floor_up)
        opportunities.append({
            "direction": "UP",
            "polymarket_prob": market_p,
            "model_prob": model_p,
            "edge": edge_up,
            "has_edge": _has_edge(model_p, market_p, alpha_up, floor_up),
            "required_prob": req,
            "market_entry": market_p,
            "take_profit": market_p * 1.20,
        })

    # Check DOWN opportunity (model says price ends below barrier)
    if poly_data["prob_down"] and model_data["prob_below"]:
        market_p = poly_data["prob_down"]
        model_p = model_data["prob_below"]
        edge_down = model_p / market_p if market_p > 0 else 0
        req = required_model_prob(market_p, alpha_down, floor_down)
        opportunities.append({
            "direction": "DOWN",
            "polymarket_prob": market_p,
            "model_prob": model_p,
            "edge": edge_down,
            "has_edge": _has_edge(model_p, market_p, alpha_down, floor_down),
            "required_prob": req,
            "market_entry": market_p,
            "take_profit": market_p * 1.20,
        })

    return opportunities


def display_results(poly_data: dict, model_data: dict, opportunities: list,
                    alpha_up: float = 1.5, alpha_down: float = 1.5,
                    floor_up: float = 0.35, floor_down: float = 0.35):
    """Display comparison and opportunities."""
    barrier = poly_data["barrier"]
    hours = poly_data["hours"]
    minutes = poly_data["minutes"]
    spot = model_data["spot_price"] or poly_data["current_price"]

    print()
    print("=" * 60)
    print("           POLYMARKET EDGE FINDER")
    print("=" * 60)
    print(f" Barrier: ${barrier:,.2f} (price to beat)")
    print(f" Time remaining: {hours}h {minutes}m")
    if spot:
        print(f" Current BTC: ${spot:,.2f}")
    if alpha_up == alpha_down and floor_up == floor_down:
        print(f" Edge curve: alpha={alpha_up}, floor={floor_up:.0%}")
    else:
        print(f" Edge curve: UP alpha={alpha_up} floor={floor_up:.0%} | DOWN alpha={alpha_down} floor={floor_down:.0%}")
    print("-" * 60)

    # Comparison table
    print(f" {'Direction':<18} {'Polymarket':>12} {'Model':>10} {'Required':>10} {'Edge':>10}")
    print("-" * 60)

    for opp in opportunities:
        direction = opp["direction"]
        if direction == "UP":
            label = f"UP (>${barrier:,.0f})"
        else:
            label = f"DOWN (<${barrier:,.0f})"

        poly_pct = f"{opp['polymarket_prob'] * 100:.1f}%"
        model_pct = f"{opp['model_prob'] * 100:.1f}%"
        req_pct = f"{opp['required_prob'] * 100:.1f}%"
        edge_str = f"{opp['edge']:.2f}x"

        print(f" {label:<18} {poly_pct:>12} {model_pct:>10} {req_pct:>10} {edge_str:>10}")

    print("-" * 60)
    print()

    # Check for opportunities
    edge_opportunities = [o for o in opportunities if o["has_edge"]]

    if not edge_opportunities:
        if alpha_up == alpha_down and floor_up == floor_down:
            print(f" No edge found (alpha={alpha_up}, floor={floor_up:.0%}).")
        else:
            print(f" No edge found.")
        print()
        print("=" * 60)
        return

    # Display each opportunity
    for opp in edge_opportunities:
        print(f" >>> OPPORTUNITY: BUY {opp['direction']} <<<")
        print()
        print(f" Polymarket says: {opp['polymarket_prob'] * 100:.1f}%")
        print(f" Model says:      {opp['model_prob'] * 100:.1f}%")
        print(f" Required:        {opp['required_prob'] * 100:.1f}%")
        print(f" Edge ratio:      {opp['edge']:.2f}x")
        print()
        print(" Entry Prices:")
        print(f"   Market buy:      ${opp['market_entry']:.3f}")
        print()
        print(f" Take Profit:       ${opp['take_profit']:.3f} (20% gain)")
        print()

    print("=" * 60)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Find edge between Polymarket prices and model probabilities"
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Show raw output from underlying scripts"
    )
    parser.add_argument(
        "--alpha-up",
        type=float,
        default=1.5,
        help="Edge curve exponent for UP bets (default: 1.5). Higher = more edge required at high confidence."
    )
    parser.add_argument(
        "--alpha-down",
        type=float,
        default=1.5,
        help="Edge curve exponent for DOWN bets (default: 1.5)."
    )
    parser.add_argument(
        "--floor-up",
        type=float,
        default=0.65,
        help="Minimum model probability floor for UP bets (default: 0.65)"
    )
    parser.add_argument(
        "--floor-down",
        type=float,
        default=0.65,
        help="Minimum model probability floor for DOWN bets (default: 0.65)"
    )

    args = parser.parse_args()

    print("Fetching Polymarket data...")
    poly_data = run_polymarket_script(verbose=args.verbose)

    if poly_data["barrier"] is None:
        print("Error: Could not parse Polymarket data")
        sys.exit(1)

    if poly_data["hours_remaining"] <= 0:
        print("Error: Market has expired or no time remaining")
        sys.exit(1)

    print(f"Running model calibration for ${poly_data['barrier']:,.0f} reference price...")
    print(f"Time horizon: {poly_data['hours']}h {poly_data['minutes']}m")
    print()

    model_data = run_terminal_script(
        poly_data["barrier"],
        poly_data["hours_remaining"],
        verbose=args.verbose
    )

    if model_data["prob_above"] is None or model_data["prob_below"] is None:
        print("Error: Could not parse model probabilities")
        if args.verbose:
            print("Model output:")
            print(model_data["raw_output"])
        sys.exit(1)

    opportunities = find_opportunities(
        poly_data, model_data,
        alpha_up=args.alpha_up, alpha_down=args.alpha_down,
        floor_up=args.floor_up, floor_down=args.floor_down,
    )
    display_results(
        poly_data, model_data, opportunities,
        alpha_up=args.alpha_up, alpha_down=args.alpha_down,
        floor_up=args.floor_up, floor_down=args.floor_down,
    )


if __name__ == "__main__":
    main()
