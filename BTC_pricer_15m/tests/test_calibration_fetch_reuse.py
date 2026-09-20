from __future__ import annotations

import logging
import json
from datetime import datetime, timezone
from types import SimpleNamespace

import cli_terminal
from btc_pricer.api.deribit import OptionData
from btc_pricer.cli import common
from btc_pricer.config import Config
from btc_pricer.models.ssvi import SSVISurfaceParams
from btc_pricer.models.terminal_probability import MCResult
from scripts.polymarket_edge import TerminalCalibrationWorker


def _option(expiry: str, strike: float, option_type: str, ttm: float) -> OptionData:
    forward = 100.0
    return OptionData(
        instrument_name=f"BTC-{expiry}-{int(strike)}-{'C' if option_type == 'call' else 'P'}",
        strike=strike,
        option_type=option_type,
        expiration_timestamp=1,
        expiration_date=expiry,
        bid_price=0.010,
        ask_price=0.011,
        mark_price=0.0105,
        mark_iv=0.50,
        bid_iv=None,
        ask_iv=None,
        open_interest=1.0,
        underlying_price=forward,
        spot_price=100.0,
        time_to_expiry=ttm,
    )


def _options_snapshot() -> dict[str, list[OptionData]]:
    expiry = "27APR26"
    ttm = 3.0 / 365.0
    return {
        expiry: [
            _option(expiry, 80.0, "put", ttm),
            _option(expiry, 90.0, "put", ttm),
            _option(expiry, 100.0, "call", ttm),
            _option(expiry, 110.0, "call", ttm),
            _option(expiry, 120.0, "call", ttm),
            _option(expiry, 130.0, "call", ttm),
        ]
    }


def _surface_params() -> SSVISurfaceParams:
    return SSVISurfaceParams(
        rho=-0.25,
        eta=1.2,
        lam=0.25,
        thetas=[0.001, 0.002],
        ttms=[1.0 / 365.0, 3.0 / 365.0],
        expiry_names=["25APR26", "27APR26"],
    )


class _FakeBL:
    def __init__(self, *args, **kwargs) -> None:
        pass

    def extract_from_ssvi(self, params, forward):
        return SimpleNamespace(is_valid=True)

    def probability_above(self, rnd, target):
        return 0.55

    def probability_below(self, rnd, target):
        return 0.45


class _NoFetchClient:
    def __init__(self) -> None:
        self.fetch_count = 0

    def fetch_all_options(self, currency: str):
        self.fetch_count += 1
        raise AssertionError(f"unexpected fetch_all_options({currency})")


def test_calibrate_to_expiry_reuses_prefetched_snapshot(monkeypatch) -> None:
    snapshot = _options_snapshot()
    client = _NoFetchClient()
    monkeypatch.setattr(
        common,
        "fetch_spot_with_fallback",
        lambda deribit_spot: (deribit_spot, "test"),
    )

    heston_data, ssvi_data, spot, expiry, forward, ttm = common.calibrate_to_expiry(
        client,
        Config(),
        target_expiry="27APR26",
        return_both=True,
        skip_heston=True,
        options_by_expiry=snapshot,
    )

    assert client.fetch_count == 0
    assert heston_data is None
    assert ssvi_data is None
    assert spot == 100.0
    assert expiry == "27APR26"
    assert forward == 100.0
    assert ttm == 3.0 / 365.0


def test_terminal_until_path_reuses_expiry_selection_snapshot(monkeypatch) -> None:
    snapshot = _options_snapshot()
    calls = {"fetches": 0, "calibration_snapshot": None, "surface_snapshot": None}

    class FakeClient:
        def __init__(self, *args, **kwargs) -> None:
            pass

        def fetch_all_options(self, currency: str):
            calls["fetches"] += 1
            return snapshot

    def fake_calibrate_to_expiry(
        client,
        config,
        target_expiry=None,
        return_both=False,
        skip_heston=False,
        options_by_expiry=None,
        spot_override=None,
    ):
        calls["calibration_snapshot"] = options_by_expiry
        return None, None, 100.0, target_expiry, 100.0, 3.0 / 365.0

    def fake_fit_ssvi_surface_for_ttm(
        client,
        config,
        target_ttm,
        options_by_expiry=None,
        warm_start_surface_params=None,
        precomputed_fingerprint=None,
        precomputed_slices=None,
    ):
        calls["surface_snapshot"] = options_by_expiry
        return None

    monkeypatch.setattr(cli_terminal, "DeribitClient", FakeClient)
    monkeypatch.setattr(cli_terminal, "calibrate_to_expiry", fake_calibrate_to_expiry)
    monkeypatch.setattr(cli_terminal, "fit_ssvi_surface_for_ttm", fake_fit_ssvi_surface_for_ttm)
    monkeypatch.setattr(
        cli_terminal,
        "parse_datetime_with_timezone",
        lambda raw: (datetime(2026, 4, 24, 18, 15, tzinfo=timezone.utc), "UTC"),
    )
    monkeypatch.setattr(cli_terminal, "calculate_ttm_to_target", lambda target: 3.0 / 365.0)
    monkeypatch.setattr(
        cli_terminal,
        "find_closest_expiry_after",
        lambda options_by_expiry, target_utc: "27APR26",
    )

    args = SimpleNamespace(
        targets=None,
        target=101.0,
        sims=10,
        steps_per_day=10,
        until="2026-04-24T18:15:00+00:00",
        expiry=None,
        ttm=None,
        spot=None,
        direction="up",
        skip_heston=True,
        skip_ssvi_surface=False,
        output=None,
    )

    cli_terminal.run_terminal(args, Config(), logging.getLogger("test"))

    assert calls["fetches"] == 1
    assert calls["calibration_snapshot"] is snapshot
    assert calls["surface_snapshot"] is snapshot


def test_terminal_reuses_surface_fit_when_filtered_deribit_snapshot_unchanged(
    monkeypatch,
    tmp_path,
) -> None:
    snapshot = _options_snapshot()
    calls = {"fetches": 0, "fits": 0, "mc": 0}
    fit_results = []
    surface = _surface_params()

    class FakeClient:
        def fetch_all_options(self, currency: str):
            calls["fetches"] += 1
            return snapshot

    class FakeCalculator:
        def __init__(self, n_simulations, n_steps_per_day, use_antithetic=True):
            self.n_simulations = n_simulations

        def terminal_probability_ssvi(self, params, spot, forward, target, direction):
            calls["mc"] += 1
            probability = 0.60 + calls["mc"] * 0.01
            return MCResult(
                target_price=target,
                spot=spot,
                ttm=params.ttm,
                terminal_probability=probability,
                confidence_interval=(probability - 0.01, probability + 0.01),
                n_simulations=self.n_simulations,
                direction=direction,
                method="ssvi_local_vol",
            )

    def fake_calibrate_to_expiry(*args, **kwargs):
        return None, None, 100.0, "27APR26", 100.0, 3.0 / 365.0

    def fake_fit_ssvi_surface_for_ttm(
        client,
        config,
        target_ttm,
        options_by_expiry=None,
        warm_start_surface_params=None,
        precomputed_fingerprint=None,
        precomputed_slices=None,
    ):
        calls["fits"] += 1
        result = common.SurfaceFitResult(
            surface.get_params_for_ttm(target_ttm),
            surface,
            0.99,
            "fake surface",
            0.25,
            "full_de",
            False,
        )
        fit_results.append(result)
        return result

    monkeypatch.setattr(cli_terminal, "calibrate_to_expiry", fake_calibrate_to_expiry)
    monkeypatch.setattr(cli_terminal, "fit_ssvi_surface_for_ttm", fake_fit_ssvi_surface_for_ttm)
    monkeypatch.setattr(cli_terminal, "TerminalProbabilityCalculator", FakeCalculator)
    monkeypatch.setattr(cli_terminal, "BreedenLitzenberger", _FakeBL)
    monkeypatch.setattr(
        cli_terminal,
        "parse_datetime_with_timezone",
        lambda raw: (datetime(2026, 4, 24, 18, 15, tzinfo=timezone.utc), "UTC"),
    )
    monkeypatch.setattr(cli_terminal, "calculate_ttm_to_target", lambda target: 3.0 / 365.0)
    monkeypatch.setattr(cli_terminal, "find_closest_expiry_after", lambda options, target: "27APR26")

    args = SimpleNamespace(
        targets=None,
        target=101.0,
        sims=10,
        steps_per_day=10,
        until="2026-04-24T18:15:00+00:00",
        expiry=None,
        ttm=None,
        spot=None,
        direction="up",
        skip_heston=True,
        skip_ssvi_surface=False,
        output=tmp_path / "terminal.json",
    )

    cli_terminal.run_terminal(args, Config(), logging.getLogger("test"), client=FakeClient())
    first_payload = json.loads(args.output.read_text(encoding="utf-8"))

    cli_terminal.run_terminal(
        args,
        Config(),
        logging.getLogger("test"),
        client=FakeClient(),
        cached_surface_result=fit_results[0],
    )
    second_payload = json.loads(args.output.read_text(encoding="utf-8"))

    assert calls["fetches"] == 2
    assert calls["fits"] == 1
    assert calls["mc"] == 2
    assert first_payload["timing"]["surface_fit_reused"] is False
    assert second_payload["timing"]["surface_fit_reused"] is True
    assert second_payload["timing"]["surface_fit_method"] == "cached_surface"
    assert second_payload["timing"]["surface_fit_s"] == 0.0


def test_terminal_refits_when_filtered_deribit_price_changes(monkeypatch, tmp_path) -> None:
    first_snapshot = _options_snapshot()
    second_snapshot = _options_snapshot()
    second_snapshot["27APR26"][0].mark_iv = 0.55
    snapshots = [first_snapshot, second_snapshot]
    calls = {"fits": 0}
    fit_results = []
    surface = _surface_params()

    class FakeClient:
        def fetch_all_options(self, currency: str):
            return snapshots.pop(0)

    class FakeCalculator:
        def __init__(self, n_simulations, n_steps_per_day, use_antithetic=True):
            self.n_simulations = n_simulations

        def terminal_probability_ssvi(self, params, spot, forward, target, direction):
            return MCResult(target, spot, params.ttm, 0.6, (0.59, 0.61), self.n_simulations, direction, "ssvi")

    def fake_calibrate_to_expiry(*args, **kwargs):
        return None, None, 100.0, "27APR26", 100.0, 3.0 / 365.0

    def fake_fit_ssvi_surface_for_ttm(client, config, target_ttm, options_by_expiry=None, warm_start_surface_params=None, precomputed_fingerprint=None, precomputed_slices=None):
        calls["fits"] += 1
        result = common.SurfaceFitResult(
            surface.get_params_for_ttm(target_ttm),
            surface,
            0.99,
            "fake surface",
            0.25,
            "full_de",
            False,
        )
        fit_results.append(result)
        return result

    monkeypatch.setattr(cli_terminal, "calibrate_to_expiry", fake_calibrate_to_expiry)
    monkeypatch.setattr(cli_terminal, "fit_ssvi_surface_for_ttm", fake_fit_ssvi_surface_for_ttm)
    monkeypatch.setattr(cli_terminal, "TerminalProbabilityCalculator", FakeCalculator)
    monkeypatch.setattr(cli_terminal, "BreedenLitzenberger", _FakeBL)
    monkeypatch.setattr(
        cli_terminal,
        "parse_datetime_with_timezone",
        lambda raw: (datetime(2026, 4, 24, 18, 15, tzinfo=timezone.utc), "UTC"),
    )
    monkeypatch.setattr(cli_terminal, "calculate_ttm_to_target", lambda target: 3.0 / 365.0)
    monkeypatch.setattr(cli_terminal, "find_closest_expiry_after", lambda options, target: "27APR26")

    args = SimpleNamespace(
        targets=None,
        target=101.0,
        sims=10,
        steps_per_day=10,
        until="2026-04-24T18:15:00+00:00",
        expiry=None,
        ttm=None,
        spot=None,
        direction="up",
        skip_heston=True,
        skip_ssvi_surface=False,
        output=tmp_path / "terminal.json",
    )

    cli_terminal.run_terminal(args, Config(), logging.getLogger("test"), client=FakeClient())
    cli_terminal.run_terminal(
        args,
        Config(),
        logging.getLogger("test"),
        client=FakeClient(),
        cached_surface_result=fit_results[0],
    )

    assert calls["fits"] == 2


def test_terminal_worker_does_not_reuse_surface_across_changed_market_context(
    tmp_path,
) -> None:
    calls = []
    surface = _surface_params()
    worker = TerminalCalibrationWorker()
    worker._config = Config()
    worker._client = object()

    def fake_run_terminal(args, config, logger, client=None, warm_start_surface_params=None, cached_surface_result=None, spot_provider=None):
        calls.append(cached_surface_result is not None)
        params_at_ttm = surface.get_params_for_ttm(3.0 / 365.0)
        payload = {
            "spot_price": 100.0,
            "target_price": args.target,
            "direction": "up",
            "ttm_days": 3.0,
            "preferred_model": "ssvi_surface",
            "ssvi_surface": {
                "prob_above": 0.6,
                "prob_below": 0.4,
                "r_squared": 0.99,
                "params": params_at_ttm.to_dict(),
                "surface_params": surface.to_dict(),
            },
            "avg_prob_above": 0.6,
            "avg_prob_below": 0.4,
            "bl_prob_above": None,
            "bl_prob_below": None,
            "timing": {
                "surface_fit_method": "cached_surface" if cached_surface_result is not None else "full_de",
                "surface_warm_start_used": False,
                "surface_fit_reused": cached_surface_result is not None,
                "surface_fit_s": 0.0 if cached_surface_result is not None else 0.25,
                "deribit_surface_fingerprint": "same-fingerprint",
            },
        }
        args.output.write_text(json.dumps(payload), encoding="utf-8")

    worker._run_terminal = fake_run_terminal

    worker.run(
        barrier=101.0,
        hours=1.0,
        json_file=tmp_path / "terminal.json",
        market_end_time="2026-04-24T18:15:00+00:00",
        skip_heston=True,
    )
    worker.run(
        barrier=101.0,
        hours=1.0,
        json_file=tmp_path / "terminal.json",
        market_end_time="2026-04-24T18:15:00+00:00",
        skip_heston=True,
    )
    worker.run(
        barrier=102.0,
        hours=1.0,
        json_file=tmp_path / "terminal.json",
        market_end_time="2026-04-24T18:15:00+00:00",
        skip_heston=True,
    )

    assert calls == [False, True, False]
