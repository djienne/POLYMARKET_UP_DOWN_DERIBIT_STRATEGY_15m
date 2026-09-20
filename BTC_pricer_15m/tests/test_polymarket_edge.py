"""Regression tests for JSON fallback handling in polymarket_edge."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts import polymarket_edge


def test_run_terminal_script_falls_back_when_json_has_no_probabilities(monkeypatch, tmp_path):
    json_path = tmp_path / "terminal.json"
    json_path.write_text(json.dumps({"preferred_model": "ssvi_surface"}))

    class _Result:
        returncode = 0
        stdout = "Spot Price:        $103,150.00 (Binance)\nSSVI Surface MC       0.992     98.4% +0.0%            1.6%\n"
        stderr = ""

    monkeypatch.setattr(polymarket_edge.subprocess, "run", lambda *args, **kwargs: _Result())

    result = polymarket_edge.run_terminal_script(
        barrier=100000.0,
        hours=1.0,
        json_path=Path(json_path),
        use_inprocess=False,
    )

    assert result["prob_above"] == pytest.approx(0.984)
    assert result["prob_below"] == pytest.approx(0.016)


def test_run_terminal_script_uses_inprocess_worker_by_default(monkeypatch, tmp_path):
    json_path = tmp_path / "terminal.json"
    calls = {}

    class _Worker:
        def run(self, **kwargs):
            calls.update(kwargs)
            return {
                "prob_above": 0.61,
                "prob_below": 0.39,
                "spot_price": 101000.0,
                "model_used": "ssvi_surface",
            }

    monkeypatch.setattr(polymarket_edge, "_get_terminal_worker", lambda: _Worker())
    monkeypatch.setattr(
        polymarket_edge.subprocess,
        "run",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("subprocess used")),
    )

    result = polymarket_edge.run_terminal_script(
        barrier=100000.0,
        hours=1.0,
        json_path=json_path,
        market_end_time="2026-04-24T18:15:00+00:00",
        skip_heston=True,
    )

    assert result["prob_above"] == pytest.approx(0.61)
    assert result["prob_below"] == pytest.approx(0.39)
    assert calls["json_file"] == json_path
    assert calls["market_end_time"] == "2026-04-24T18:15:00+00:00"
    assert calls["skip_heston"] is True


def test_run_terminal_script_falls_back_to_subprocess_on_worker_error(monkeypatch, tmp_path):
    json_path = tmp_path / "terminal.json"
    json_path.write_text(json.dumps({"preferred_model": "ssvi_surface"}))

    class _Worker:
        def run(self, **kwargs):
            raise RuntimeError("boom")

    class _Result:
        returncode = 0
        stdout = "Spot Price:        $103,150.00 (Binance)\nSSVI Surface MC       0.992     98.4% +0.0%            1.6%\n"
        stderr = ""

    monkeypatch.setattr(polymarket_edge, "_get_terminal_worker", lambda: _Worker())
    monkeypatch.setattr(polymarket_edge.subprocess, "run", lambda *args, **kwargs: _Result())

    result = polymarket_edge.run_terminal_script(
        barrier=100000.0,
        hours=1.0,
        json_path=json_path,
    )

    assert result["prob_above"] == pytest.approx(0.984)
    assert result["prob_below"] == pytest.approx(0.016)


def test_parse_terminal_json_prefers_selected_mc_probability():
    result = polymarket_edge.parse_terminal_json(
        {
            "preferred_model": "ssvi_surface",
            "probability_mode": "mc",
            "probability_source_label": "SSVI Surface MC",
            "selected_prob_above": 0.62,
            "selected_prob_below": 0.38,
            "avg_prob_above": 0.62,
            "avg_prob_below": 0.38,
            "ssvi_surface": {
                "prob_above": None,
                "prob_below": None,
                "mc_prob_above": 0.62,
                "mc_prob_below": 0.38,
                "r_squared": 0.91,
            },
        }
    )

    assert result["prob_above"] == pytest.approx(0.62)
    assert result["prob_below"] == pytest.approx(0.38)
    assert result["surface_mc_above"] == pytest.approx(0.62)
    assert result["surface_bl_above"] is None
    assert result["probability_mode"] == "mc"
    assert result["probability_source_label"] == "SSVI Surface MC"


def test_terminal_calibration_worker_runs_cli_terminal_in_process(monkeypatch, tmp_path):
    json_path = tmp_path / "terminal.json"
    calls = {}

    class _Client:
        pass

    def fake_load_config(path, logger):
        calls["config_path"] = path
        return SimpleNamespace(api=object(), validation=object())

    def fake_run_terminal(args, config, logger, client=None, warm_start_surface_params=None, cached_surface_result=None, spot_provider=None):
        calls["target"] = args.target
        calls["ttm"] = args.ttm
        calls["client"] = client
        calls["warm_start_surface_params"] = warm_start_surface_params
        calls["cached_surface_result"] = cached_surface_result
        print("in-process output")
        args.output.write_text(
            json.dumps(
                {
                    "spot_price": 103000.0,
                    "preferred_model": "ssvi_surface",
                    "ssvi_surface": {
                        "prob_above": 0.7,
                        "prob_below": 0.3,
                        "r_squared": 0.95,
                        "surface_params": {
                            "rho": -0.25,
                            "eta": 1.4,
                            "lam": 0.2,
                            "thetas": [0.005, 0.01],
                            "ttms": [0.005, 0.01],
                            "expiry_names": ["A", "B"],
                        },
                    },
                    "avg_prob_above": 0.69,
                    "avg_prob_below": 0.31,
                    "timing": {
                        "surface_fit_method": "full_de",
                        "surface_warm_start_used": False,
                    },
                }
            ),
            encoding="utf-8",
        )

    monkeypatch.setattr("btc_pricer.cli.common.load_config", fake_load_config)
    monkeypatch.setattr("btc_pricer.api.deribit.DeribitClient", lambda *args, **kwargs: _Client())
    monkeypatch.setattr("cli_terminal.run_terminal", fake_run_terminal)

    worker = polymarket_edge.TerminalCalibrationWorker(config_path=tmp_path / "config.yaml")
    result = worker.run(
        barrier=100000.0,
        hours=1.5,
        json_file=json_path,
        skip_heston=True,
    )

    assert result["prob_above"] == pytest.approx(0.69)
    assert result["prob_below"] == pytest.approx(0.31)
    assert result["raw_output"] == "in-process output\n"
    assert result["calibration_seconds"] >= 0.0
    assert calls["target"] == 100000.0
    assert calls["ttm"] == pytest.approx(1.5 / 24.0)
    assert isinstance(calls["client"], _Client)
    assert calls["warm_start_surface_params"] is None
    assert worker._last_surface_params is not None
    previous_surface_params = worker._last_surface_params

    worker.run(
        barrier=100001.0,
        hours=1.0,
        json_file=json_path,
        market_end_time="2026-04-24T18:15:00+00:00",
    )
    assert calls["client"] is worker._client
    assert calls["warm_start_surface_params"] is previous_surface_params


def test_terminal_calibration_worker_does_not_overwrite_warm_start_on_failure(monkeypatch, tmp_path):
    json_path = tmp_path / "terminal.json"
    calls = {"count": 0}

    class _Client:
        pass

    def fake_load_config(path, logger):
        return SimpleNamespace(api=object(), validation=object())

    def fake_run_terminal(args, config, logger, client=None, warm_start_surface_params=None, cached_surface_result=None, spot_provider=None):
        calls["count"] += 1
        calls["warm_start_surface_params"] = warm_start_surface_params
        calls["cached_surface_result"] = cached_surface_result
        if calls["count"] == 1:
            args.output.write_text(
                json.dumps(
                    {
                        "spot_price": 103000.0,
                        "preferred_model": "ssvi_surface",
                        "ssvi_surface": {
                            "prob_above": 0.7,
                            "prob_below": 0.3,
                            "r_squared": 0.95,
                            "surface_params": {
                                "rho": -0.25,
                                "eta": 1.4,
                                "lam": 0.2,
                                "thetas": [0.005, 0.01],
                                "ttms": [0.005, 0.01],
                                "expiry_names": ["A", "B"],
                            },
                        },
                    }
                ),
                encoding="utf-8",
            )
            return
        raise RuntimeError("calibration failed")

    monkeypatch.setattr("btc_pricer.cli.common.load_config", fake_load_config)
    monkeypatch.setattr("btc_pricer.api.deribit.DeribitClient", lambda *args, **kwargs: _Client())
    monkeypatch.setattr("cli_terminal.run_terminal", fake_run_terminal)

    worker = polymarket_edge.TerminalCalibrationWorker(config_path=tmp_path / "config.yaml")
    worker.run(barrier=100000.0, hours=1.0, json_file=json_path)
    previous = worker._last_surface_params

    with pytest.raises(RuntimeError):
        worker.run(barrier=100001.0, hours=1.0, json_file=json_path)

    assert worker._last_surface_params is previous
    assert calls["warm_start_surface_params"] is previous
