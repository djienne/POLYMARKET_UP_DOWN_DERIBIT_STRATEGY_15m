import csv
import datetime as dt
import json
import pytest
from pathlib import Path
from types import SimpleNamespace

from scripts import live_watchdog
from scripts.live_runtime_state import desired_live_running, safety_stop_active


def _args(**overrides):
    base = {
        "parity_instance": 850,
        "parity_time_tol_seconds": 30.0,
        "parity_price_tol": 0.10,
        "parity_entry_grace_seconds": 20.0,
        "parity_close_grace_seconds": 60.0,
        "parity_close_time_tol_seconds": 90.0,
    }
    base.update(overrides)
    return SimpleNamespace(**base)


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def test_watchdog_ignores_cloudflare_html_body_lines():
    assert not live_watchdog.trigger_matches(
        'btc_pricer_15m_live | 16:56:11 ERROR py_clob_client_v2.http_helpers.helpers: '
        'request error status=403 url=https://clob.polymarket.com/auth/api-key'
    )
    assert not live_watchdog.trigger_matches(
        'btc_pricer_15m_live | <link rel="stylesheet" id="cf_styles-css" '
        'href="/cdn-cgi/styles/cf.errors.css" />'
    )
    assert not live_watchdog.trigger_matches(
        "btc_pricer_15m_live | <title>Attention Required! | Cloudflare</title>"
    )
    assert not live_watchdog.trigger_matches(
        'btc_pricer_15m_live | <div class="cf-alert cf-alert-error cf-cookie-error" '
        'id="cookie-alert" data-translate="enable_cookies">Please enable cookies.</div>'
    )
    assert not live_watchdog.trigger_matches(
        'btc_pricer_15m_live | <span class="cf-no-screenshot error"></span>'
    )
    assert live_watchdog.trigger_matches("btc_pricer_15m_live | CRITICAL TP REJECT")


def test_resolve_parity_instance_prefers_unified_yaml(monkeypatch, tmp_path):
    monkeypatch.setattr(live_watchdog, "repo_root", lambda: tmp_path)
    (tmp_path / "config.yaml").write_text(
        """
grid:
  grid:
    alpha_up: [1.0, 2.5]
    alpha_down: [1.0]
    floor_up: [0.65]
    floor_down: [0.45]
    tp_percentage: [0.30]
    stop_loss_pct: [0.0]
live:
  edge_alpha_up: 2.5
  edge_alpha_down: 1.0
  edge_floor_up: 0.65
  edge_floor_down: 0.45
  tp_percentage: 0.30
  stop_loss_pct: 0.0
""",
        encoding="utf-8",
    )

    assert live_watchdog.resolve_parity_instance(_args(parity_instance=None)) == 1


def test_resolve_parity_instance_includes_appended_stop_loss(monkeypatch, tmp_path):
    monkeypatch.setattr(live_watchdog, "repo_root", lambda: tmp_path)
    (tmp_path / "config.yaml").write_text(
        """
grid:
  append_stop_loss_pct: [0.40]
  grid:
    alpha_up: [1.0, 2.5]
    alpha_down: [1.0]
    floor_up: [0.65]
    floor_down: [0.45]
    tp_percentage: [0.30]
    stop_loss_pct: [0.0, 0.20]
live:
  edge_alpha_up: 2.5
  edge_alpha_down: 1.0
  edge_floor_up: 0.65
  edge_floor_down: 0.45
  tp_percentage: 0.30
  stop_loss_pct: 0.40
""",
        encoding="utf-8",
    )

    assert live_watchdog.resolve_parity_instance(_args(parity_instance=None)) == 5


def test_parity_detects_opposite_side_entry(monkeypatch, tmp_path):
    monkeypatch.setattr(live_watchdog, "repo_root", lambda: tmp_path)
    market_id = "btc-updown-15m-1777652400"
    (tmp_path / "results").mkdir()

    _write_csv(
        tmp_path / "results" / "trades.csv",
        [
            "instance_id",
            "timestamp",
            "event",
            "direction",
            "market_id",
            "entry_price",
            "exit_price",
            "shares",
            "pnl",
            "pnl_pct",
            "capital",
            "model_prob",
            "poly_prob",
            "spot_price",
            "barrier",
            "liquidity_mode",
            "settlement_source",
            "effective_tp_pct",
        ],
        [
            {
                "instance_id": "850",
                "timestamp": "2026-05-01T16:24:10.239929+00:00",
                "event": "ENTRY",
                "direction": "DOWN",
                "market_id": market_id,
                "entry_price": "0.8000",
            }
        ],
    )
    (tmp_path / "results" / "15m_live_state.json").write_text(
        json.dumps(
            {
                "open_positions": [
                    {
                        "id": "f52dc2ce",
                        "direction": "UP",
                        "market_id": market_id,
                        "entry_price": 0.92,
                        "opened_at": "2026-05-01T16:24:04.705507+00:00",
                    }
                ],
                "closed_positions": [],
            }
        ),
        encoding="utf-8",
    )

    report = live_watchdog.parity_check(
        _args(),
        dt.datetime(2026, 5, 1, 16, 0, tzinfo=dt.timezone.utc),
    )

    assert report is not None
    assert report["status"] == "mismatch"
    assert report["mismatches"][0]["kind"] == "entry_direction_mismatch"
    assert report["mismatches"][0]["grid_direction"] == "DOWN"
    assert report["mismatches"][0]["live_direction"] == "UP"


@pytest.mark.parametrize("stop_rc", [0, 1])
def test_diagnosis_latches_before_stop_even_with_custom_output_directory(monkeypatch, tmp_path, stop_rc):
    monkeypatch.setattr(live_watchdog, "repo_root", lambda: tmp_path)
    calls = []

    def capture(command, output, timeout=60):
        calls.append(command)
        if "stop" in command:
            assert safety_stop_active(tmp_path / "results")
            assert not desired_live_running(tmp_path / "results")
            return stop_rc
        return 0

    monkeypatch.setattr(live_watchdog, "run_capture", capture)
    args = _args(diagnostics_root="custom_diagnostics", service="live", profile="local-live",
                 log_tail=2, container_name="btc_pricer_15m_live")
    assert live_watchdog.diagnose_and_stop(args, "ERROR test fault") == 2
    assert calls[0] == ["docker", "compose", "--profile", "local-live", "stop", "live"]
    latch = (tmp_path / "results/diagnostics/live_watchdog_last_trigger.txt").read_text()
    assert ("STOP FAILED" in latch) is bool(stop_rc)
    inspect_command = next(command for command in calls if "inspect" in command)
    assert "--format" in inspect_command
    assert all(".Config" not in argument for argument in inspect_command)
