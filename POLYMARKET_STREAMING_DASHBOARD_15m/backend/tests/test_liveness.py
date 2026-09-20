from pathlib import Path
import json
import os
import time
import pytest

from app.collector import liveness
from app.collector.liveness import (
    _active_lock_exists,
    _active_lock_paths,
    _live_position_status,
    current_liveness,
)
from app.config import settings


def _quiet_probe(monkeypatch):
    monkeypatch.setattr(liveness._cpu_sampler, "sample", lambda: None)
    monkeypatch.setattr(liveness.location_probe, "active_ping", lambda: (None, None, "local"))


def test_live_liveness_accepts_single_trader_lock(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(settings, "mode", "live")
    monkeypatch.setattr(settings, "results_dir", tmp_path)

    (tmp_path / "single_trader.lock").write_text("pid", encoding="utf-8")

    assert _active_lock_paths()[0] == tmp_path / "single_trader.lock"
    assert _active_lock_exists() is True


def test_dry_run_liveness_uses_grid_lock(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(settings, "mode", "dry_run")
    monkeypatch.setattr(settings, "results_dir", tmp_path)

    (tmp_path / "trader.lock").write_text("pid", encoding="utf-8")

    assert _active_lock_paths() == [tmp_path / "trader.lock"]
    assert _active_lock_exists() is True


def test_dry_run_liveness_uses_state_snapshot_heartbeat(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(settings, "mode", "dry_run")
    monkeypatch.setattr(settings, "results_dir", tmp_path)
    _quiet_probe(monkeypatch)

    now = time.time()
    (tmp_path / "trader.lock").write_text("pid", encoding="utf-8")
    (tmp_path / ".live_location").write_text("stopped", encoding="utf-8")
    (tmp_path / "terminal_data.json").write_text("old terminal", encoding="utf-8")
    (tmp_path / "state_snapshot.json").write_text("fresh state", encoding="utf-8")
    os.utime(tmp_path / "terminal_data.json", (now - 300, now - 300))
    os.utime(tmp_path / "state_snapshot.json", (now, now))

    info = current_liveness()

    assert info.bot_live is True
    assert info.lock_exists is True
    assert info.terminal_age_s is not None
    assert info.terminal_age_s < 60.0
    assert info.execution_location == "local"
    assert info.live_position_open is False


def test_live_position_status_reads_live_state_open_position(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(settings, "results_dir", tmp_path)
    (tmp_path / "15m_live_state.json").write_text(
        json.dumps(
            {
                "open_positions": [
                    {
                        "direction": "UP",
                        "market_id": "btc-updown-15m-1778527800",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    is_open, direction, market_id = _live_position_status()

    assert is_open is True
    assert direction == "UP"
    assert market_id == "btc-updown-15m-1778527800"


def test_live_position_status_handles_missing_empty_and_bad_state(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(settings, "results_dir", tmp_path)

    assert _live_position_status() == (False, None, None)

    state_path = tmp_path / "15m_live_state.json"
    state_path.write_text(json.dumps({"open_positions": []}), encoding="utf-8")
    assert _live_position_status() == (False, None, None)

    state_path.write_text(json.dumps({"open_positions": {}}), encoding="utf-8")
    assert _live_position_status() == (False, None, None)

    state_path.write_text("{not-json", encoding="utf-8")
    assert _live_position_status() == (False, None, None)


@pytest.mark.parametrize("mode,lock", [("live", "single_trader.lock"), ("dry_run", "trader.lock")])
def test_shared_output_cannot_mask_stale_trader_heartbeat(tmp_path: Path, monkeypatch, mode, lock):
    monkeypatch.setattr(settings, "mode", mode)
    monkeypatch.setattr(settings, "results_dir", tmp_path)
    _quiet_probe(monkeypatch)
    now = time.time()
    heartbeat = tmp_path / lock
    heartbeat.write_text("123")
    os.utime(heartbeat, (now - 3600, now - 3600))
    for name in ("terminal_data.shared.json", "terminal_data.shared_calc.json", "state_snapshot.json", "15m_live_state.json"):
        (tmp_path / name).write_text("{}")
    assert current_liveness().bot_live is False
    os.utime(heartbeat, (now, now))
    assert current_liveness().bot_live is True
    heartbeat.unlink()
    assert current_liveness().bot_live is False


def test_live_dashboard_watches_shared_calc_terminal_files(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(settings, "mode", "live")
    monkeypatch.setattr(settings, "results_dir", tmp_path)

    names = [path.name for path in settings.terminal_paths()]

    assert names == [
        "terminal_data.shared.json",
        "terminal_data.shared_calc.json",
        "terminal_data.live.json",
        "terminal_data.json",
    ]
