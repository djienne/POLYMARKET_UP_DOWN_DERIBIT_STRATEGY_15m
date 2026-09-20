import importlib.util
from argparse import Namespace
from pathlib import Path
from types import SimpleNamespace

from BTC_pricer_15m.scripts.live_runtime_state import desired_live_running, latch_live_stop


def load_ops(tmp_path, monkeypatch):
    path = Path(__file__).resolve().parents[1] / "manage.py"
    spec = importlib.util.spec_from_file_location("stack_ops_under_test", path)
    ops = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(ops)
    monkeypatch.setattr(ops, "STACK_DIR", tmp_path)
    monkeypatch.setattr(ops, "MAINTENANCE_MARKER", tmp_path / "results/.maintenance_stop")
    monkeypatch.setattr(ops, "require_docker", lambda: None)
    monkeypatch.setattr(ops, "ensure_docker", lambda **kwargs: True)
    return ops


def test_root_stop_persists_intent_then_stops_complete_stack(tmp_path, monkeypatch):
    ops = load_ops(tmp_path, monkeypatch)
    calls = []

    def compose(*args):
        assert not desired_live_running(tmp_path / "results")
        assert (tmp_path / "results/.live_desired.json").exists()
        assert (tmp_path / "results/.maintenance_stop").exists()
        calls.append(args)
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(ops, "_compose", compose)
    assert ops.cmd_stop(Namespace()) == 0
    assert calls[0][0:2] == ("stop", "watchdog")
    assert set(calls[0][1:]) == {"redis", "spot", "calc", "grid", "live", "docker-logs", "dashboard", "watchdog"}


def test_root_start_cannot_acknowledge_a_safety_stop_implicitly(tmp_path, monkeypatch):
    ops = load_ops(tmp_path, monkeypatch)
    calls = []
    monkeypatch.setattr(ops, "_compose", lambda *args: calls.append(args) or SimpleNamespace(returncode=0))
    latch_live_stop(tmp_path / "results", "known fault")
    assert ops.cmd_start(Namespace(build=False, acknowledge_safety_stop=False)) == 1
    assert calls == []
    assert ops.cmd_start(Namespace(build=False, acknowledge_safety_stop=True)) == 0
    assert desired_live_running(tmp_path / "results")
    assert set(calls[0][2:]) == {"redis", "spot", "calc", "grid", "live", "docker-logs", "dashboard", "watchdog"}
