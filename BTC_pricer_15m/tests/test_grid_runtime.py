"""Tests for grid runtime control helpers."""

import json
import os
from argparse import Namespace
from pathlib import Path

import pytest

from scripts.trader import grid_runtime
from scripts.trader.main import _restart_grid_cli


def test_find_grid_processes_ignores_trader_mode(monkeypatch, tmp_path):
    monkeypatch.setattr(grid_runtime, "read_lock_pid", lambda path: None)
    monkeypatch.setattr(
        grid_runtime,
        "list_python_processes",
        lambda: [
            {"pid": 100, "ppid": 1, "command_line": "python -m scripts.trader.main --mode trader"},
            {"pid": 200, "ppid": 1, "command_line": "python -m scripts.trader.main --mode grid -v"},
            {"pid": 300, "ppid": 200, "command_line": f"python {tmp_path}\\cli_terminal.py --target 1"},
        ],
    )

    state = grid_runtime.find_grid_processes(tmp_path)

    assert [proc["pid"] for proc in state["main_processes"]] == [200]
    assert [proc["pid"] for proc in state["child_processes"]] == [300]


def test_grid_command_matching_tokenizes_mode_flag():
    assert grid_runtime._is_grid_main_command("python -m scripts.trader.main --mode grid")
    assert not grid_runtime._is_grid_main_command("python -m scripts.trader.main  --mode   trader")
    assert not grid_runtime._is_grid_main_command("python -m scripts.trader.main --mode=trader")


def test_windows_pid_check_uses_exact_tasklist_pid(monkeypatch):
    class Result:
        returncode = 0
        stdout = '"Image Name","PID","Session Name","Session#","Mem Usage"\n"python.exe","1234","Console","1","1,000 K"\n'

    monkeypatch.setattr(grid_runtime.sys, "platform", "win32")
    monkeypatch.setattr(grid_runtime.subprocess, "run", lambda *args, **kwargs: Result())

    assert grid_runtime.is_pid_alive(12) is False
    assert grid_runtime.is_pid_alive(1234) is True


def test_clear_stale_lock_respects_live_pid(monkeypatch, tmp_path):
    lock_path = tmp_path / grid_runtime.GRID_LOCK_PATH
    lock_path.parent.mkdir(parents=True)
    lock_path.write_text("123")
    monkeypatch.setattr(grid_runtime, "is_pid_alive", lambda pid: True)

    grid_runtime.clear_stale_lock(tmp_path)

    assert lock_path.exists()

    monkeypatch.setattr(grid_runtime, "is_pid_alive", lambda pid: False)
    grid_runtime.clear_stale_lock(tmp_path)
    assert not lock_path.exists()


def test_clear_stale_lock_removes_stopped_container_lock(monkeypatch, tmp_path):
    lock_path = tmp_path / grid_runtime.GRID_LOCK_PATH
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path.write_text(json.dumps({
        "schema_version": 2,
        "project": "POLYMARKET_UP_DOWN_15m",
        "mode": "grid",
        "lock_kind": "file_flock",
        "hostname": "container-a",
        "pid": 1,
        "container_id": "container-a",
        "token": "token-a",
    }))
    monkeypatch.setattr(grid_runtime, "docker_compose_grid_service", lambda workdir: None)
    monkeypatch.setattr(
        grid_runtime,
        "is_pid_alive",
        lambda pid: (_ for _ in ()).throw(AssertionError("container PID must not be checked on host")),
    )

    grid_runtime.clear_stale_lock(tmp_path)

    assert not lock_path.exists()


def test_clear_stale_lock_refuses_foreign_host_legacy_lock(monkeypatch, tmp_path):
    lock_path = tmp_path / grid_runtime.GRID_LOCK_PATH
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path.write_text("remote-host:123")
    monkeypatch.setattr(grid_runtime, "docker_compose_grid_service", lambda workdir: None)
    monkeypatch.setattr("socket.gethostname", lambda: "local-host")
    monkeypatch.setattr(grid_runtime, "is_pid_alive", lambda pid: False)

    grid_runtime.clear_stale_lock(tmp_path)

    assert lock_path.exists()


def test_stop_grid_processes_preserves_history(monkeypatch, tmp_path):
    for rel_path in grid_runtime.GRID_HISTORY_FILES:
        path = tmp_path / rel_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("history")

    lock_path = tmp_path / grid_runtime.GRID_LOCK_PATH
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path.write_text("200")

    states = iter([
        {
            "lock_pid": 200,
            "main_processes": [{"pid": 200, "ppid": 1, "command_line": "python -m scripts.trader.main --mode grid"}],
            "child_processes": [{"pid": 201, "ppid": 200, "command_line": "python cli_terminal.py"}],
        },
        {"lock_pid": 200, "main_processes": [], "child_processes": []},
    ])

    monkeypatch.setattr(grid_runtime, "find_grid_processes", lambda workdir: next(states))
    monkeypatch.setattr(grid_runtime, "_wait_for_pids", lambda pids, timeout: True)
    monkeypatch.setattr(grid_runtime, "is_pid_alive", lambda pid: False)

    result = grid_runtime.stop_grid_processes(tmp_path, wait_seconds=1)

    assert result["stopped"] is True
    assert result["forced"] is False
    assert not (tmp_path / grid_runtime.GRID_STOP_SENTINEL).exists()
    for rel_path in grid_runtime.GRID_HISTORY_FILES:
        assert (tmp_path / rel_path).exists()


def test_stop_grid_processes_waits_for_active_pids(monkeypatch, tmp_path):
    lock_path = tmp_path / grid_runtime.GRID_LOCK_PATH
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path.write_text("200")

    states = iter([
        {
            "lock_pid": 200,
            "main_processes": [{"pid": 200, "ppid": 1, "command_line": "python -m scripts.trader.main --mode grid"}],
            "child_processes": [{"pid": 201, "ppid": 200, "command_line": "python cli_terminal.py"}],
        },
        {"lock_pid": 200, "main_processes": [], "child_processes": []},
    ])
    wait_calls = []

    monkeypatch.setattr(grid_runtime, "find_grid_processes", lambda workdir: next(states))
    monkeypatch.setattr(grid_runtime, "_wait_for_pids", lambda pids, timeout: wait_calls.append((set(pids), timeout)) or True)
    monkeypatch.setattr(grid_runtime, "is_pid_alive", lambda pid: False)

    result = grid_runtime.stop_grid_processes(tmp_path, wait_seconds=12)

    assert wait_calls == [({200, 201}, 12)]
    assert result["stopped"] is True
    assert not (tmp_path / grid_runtime.GRID_STOP_SENTINEL).exists()


def test_restart_cli_preserves_existing_history(monkeypatch, tmp_path):
    before = {
        str(path): {"exists": True, "size": 10}
        for path in grid_runtime.GRID_HISTORY_FILES
    }
    after = {
        str(path): {"exists": True, "size": 10}
        for path in grid_runtime.GRID_HISTORY_FILES
    }

    monkeypatch.chdir(tmp_path)
    states = iter([before, after])
    monkeypatch.setattr("scripts.trader.main.history_state", lambda workdir: next(states))
    monkeypatch.setattr("scripts.trader.main.stop_grid_processes", lambda workdir: {"stopped": True, "forced": False, "remaining": {"main_processes": []}})
    monkeypatch.setattr("scripts.trader.main.ensure_history_preserved", lambda old, new: None)
    started = {}
    monkeypatch.setattr("scripts.trader.main.start_grid_process", lambda workdir, config_path, verbose: started.setdefault("pid", 777))

    _restart_grid_cli(Namespace(config="config_grid.json"))

    assert started["pid"] == 777


def test_restart_cli_raises_if_stop_incomplete(monkeypatch, tmp_path):
    before = {
        str(path): {"exists": True, "size": 10}
        for path in grid_runtime.GRID_HISTORY_FILES
    }
    after = {
        str(path): {"exists": True, "size": 10}
        for path in grid_runtime.GRID_HISTORY_FILES
    }

    monkeypatch.chdir(tmp_path)
    states = iter([before, after])
    monkeypatch.setattr("scripts.trader.main.history_state", lambda workdir: next(states))
    monkeypatch.setattr(
        "scripts.trader.main.stop_grid_processes",
        lambda workdir: {
            "stopped": False,
            "forced": True,
            "remaining": {"main_processes": [{"pid": 200, "command_line": "python -m scripts.trader.main --mode grid"}]},
        },
    )
    monkeypatch.setattr("scripts.trader.main.ensure_history_preserved", lambda old, new: None)

    with pytest.raises(RuntimeError, match="still running"):
        _restart_grid_cli(Namespace(config="config_grid.json"))


def test_ensure_history_preserved_raises_if_file_disappears():
    before = {"results/trades.csv": {"exists": True, "size": 100}}
    after = {"results/trades.csv": {"exists": False, "size": None}}

    with pytest.raises(RuntimeError):
        grid_runtime.ensure_history_preserved(before, after)


def test_acquire_lock_reuses_same_host_dead_pid_lock(monkeypatch, tmp_path):
    import scripts.trader.main as trader_main
    from scripts.trader.main import _acquire_lock, _release_lock

    lock_path = tmp_path / "trader.lock"
    lock_path.write_text("new_container_def456:7")

    monkeypatch.setattr("socket.gethostname", lambda: "new_container_def456")
    monkeypatch.setattr(trader_main, "_is_pid_alive", lambda pid: False)

    assert _acquire_lock(lock_path) is True
    owner = json.loads(lock_path.read_text())
    assert owner["hostname"] == "new_container_def456"
    assert owner["pid"] == os.getpid()
    _release_lock()


def test_acquire_lock_refuses_different_host_outside_docker(monkeypatch, tmp_path):
    import scripts.trader.main as trader_main
    from scripts.trader.main import _acquire_lock

    lock_path = tmp_path / "trader.lock"
    lock_path.write_text("remote_server:100")

    monkeypatch.setattr(trader_main.sys, "platform", "win32")
    monkeypatch.setattr("socket.gethostname", lambda: "local_machine")

    assert _acquire_lock(lock_path) is False
    assert lock_path.read_text() == "remote_server:100"
