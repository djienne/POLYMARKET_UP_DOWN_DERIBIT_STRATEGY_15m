import live_manager
import pytest
from scripts.live_runtime_state import desired_live_running, latch_live_stop, safety_stop_active


def test_live_history_backup_copies_state_trade_and_equity_files(tmp_path, monkeypatch):
    monkeypatch.setattr(live_manager, "RESULTS_DIR", tmp_path)
    backup_dir = tmp_path / "live_history_backups"
    monkeypatch.setattr(live_manager, "LIVE_HISTORY_BACKUP_DIR", backup_dir)
    required_files = ("15m_live_state.json", "15m_live_trades.csv", "15m_live_equity.csv")
    for name in required_files:
        (tmp_path / name).write_text(f"{name}\n", encoding="utf-8")

    created = live_manager.backup_live_history("unit_test")

    assert created is not None
    for name in required_files:
        assert (created / name).read_text(encoding="utf-8") == f"{name}\n"


def test_local_start_uses_only_local_docker(tmp_path, monkeypatch):
    calls: list[list[str]] = []
    monkeypatch.setattr(live_manager, "RESULTS_DIR", tmp_path)
    monkeypatch.setattr(live_manager, "LOCATION_PATH", tmp_path / ".live_location")
    monkeypatch.setattr(live_manager, "LEGACY_SYNC_PID_PATH", tmp_path / ".vps_sync.pid")
    monkeypatch.setattr(live_manager, "LEGACY_SYNC_HEARTBEAT_PATH", tmp_path / ".vps_sync_last")
    monkeypatch.setattr(live_manager, "legacy_offload_running", lambda: False)
    monkeypatch.setattr(live_manager, "_docker_compose", lambda args: calls.append(args))

    live_manager.switch_local()

    assert calls == [["--profile", "local-live", "up", "-d", "live"]]
    assert (tmp_path / ".live_location").read_text(encoding="ascii") == "local"


def test_vps_command_is_removed(monkeypatch):
    called = False

    def fake_compose(args):
        nonlocal called
        called = True

    monkeypatch.setattr(live_manager, "_docker_compose", fake_compose)

    assert live_manager.dispatch(["vps"]) == 2
    assert not called


def test_live_stop_persists_intent_before_docker_stop(tmp_path, monkeypatch):
    monkeypatch.setattr(live_manager, "RESULTS_DIR", tmp_path)
    monkeypatch.setattr(live_manager, "LOCATION_PATH", tmp_path / ".live_location")
    monkeypatch.setattr(live_manager, "stop_legacy_vps_sync_loop", lambda: None)
    monkeypatch.setattr(live_manager, "stop_legacy_offload", lambda: None)
    calls = []

    def stop():
        assert not desired_live_running(tmp_path)
        assert (tmp_path / ".live_desired.json").exists()
        calls.append("stop")

    monkeypatch.setattr(live_manager, "stop_local_live", stop)
    live_manager.switch_stop()
    assert calls == ["stop"]


def test_live_resume_requires_acknowledgment_and_preserves_diagnosis(tmp_path, monkeypatch):
    monkeypatch.setattr(live_manager, "RESULTS_DIR", tmp_path)
    monkeypatch.setattr(live_manager, "LOCATION_PATH", tmp_path / ".live_location")
    monkeypatch.setattr(live_manager, "stop_legacy_vps_sync_loop", lambda: None)
    monkeypatch.setattr(live_manager, "stop_legacy_offload", lambda: None)
    calls = []
    monkeypatch.setattr(live_manager, "start_local_live", lambda: calls.append("start"))
    latch_live_stop(tmp_path, "test fault")
    with pytest.raises(RuntimeError, match="acknowledge"):
        live_manager.switch_local()
    assert not calls and safety_stop_active(tmp_path)
    live_manager.switch_local(acknowledge_safety_stop=True)
    assert calls == ["start"]
    assert desired_live_running(tmp_path)
    assert not safety_stop_active(tmp_path)
    assert list((tmp_path / "diagnostics").glob("live_watchdog_acknowledged_*.txt"))
