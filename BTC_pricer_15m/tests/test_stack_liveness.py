from __future__ import annotations

import json
import os

import pytest

from scripts import stack_liveness as watchdog
from scripts.live_runtime_state import (
    desired_live_running,
    initialize_live_desired,
    latch_live_stop,
    prepare_live_start,
    safety_stop_active,
    set_live_desired,
)


@pytest.fixture
def runtime(tmp_path, monkeypatch):
    monkeypatch.setattr(watchdog, "RESULTS_DIR", tmp_path)
    monkeypatch.setattr(watchdog, "MARKER_PATH", tmp_path / ".maintenance_stop")
    monkeypatch.setattr(watchdog, "HEARTBEAT_PATH", tmp_path / "watchdog.health")
    state = {name: ("running", "Up (healthy)") for name in watchdog.EXPECTED}
    calls = []

    def docker(*args, **kwargs):
        calls.append(args)
        if args[0] == "start":
            state[args[1]] = ("running", "Up (healthy)")
        return 0, ""

    monkeypatch.setattr(watchdog, "snapshot", lambda: (state.copy(), None))
    monkeypatch.setattr(watchdog, "docker", docker)
    monkeypatch.setattr(watchdog, "wait_for_health", lambda names, budget: [])
    prepare_live_start(tmp_path)
    return tmp_path, state, calls


def test_safety_latch_survives_time_unrelated_recovery_and_watchdog_restart(runtime):
    root, state, calls = runtime
    latch_live_stop(root, "opposite-side entry")
    # An unrelated operator-start marker cannot acknowledge a fault.
    set_live_desired(root, running=True, reason="older start intent")
    trigger = root / "diagnostics/live_watchdog_last_trigger.txt"
    os.utime(trigger, (1, 1))
    state[watchdog.LIVE_CONTAINER] = ("exited", "Exited")
    state["polymarket_15m_docker_logs"] = ("exited", "Exited")

    for previous_recovery in (0, 2, 10**10):
        assert watchdog.live_trigger_active(previous_recovery)
        watchdog.recover(watchdog.stopped_containers(state), hold_live=True)
        initialize_live_desired(root, observed_running=False)

    assert ("start", "polymarket_15m_docker_logs") in calls
    assert ("start", watchdog.LIVE_CONTAINER) not in calls
    assert safety_stop_active(root)
    with pytest.raises(RuntimeError, match="acknowledge-safety-stop"):
        prepare_live_start(root)
    prepare_live_start(root, acknowledge_safety_stop=True)
    assert not safety_stop_active(root)
    watchdog.recover(watchdog.stopped_containers(state), hold_live=False)
    assert ("start", watchdog.LIVE_CONTAINER) in calls


def test_manual_live_stop_survives_recovery_without_fault_trigger(runtime):
    root, state, calls = runtime
    set_live_desired(root, running=False, reason="operator stop")
    state[watchdog.LIVE_CONTAINER] = ("exited", "Exited")
    # Recover must recheck intent even if its caller took an older snapshot.
    watchdog.recover([watchdog.LIVE_CONTAINER], hold_live=False)
    assert not calls
    assert not safety_stop_active(root)


@pytest.mark.parametrize("legacy,observed,expected", [
    ("stopped", True, False), ("local", False, True), (None, False, False), (None, True, True),
])
def test_migration_preserves_intent_without_starting_ambiguous_live(tmp_path, legacy, observed, expected):
    if legacy is not None:
        (tmp_path / ".live_location").write_text(legacy)
    initialize_live_desired(tmp_path, observed_running=observed)
    assert desired_live_running(tmp_path) is expected
    initialize_live_desired(tmp_path, observed_running=not observed)
    assert desired_live_running(tmp_path) is expected


def test_corrupt_intent_is_stopped_not_overwritten(runtime):
    root, _, _ = runtime
    (root / ".live_desired.json").write_text("{broken")
    initialize_live_desired(root, observed_running=True)
    assert not desired_live_running(root)


def test_running_unhealthy_dependency_blocks_live_but_not_independent_logs(runtime):
    _, state, calls = runtime
    state["btc_pricer_15m_calc"] = ("running", "Up (unhealthy)")
    state[watchdog.LIVE_CONTAINER] = ("exited", "Exited")
    state["polymarket_15m_docker_logs"] = ("exited", "Exited")
    ok, detail = watchdog.recover(watchdog.stopped_containers(state), hold_live=False)
    assert not ok
    assert "waiting for healthy btc_pricer_15m_calc" in detail
    assert ("start", watchdog.LIVE_CONTAINER) not in calls
    assert ("start", "polymarket_15m_docker_logs") in calls
    state["btc_pricer_15m_calc"] = ("running", "Up (healthy)")
    assert watchdog.recover(watchdog.stopped_containers(state), hold_live=False)[0]
    assert ("start", watchdog.LIVE_CONTAINER) in calls


def test_newly_started_unhealthy_dependency_prevents_downstream(runtime, monkeypatch):
    _, state, calls = runtime
    state["btc_pricer_15m_redis"] = ("exited", "Exited")
    state["btc_pricer_15m_spot"] = ("exited", "Exited")

    def wait(names, budget):
        state[names[0]] = ("running", "Up (unhealthy)")
        return names

    monkeypatch.setattr(watchdog, "wait_for_health", wait)
    ok, detail = watchdog.recover(watchdog.stopped_containers(state), hold_live=False)
    assert not ok and "unhealthy after start" in detail
    assert calls == [("start", "btc_pricer_15m_redis")]


def test_recovery_rechecks_maintenance_before_any_start(runtime):
    root, state, calls = runtime
    state["btc_pricer_15m_grid"] = ("exited", "Exited")
    (root / ".maintenance_stop").write_text(json.dumps({"reason": "maintenance"}))
    assert not watchdog.recover(["btc_pricer_15m_grid"], hold_live=False)[0]
    assert not calls


def test_retry_backoff_remains_finite_after_many_failures(monkeypatch):
    monkeypatch.setattr(watchdog, "POLL_SECONDS", 60)
    assert [watchdog.retry_delay(n) for n in (1, 2, 3, 4, 50)] == [60, 120, 240, 300, 300]
