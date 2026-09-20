from __future__ import annotations

import json
import subprocess
from datetime import datetime, timezone
from types import SimpleNamespace

from scripts.local_calibration_offload import (
    enqueue_remote_event,
    load_remote_market_context,
    parse_market_context,
)
from scripts.trader.hybrid_calibration import CalibrationEvent, MarketContext


def test_parse_market_context_from_live_state() -> None:
    ctx = parse_market_context(
        {
            "current_market": {
                "market_id": "btc-updown-15m-1776928500",
                "reference_price": 78224.8401,
            }
        }
    )

    assert ctx is not None
    assert ctx.market_id == "btc-updown-15m-1776928500"
    assert ctx.barrier == 78224.8401
    assert ctx.market_end_time_iso == "2026-04-23T07:30:00+00:00"


def test_parse_market_context_rejects_missing_or_malformed_state() -> None:
    assert parse_market_context({}) is None
    assert parse_market_context({"current_market": {"market_id": "bad", "reference_price": 1}}) is None
    assert parse_market_context({"current_market": {"market_id": "btc-updown-15m-1"}}) is None


def test_load_remote_market_context_uses_timeout_and_parses_json(monkeypatch) -> None:
    calls = []

    def fake_run(cmd, **kwargs):
        calls.append((cmd, kwargs))
        return SimpleNamespace(
            stdout=json.dumps(
                {
                    "current_market": {
                        "market_id": "btc-updown-15m-1776928500",
                        "reference_price": 78224.8401,
                    }
                }
            )
        )

    monkeypatch.setattr(subprocess, "run", fake_run)

    ctx = load_remote_market_context(
        host="example.com",
        user="ubuntu",
        remote_dir="/opt/app",
        ssh_key="key.pem",
    )

    assert ctx is not None
    assert ctx.market_id == "btc-updown-15m-1776928500"
    assert calls[0][1]["timeout"] == 15
    assert "StrictHostKeyChecking=accept-new" in calls[0][0]


def test_enqueue_remote_event_uses_timeout_and_atomic_remote_move(monkeypatch) -> None:
    calls = []

    def fake_run(cmd, **kwargs):
        calls.append((cmd, kwargs))
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(subprocess, "run", fake_run)
    ctx = MarketContext(
        market_id="btc-updown-15m-1776928500",
        barrier=78224.8401,
        market_end_time_iso="2026-04-23T07:30:00+00:00",
    )
    event = CalibrationEvent.from_parts(
        source="test",
        context=ctx,
        ready_at=datetime.now(timezone.utc),
        model_data={"prob_above": 0.5, "prob_below": 0.5},
        terminal_payload={"ok": True},
    )

    enqueue_remote_event(
        event,
        host="example.com",
        user="ubuntu",
        remote_dir="/opt/app",
        ssh_key="key.pem",
    )

    cmd, kwargs = calls[0]
    assert kwargs["timeout"] == 15
    assert kwargs["check"] is True
    assert "StrictHostKeyChecking=accept-new" in cmd
    assert "mv" in cmd[-1]


def test_load_remote_market_context_returns_none_on_timeout(monkeypatch) -> None:
    def fake_run(*args, **kwargs):
        raise subprocess.TimeoutExpired(cmd=args[0], timeout=kwargs.get("timeout"))

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert load_remote_market_context(
        host="example.com",
        user="ubuntu",
        remote_dir="/opt/app",
        ssh_key="key.pem",
    ) is None
