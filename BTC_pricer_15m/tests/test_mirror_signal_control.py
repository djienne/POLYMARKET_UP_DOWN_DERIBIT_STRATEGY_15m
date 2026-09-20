from __future__ import annotations

import json
import threading
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts.trader.config import GridConfig, TraderConfig
from scripts.trader.executor import OrderResult
from scripts.trader.mirror_control import (
    MirrorTradeSignal,
    RedisMirrorSignalSubscriber,
    mirror_config_hash_from_trader,
    resolve_mirror_instance_id,
)
from scripts.trader.position import Direction, MicroPosition, TradeResult
from scripts.trader.single_trader import EntryAttemptResult, SingleTrader
from scripts.trader.state import StateManager
from tests.conftest import make_order_book, make_tick


class _AckSink:
    def __init__(self):
        self.acks: list[tuple[str, str, str]] = []
        self.details: list[dict] = []
        self.delivery_acks: list[str] = []
        self.json_keys: dict[str, dict | None] = {}

    def ack_signal(self, signal, *, status: str, reason: str = "", details: dict | None = None) -> None:
        self.acks.append((signal.signal_id, status, reason))
        self.details.append(details or {})

    def ack_message(self, message_id: str) -> None:
        self.delivery_acks.append(message_id)

    def get_json_key(self, key: str):
        return self.json_keys.get(key)


class _Exec:
    is_live = True

    def __init__(self):
        self.positions: dict[str, float] = {}
        self.orders: dict[str, dict] = {}
        self.cancelled: list[str] = []
        self.sold: list[tuple[str, float]] = []
        self.best_bid = 0.40
        self.best_ask = 0.42
        self.cancel_ok = True
        self.position_error: RuntimeError | None = None
        self.sell_success = True
        self.sell_avg_fill_price: float | None = None
        self.sell_error_message = "sell failed"
        self.proceeds_from_trades: float | None = None

    def get_order_status(self, order_id):
        return self.orders.get(order_id, {"status": "live", "size_matched": 0})

    def cancel_order(self, order_id):
        self.cancelled.append(order_id)
        if not self.cancel_ok:
            return False
        self.orders[order_id] = {"status": "cancelled"}
        return True

    def get_position(self, token_id, strict=False):
        if strict and self.position_error is not None:
            raise self.position_error
        return self.positions.get(token_id, 0.0)

    def get_best_bid(self, token_id):
        return self.best_bid

    def get_best_ask(self, token_id):
        return self.best_ask

    def sell_market(self, token_id, shares):
        self.sold.append((token_id, shares))
        if not self.sell_success:
            return OrderResult(
                success=False,
                error_message=self.sell_error_message,
            )
        avg_fill_price = self.best_bid if self.sell_avg_fill_price is None else self.sell_avg_fill_price
        self.positions[token_id] = max(0.0, self.positions.get(token_id, 0.0) - shares)
        return OrderResult(
            success=True,
            order_id=f"sell-{len(self.sold)}",
            shares_filled=shares,
            avg_fill_price=avg_fill_price,
            total_cost=shares * avg_fill_price,
        )

    def get_proceeds_from_trades(self, **kwargs):
        return self.proceeds_from_trades


def _config(tmp_path: Path, **overrides) -> TraderConfig:
    return TraderConfig(
        paper_mode=False,
        live_trading_enabled=True,
        require_confirmation=False,
        reconciliation_enabled=False,
        mirror_signal_control_enabled=True,
        mirror_signal_async_enabled=True,
        mirror_instance_id="773",
        local_entry_enabled=False,
        local_stop_loss_enabled=False,
        dry_tp_live_fill_grace_seconds=120.0,
        state_file=tmp_path / "paper_state.json",
        live_state_file=tmp_path / "live_state.json",
        trades_file=tmp_path / "paper_trades.csv",
        live_trades_file=tmp_path / "live_trades.csv",
        equity_file=tmp_path / "paper_equity.csv",
        live_equity_file=tmp_path / "live_equity.csv",
        risk_events_file=tmp_path / "risk.csv",
        risk_events_current_file=tmp_path / "risk.current.csv",
        live_tick_eval_file=tmp_path / "live_tick_eval.csv",
        friction_realized_file=tmp_path / "friction.csv",
        orderbook_file=tmp_path / "orderbook.csv",
        mirror_signal_audit_file=tmp_path / "mirror_signal_audit.csv",
        mirror_signal_audit_current_file=tmp_path / "mirror_signal_audit.current.csv",
        **overrides,
    )


def _position(market_id="btc-updown-15m-1778458500", token_id="up-token") -> MicroPosition:
    return MicroPosition(
        direction=Direction.UP,
        token_id=token_id,
        entry_price=0.55,
        shares=10.0,
        cost_basis=5.50,
        tp_price=0.72,
        stop_loss_price=0.44,
        opened_at=datetime.now(timezone.utc),
        market_id=market_id,
        peak_price=0.55,
        market_title="BTC Up or Down",
        reference_price=100000.0,
    )


def _arm_dry_tp_fallback(position: MicroPosition) -> None:
    position.pending_dry_tp_signal_id = "sig-tp"
    position.dry_tp_seen_at = datetime.now(timezone.utc) - timedelta(seconds=121)
    position.dry_tp_market_exit_deadline = datetime.now(timezone.utc) - timedelta(seconds=1)


def _risk_event_count(trader: SingleTrader, kind: str) -> int:
    path = trader.config.risk_events_file
    if not path.exists():
        return 0
    return path.read_text(encoding="utf-8").count(f",{kind},")


def _trader(tmp_path: Path, executor: _Exec | None = None, **overrides) -> SingleTrader:
    cfg = _config(tmp_path, **overrides)
    trader = SingleTrader.__new__(SingleTrader)
    trader.config = cfg
    trader.verbose = False
    trader.state = StateManager(cfg)
    trader.executor = executor or _Exec()
    trader.log = lambda message: None
    trader._mirror_instance_id = 773
    trader._mirror_config_hash = mirror_config_hash_from_trader(cfg)
    trader._mirror_subscriber = _AckSink()
    trader._mirror_async_subscriber = None
    trader._current_market_id = None
    trader._last_tick = None
    trader._last_tick_received_at = None
    trader._last_heartbeat = None
    trader._last_reconciliation = None
    trader._tp_status_failures = {}
    trader._tp_backup_failures = {}
    trader._recently_settled = {}
    trader._ensure_mirror_async_runtime()
    trader.state.configure_mirror_control(
        enabled=True,
        instance_id=773,
        config_hash=trader._mirror_config_hash,
    )
    return trader


def _signal(
    trader: SingleTrader,
    event: str,
    *,
    signal_id: str | None = None,
    instance_id: int = 773,
    config_hash: str | None = None,
    published_at: datetime | None = None,
) -> MirrorTradeSignal:
    now = published_at or datetime.now(timezone.utc)
    return MirrorTradeSignal(
        signal_id=signal_id or f"sig-{event.lower()}",
        source_instance_id=instance_id,
        source_config_hash=config_hash if config_hash is not None else trader._mirror_config_hash,
        market_id="btc-updown-15m-1778458500",
        market_title="BTC Up or Down",
        direction="UP",
        event=event,
        dry_entry_price=0.55,
        dry_exit_price=0.72 if event == "TP_FILLED" else None,
        dry_shares=10.0,
        publish_timestamp=now,
        expires_at=now + timedelta(seconds=30),
        trigger_bid=0.40,
        trigger_ask=0.56,
        trigger_mid=0.48,
        barrier=100000.0,
        spot_price=100100.0,
    )


def test_mirror_instance_auto_resolves_current_live_params_to_1271():
    root = Path(__file__).resolve().parents[1]
    grid_cfg = GridConfig.from_json(root / "config.yaml")
    live_cfg = TraderConfig.from_json(root / "config.yaml")

    assert resolve_mirror_instance_id(
        grid_config=grid_cfg,
        live_config=live_cfg,
        requested="auto",
    ) == 1271


def test_current_live_params_with_sl20_resolve_to_815():
    root = Path(__file__).resolve().parents[1]
    grid_cfg = GridConfig.from_json(root / "config.yaml")
    live_cfg = TraderConfig.from_json(root / "config.yaml")
    live_cfg.stop_loss_pct = 0.20

    assert resolve_mirror_instance_id(
        grid_config=grid_cfg,
        live_config=live_cfg,
        requested="auto",
    ) == 815


def test_mirror_control_rejects_local_entry_footgun(tmp_path):
    with pytest.raises(ValueError, match="local_entry_enabled must be false"):
        TraderConfig(
            paper_mode=False,
            live_trading_enabled=True,
            mirror_signal_control_enabled=True,
            mirror_controls_entry=True,
            local_entry_enabled=True,
            live_state_file=tmp_path / "live_state.json",
            live_trades_file=tmp_path / "live_trades.csv",
            live_equity_file=tmp_path / "live_equity.csv",
        )


def test_wrong_instance_hash_stale_and_duplicate_signals_are_rejected(tmp_path):
    trader = _trader(tmp_path)
    tick = make_tick(market_id="btc-updown-15m-1778458500")

    trader._process_mirror_signal(_signal(trader, "STOP_LOSS", instance_id=1), tick)
    trader._process_mirror_signal(_signal(trader, "STOP_LOSS", signal_id="bad-hash", config_hash="bad"), tick)
    trader._process_mirror_signal(
        _signal(
            trader,
            "STOP_LOSS",
            signal_id="stale",
            published_at=datetime.now(timezone.utc) - timedelta(seconds=120),
        ),
        tick,
    )
    trader.state.mark_mirror_signal_processed("dupe")
    trader._process_mirror_signal(_signal(trader, "STOP_LOSS", signal_id="dupe"), tick)

    reasons = [ack[2] for ack in trader._mirror_subscriber.acks]
    assert "wrong_instance" in reasons
    assert "wrong_config_hash" in reasons
    assert "stale" in reasons
    assert trader._mirror_subscriber.acks[-1] == ("dupe", "duplicate", "")


def test_duplicate_mirror_acks_are_throttled(tmp_path):
    trader = _trader(tmp_path, mirror_signal_ack_duplicate_throttle_seconds=30.0)
    signal = _signal(trader, "STOP_LOSS", signal_id="dupe")
    trader.state.mark_mirror_signal_processed("dupe")

    trader._process_mirror_signal(signal, None)
    trader._process_mirror_signal(signal, None)

    assert trader._mirror_subscriber.acks == [("dupe", "duplicate", "")]


def test_duplicate_ack_throttle_cache_is_pruned(tmp_path):
    trader = _trader(tmp_path, mirror_signal_ack_duplicate_throttle_seconds=30.0)
    trader._mirror_ack_last_sent_at = {
        ("old", "duplicate", ""): 100.0,
        ("new", "duplicate", ""): 190.0,
    }

    trader._prune_mirror_ack_throttle(now_monotonic=200.0)

    assert ("old", "duplicate", "") not in trader._mirror_ack_last_sent_at
    assert ("new", "duplicate", "") in trader._mirror_ack_last_sent_at


def test_mirror_entry_calls_execute_entry_without_local_edge_decision(tmp_path):
    trader = _trader(tmp_path)
    tick = make_tick(market_id="btc-updown-15m-1778458500")
    calls = []

    def fake_execute(opportunity):
        calls.append(opportunity)
        pos = _position(market_id=tick.market_id, token_id=tick.up_token_id)
        trader.state.open_positions.append(pos)
        return SimpleNamespace(entered=True, position=pos)

    trader.execute_entry = fake_execute
    trader._process_mirror_signal(_signal(trader, "ENTRY"), tick)

    assert len(calls) == 1
    assert calls[0]["direction"] == "UP"
    assert calls[0]["mirror_signal_id"] == "sig-entry"
    assert calls[0]["mirror_controlled_entry"] is True
    assert trader.state.open_positions[0].opened_from_mirror_signal_id == "sig-entry"


def test_mirror_entry_too_old_is_rejected_before_live_attempt(tmp_path):
    trader = _trader(tmp_path, mirror_signal_entry_max_consumption_age_seconds=15.0)
    tick = make_tick(market_id="btc-updown-15m-1778458500")
    calls = []
    trader.execute_entry = lambda opportunity: calls.append(opportunity)
    signal = _signal(
        trader,
        "ENTRY",
        published_at=datetime.now(timezone.utc) - timedelta(seconds=16),
    )

    trader._process_mirror_signal(signal, tick)

    assert calls == []
    assert trader.state.has_processed_mirror_signal(signal.signal_id)
    assert trader._mirror_subscriber.acks[-1] == (
        signal.signal_id,
        "rejected",
        "entry_signal_too_old",
    )


def test_mirror_entry_rejects_same_market_reentry_guard(tmp_path):
    trader = _trader(tmp_path)
    tick = make_tick(market_id="btc-updown-15m-1778458500")
    calls = []
    trader.execute_entry = lambda opportunity: calls.append(opportunity)
    trader.state.last_entered_market_id = tick.market_id
    signal = _signal(trader, "ENTRY")

    trader._process_mirror_signal(signal, tick)

    assert calls == []
    assert trader.state.has_processed_mirror_signal(signal.signal_id)
    assert trader._mirror_subscriber.acks[-1] == (
        signal.signal_id,
        "rejected",
        "already_entered_market",
    )


def test_mirror_entry_edge_lost_after_recheck_is_retryable(tmp_path):
    trader = _trader(tmp_path, mirror_signal_entry_retry_delay_seconds=0.0)
    tick = make_tick(market_id="btc-updown-15m-1778458500")
    signal = _signal(trader, "ENTRY")

    trader.execute_entry = lambda opportunity: EntryAttemptResult(
        attempted=True,
        rejection_reason="delayed_book_recheck_failed",
        rejection_details={"reason": "edge_lost_after_recheck"},
    )

    trader._process_mirror_signal(signal, tick)

    assert not trader.state.has_processed_mirror_signal(signal.signal_id)
    assert [item.signal_id for item in trader._pending_async_mirror_entries] == [signal.signal_id]
    assert trader._mirror_subscriber.acks[-1] == (
        signal.signal_id,
        "retryable",
        "entry_retryable:delayed_book_recheck_failed",
    )


def test_mirror_entry_audit_records_latency_attempt_and_outcome(tmp_path):
    trader = _trader(tmp_path, mirror_signal_entry_retry_delay_seconds=0.0)
    tick = make_tick(market_id="btc-updown-15m-1778458500")
    signal = _signal(trader, "ENTRY")
    trader.execute_entry = lambda opportunity: EntryAttemptResult(
        attempted=True,
        rejection_reason="delayed_book_recheck_failed",
        rejection_details={"reason": "edge_lost_after_recheck"},
    )

    trader._note_mirror_signal_received(signal, stream_id="1-0", source="async")
    trader._process_mirror_signal(signal, tick)

    audit_text = trader.config.mirror_signal_audit_current_file.read_text(encoding="utf-8")
    assert "publish_to_receive_s" in audit_text
    assert "receive_to_attempt_s" in audit_text
    assert signal.signal_id in audit_text
    assert "retryable" in audit_text
    assert "delayed_book_recheck_failed" in audit_text


def test_mirror_entry_recheck_has_no_artificial_sleep_and_bypasses_edge(tmp_path, monkeypatch):
    trader = _trader(tmp_path, live_recheck_delay_seconds=99.0)

    class FakeOrderBookClient:
        def fetch_order_book(self, token_id):
            return make_order_book(best_ask=0.55, ask_size=1000.0)

        def simulate_market_buy(self, book, usd_amount):
            return usd_amount / 0.55, 0.55, usd_amount

    def fail_sleep(seconds):
        raise AssertionError(f"mirror entry recheck slept for {seconds}s")

    monkeypatch.setattr("scripts.trader.risk_engine.time.sleep", fail_sleep)
    trader.order_book_client = FakeOrderBookClient()
    trader.risk_engine = trader._create_risk_engine()

    price = trader._simulate_live_entry_recheck(
        "up-token",
        5.75,
        {"direction": "UP", "model_prob": 0.01, "mirror_controlled_entry": True},
    )

    assert price == pytest.approx(0.55)
    assert trader._get_risk_engine().last_entry_recheck_details["delay_s"] == "0.000"
    assert trader._get_risk_engine().last_entry_recheck_details["reason"] == "accepted_mirror_edge_bypassed"


def test_mirror_entry_post_only_failure_is_retryable(tmp_path):
    trader = _trader(tmp_path, mirror_signal_entry_retry_delay_seconds=0.0)
    tick = make_tick(market_id="btc-updown-15m-1778458500")
    signal = _signal(trader, "ENTRY")

    trader.execute_entry = lambda opportunity: EntryAttemptResult(
        attempted=True,
        rejection_reason="order_failed",
        rejection_details={
            "error": (
                "Limit buy failed: PolyApiException[status_code=503, "
                "error_message={'error': 'post-only mode: only post-only orders "
                "and cancels are allowed', 'code': 'post_only_mode'}]"
            ),
        },
        order_metadata={"final_reason": "exception", "error": "post_only_mode"},
    )

    trader._process_mirror_signal(signal, tick)

    assert not trader.state.has_processed_mirror_signal(signal.signal_id)
    assert [item.signal_id for item in trader._pending_async_mirror_entries] == [signal.signal_id]
    assert trader._mirror_subscriber.acks[-1] == (
        signal.signal_id,
        "retryable",
        "entry_retryable:order_failed",
    )
    assert _risk_event_count(trader, "mirror_entry_retryable_failed") == 1
    assert _risk_event_count(trader, "mirror_entry_retry_queued") == 1


def test_mirror_entry_retry_later_succeeds_and_marks_processed(tmp_path):
    trader = _trader(tmp_path, mirror_signal_entry_retry_delay_seconds=0.0)
    tick = make_tick(market_id="btc-updown-15m-1778458500")
    signal = _signal(trader, "ENTRY")
    calls = []

    def fake_execute(opportunity):
        calls.append(opportunity)
        if len(calls) == 1:
            return EntryAttemptResult(
                attempted=True,
                rejection_reason="order_failed",
                rejection_details={"error": "Limit buy failed: status_code=503 post_only_mode"},
            )
        pos = _position(market_id=tick.market_id, token_id=tick.up_token_id)
        trader.state.open_positions.append(pos)
        return EntryAttemptResult(position=pos, attempted=True)

    trader.execute_entry = fake_execute

    trader._process_mirror_signal(signal, tick)
    trader._drain_pending_async_mirror_entries(tick)

    assert len(calls) == 2
    assert trader._pending_async_mirror_entries == []
    assert trader.state.has_processed_mirror_signal(signal.signal_id)
    assert trader.state.open_positions[0].opened_from_mirror_signal_id == signal.signal_id
    assert trader._mirror_subscriber.acks[-1] == (signal.signal_id, "accepted", "entered")


def test_mirror_entry_nonretryable_order_failure_is_final(tmp_path):
    trader = _trader(tmp_path)
    tick = make_tick(market_id="btc-updown-15m-1778458500")
    signal = _signal(trader, "ENTRY")

    trader.execute_entry = lambda opportunity: EntryAttemptResult(
        attempted=True,
        rejection_reason="order_failed",
        rejection_details={"error": "Limit buy failed: invalid signature"},
    )

    trader._process_mirror_signal(signal, tick)

    assert trader._pending_async_mirror_entries == []
    assert trader.state.has_processed_mirror_signal(signal.signal_id)
    assert trader._mirror_subscriber.acks[-1] == (
        signal.signal_id,
        "rejected",
        "entry_failed:order_failed",
    )


def test_mirror_tp_exit_cancels_pending_retryable_entry_when_live_flat(tmp_path):
    trader = _trader(tmp_path, mirror_signal_entry_retry_delay_seconds=0.0)
    tick = make_tick(market_id="btc-updown-15m-1778458500")
    entry_signal = _signal(trader, "ENTRY", signal_id="sig-entry-retry")
    calls = []

    def retryable_failure(opportunity):
        calls.append(opportunity)
        return EntryAttemptResult(
            attempted=True,
            rejection_reason="order_failed",
            rejection_details={"error": "Limit buy failed: status_code=503 post_only_mode"},
        )

    trader.execute_entry = retryable_failure

    trader._process_mirror_signal(entry_signal, tick)
    trader._process_mirror_signal(_signal(trader, "TP_FILLED", signal_id="sig-tp-after-entry"), None)
    trader._drain_pending_async_mirror_entries(tick)

    assert len(calls) == 1
    assert trader._pending_async_mirror_entries == []
    assert trader.state.has_processed_mirror_signal(entry_signal.signal_id)
    assert (
        entry_signal.signal_id,
        "rejected",
        "entry_cancelled_by_dry_exit",
    ) in trader._mirror_subscriber.acks
    assert trader._mirror_subscriber.acks[-1] == (
        "sig-tp-after-entry",
        "accepted",
        "already_flat",
    )
    assert _risk_event_count(trader, "mirror_entry_retry_cancelled_by_exit") == 1


def test_mirror_stop_loss_cancels_pending_retryable_entry_when_live_flat(tmp_path):
    trader = _trader(tmp_path, mirror_signal_entry_retry_delay_seconds=0.0)
    tick = make_tick(market_id="btc-updown-15m-1778458500")
    entry_signal = _signal(trader, "ENTRY", signal_id="sig-entry-stop-loss")
    calls = []

    trader.execute_entry = lambda opportunity: (
        calls.append(opportunity)
        or EntryAttemptResult(
            attempted=True,
            rejection_reason="order_failed",
            rejection_details={"error": "Limit buy failed: status_code=503 post_only_mode"},
        )
    )

    trader._process_mirror_signal(entry_signal, tick)
    trader._process_mirror_signal(_signal(trader, "STOP_LOSS", signal_id="sig-sl-after-entry"), None)
    trader._drain_pending_async_mirror_entries(tick)

    assert len(calls) == 1
    assert trader._pending_async_mirror_entries == []
    assert trader.state.has_processed_mirror_signal(entry_signal.signal_id)
    assert (
        entry_signal.signal_id,
        "rejected",
        "entry_cancelled_by_dry_exit",
    ) in trader._mirror_subscriber.acks
    assert trader._mirror_subscriber.acks[-1] == (
        "sig-sl-after-entry",
        "accepted",
        "already_flat",
    )


def test_mirror_exit_does_not_cancel_unrelated_pending_entry(tmp_path):
    trader = _trader(tmp_path, mirror_signal_entry_retry_delay_seconds=0.0)
    tick = make_tick(market_id="btc-updown-15m-1778458500")
    entry_signal = _signal(trader, "ENTRY", signal_id="sig-entry-retry")

    trader.execute_entry = lambda opportunity: EntryAttemptResult(
        attempted=True,
        rejection_reason="order_failed",
        rejection_details={"error": "Limit buy failed: status_code=503 post_only_mode"},
    )

    trader._process_mirror_signal(entry_signal, tick)
    unrelated_exit = replace(
        _signal(trader, "TP_FILLED", signal_id="sig-tp-other-market"),
        market_id="btc-updown-15m-other",
    )
    trader._process_mirror_signal(unrelated_exit, None)

    assert [item.signal_id for item in trader._pending_async_mirror_entries] == [entry_signal.signal_id]
    assert not trader.state.has_processed_mirror_signal(entry_signal.signal_id)
    assert (
        entry_signal.signal_id,
        "rejected",
        "entry_cancelled_by_dry_exit",
    ) not in trader._mirror_subscriber.acks
    assert trader.state.has_processed_mirror_signal(unrelated_exit.signal_id)


def test_mirror_stop_loss_cancels_tp_and_market_sells(tmp_path, monkeypatch):
    monkeypatch.setattr("scripts.trader.single_trader.time.sleep", lambda _: None)
    executor = _Exec()
    executor.positions["up-token"] = 10.0
    trader = _trader(tmp_path, executor)
    pos = _position()
    pos.tp_order_id = "tp-1"
    trader.state.open_positions.append(pos)

    trader._process_mirror_signal(_signal(trader, "STOP_LOSS"), None)

    assert executor.cancelled == ["tp-1"]
    assert executor.sold == [("up-token", 10.0)]
    assert trader.state.open_positions == []
    assert trader.state.closed_positions[0].result == TradeResult.STOP_LOSS


def test_mirror_stop_loss_cancel_failure_is_retryable(tmp_path, monkeypatch):
    monkeypatch.setattr("scripts.trader.single_trader.time.sleep", lambda _: None)
    executor = _Exec()
    executor.positions["up-token"] = 10.0
    executor.cancel_ok = False
    trader = _trader(tmp_path, executor)
    pos = _position()
    pos.tp_order_id = "tp-1"
    trader.state.open_positions.append(pos)
    signal = _signal(trader, "STOP_LOSS")

    trader._process_mirror_signal(signal, None)

    assert executor.cancelled == ["tp-1"]
    assert executor.sold == []
    assert not trader.state.has_processed_mirror_signal(signal.signal_id)
    assert trader._mirror_subscriber.acks[-1] == (
        signal.signal_id,
        "retryable",
        "stop_loss_not_closed",
    )

    executor.cancel_ok = True
    trader._process_mirror_signal(signal, None)

    assert executor.cancelled == ["tp-1", "tp-1"]
    assert executor.sold == [("up-token", 10.0)]
    assert trader.state.has_processed_mirror_signal(signal.signal_id)
    assert trader.state.open_positions == []
    assert trader.state.closed_positions[0].result == TradeResult.STOP_LOSS


def test_mirror_stop_loss_no_bid_is_retryable(tmp_path):
    executor = _Exec()
    executor.positions["up-token"] = 10.0
    executor.best_bid = None
    trader = _trader(tmp_path, executor)
    pos = _position()
    trader.state.open_positions.append(pos)
    signal = replace(_signal(trader, "STOP_LOSS"), trigger_bid=None)

    trader._process_mirror_signal(signal, None)

    assert executor.cancelled == []
    assert executor.sold == []
    assert not trader.state.has_processed_mirror_signal(signal.signal_id)
    assert trader._mirror_subscriber.acks[-1] == (
        signal.signal_id,
        "retryable",
        "no_bid",
    )
    assert trader.state.open_positions == [pos]


def test_mirror_stop_loss_prefers_fresh_bid_over_signal_bid(tmp_path, monkeypatch):
    monkeypatch.setattr("scripts.trader.single_trader.time.sleep", lambda _: None)
    executor = _Exec()
    executor.positions["up-token"] = 10.0
    executor.best_bid = 0.35
    executor.sell_avg_fill_price = 0.35
    trader = _trader(tmp_path, executor)
    pos = _position()
    trader.state.open_positions.append(pos)

    trader._process_mirror_signal(replace(_signal(trader, "STOP_LOSS"), trigger_bid=0.50), None)

    assert executor.sold == [("up-token", 10.0)]
    assert trader.state.closed_positions[0].exit_price == pytest.approx(0.35)


def test_async_mirror_stop_loss_closes_without_tick(tmp_path, monkeypatch):
    monkeypatch.setattr("scripts.trader.single_trader.time.sleep", lambda _: None)
    executor = _Exec()
    executor.positions["up-token"] = 10.0
    trader = _trader(tmp_path, executor)
    pos = _position()
    pos.tp_order_id = "tp-1"
    trader.state.open_positions.append(pos)

    trader._process_async_mirror_signal(_signal(trader, "STOP_LOSS"))

    assert executor.cancelled == ["tp-1"]
    assert executor.sold == [("up-token", 10.0)]
    assert trader.state.open_positions == []
    assert trader.state.closed_positions[0].result == TradeResult.STOP_LOSS


def test_async_mirror_stop_loss_is_idempotent_with_tick_scan(tmp_path, monkeypatch):
    monkeypatch.setattr("scripts.trader.single_trader.time.sleep", lambda _: None)
    executor = _Exec()
    executor.positions["up-token"] = 10.0
    trader = _trader(tmp_path, executor)
    pos = _position()
    pos.tp_order_id = "tp-1"
    trader.state.open_positions.append(pos)
    signal = _signal(trader, "STOP_LOSS")

    trader._process_async_mirror_signal(signal)
    trader._process_mirror_signal(signal, None)

    assert executor.sold == [("up-token", 10.0)]
    assert trader._mirror_subscriber.acks[-1] == (signal.signal_id, "duplicate", "")


def test_async_mirror_entry_uses_fresh_matching_tick(tmp_path):
    trader = _trader(tmp_path)
    tick = make_tick(market_id="btc-updown-15m-1778458500")
    trader._last_tick = tick
    trader._last_tick_received_at = datetime.now(timezone.utc)
    calls = []

    def fake_execute(opportunity):
        calls.append(opportunity)
        pos = _position(market_id=tick.market_id, token_id=tick.up_token_id)
        trader.state.open_positions.append(pos)
        return SimpleNamespace(entered=True, position=pos)

    trader.execute_entry = fake_execute
    trader._process_async_mirror_signal(_signal(trader, "ENTRY"))

    assert len(calls) == 1
    assert calls[0]["tick"] is tick
    assert trader.state.open_positions[0].opened_from_mirror_signal_id == "sig-entry"


def test_async_mirror_entry_without_fresh_tick_is_queued(tmp_path):
    trader = _trader(tmp_path)
    calls = []
    trader.execute_entry = lambda opportunity: calls.append(opportunity)

    signal = _signal(trader, "ENTRY")
    trader._process_async_mirror_signal(signal)

    assert calls == []
    assert not trader.state.has_processed_mirror_signal(signal.signal_id)
    assert [item.signal_id for item in trader._pending_async_mirror_entries] == [signal.signal_id]
    assert trader._mirror_subscriber.acks == []


def test_queued_async_mirror_entry_drains_on_matching_tick(tmp_path):
    trader = _trader(tmp_path)
    tick = make_tick(market_id="btc-updown-15m-1778458500")
    signal = _signal(trader, "ENTRY")
    trader._pending_async_mirror_entries = [signal]
    calls = []

    def fake_execute(opportunity):
        calls.append(opportunity)
        pos = _position(market_id=tick.market_id, token_id=tick.up_token_id)
        trader.state.open_positions.append(pos)
        return SimpleNamespace(entered=True, position=pos)

    trader.execute_entry = fake_execute
    trader._drain_pending_async_mirror_entries(tick)

    assert len(calls) == 1
    assert trader._pending_async_mirror_entries == []
    assert trader.state.has_processed_mirror_signal(signal.signal_id)


def test_queued_async_mirror_entry_expires_without_entering(tmp_path):
    trader = _trader(tmp_path)
    tick = make_tick(market_id="btc-updown-15m-1778458500")
    signal = _signal(trader, "ENTRY")
    expired = replace(
        signal,
        publish_timestamp=datetime.now(timezone.utc) - timedelta(seconds=120),
        expires_at=datetime.now(timezone.utc) - timedelta(seconds=1),
    )
    trader._pending_async_mirror_entries = [expired]
    calls = []
    trader.execute_entry = lambda opportunity: calls.append(opportunity)

    trader._drain_pending_async_mirror_entries(tick)

    assert calls == []
    assert trader._pending_async_mirror_entries == []
    assert trader.state.has_processed_mirror_signal(signal.signal_id)
    assert trader._mirror_subscriber.acks[-1] == (
        signal.signal_id,
        "rejected",
        "entry_signal_too_old",
    )


def test_async_mirror_tp_signal_starts_fallback_timer(tmp_path):
    trader = _trader(tmp_path)
    pos = _position()
    trader.state.open_positions.append(pos)

    trader._process_async_mirror_signal(_signal(trader, "TP_FILLED"))

    assert pos.pending_dry_tp_signal_id == "sig-tp_filled"
    assert pos.dry_tp_market_exit_deadline is not None


def test_async_mirror_stop_loss_waits_for_action_lock(tmp_path, monkeypatch):
    monkeypatch.setattr("scripts.trader.single_trader.time.sleep", lambda _: None)
    executor = _Exec()
    executor.positions["up-token"] = 10.0
    trader = _trader(tmp_path, executor, mirror_signal_lock_warn_seconds=0)
    pos = _position()
    pos.tp_order_id = "tp-1"
    trader.state.open_positions.append(pos)
    signal = _signal(trader, "STOP_LOSS")
    done = threading.Event()

    trader._mirror_action_lock.acquire()

    def run_async():
        try:
            trader._process_async_mirror_signal(signal)
        finally:
            done.set()

    thread = threading.Thread(target=run_async)
    thread.start()
    threading.Event().wait(0.05)
    assert executor.sold == []

    trader._mirror_action_lock.release()
    assert done.wait(2.0)
    thread.join(timeout=2.0)
    trader._process_mirror_signal(signal, None)

    assert executor.sold == [("up-token", 10.0)]
    assert trader._mirror_subscriber.acks[-1] == (signal.signal_id, "duplicate", "")


def test_mirror_listener_shutdown_closes_async_subscriber(tmp_path):
    trader = _trader(tmp_path)

    class FakeSubscriber:
        def __init__(self):
            self.closed = False

        def close(self):
            self.closed = True

    subscriber = FakeSubscriber()
    trader._mirror_async_subscriber = subscriber
    trader._mirror_listener_thread = None

    trader._stop_mirror_signal_listener()

    assert subscriber.closed is True
    assert trader._mirror_async_subscriber is None


def test_local_live_stop_loss_does_not_trigger_under_mirror_control(tmp_path):
    executor = _Exec()
    executor.positions["up-token"] = 10.0
    trader = _trader(tmp_path, executor)
    trader._last_tick = replace(
        make_tick(market_id="btc-updown-15m-1778458500"),
        time_remaining_seconds=300.0,
    )
    trader.state.open_positions.append(_position())

    trader.check_tp_fills()

    assert executor.sold == []
    assert len(trader.state.open_positions) == 1


def test_dry_tp_signal_starts_timer_and_market_exits_after_grace(tmp_path):
    executor = _Exec()
    executor.positions["up-token"] = 10.0
    trader = _trader(tmp_path, executor)
    pos = _position()
    pos.tp_order_id = "tp-1"
    trader.state.open_positions.append(pos)

    trader._process_mirror_signal(_signal(trader, "TP_FILLED"), None)

    assert pos.pending_dry_tp_signal_id == "sig-tp_filled"
    assert pos.dry_tp_market_exit_deadline is not None
    pos.dry_tp_market_exit_deadline = datetime.now(timezone.utc) - timedelta(seconds=1)
    trader._check_dry_tp_fallbacks(make_tick(market_id=pos.market_id))

    assert executor.cancelled == ["tp-1"]
    assert executor.sold == [("up-token", 10.0)]
    assert trader.state.open_positions == []
    assert trader.state.closed_positions[0].result == TradeResult.TP_SIGNAL_MARKET_EXIT


def test_dry_tp_signal_immediate_mode_market_exits_without_grace(tmp_path):
    executor = _Exec()
    executor.positions["up-token"] = 10.0
    trader = _trader(tmp_path, executor, dry_tp_signal_action="immediate_market_exit")
    pos = _position()
    pos.tp_order_id = "tp-1"
    trader.state.open_positions.append(pos)

    trader._process_mirror_signal(_signal(trader, "TP_FILLED"), None)

    assert executor.cancelled == ["tp-1"]
    assert executor.sold == [("up-token", 10.0)]
    assert trader.state.open_positions == []
    assert trader.state.closed_positions[0].result == TradeResult.TP_SIGNAL_MARKET_EXIT
    assert trader._mirror_subscriber.acks[-1] == (
        "sig-tp_filled",
        "accepted",
        "market_exited",
    )


def test_dry_tp_fallback_skips_market_sell_if_expired(tmp_path):
    executor = _Exec()
    executor.positions["up-token"] = 10.0
    trader = _trader(tmp_path, executor)
    pos = _position()
    pos.tp_order_id = "tp-1"
    _arm_dry_tp_fallback(pos)
    trader.state.open_positions.append(pos)
    tick = replace(make_tick(market_id=pos.market_id), is_expired=True)

    trader._check_dry_tp_fallbacks(tick)

    assert executor.cancelled == []
    assert executor.sold == []
    assert trader.state.open_positions == [pos]
    assert pos.pending_dry_tp_signal_id == ""
    assert pos.dry_tp_seen_at is None
    assert pos.dry_tp_market_exit_deadline is None


def test_dry_tp_fallback_disabled_clears_after_single_skip(tmp_path):
    executor = _Exec()
    executor.positions["up-token"] = 10.0
    trader = _trader(tmp_path, executor, dry_tp_market_exit_enabled=False)
    pos = _position()
    _arm_dry_tp_fallback(pos)
    trader.state.open_positions.append(pos)

    trader._check_dry_tp_fallbacks(make_tick(market_id=pos.market_id))

    assert executor.cancelled == []
    assert executor.sold == []
    assert pos.pending_dry_tp_signal_id == ""
    assert pos.dry_tp_seen_at is None
    assert pos.dry_tp_market_exit_deadline is None
    assert _risk_event_count(trader, "tp_signal_market_exit_skipped") == 1

    trader._check_dry_tp_fallbacks(make_tick(market_id=pos.market_id))

    assert _risk_event_count(trader, "tp_signal_market_exit_skipped") == 1


def test_dry_tp_fallback_cancel_failure_stays_retryable(tmp_path):
    executor = _Exec()
    executor.positions["up-token"] = 10.0
    executor.cancel_ok = False
    trader = _trader(tmp_path, executor)
    pos = _position()
    pos.tp_order_id = "tp-1"
    _arm_dry_tp_fallback(pos)
    trader.state.open_positions.append(pos)

    trader._check_dry_tp_fallbacks(make_tick(market_id=pos.market_id))

    assert executor.cancelled == ["tp-1"]
    assert executor.sold == []
    assert pos.pending_dry_tp_signal_id == "sig-tp"
    assert pos.dry_tp_market_exit_deadline is not None

    executor.cancel_ok = True
    trader._check_dry_tp_fallbacks(make_tick(market_id=pos.market_id))

    assert executor.cancelled == ["tp-1", "tp-1"]
    assert executor.sold == [("up-token", 10.0)]
    assert trader.state.open_positions == []
    assert trader.state.closed_positions[0].result == TradeResult.TP_SIGNAL_MARKET_EXIT


def test_dry_tp_fallback_balance_failure_stays_retryable(tmp_path):
    executor = _Exec()
    executor.positions["up-token"] = 10.0
    executor.position_error = RuntimeError("balance unavailable")
    trader = _trader(tmp_path, executor)
    pos = _position()
    _arm_dry_tp_fallback(pos)
    trader.state.open_positions.append(pos)

    trader._check_dry_tp_fallbacks(make_tick(market_id=pos.market_id))

    assert executor.sold == []
    assert pos.pending_dry_tp_signal_id == "sig-tp"
    assert pos.dry_tp_market_exit_deadline is not None
    assert trader.state.open_positions == [pos]


def test_dry_tp_fallback_sell_failure_stays_retryable(tmp_path):
    executor = _Exec()
    executor.positions["up-token"] = 10.0
    executor.sell_success = False
    trader = _trader(tmp_path, executor)
    pos = _position()
    _arm_dry_tp_fallback(pos)
    trader.state.open_positions.append(pos)

    trader._check_dry_tp_fallbacks(make_tick(market_id=pos.market_id))

    assert executor.sold == [("up-token", 10.0)]
    assert pos.pending_dry_tp_signal_id == "sig-tp"
    assert pos.dry_tp_market_exit_deadline is not None
    assert trader.state.open_positions == [pos]


def test_dry_tp_fallback_proceeds_unverified_clears_for_reconciliation(tmp_path):
    executor = _Exec()
    executor.positions["up-token"] = 10.0
    executor.sell_avg_fill_price = 0.0
    trader = _trader(tmp_path, executor)
    pos = _position()
    _arm_dry_tp_fallback(pos)
    trader.state.open_positions.append(pos)

    trader._check_dry_tp_fallbacks(make_tick(market_id=pos.market_id))

    assert executor.sold == [("up-token", 10.0)]
    assert pos.pending_settlement is True
    assert pos.pending_settlement_reason == "tp_signal_market_exit_proceeds_unverified"
    assert pos.pending_dry_tp_signal_id == ""
    assert pos.dry_tp_seen_at is None
    assert pos.dry_tp_market_exit_deadline is None
    assert trader.state.open_positions == [pos]


def test_restart_preserves_processed_signals_and_pending_tp_deadline(tmp_path):
    cfg = _config(tmp_path)
    state = StateManager(cfg)
    state.configure_mirror_control(enabled=True, instance_id=773, config_hash="hash")
    state.mark_mirror_signal_processed("sig-1")
    pos = _position()
    pos.pending_dry_tp_signal_id = "sig-tp"
    pos.dry_tp_seen_at = datetime.now(timezone.utc)
    pos.dry_tp_market_exit_deadline = pos.dry_tp_seen_at + timedelta(seconds=120)
    state.open_positions.append(pos)
    state.save()

    restored = StateManager(cfg)
    assert restored.load() is True
    assert restored.has_processed_mirror_signal("sig-1")
    assert restored.open_positions[0].pending_dry_tp_signal_id == "sig-tp"
    assert restored.open_positions[0].dry_tp_market_exit_deadline == pos.dry_tp_market_exit_deadline


def test_sqlite_mirror_signal_status_survives_legacy_processed_cache_eviction(tmp_path):
    cfg = _config(
        tmp_path,
        persistence_backend="sqlite",
        live_sqlite_db_file=tmp_path / "live.sqlite",
    )
    state = StateManager(cfg)
    state.mark_mirror_signal_processed(
        "sig-durable",
        status="accepted",
        reason="entered",
        event="ENTRY",
        market_id="market-1",
    )
    state.processed_mirror_signal_ids = []

    restored = StateManager(cfg)

    assert restored.has_processed_mirror_signal("sig-durable")
    row = restored.get_mirror_signal_status("sig-durable")
    assert row is not None
    assert row["status"] == "accepted"


def test_mirror_signal_raw_reader_decodes_without_ack():
    signal = MirrorTradeSignal(
        signal_id="sig-raw",
        source_instance_id=773,
        source_config_hash="hash",
        market_id="market",
        event="ENTRY",
        direction="UP",
    )

    class FakeRedis:
        def __init__(self):
            self.xread_args = None
            self.xack_called = False

        def xread(self, streams, **kwargs):
            self.xread_args = (streams, kwargs)
            return [
                (
                    b"trade.mirror.signals",
                    [(b"1-0", {b"payload": json.dumps(signal.to_dict()).encode("utf-8")})],
                )
            ]

        def xack(self, *args):
            self.xack_called = True

    fake = FakeRedis()
    subscriber = RedisMirrorSignalSubscriber.__new__(RedisMirrorSignalSubscriber)
    subscriber.stream = "trade.mirror.signals"
    subscriber.count = 50
    subscriber._client = fake

    messages = subscriber.read_after("0-0", block_ms=250, count=1)

    assert messages == [("1-0", signal)]
    assert fake.xread_args == ({"trade.mirror.signals": "0-0"}, {"count": 1, "block": 250})
    assert fake.xack_called is False


def test_mirror_signal_consumer_group_reader_defers_stream_ack():
    signal = MirrorTradeSignal(
        signal_id="sig-group",
        source_instance_id=773,
        source_config_hash="hash",
        market_id="market",
        event="ENTRY",
        direction="UP",
    )

    class FakeRedis:
        def __init__(self):
            self.xreadgroup_args = None
            self.xack_called = False

        def xreadgroup(self, group, consumer, streams, **kwargs):
            self.xreadgroup_args = (group, consumer, streams, kwargs)
            return [
                (
                    b"trade.mirror.signals",
                    [(b"7-0", {b"payload": json.dumps(signal.to_dict()).encode("utf-8")})],
                )
            ]

        def xack(self, *args):
            self.xack_called = True

    fake = FakeRedis()
    subscriber = RedisMirrorSignalSubscriber.__new__(RedisMirrorSignalSubscriber)
    subscriber.stream = "trade.mirror.signals"
    subscriber.group = "live.mirror"
    subscriber.consumer = "live"
    subscriber.count = 50
    subscriber.block_ms = None
    subscriber._client = fake

    messages = subscriber.read_new(block_ms=50, count=1)

    assert messages == [("7-0", signal)]
    assert fake.xreadgroup_args == (
        "live.mirror",
        "live",
        {"trade.mirror.signals": ">"},
        {"count": 1, "block": 50},
    )
    assert fake.xack_called is False


def test_mirror_delivery_acks_stream_only_after_terminal_status(tmp_path):
    trader = _trader(tmp_path)
    tick = make_tick(market_id="btc-updown-15m-1778458500")
    signal = _signal(trader, "ENTRY")

    def fake_execute(opportunity):
        pos = _position(market_id=tick.market_id, token_id=tick.up_token_id)
        trader.state.open_positions.append(pos)
        return SimpleNamespace(entered=True, position=pos)

    trader.execute_entry = fake_execute
    trader._process_mirror_delivery("99-0", signal, tick, source="sync_poll")

    assert trader._mirror_subscriber.delivery_acks == ["99-0"]
    assert trader._mirror_subscriber.acks[-1] == (signal.signal_id, "accepted", "entered")


def test_retryable_mirror_delivery_keeps_stream_unacked(tmp_path):
    trader = _trader(tmp_path, mirror_signal_entry_retry_delay_seconds=0.0)
    tick = make_tick(market_id="btc-updown-15m-1778458500")
    signal = _signal(trader, "ENTRY")
    trader.execute_entry = lambda opportunity: EntryAttemptResult(
        attempted=True,
        rejection_reason="order_failed",
        rejection_details={"error": "status_code=503 post_only_mode"},
    )

    trader._process_mirror_delivery("100-0", signal, tick, source="sync_poll")

    assert trader._mirror_subscriber.delivery_acks == []
    assert trader._mirror_subscriber.acks[-1] == (
        signal.signal_id,
        "retryable",
        "entry_retryable:order_failed",
    )


def test_sync_mirror_poll_uses_consumer_group_reader_not_recent(tmp_path):
    trader = _trader(tmp_path)
    tick = make_tick(market_id="btc-updown-15m-1778458500")
    signal = _signal(trader, "ENTRY", signal_id="sig-consumer-group")

    class FakeSubscriber(_AckSink):
        def __init__(self):
            super().__init__()
            self.read_new_called = False

        def read_new(self, *, block_ms=None, count=None):
            self.read_new_called = True
            return [("200-0", signal)]

        def claim_pending(self, **kwargs):
            return []

        def recent(self, *, count=100):
            raise AssertionError("raw recent() must not execute mirror signals")

    subscriber = FakeSubscriber()
    trader._mirror_subscriber = subscriber
    trader._mirror_listener_thread = None

    def fake_execute(opportunity):
        pos = _position(market_id=tick.market_id, token_id=tick.up_token_id)
        trader.state.open_positions.append(pos)
        return SimpleNamespace(entered=True, position=pos)

    trader.execute_entry = fake_execute
    trader._process_mirror_signals(tick)

    assert subscriber.read_new_called is True
    assert subscriber.delivery_acks == ["200-0"]
    assert trader.state.has_processed_mirror_signal(signal.signal_id)


def test_pending_mirror_signal_recovery_claims_and_processes(tmp_path):
    trader = _trader(tmp_path)
    tick = make_tick(market_id="btc-updown-15m-1778458500")
    signal = _signal(trader, "ENTRY", signal_id="sig-claimed")

    class FakeSubscriber(_AckSink):
        def __init__(self):
            super().__init__()
            self.claimed = False

        def claim_pending(self, **kwargs):
            if self.claimed:
                return []
            self.claimed = True
            return [("201-0", signal)]

        def read_new(self, **kwargs):
            return []

    subscriber = FakeSubscriber()
    trader._mirror_subscriber = subscriber

    def fake_execute(opportunity):
        pos = _position(market_id=tick.market_id, token_id=tick.up_token_id)
        trader.state.open_positions.append(pos)
        return SimpleNamespace(entered=True, position=pos)

    trader.execute_entry = fake_execute
    trader._process_mirror_signals(tick)

    assert subscriber.claimed is True
    assert subscriber.delivery_acks == ["201-0"]
    assert trader.state.has_processed_mirror_signal(signal.signal_id)


def test_mirror_publisher_heartbeat_stale_is_audited(tmp_path):
    trader = _trader(
        tmp_path,
        mirror_heartbeat_stale_seconds=60.0,
        mirror_heartbeat_check_interval_seconds=0.0,
    )
    old_ts = datetime.now(timezone.utc) - timedelta(seconds=90)
    trader._mirror_subscriber.json_keys["trade.mirror.heartbeat"] = {
        "timestamp": old_ts.isoformat(),
        "market_id": "mkt",
        "source_instance_id": 773,
    }

    trader._check_mirror_publisher_heartbeat()

    assert _risk_event_count(trader, "mirror_publisher_heartbeat_stale") == 1


def test_mirror_signal_raw_reader_skips_bad_payload():
    class FakeRedis:
        def xread(self, streams, **kwargs):
            return [(b"trade.mirror.signals", [(b"1-0", {b"payload": b"not-json"})])]

    subscriber = RedisMirrorSignalSubscriber.__new__(RedisMirrorSignalSubscriber)
    subscriber.stream = "trade.mirror.signals"
    subscriber.count = 50
    subscriber._client = FakeRedis()

    assert subscriber.read_after("0-0") == [("1-0", None)]
