from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import scripts.trader.hybrid_calibration as hybrid_calibration_module
from btc_pricer.edge import required_model_prob
from scripts.trader.config import GridConfig, InstanceParams
from scripts.trader.data_feed import SharedDataFeed, TickData
from scripts.trader.grid_execution import dispatch_grid_tick
from scripts.trader.hybrid_calibration import CalibrationEvent, HybridCalibrationBroker
from scripts.trader.trader import PaperInstance
from tests.conftest import make_order_book


def _patch_feed_externals(monkeypatch, *, spot: float) -> None:
    monkeypatch.setattr("scripts.trader.data_feed.find_active_15m_market", lambda: None)
    monkeypatch.setattr("scripts.trader.data_feed.get_current_btc_price", lambda: spot)
    monkeypatch.setattr("scripts.trader.data_feed.get_coinbase_exchange_ticker_price", lambda: None)
    monkeypatch.setattr("scripts.trader.data_feed.get_coinbase_spot_price", lambda: None)
    monkeypatch.setattr("scripts.trader.data_feed.get_binance_price_at", lambda _timestamp: spot)
    monkeypatch.setattr("scripts.trader.trader.is_polymarket_operational", lambda: True)


def _freeze_feed_time(monkeypatch, now: datetime) -> None:
    monkeypatch.setattr(
        "scripts.trader.data_feed.datetime",
        type("FakeDateTime", (), {"now": staticmethod(lambda tz=None: now)}),
    )


def _entry_signal(instance_id: int, *, price: float, model_prob: float) -> dict:
    required = required_model_prob(price, alpha=1.0, floor=0.45)
    return {
        "instance_id": instance_id,
        "direction": "UP",
        "edge_ratio": round(model_prob / price, 4),
        "executable_price": price,
        "required_prob": round(required, 4),
        "model_prob": model_prob,
    }


def _calibration_event(
    *,
    market_id: str,
    barrier: float,
    market_end: datetime,
    ready_at: datetime,
    entry_signals: tuple,
) -> CalibrationEvent:
    return CalibrationEvent(
        schema_version=3,
        event_id=f"{int(ready_at.timestamp() * 1000)}-entry-pipeline",
        source="shared_calc",
        market_id=market_id,
        barrier=barrier,
        market_end_time_iso=market_end.isoformat(),
        ready_at_iso=ready_at.isoformat(),
        ready_at_unix_ms=int(ready_at.timestamp() * 1000),
        prob_above=0.90,
        prob_below=0.10,
        model_used="ssvi_surface",
        calibration_seconds=1.25,
        terminal_payload={
            "timestamp": ready_at.isoformat(),
            "timing": {"surface_fit_s": 0.75, "mc_s": 0.50},
            "avg_prob_above": 0.90,
            "avg_prob_below": 0.10,
        },
        barrier_source="chainlink",
        barrier_offset_s=0.0,
        spot_price=barrier + 100.0,
        spot_source="calc",
        up_best_ask=0.50,
        down_best_ask=0.50,
        entry_signals=entry_signals,
    )


def test_grid_entry_pipeline_survives_calibration_snapshot_write_failure(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """A calibration mirror-file failure must never block position entry.

    This is the guardrail for liveness/dashboard/flicker fixes touching
    terminal_data*.json: the grid must keep the accepted calc event in memory,
    emit a tick with fresh entry signals, and enter when the market conditions
    are otherwise valid.
    """
    now = datetime.now(timezone.utc)
    market_id = "btc-updown-15m-entry-pipeline"
    barrier = 80_000.0
    market_start = now - timedelta(seconds=360)
    market_end = market_start + timedelta(minutes=15)

    config = GridConfig(
        starting_capital=1_000.0,
        order_size_pct=0.10,
        no_trade_first_seconds=300,
        no_trade_last_seconds=180,
        paper_recheck_delay_seconds=0.0,
        paper_delayed_book_slippage=0.0,
    )
    _patch_feed_externals(monkeypatch, spot=barrier + 100.0)
    _freeze_feed_time(monkeypatch, now)

    results_dir = tmp_path / "results"
    grid_inbox = results_dir / "calibration_inbox_grid"
    grid_inbox.mkdir(parents=True)
    broker = HybridCalibrationBroker(
        results_dir=results_dir,
        enable_local_worker=False,
        inbox_dir=grid_inbox,
    )

    signal = _entry_signal(0, price=0.50, model_prob=0.90)
    event = _calibration_event(
        market_id=market_id,
        barrier=barrier,
        market_end=market_end,
        ready_at=now,
        entry_signals=(signal,),
    )
    assert broker.publish(event) is True

    original_write = hybrid_calibration_module._atomic_write_json

    def flaky_snapshot_write(path: Path, payload: dict) -> None:
        if path.name == "terminal_data.grid.json":
            raise PermissionError("simulated locked terminal_data.grid.json")
        original_write(path, payload)

    monkeypatch.setattr(
        hybrid_calibration_module,
        "_atomic_write_json",
        flaky_snapshot_write,
    )

    feed = SharedDataFeed(config)
    feed.results_dir = results_dir
    feed._hybrid_broker = broker
    feed._current_market = {
        "market_id": market_id,
        "title": "Bitcoin Up or Down - Test",
        "end_date_utc": market_end,
        "window_start_utc": market_start,
        "prob_up": 0.50,
        "prob_down": 0.50,
        "token_ids": {"UP": "up-token", "DOWN": "down-token"},
    }
    feed._current_market_id = market_id
    feed._barrier = barrier
    feed._barrier_source = "chainlink"
    feed._barrier_captured = True
    feed._barrier_finalized = True
    up_book = make_order_book(best_bid=0.49, best_ask=0.50, bid_size=500, ask_size=500)
    down_book = make_order_book(best_bid=0.49, best_ask=0.50, bid_size=500, ask_size=500)
    feed._fetch_order_books = lambda: (
        up_book,
        down_book,
        up_book,
        down_book,
        0.50,
        0.50,
    )

    tick = feed.tick()

    assert tick is not None
    assert tick.barrier_confirmed is True
    assert tick.entry_signals == (signal,)

    instance = PaperInstance(
        id=0,
        params=InstanceParams(
            alpha_up=1.0,
            alpha_down=1.0,
            floor_up=0.45,
            floor_down=0.45,
            tp_percentage=0.15,
            stop_loss_pct=0.20,
        ),
        shared_config=config,
    )
    diagnostics = []

    events = dispatch_grid_tick([instance], tick, config, diagnostics=diagnostics)

    assert [event.event for event in events] == ["ENTRY"]
    assert instance.position is not None
    assert instance.position.market_id == market_id
    assert diagnostics and diagnostics[0].entered is True


def test_grid_entry_pipeline_blocks_entry_when_polymarket_status_is_down(
    monkeypatch,
) -> None:
    """A valid entry setup must still reject when the venue status is unknown/down."""
    now = datetime.now(timezone.utc)
    market_id = "btc-updown-15m-status-gated"
    barrier = 80_000.0
    config = GridConfig(
        starting_capital=1_000.0,
        order_size_pct=0.10,
        no_trade_first_seconds=300,
        no_trade_last_seconds=180,
        paper_recheck_delay_seconds=0.0,
        paper_delayed_book_slippage=0.0,
    )
    signal = _entry_signal(0, price=0.50, model_prob=0.90)
    up_book = make_order_book(best_bid=0.49, best_ask=0.50, bid_size=500, ask_size=500)
    down_book = make_order_book(best_bid=0.49, best_ask=0.50, bid_size=500, ask_size=500)
    tick = TickData(
        timestamp=now,
        market_id=market_id,
        market_title="Bitcoin Up or Down - Test",
        barrier=barrier,
        spot_price=barrier + 100.0,
        time_remaining_seconds=540.0,
        elapsed_seconds=360.0,
        window_duration_seconds=900.0,
        prob_above=0.90,
        prob_below=0.10,
        poly_prob_up=0.50,
        poly_prob_down=0.50,
        up_order_book=up_book,
        down_order_book=down_book,
        canonical_up_ask=0.50,
        canonical_down_ask=0.50,
        up_order_book_delayed=up_book,
        down_order_book_delayed=down_book,
        up_token_id="up-token",
        down_token_id="down-token",
        model_used="ssvi_surface",
        calibration_seconds=1.25,
        market_end_time=now + timedelta(seconds=540),
        barrier_source="chainlink",
        barrier_confirmed=True,
        entry_signals=(signal,),
        entry_signal_age_s=0.0,
        entry_signal_market_id=market_id,
    )
    instance = PaperInstance(
        id=0,
        params=InstanceParams(
            alpha_up=1.0,
            alpha_down=1.0,
            floor_up=0.45,
            floor_down=0.45,
            tp_percentage=0.15,
            stop_loss_pct=0.20,
        ),
        shared_config=config,
    )
    monkeypatch.setattr("scripts.trader.trader.is_polymarket_operational", lambda: False)
    diagnostics = []

    events = dispatch_grid_tick([instance], tick, config, diagnostics=diagnostics)

    assert events == []
    assert instance.position is None
    assert diagnostics
    assert diagnostics[0].entered is False
    assert diagnostics[0].rejection_reason == "polymarket_down"
