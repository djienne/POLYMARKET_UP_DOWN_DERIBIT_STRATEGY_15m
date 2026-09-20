import asyncio
from datetime import datetime, timezone

import pytest

from scripts.trader.config import DEFAULT_DRY_RUN_FRICTION_PCT, GridConfig, InstanceParams
from scripts.trader.data_feed import TickData
from scripts.trader.order_book import OrderBookSnapshot, clone_order_book
from scripts.trader.trader import PaperInstance


@pytest.fixture(autouse=True)
def _offline_clob_reads_probe(monkeypatch):
    """Keep the entry gates hermetic: no network probes and no reads of the
    PRODUCTION health flag from tests (try_entry consults all of these, so
    a live Polymarket maintenance window or taker outage would otherwise fail
    every entry test). Gate tests override these explicitly, and any test that
    points ``clob_health_shared_path`` at a tmp flag still exercises the real
    ``taker_outage_active``."""
    for target in (
        "scripts.trader.trader.clob_reads_healthy",
        "scripts.trader.single_trader.clob_reads_healthy",
    ):
        monkeypatch.setattr(target, lambda *a, **k: (True, None), raising=False)
    for target in (
        "scripts.trader.trader.is_polymarket_operational",
        "scripts.trader.single_trader.is_polymarket_operational",
    ):
        monkeypatch.setattr(target, lambda *a, **k: True, raising=False)

    # The shared taker-outage flag defaults to the repo-relative production
    # path; only neutralize reads of THAT path so tmp-path flag tests behave
    # exactly as in production.
    from scripts.trader import clob_health as _clob_health

    _real_taker_outage_active = _clob_health.taker_outage_active
    _production_default = "results/15m_clob_health.json"

    def _hermetic_taker_outage_active(path=None, **kwargs):
        if path is None or str(path) == _production_default:
            return (False, None, None)
        return _real_taker_outage_active(path, **kwargs)

    monkeypatch.setattr(
        "scripts.trader.clob_health.taker_outage_active",
        _hermetic_taker_outage_active,
        raising=False,
    )
    monkeypatch.setattr(
        "scripts.trader.trader.taker_outage_active",
        _hermetic_taker_outage_active,
        raising=False,
    )


def _close_idle_event_loops() -> None:
    """Clean only the calling test thread's current loop.

    A background websocket loop is briefly idle during reconnect backoff;
    closing arbitrary GC-discovered loops raced its next session coroutine.
    Its owning feed must cancel its task and join its thread instead.
    """
    local = getattr(asyncio.get_event_loop_policy(), "_local", None)
    loop = getattr(local, "_loop", None)
    if loop is not None and not loop.is_closed() and not loop.is_running():
        loop.close()
    try:
        asyncio.set_event_loop(None)
    except RuntimeError:
        pass


@pytest.hookimpl(hookwrapper=True, trylast=True)
def pytest_runtest_protocol(item, nextitem):
    yield
    _close_idle_event_loops()


def pytest_sessionfinish(session, exitstatus):
    _close_idle_event_loops()


def make_order_book(
    mid_price=0.50,
    spread=0.02,
    depth=1000.0,
    *,
    best_bid=None,
    best_ask=None,
    bid_size=None,
    ask_size=None,
    levels: int = 3,
):
    """Create a mock order book with realistic depth for testing."""
    ask = round(mid_price + spread / 2, 4) if best_ask is None else best_ask
    bid = round(mid_price - spread / 2, 4) if best_bid is None else best_bid
    bid_depth = depth if bid_size is None else bid_size
    ask_depth = depth if ask_size is None else ask_size
    bid_levels = [(bid - 0.01 * i, bid_depth) for i in range(levels)]
    ask_levels = [(ask + 0.01 * i, ask_depth) for i in range(levels)]
    return OrderBookSnapshot(
        token_id="test_token",
        best_bid=bid,
        best_ask=ask,
        bids=bid_levels,
        asks=ask_levels,
        timestamp="",
    )


@pytest.fixture
def default_config():
    """Default GridConfig for testing."""
    return GridConfig(
        starting_capital=1000.0,
        order_size_pct=0.10,
        friction_pct=DEFAULT_DRY_RUN_FRICTION_PCT,
        max_entry_price=0.95,
        no_trade_first_seconds=300,
        no_trade_last_seconds=180,
    )


@pytest.fixture
def default_params():
    """Default InstanceParams for testing."""
    return InstanceParams(
        alpha_up=1.5,
        alpha_down=1.5,
        floor_up=0.55,
        floor_down=0.55,
        tp_percentage=0.15,
        stop_loss_pct=0.0,
    )


@pytest.fixture
def instance(default_config, default_params):
    """A single PaperInstance for testing."""
    return PaperInstance(id=0, params=default_params, shared_config=default_config)


def make_tick(
    market_id="btc-updown-15m-1776004200",
    barrier=83000.0,
    spot_price=83100.0,
    time_remaining_seconds=600.0,
    elapsed_seconds=300.0,
    prob_above=0.55,
    prob_below=None,
    poly_prob_up=0.50,
    poly_prob_down=None,
    model_used="ssvi_surface",
    calibration_seconds=30.0,
    barrier_source="chainlink",
    barrier_confirmed=True,
    is_new_market=False,
    is_expired=False,
    settlement_pending=False,
    final_spot=None,
    final_spot_source="",
) -> TickData:
    """Create a TickData for testing."""
    if prob_below is None:
        prob_below = 1.0 - prob_above
    if poly_prob_down is None:
        poly_prob_down = 1.0 - poly_prob_up
    up_book = make_order_book(poly_prob_up)
    down_book = make_order_book(poly_prob_down)
    return TickData(
        timestamp=datetime.now(timezone.utc),
        market_id=market_id,
        market_title="Bitcoin Up or Down - Test",
        barrier=barrier,
        spot_price=spot_price,
        time_remaining_seconds=time_remaining_seconds,
        elapsed_seconds=elapsed_seconds,
        window_duration_seconds=900.0,
        prob_above=prob_above,
        prob_below=prob_below,
        poly_prob_up=poly_prob_up,
        poly_prob_down=poly_prob_down,
        up_order_book=up_book,
        down_order_book=down_book,
        up_order_book_delayed=clone_order_book(up_book),
        down_order_book_delayed=clone_order_book(down_book),
        up_token_id="test_up_token",
        down_token_id="test_down_token",
        model_used=model_used,
        calibration_seconds=calibration_seconds,
        market_end_time=datetime.now(timezone.utc),
        barrier_source=barrier_source,
        barrier_confirmed=barrier_confirmed,
        is_new_market=is_new_market,
        is_expired=is_expired,
        settlement_pending=settlement_pending,
        final_spot=final_spot,
        final_spot_source=final_spot_source,
    )


def make_test_single_trader(
    tmp_path,
    *,
    executor,
    order_book_client,
    **config_overrides,
):
    """Construct ``SingleTrader`` through ``__init__`` with a stub executor."""
    from scripts.trader.config import TraderConfig
    from scripts.trader.single_trader import SingleTrader

    config_kwargs = {
        "paper_mode": False,
        "live_trading_enabled": True,
        "require_confirmation": False,
        "risk_events_file": tmp_path / "15m_risk_events.csv",
        "risk_events_current_file": tmp_path / "15m_risk_events.current.csv",
        "live_tick_eval_file": tmp_path / "15m_live_tick_eval.current.csv",
        "friction_realized_file": tmp_path / "15m_friction_realized.csv",
        "state_file": tmp_path / "paper_state.json",
        "live_state_file": tmp_path / "live_state.json",
        "trades_file": tmp_path / "paper_trades.csv",
        "live_trades_file": tmp_path / "live_trades.csv",
        "equity_file": tmp_path / "paper_equity.csv",
        "live_equity_file": tmp_path / "live_equity.csv",
        "orderbook_file": tmp_path / "orderbook.csv",
    }
    config_kwargs.update(config_overrides)
    config = TraderConfig(**config_kwargs)

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(SingleTrader, "_create_executor", lambda self: executor)
        trader = SingleTrader(config, verbose=False)

    trader.order_book_client = order_book_client
    trader.risk_engine = trader._create_risk_engine()
    trader.settlement_service = trader._create_settlement_service()
    trader.position_tracker = trader._create_position_tracker()
    trader.order_execution_service = trader._create_order_execution_service()
    trader.reconciliation_service = trader._create_reconciliation_service()
    trader.log = lambda message: None
    return trader
