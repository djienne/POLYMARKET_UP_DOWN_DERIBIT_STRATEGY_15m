"""Current-contract regression through producer payloads, feed and settlement."""
import json
import logging
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from scripts.trader.contracts import TWAP_PAYOFF, MarketResolution, entry_inputs_valid, parse_market_contract
from scripts.trader.data_feed import SharedDataFeed
from scripts.trader.config import GridConfig
from scripts.trader.hybrid_calibration import CalibrationEvent, HybridCalibrationBroker, MarketContext
from scripts.trader.market_orchestrator import MarketOrchestrator
from tests.conftest import make_order_book
from tests.test_twap_contracts import gamma_market


def modern_feed(tmp_path, monkeypatch):
    now = datetime.now(timezone.utc)
    contract = parse_market_contract(gamma_market())
    cfg = GridConfig(paper_recheck_delay_seconds=0, paper_delayed_book_slippage=0)
    feed = SharedDataFeed(cfg)
    feed.results_dir = tmp_path
    feed._current_market_id = "twap-market"
    feed._current_market = {"market_id": "twap-market", "title": "TWAP test",
        "window_start_utc": now-timedelta(seconds=400), "end_date_utc": now+timedelta(seconds=500),
        "contract_metadata": contract.to_dict(), "token_ids": contract.outcome_token_ids}
    monkeypatch.setattr(feed, "_scan_market", lambda: None)
    monkeypatch.setattr(feed, "_resolve_current_spot", lambda: (80_100.0, "test"))
    monkeypatch.setattr(feed, "_binance_sanity_passes", lambda *args: True)
    book = make_order_book(best_bid=0.49, best_ask=0.50, levels=1)
    monkeypatch.setattr(feed, "_fetch_order_books", lambda: (book, book, book, book, 0.5, 0.5))
    broker = HybridCalibrationBroker(results_dir=tmp_path, enable_local_worker=False, enable_remote_inbox=False,
                                     push_consumer_enabled=False)
    feed._hybrid_broker = broker
    feed._local_signal_params = [{"instance_id": 0, "alpha_up": 1.0, "alpha_down": 1.0,
        "floor_up": 0.45, "floor_down": 0.45, "max_entry_price": 0.95}]
    context = MarketContext("twap-market", 80_000, (now+timedelta(seconds=500)).isoformat(),
                            barrier_source="polymarket", contract_metadata=contract.to_dict())
    event = CalibrationEvent.from_parts(source="calc", context=context, ready_at=now,
        model_data={"prob_above": 0.9, "prob_below": 0.1, "spot_price": 80_100,
            "payoff_kind": TWAP_PAYOFF, "averaging_window_seconds": 60,
            "probability_evaluated_at": now.isoformat(), "probability_spot_observed_at": now.isoformat()}, terminal_payload={})
    assert broker.publish(event)
    return feed, broker, event, contract


def test_feed_uses_fresh_average_scalar_and_suppresses_stale_or_legacy_event(tmp_path, monkeypatch):
    feed, broker, event, _contract = modern_feed(tmp_path, monkeypatch)
    tick = feed.tick()
    assert tick.prob_source == "twap_mc_scalar"
    assert tick.prob_above == 0.9 and len(tick.entry_signals) == 1
    assert entry_inputs_valid(tick, now=tick.timestamp) == (True, "")
    stale = replace(event, event_id=event.event_id+"z",
                    probability_evaluated_at_iso=(datetime.now(timezone.utc)-timedelta(seconds=3)).isoformat())
    assert broker.publish(stale)
    assert feed.tick().entry_signals == ()
    legacy = replace(stale, event_id=stale.event_id+"z", schema_version=4, payoff_kind="legacy",
                     entry_signals=event.entry_signals)
    assert broker.publish(legacy)
    assert feed.tick().entry_signals == ()


def test_expiry_emits_actual_token_payout_without_final_spot(tmp_path, monkeypatch):
    feed, _broker, _event, contract = modern_feed(tmp_path, monkeypatch)
    now = datetime.now(timezone.utc)
    feed._current_market["end_date_utc"] = now-timedelta(seconds=1)
    store = MarketOrchestrator(settlements_dir=tmp_path/"settlements")
    store.write_strike(market_id="twap-market", window_start=now-timedelta(seconds=901), window_end=now-timedelta(seconds=1),
                       strike=80_000, strike_offset_s=0, strike_source="polymarket", contract_metadata=contract.to_dict())
    store.finalize_payout("twap-market", MarketResolution(contract.condition_id,
        {"up-token": 0.0, "down-token": 1.0}, "polymarket_resolved_outcome", now.isoformat()))
    expired = feed.tick()
    assert expired.is_expired and not expired.settlement_pending
    assert expired.settlement_payouts == {"up-token": 0.0, "down-token": 1.0}
    assert expired.final_spot is None


def test_pending_old_payout_does_not_pin_active_market_discovery(tmp_path, monkeypatch):
    import scripts.trader.data_feed as module
    feed, broker, event, contract = modern_feed(tmp_path, monkeypatch)
    now = datetime.now(timezone.utc)
    feed._current_market["end_date_utc"] = now-timedelta(seconds=1)
    pending = feed.tick()
    assert pending.settlement_pending and feed._expiry_reported
    assert feed.next_sleep() == 2.0
    next_contract = replace(contract, condition_id="next-condition", outcome_token_ids={"UP": "new-up", "DOWN": "new-down"})
    next_data = {"market_id": "next-market", "title": "next window", "window_start_utc": now,
                 "end_date_utc": now+timedelta(minutes=15), "contract_metadata": next_contract.to_dict(),
                 "token_ids": next_contract.outcome_token_ids}
    monkeypatch.setattr(feed, "_scan_market", SharedDataFeed._scan_market.__get__(feed))
    monkeypatch.setattr(module, "find_active_15m_market", lambda: {"next": True})
    monkeypatch.setattr(module, "extract_market_data", lambda raw: next_data)
    next_event = replace(event, event_id=event.event_id+"z", market_id="next-market",
                         market_end_time_iso=next_data["end_date_utc"].isoformat(), contract_metadata=next_contract.to_dict())
    assert broker.publish(next_event)
    tick = feed.tick()
    assert tick.market_id == "next-market"
    assert tick.is_new_market and not tick.settlement_pending
    assert tick.time_remaining_seconds > 0


def test_discovery_wait_returns_no_repeated_expired_work_and_is_paced(tmp_path, monkeypatch):
    feed, _broker, _event, _contract = modern_feed(tmp_path, monkeypatch)
    feed._current_market["end_date_utc"] = datetime.now(timezone.utc)-timedelta(seconds=1)
    assert feed.tick().settlement_pending
    assert feed.tick() is None
    assert feed.next_sleep() == 2.0


@pytest.mark.parametrize("slow_stage", ["book", "spot"])
def test_delayed_io_crossing_expiry_primes_immediate_discovery(tmp_path, monkeypatch, slow_stage):
    import scripts.trader.data_feed as module
    feed, _broker, _event, _contract = modern_feed(tmp_path, monkeypatch)
    clock = {"now": datetime.now(timezone.utc)}
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return clock["now"]
    feed._current_market["end_date_utc"] = clock["now"]+timedelta(seconds=1)
    feed._last_market_scan = clock["now"].timestamp()
    method = "_fetch_order_books" if slow_stage == "book" else "_resolve_current_spot"
    original = getattr(feed, method)
    def delayed():
        clock["now"] += timedelta(seconds=2)
        return original()
    monkeypatch.setattr(module, "datetime", Clock)
    monkeypatch.setattr(feed, method, delayed)
    tick = feed.tick()
    assert tick.time_remaining_seconds == 0
    assert tick.settlement_pending and feed._expiry_reported
    assert feed._last_market_scan == 0
    assert feed.next_sleep() == 2.0


def test_latest_v5_is_selected_after_book_wait(tmp_path, monkeypatch):
    feed, broker, event, _contract = modern_feed(tmp_path, monkeypatch)
    original_books = feed._fetch_order_books
    published = {}
    def poll_arrived():
        if published.get("arrived") is not None and not published.get("consumed"):
            assert broker.publish(published["arrived"])
            published["consumed"] = True
    monkeypatch.setattr(broker, "poll_inbox", poll_arrived)
    def books_with_new_calibration():
        now = datetime.now(timezone.utc)
        updated = replace(event, event_id=event.event_id+"z", ready_at_iso=now.isoformat(),
            ready_at_unix_ms=int(now.timestamp()*1000), prob_above=0.8, prob_below=0.2,
            probability_evaluated_at_iso=now.isoformat(), probability_spot_observed_at_iso=now.isoformat())
        published["arrived"] = updated
        published["event"] = updated
        return original_books()
    monkeypatch.setattr(feed, "_fetch_order_books", books_with_new_calibration)
    tick = feed.tick()
    assert tick.prob_above == 0.8
    assert tick.probability_evaluated_at.isoformat() == published["event"].probability_evaluated_at_iso


def test_terminal_pipeline_routes_twap_and_refreshes_the_forecast_horizon(tmp_path, monkeypatch):
    import cli_terminal
    from btc_pricer.config import Config
    from btc_pricer.models.ssvi import SSVISurfaceParams
    from btc_pricer.models.terminal_probability import TerminalProbabilityCalculator
    from btc_pricer.constants import DAYS_PER_YEAR
    now = datetime.now(timezone.utc)
    end = now + timedelta(seconds=300)
    surface = SSVISurfaceParams(rho=0.0, eta=0.01, lam=0.0, thetas=[0.04/365.25], ttms=[1/365.25], expiry_names=["fixture"])
    params = surface.get_params_for_ttm(300/(86400*DAYS_PER_YEAR))
    result = SimpleNamespace(params_at_ttm=params, surface_params=surface, r2=0.99, info=None,
        elapsed=0.0, fit_method="fixture", warm_start_used=False, deribit_surface_fingerprint="test", surface_fit_reused=False)
    monkeypatch.setattr(cli_terminal, "prepare_ssvi_surface_inputs", lambda *args: ("test", []))
    monkeypatch.setattr(cli_terminal, "find_closest_expiry_after", lambda *args: "fixture")
    monkeypatch.setattr(cli_terminal, "calibrate_to_expiry", lambda *args, **kw: (None, None, 80_000, "fixture", 80_000, params.ttm))
    monkeypatch.setattr(cli_terminal, "fit_ssvi_surface_for_ttm", lambda *args, **kw: result)
    def wrong_payoff(*args, **kwargs):
        raise AssertionError("terminal-spot MC cannot price the TWAP contract")
    monkeypatch.setattr(TerminalProbabilityCalculator, "terminal_probability_ssvi", wrong_payoff)
    args = SimpleNamespace(targets=None, target=80_005.0, sims=4_000, steps_per_day=1440,
        until=end.isoformat(), skip_heston=True, skip_ssvi_surface=False, spot=80_000.0,
        direction="up", output=tmp_path/"payload.json", payoff_kind=TWAP_PAYOFF, averaging_window_seconds=60)
    config = Config()
    config.model.probability_mode = "cf"  # must not silently reuse a terminal digital
    payload = cli_terminal.run_terminal(args, config, logging.getLogger("twap-test"),
        client=SimpleNamespace(fetch_all_options=lambda currency: {"fixture": [object()]}), spot_provider=lambda: 80_010.0)
    assert payload["payoff_kind"] == TWAP_PAYOFF and payload["probability_mode"] == "mc"
    assert payload["spot_price"] == 80_010.0
    evaluated_at = datetime.fromisoformat(payload["probability_evaluated_at"])
    assert payload["ttm_days"] * 86400 == pytest.approx((end-evaluated_at).total_seconds())
    completed_at = datetime.fromisoformat(payload["timestamp"])
    assert (completed_at-evaluated_at).total_seconds() >= payload["timing"]["mc_s"] - 0.001
    assert "terminal_return_quantiles" not in payload
    assert payload["cf_mc_divergence"] is None
    assert payload["selected_prob_above"] + payload["selected_prob_below"] == pytest.approx(1.0)
    def failed_refresh():
        raise ValueError("fresh_spot_timeout")
    for provider in (lambda: None, failed_refresh):
        with pytest.raises(ValueError, match="TWAP spot refresh"):
            cli_terminal.run_terminal(args, config, logging.getLogger("twap-test"),
                client=SimpleNamespace(fetch_all_options=lambda currency: {"fixture": [object()]}),
                spot_provider=provider)


def test_oracle_average_preserves_precision_and_cannot_overwrite_spot(tmp_path):
    from scripts.trader.chainlink_feed import ChainlinkPriceFeed
    from scripts.trader.spot_daemon import SpotDaemon
    feed = ChainlinkPriceFeed()
    raw = {"topic": "crypto_prices_twap_sixty", "payload": {"symbol": "btc/usd", "timestamp": 1788594300000,
        "value": 80_000.0, "full_accuracy_value": "80000123456789012345678", "window_s": 60}}
    feed._process_message(json.dumps(raw))
    assert feed.get_latest_price() is None
    assert feed.get_latest_twap()["value"] == "80000.123456789012345678"
    published = []
    daemon = SpotDaemon.__new__(SpotDaemon)
    daemon._chainlink = feed
    daemon._last_twap_timestamp = 0
    daemon._publisher = SimpleNamespace(publish_twap=lambda observation: published.append(observation))
    daemon.config = SimpleNamespace(health_path=tmp_path/"spot.health")
    daemon._publish_twap()
    daemon._publish_twap()
    assert len(published) == 1
    records = list((tmp_path/"oracle").glob("*.jsonl"))
    assert len(records) == 1 and json.loads(records[0].read_text())["value"] == "80000.123456789012345678"


def test_single_settlement_cannot_fall_back_to_an_instant_price(tmp_path, monkeypatch):
    from scripts.trader.settlement import SettlementService
    contract = parse_market_contract(gamma_market())
    position = SimpleNamespace(contract_metadata=contract.to_dict(), market_id="market", token_id="down-token", direction="DOWN")
    state = SimpleNamespace(state_file=tmp_path/"state.json", open_positions=[position], save=lambda: None)
    store = MarketOrchestrator(settlements_dir=tmp_path/"settlements")
    now = datetime.now(timezone.utc)
    store.finalize_payout("market", MarketResolution(contract.condition_id, {"up-token": 1.0, "down-token": 0.0},
                                                   "polymarket_resolved_outcome", now.isoformat()))
    service = SettlementService(SimpleNamespace(), SimpleNamespace(), state, market_orchestrator=store,
        chainlink_price_at_fn=lambda *args: pytest.fail("instant price fallback"),
        binance_price_at_fn=lambda *args: pytest.fail("exchange price fallback"))
    assert service.resolve_final_spot(position) == (None, "authoritative_payout_pending")
    calls = []
    monkeypatch.setattr(service, "confirm_live_orders_inactive_for_resolution", lambda position: True)
    monkeypatch.setattr(service, "settle_position_at_final", lambda position, price, source, **kwargs: calls.append((price, kwargs["payout"])))
    assert service.attempt_settlement(position)
    assert calls == [(None, 0.0)]


def test_failed_twap_worker_does_not_publish_a_retimestamped_subprocess_result(monkeypatch):
    from scripts import polymarket_edge
    def failed_worker(**kwargs):
        raise RuntimeError("calibration failed after sampling refreshed spot")
    monkeypatch.setattr(polymarket_edge, "_get_terminal_worker", lambda: SimpleNamespace(run=failed_worker))
    monkeypatch.setattr(polymarket_edge, "_run_terminal_subprocess", lambda *args, **kwargs: pytest.fail("stale-input subprocess fallback"))
    result = polymarket_edge.run_terminal_script(80_000, 0.1, payoff_kind=TWAP_PAYOFF, use_inprocess=True)
    assert result["prob_above"] is None


def test_republished_exchange_median_retains_oldest_input_age():
    import time
    from scripts.trader.spot_aggregator import SpotAggregator, BinanceWebSocketFeed
    from scripts.trader.spot_daemon import SpotDaemon
    from scripts.trader.spot_ipc import SpotTick
    now_ms = int(time.time() * 1000)
    feeds = {}
    for index, lag in enumerate((0, 500, 3000)):
        feed = BinanceWebSocketFeed()
        feed._buffer_point(now_ms-lag, 80_000+index)
        feeds[str(index)] = feed
    aggregator = SpotAggregator(enabled_feeds=[], rest_last_resort_enabled=False)
    aggregator._feeds = feeds
    price, source, details = aggregator.get_aggregated_spot()
    source_ms, received_ms = SpotDaemon._timestamps_for_source(source, details)
    assert source_ms == now_ms - 3000
    tick = SpotTick.create(price=price, source=source, sequence=1, source_timestamp_ms=source_ms, received_at_ms=received_ms)
    assert not tick.is_fresh(max_age_s=2.0)


@pytest.mark.parametrize("feed_kind", ["chainlink", "exchange"])
def test_stopping_websocket_cancels_and_joins_its_own_session(monkeypatch, feed_kind):
    import asyncio
    import threading
    from scripts.trader.chainlink_feed import ChainlinkPriceFeed
    from scripts.trader.spot_aggregator import BinanceWebSocketFeed
    feed = ChainlinkPriceFeed() if feed_kind == "chainlink" else BinanceWebSocketFeed()
    started, cleaned = threading.Event(), threading.Event()
    async def session():
        started.set()
        try:
            await asyncio.Future()
        finally:
            cleaned.set()
    monkeypatch.setattr(feed, "_ws_session", session)
    feed.start()
    try:
        assert started.wait(2)
    finally:
        feed.stop()
    assert cleaned.is_set()
    assert not feed._thread.is_alive()
    assert feed._loop is None


def test_oneshot_spot_reader_cannot_accept_twap_as_spot(monkeypatch):
    from scripts import polymarket_btc_15m as module
    def fetch(subscription, parse, timeout=10):
        payload = {"symbol": "btc/usd", "timestamp": 1788594300000, "value": 80_000}
        assert parse({"topic": "crypto_prices_twap_sixty", "payload": payload}) is None
        return parse({"topic": "crypto_prices_chainlink", "payload": payload})
    monkeypatch.setattr(module, "_fetch_chainlink_ws", fetch)
    assert module.get_chainlink_btc_price() == 80_000


def test_twap_threshold_cannot_fall_back_to_an_instant_oracle_point(monkeypatch):
    import scripts.trader.calc_daemon as module
    now = datetime.now(timezone.utc)
    contract = parse_market_contract(gamma_market())
    data = {"market_id": "market", "window_start_utc": now-timedelta(seconds=35),
            "end_date_utc": now+timedelta(seconds=865), "contract_metadata": contract.to_dict()}
    monkeypatch.setattr(module, "find_active_15m_market", lambda: {"exists": True})
    monkeypatch.setattr(module, "extract_market_data", lambda event: data)
    chainlink = SimpleNamespace(is_healthy=True, get_first_at_or_after=lambda *args, **kwargs: pytest.fail("instant threshold fallback"))
    pm = SimpleNamespace(ptb_for_window=lambda *args: None)
    assert module._resolve_active_market_context(chainlink=chainlink, polymarket=pm,
        locked_barriers={"market": (80_000, "chainlink", 0.0)}) is None


def test_same_price_post_fit_refresh_keeps_new_source_timestamp(tmp_path, monkeypatch):
    import scripts.trader.calc_daemon as module
    from scripts.trader.spot_ipc import SpotTick
    from tests.test_calc_daemon import _setup_daemon_with_mocked_io
    daemon, context = _setup_daemon_with_mocked_io(tmp_path)
    context = replace(context, barrier_source="polymarket", contract_metadata=parse_market_contract(gamma_market()).to_dict())
    now = datetime.now(timezone.utc)
    ticks = [SpotTick.create(price=80_000, source="polymarket_rtds", sequence=1,
                            source_timestamp_ms=int(now.timestamp()*1000)-1500),
             SpotTick.create(price=80_000, source="polymarket_rtds", sequence=2,
                            source_timestamp_ms=int(now.timestamp()*1000)-1000)]
    sequence = iter(ticks)
    daemon._spot_feed = SimpleNamespace(latest=lambda **kwargs: next(sequence))
    monkeypatch.setattr(daemon, "_resolve_context_cached", lambda status: context)
    started = now-timedelta(seconds=1)
    def calibration(**kwargs):
        assert kwargs["spot_provider"]() == kwargs["spot"] == 80_000
        return {"prob_above": 0.6, "prob_below": 0.4, "spot_price": 80_000,
                "payoff_kind": TWAP_PAYOFF, "averaging_window_seconds": 60,
                "probability_evaluated_at": started.isoformat(),
                "terminal_payload": {"timestamp": now.isoformat(), "timing": {"mc_s": 1.0}}}
    monkeypatch.setattr(module, "run_terminal_script", calibration)
    event = daemon.run_once()
    assert event.spot_sequence == 2
    assert event.probability_spot_observed_at_iso == datetime.fromtimestamp(ticks[1].source_timestamp_ms/1000, timezone.utc).isoformat()
    assert event.probability_evaluated_at_iso == started.isoformat()
    assert (event.ready_at-started).total_seconds() >= event.terminal_payload["timing"]["mc_s"]
    daemon._settlement_pool.shutdown(wait=True)


@pytest.mark.parametrize("mode", ["fresh", "same_price", "stale", "future", "rollover", "no_update"])
def test_twap_producer_waits_for_usable_source_observation(tmp_path, monkeypatch, mode):
    import scripts.trader.calc_daemon as module
    from scripts.trader.spot_ipc import SpotTick
    from tests.test_calc_daemon import _setup_daemon_with_mocked_io
    daemon, context = _setup_daemon_with_mocked_io(tmp_path)
    context = replace(context, barrier_source="polymarket", contract_metadata=parse_market_contract(gamma_market()).to_dict())
    now = datetime.now(timezone.utc)
    old = SpotTick.create(price=80_000, source="polymarket_rtds", sequence=1,
                         source_timestamp_ms=int(now.timestamp()*1000)-1700)
    age_ms = {"stale": 1700, "future": -1000}.get(mode, 500)
    new = SpotTick.create(price=80_000, source="polymarket_rtds", sequence=2,
                         source_timestamp_ms=int(now.timestamp()*1000)-age_ms)
    if mode == "same_price":
        old = replace(old, source_timestamp_ms=new.source_timestamp_ms)
        daemon._last_priced_observation = (old.source, old.source_timestamp_ms)
        new = replace(new, source_timestamp_ms=new.source_timestamp_ms+1)
    waits = []
    def wait(sequence, timeout_s):
        assert 0 < timeout_s <= 1.0
        waits.append(sequence)
        if mode == "no_update":
            import time
            time.sleep(timeout_s)
            return old
        if mode in ("stale", "future"):
            daemon._stop = True
        if mode == "rollover":
            monkeypatch.setattr(module, "_utc_now", lambda: context.market_end_time)
        return new
    daemon._spot_feed = SimpleNamespace(latest=lambda **kw: old, wait_for_update=wait)
    monkeypatch.setattr(daemon, "_resolve_context_cached", lambda status: context)
    def calibration(**kwargs):
        if mode in ("stale", "future", "rollover", "no_update"):
            reason = "twap_history_window" if mode == "rollover" else "fresh_spot_timeout"
            with pytest.raises(ValueError, match=reason):
                kwargs["spot_provider"]()
            return {"error": "refresh unavailable"}
        assert kwargs["spot_provider"]() == new.price
        return {"prob_above": 0.6, "prob_below": 0.4, "payoff_kind": TWAP_PAYOFF,
                "probability_evaluated_at": now.isoformat(), "terminal_payload": {"timing": {}}}
    monkeypatch.setattr(module, "run_terminal_script", calibration)
    try:
        event = daemon.run_once()
        assert waits == [1]
        if mode in ("stale", "future", "rollover", "no_update"):
            assert event is None
        else:
            assert event.spot_sequence == 2
            assert event.probability_spot_observed_at_iso == datetime.fromtimestamp(new.source_timestamp_ms/1000, timezone.utc).isoformat()
            assert event.terminal_payload["timing"]["pricing_spot_age_s"] < 1.4
    finally:
        daemon._settlement_pool.shutdown(wait=True)
        daemon._book_pool.shutdown(wait=True)


def test_twap_late_books_share_remaining_budget_and_clear_old_quotes(tmp_path, monkeypatch):
    import scripts.trader.calc_daemon as module
    from concurrent.futures import Future
    from tests.test_calc_daemon import _setup_daemon_with_mocked_io
    daemon, _ = _setup_daemon_with_mocked_io(tmp_path)
    now = datetime.now(timezone.utc)
    data = {"payoff_kind": TWAP_PAYOFF, "probability_evaluated_at": now.isoformat(),
            "probability_spot_observed_at": (now-timedelta(seconds=1.9)).isoformat(),
            "up_best_ask": 0.5, "down_best_ask": 0.5}
    timeouts = []
    class LateBook:
        def result(self, timeout):
            timeouts.append(timeout)
            raise TimeoutError
    try:
        daemon._harvest_book_fetches({"UP": LateBook(), "DOWN": LateBook()}, data)
        assert timeouts == [0.0, 0.0]
        assert data["up_best_ask"] == data["down_best_ask"] == 0
        # With 0.4s of source lifetime left, reserve 0.2s for delivery.
        # If UP consumes that budget, DOWN must not receive a second one.
        clock = [0.0]
        monkeypatch.setattr(module, "time", SimpleNamespace(monotonic=lambda: clock[0]))
        monkeypatch.setattr(module, "_utc_now", lambda: now)
        data["probability_spot_observed_at"] = (now-timedelta(seconds=1.6)).isoformat()
        class SlowBook:
            def result(self, timeout):
                timeouts.append(timeout)
                clock[0] += timeout
                raise TimeoutError
        timeouts.clear()
        daemon._harvest_book_fetches({"UP": SlowBook(), "DOWN": SlowBook()}, data)
        assert timeouts == pytest.approx([0.2, 0.0])
        # A blocked request must not add another job on every producer cycle.
        daemon._book_jobs = {"UP": Future(), "DOWN": Future()}
        assert daemon._submit_book_fetches(SimpleNamespace(clob_token_ids={"UP": "u", "DOWN": "d"})) == {}
    finally:
        daemon._settlement_pool.shutdown(wait=True)
        daemon._book_pool.shutdown(wait=True)


def test_calc_entrypoint_receives_new_observation_from_real_redis(tmp_path, monkeypatch):
    """Exercise production reader construction/lifecycle, not a mock reader API."""
    import os
    import time
    import uuid
    import scripts.trader.calc_daemon as module
    from scripts.trader.spot_ipc import RedisSpotPublisher, SpotTick
    url = os.environ.get("TEST_REDIS_URL")
    if not url:
        pytest.skip("requires isolated TEST_REDIS_URL")
    prefix = f"audit:spot:{uuid.uuid4().hex}"
    publisher = RedisSpotPublisher(redis_url=url, latest_key=prefix, stream=prefix+":stream", channel=prefix)
    publisher.publish(SpotTick.create(price=80_000, source="polymarket_rtds", sequence=1,
                                      source_timestamp_ms=int(time.time()*1000)-3000))
    cfg = tmp_path/"calc.json"
    cfg.write_text(json.dumps({"spot_redis_url": url, "spot_redis_latest_key": prefix,
        "spot_redis_stream": prefix+":stream", "spot_redis_channel": prefix,
        "market_context_enabled": False, "inbox_targets": [str(tmp_path/"inbox")]}))
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("BTC_SPOT_REDIS_URL", url)
    monkeypatch.setattr(module.sys, "argv", ["calc", "--config", str(cfg)])
    monkeypatch.setattr(module.signal, "signal", lambda *args: None)
    def observe_once(daemon):
        feed = daemon._spot_feed
        assert feed._thread.is_alive()
        deadline = time.monotonic()+3
        while not publisher._client.pubsub_numsub(prefix)[0][1]:
            assert time.monotonic() < deadline
            time.sleep(.01)
        publisher.publish(SpotTick.create(price=80_000, source="polymarket_rtds", sequence=2))
        received = feed.wait_for_update(1, timeout_s=1)
        assert received.sequence == 2 and received.is_fresh(1.4)
    monkeypatch.setattr(module.CalcDaemon, "run_forever", observe_once)
    try:
        assert module.main() == 0
    finally:
        publisher._client.delete(prefix, prefix+":stream")
        publisher.close()
