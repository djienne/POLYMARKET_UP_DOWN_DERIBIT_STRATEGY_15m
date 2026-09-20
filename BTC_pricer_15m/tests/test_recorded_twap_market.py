"""Observed market replay, without fabricated historical depth or oracle close."""
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from btc_pricer.constants import DAYS_PER_YEAR
from btc_pricer.models.ssvi import SSVISurfaceParams
from btc_pricer.models.terminal_probability import TerminalProbabilityCalculator
from cli_terminal import build_json_output
from scripts.polymarket_edge import parse_terminal_json
from scripts.trader._io import atomic_write_json
from scripts.trader.config import GridConfig
from scripts.trader.contracts import TWAP_PAYOFF, entry_inputs_valid, parse_market_contract, resolution_from_clob
from scripts.trader.data_feed import SharedDataFeed
from scripts.trader.grid_execution import dispatch_grid_tick
from scripts.trader.hybrid_calibration import CalibrationEvent, HybridCalibrationBroker, MarketContext
from scripts.trader.local_eval import load_signal_params
from scripts.trader.market_orchestrator import MarketOrchestrator
from scripts.trader.order_book import OrderBookSnapshot
from scripts.trader.persistence import SqliteStore
from scripts.trader.trader import PaperInstance


FIXTURE = Path(__file__).parent / "fixtures" / "recorded_twap_market_2026_09_05.json"


def test_recorded_market_through_real_mc_feed_paper_and_resolved_persistence(tmp_path, monkeypatch):
    recorded = json.loads(FIXTURE.read_text(encoding="utf-8"))
    historical = recorded["historical"]["event"]
    public = recorded["public_verification"]
    contract = parse_market_contract(public["market"])
    assert contract.supported and contract.payoff_kind == TWAP_PAYOFF
    assert public["market"]["slug"] == historical["market_id"]
    as_of = datetime.fromisoformat(historical["ready_at_iso"])
    end = datetime.fromisoformat(historical["market_end_time_iso"])
    source_at = datetime.fromtimestamp(historical["spot_details"]["source_timestamp_ms"] / 1000, timezone.utc)
    start = datetime.fromtimestamp(int(historical["market_id"].rsplit("-", 1)[1]), timezone.utc)
    raw_surface = historical["terminal_payload"]["ssvi_surface"]["surface_params"]
    surface = SSVISurfaceParams(**{key: raw_surface[key] for key in ("rho", "eta", "lam", "thetas", "ttms", "expiry_names")})
    ttm = (end-as_of).total_seconds() / (86400*DAYS_PER_YEAR)
    params = surface.get_params_for_ttm(ttm)
    spot, barrier = historical["spot_price"], historical["barrier"]
    # The old payload did not retain its smile forward. This assumption is
    # declared in the fixture and is not an assertion about the old MC output.
    calculator = TerminalProbabilityCalculator(n_simulations=300_000, seed=351)
    result = calculator.terminal_probability_twap(params, spot, spot, barrier)
    assert result.method == "ssvi_twap60_mc_approx"
    p_up = result.terminal_probability
    payload = build_json_output(None, spot, barrier, "up", ttm, surface_mc_above=p_up,
        surface_mc_below=1-p_up, surface_params_at_ttm=params, surface_params_obj=surface)
    payload.update(payoff_kind=TWAP_PAYOFF, averaging_window_seconds=60, probability_evaluated_at=as_of.isoformat(), smile_forward=spot)
    model_data = parse_terminal_json(payload)
    model_data["probability_spot_observed_at"] = source_at.isoformat()
    context = MarketContext(historical["market_id"], barrier, end.isoformat(), barrier_source="polymarket",
                           market_start_time_iso=start.isoformat(), contract_metadata=contract.to_dict())
    event = CalibrationEvent.from_parts(source="recorded_market_replay", context=context, ready_at=as_of,
                                        model_data=model_data, terminal_payload=payload)
    event = CalibrationEvent.from_dict(event.to_dict())
    assert event.schema_version == 5 and event.terminal_return_quantiles is None

    clock = {"now": as_of}
    class RecordedTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return clock["now"] if tz is not None else clock["now"].replace(tzinfo=None)
    import scripts.trader.data_feed as feed_module
    import scripts.trader.contracts as contracts_module
    import scripts.trader.hybrid_calibration as broker_module
    monkeypatch.setattr(feed_module, "datetime", RecordedTime)
    monkeypatch.setattr(contracts_module, "datetime", RecordedTime)
    monkeypatch.setattr(broker_module.time, "time", lambda: clock["now"].timestamp())
    config_path = Path(__file__).parents[1] / "config.yaml"
    cfg = GridConfig.from_file(config_path)
    broker = HybridCalibrationBroker(results_dir=tmp_path, enable_local_worker=False,
        enable_remote_inbox=False, push_consumer_enabled=False)
    assert broker.publish(event)
    feed = SharedDataFeed(cfg)
    feed.results_dir = tmp_path
    feed._hybrid_broker = broker
    feed._current_market_id = historical["market_id"]
    feed._current_market = {"market_id": historical["market_id"], "title": public["market"]["question"],
        "window_start_utc": start, "end_date_utc": end, "token_ids": contract.outcome_token_ids,
        "contract_metadata": contract.to_dict()}
    feed._local_signal_params = load_signal_params(config_path)
    monkeypatch.setattr(feed, "_scan_market", lambda: None)
    monkeypatch.setattr(feed, "_resolve_current_spot", lambda: (spot, "recorded_rtds"))
    monkeypatch.setattr(feed, "_binance_sanity_passes", lambda *args: True)
    # Only executable asks were recorded. Empty depth is honest missing
    # evidence; assigning an arbitrary quantity would manufacture fills.
    books = {side: OrderBookSnapshot(token, 0.0, historical["up_best_ask" if side == "UP" else "down_best_ask"],
                                    [], [], as_of.isoformat()) for side, token in contract.outcome_token_ids.items()}
    monkeypatch.setattr(feed, "_fetch_order_books", lambda: (books["UP"], books["DOWN"], books["UP"], books["DOWN"], None, None))
    tick = feed.tick()
    assert tick.prob_above == pytest.approx(p_up)
    assert tick.entry_signals == ()
    # The real old input was 2.667 s old at readiness. The new contract must
    # reject that source even though its recalculated probability is finite.
    assert entry_inputs_valid(tick, now=as_of)[1] == "stale_probability_spot"
    instance = PaperInstance(0, cfg.generate_params()[0], cfg)
    initial_capital = instance.capital
    assert dispatch_grid_tick([instance], tick, cfg) == []
    assert instance.position is None and instance.capital == initial_capital
    atomic_write_json(tmp_path/"paper_snapshot.json", instance.to_snapshot())
    store = MarketOrchestrator(settlements_dir=tmp_path/"settlements", sqlite_store=SqliteStore(tmp_path/"markets.sqlite"))
    store.write_strike(market_id=context.market_id, window_start=start, window_end=end, strike=barrier,
                       strike_offset_s=0, strike_source="polymarket", contract_metadata=contract.to_dict())
    resolution = resolution_from_clob(public["resolution"], contract)
    assert resolution is not None
    assert resolution.payout_by_token[contract.outcome_token_ids["UP"]] == 1
    store.finalize_payout(context.market_id, resolution)
    clock["now"] = datetime.fromisoformat(public["captured_at_utc"])
    expiry = feed.tick()
    assert expiry.is_expired and expiry.final_spot is None
    assert expiry.settlement_payouts == resolution.payout_by_token
    assert store.read_market(context.market_id)["settlement_price"] is None
    assert dispatch_grid_tick([instance], expiry, cfg) == []
    assert instance.capital == initial_capital and instance.total_pnl == 0


def test_actual_recorded_twap_message_preserves_exact_observation():
    from scripts.trader.chainlink_feed import ChainlinkPriceFeed
    public = json.loads(FIXTURE.read_text(encoding="utf-8"))["public_verification"]
    feed = ChainlinkPriceFeed()
    feed._process_message(json.dumps(public["oracle_message"]))
    actual = feed.get_latest_twap()
    assert actual["value"] == public["oracle_observation"]["value"]
    assert actual["source_timestamp_ms"] == public["oracle_observation"]["source_timestamp_ms"]
    assert feed.get_latest_price() is None
