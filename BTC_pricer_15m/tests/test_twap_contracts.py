from dataclasses import replace
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from scripts.trader.contracts import (
    TWAP_PAYOFF, entry_inputs_valid, fee_for_fill, parse_market_contract,
    resolution_from_clob,
)
from scripts.trader.hybrid_calibration import CalibrationEvent, MarketContext
from scripts.trader.local_eval import evaluate_prob_from_event
from scripts.trader.market_orchestrator import MarketOrchestrator


def gamma_market():
    # Relevant fields captured from active Gamma market 4209489 on 2026-09-05.
    return {
        "conditionId": "condition-1", "outcomes": '["Up","Down"]',
        "clobTokenIds": '["up-token","down-token"]',
        "resolutionSource": "https://data.chain.link/streams/btc-usd-twap-60s-streams",
        "feesEnabled": True,
        "feeSchedule": {"rate": 0.07, "exponent": 1, "takerOnly": True, "rebateRate": 0.2},
    }


def test_actual_contract_and_fee_schedule_are_explicit():
    contract = parse_market_contract(gamma_market())
    assert contract.supported and contract.payoff_kind == TWAP_PAYOFF
    assert contract.averaging_window_seconds == 60
    assert fee_for_fill(0.5, 100.0, contract.fee_schedule) == 1.75
    assert fee_for_fill(0.5, 100.0, contract.fee_schedule, is_maker=True) == 0
    assert fee_for_fill(0.45, 100, contract.fee_schedule) == 1.7325


@pytest.mark.parametrize("change", [
    {"resolutionSource": "https://example.test/unknown"},
    {"feesEnabled": None}, {"clobTokenIds": '["same","same"]'},
    {"feeSchedule": {"rate": 0.07, "exponent": 2, "takerOnly": True}},
])
def test_unknown_semantics_are_not_tradable(change):
    assert not parse_market_contract({**gamma_market(), **change}).supported


def test_resolution_requires_closed_market_exact_tokens_and_unique_winner(tmp_path):
    contract = parse_market_contract(gamma_market())
    raw = {"condition_id": contract.condition_id, "closed": False,
           "tokens": [{"token_id": "up-token", "winner": True}, {"token_id": "down-token", "winner": False}]}
    assert resolution_from_clob(raw, contract) is None
    raw["closed"] = True
    resolution = resolution_from_clob(raw, contract)
    assert resolution.payout_by_token == {"up-token": 1.0, "down-token": 0.0}
    store = MarketOrchestrator(settlements_dir=tmp_path)
    now = datetime.now(timezone.utc)
    store.write_strike(market_id="market", window_start=now, window_end=now+timedelta(minutes=15),
                       strike=80_000, strike_offset_s=0, strike_source="polymarket", contract_metadata=contract.to_dict())
    saved = store.finalize_payout("market", resolution)
    assert saved["settlement_price"] is None
    assert saved["settlement_payouts"] == resolution.payout_by_token
    assert not store.restore_state()[1]  # payout-finalized records are not restored pending
    raw["tokens"][1]["winner"] = True
    assert resolution_from_clob(raw, contract) is None


def test_freshness_is_based_on_pricing_and_observation_times_including_endpoints():
    now = datetime.now(timezone.utc)
    contract = parse_market_contract(gamma_market())
    tick = SimpleNamespace(contract_supported=True, payoff_kind=TWAP_PAYOFF, fee_schedule=contract.fee_schedule,
                           barrier_source="polymarket",
                           prob_above=0.0, prob_below=1.0, probability_evaluated_at=now,
                           probability_spot_observed_at=now, probability_max_age_s=2.0)
    assert entry_inputs_valid(tick, now=now) == (True, "")
    tick.probability_evaluated_at = now - timedelta(seconds=2.01)
    assert entry_inputs_valid(tick, now=now)[1] == "stale_probability"
    tick.probability_evaluated_at = now
    tick.probability_spot_observed_at = now - timedelta(seconds=2.01)
    assert entry_inputs_valid(tick, now=now)[1] == "stale_probability_spot"


def test_v5_roundtrip_does_not_relabel_terminal_quantiles_as_twap():
    now = datetime.now(timezone.utc)
    started_at = now-timedelta(seconds=1)
    contract = parse_market_contract(gamma_market())
    context = MarketContext("market", 80_000, (now+timedelta(minutes=5)).isoformat(), contract_metadata=contract.to_dict())
    event = CalibrationEvent.from_parts(source="calc", context=context, ready_at=now,
        model_data={"prob_above": 0.6, "prob_below": 0.4, "payoff_kind": TWAP_PAYOFF,
                    "averaging_window_seconds": 60, "probability_evaluated_at": started_at.isoformat(),
                    "probability_spot_observed_at": now.isoformat()}, terminal_payload={})
    decoded = CalibrationEvent.from_dict(event.to_dict())
    assert decoded.schema_version == 5 and decoded.payoff_kind == TWAP_PAYOFF
    assert decoded.probability_evaluated_at_iso == started_at.isoformat()
    assert (decoded.ready_at-datetime.fromisoformat(decoded.probability_evaluated_at_iso)).total_seconds() == 1
    assert evaluate_prob_from_event(decoded, spot=80_000, now=now) is None
    with pytest.raises(ValueError, match="quantiles"):
        replace(decoded, terminal_return_quantiles=(0.0,) * 101)
