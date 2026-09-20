"""Gnosis CTF payout ABI, source validation and explicit Gamma slot ordering."""
import json
from dataclasses import replace
from types import SimpleNamespace

import pytest
from eth_utils import keccak

from scripts.trader.contracts import (
    CONDITIONAL_TOKENS_ADDRESS, MarketContract, fetch_conditional_tokens_payout,
    fetch_market_resolution, parse_market_contract,
)
from tests.test_twap_contracts import gamma_market


def contract_reversed():
    return parse_market_contract({**gamma_market(), "conditionId": "0x"+"12"*32,
                                  "outcomes": '["Down","Up"]', "clobTokenIds": '["down-token","up-token"]'})


def rpc_session(*, denominator=1, numerators=(1, 0), slots=2, chain="0x89", inspect=None):
    def post(url, *, json, timeout):
        if inspect is not None:
            inspect(json)
        values = {1: chain, 2: f"0x{slots:064x}", 3: f"0x{denominator:064x}",
                  4: f"0x{numerators[0]:064x}", 5: f"0x{numerators[1]:064x}"}
        # RPC batch response order is not request order.
        raw = [{"jsonrpc": "2.0", "id": i, "result": values[i]} for i in (5, 3, 1, 4, 2)]
        return SimpleNamespace(raise_for_status=lambda: None, json=lambda: raw)
    return SimpleNamespace(post=post)


@pytest.mark.parametrize("reversed_outcomes", [False, True])
def test_chain_payout_uses_recorded_slots_after_json_key_sorting(reversed_outcomes):
    contract = contract_reversed() if reversed_outcomes else parse_market_contract({**gamma_market(), "conditionId": "0x"+"12"*32})
    contract = MarketContract.from_dict(json.loads(json.dumps(contract.to_dict(), sort_keys=True)))
    expected = {"down-token": 1.0, "up-token": 0.0} if reversed_outcomes else {"up-token": 1.0, "down-token": 0.0}
    def inspect(calls):
        assert calls[0]["method"] == "eth_chainId"
        for call in calls[1:]:
            assert call["method"] == "eth_call"
            assert call["params"][0]["to"] == CONDITIONAL_TOKENS_ADDRESS
        denominator_data = calls[2]["params"][0]["data"]
        assert denominator_data == "0x"+keccak(text="payoutDenominator(bytes32)")[:4].hex()+contract.condition_id[2:]
        assert calls[4]["params"][0]["data"].endswith(f"{1:064x}")
    result = fetch_conditional_tokens_payout(contract, rpc_urls=["https://rpc.test"], session=rpc_session(inspect=inspect))
    assert result.payout_by_token == expected
    assert result.source == "conditional_tokens_payout"
    assert result.details["payout_numerators"] == [1, 0]


@pytest.mark.parametrize("kwargs", [
    {"denominator": 0, "numerators": (0, 0)},
    {"denominator": 1, "numerators": (1, 1)},
    {"denominator": 2, "numerators": (1, 1)},
    {"slots": 3}, {"chain": "0x1"},
])
def test_unresolved_inconsistent_or_unsupported_payout_stays_pending(kwargs):
    assert fetch_conditional_tokens_payout(contract_reversed(), rpc_urls=["https://rpc.test"], session=rpc_session(**kwargs)) is None


def test_old_mapping_without_explicit_indices_is_not_guessed():
    contract = replace(contract_reversed(), outcome_indices={})
    session = SimpleNamespace(post=lambda *args, **kwargs: pytest.fail("unmapped outcome slots"))
    assert fetch_conditional_tokens_payout(contract, rpc_urls=["https://rpc.test"], session=session) is None


def test_stale_clob_can_resolve_through_authoritative_chain_vector():
    session = rpc_session()
    session.get = lambda *args, **kwargs: SimpleNamespace(raise_for_status=lambda: None,
        json=lambda: {"condition_id": contract_reversed().condition_id, "closed": False, "tokens": []})
    result = fetch_market_resolution(contract_reversed(), rpc_urls=["https://rpc.test"], session=session, timeout=1)
    assert result.source == "conditional_tokens_payout"
    assert result.payout_by_token["down-token"] == 1
