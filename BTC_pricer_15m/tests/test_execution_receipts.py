"""V2 ABI layout and exact cash-flow regressions; no wallet or live RPC."""
from copy import deepcopy
from decimal import Decimal
from types import SimpleNamespace

import pytest

from scripts.trader.execution_receipts import ORDER_FILLED_TOPIC, fetch_execution_receipts, parse_execution_receipts

ORDER = "0x" + "11" * 32
TX = "0x" + "22" * 32
OWNER = "0x" + "33" * 20
EXCHANGE = "0xE111180000d2663C0091e4f400237545B87B996B"


def receipt(side="BUY", tx=TX, index=0):
    # Seven non-indexed words from official V2 Events.sol: side, token,
    # makerAmount, takerAmount, collateral fee, builder, metadata.
    amounts = (50_000_000, 100_000_000) if side == "BUY" else (100_000_000, 50_000_000)
    words = [0 if side == "BUY" else 1, 123, *amounts, 1_750_000, 0, 0]
    return {"transactionHash": tx, "blockHash": "0x"+"44"*32, "status": "0x1", "logs": [{
        "address": EXCHANGE, "transactionHash": tx, "logIndex": hex(index),
        "topics": [ORDER_FILLED_TOPIC, ORDER, "0x"+"0"*24+OWNER[2:], "0x"+"0"*24+"55"*20],
        "data": "0x"+"".join(f"{word:064x}" for word in words)}]}


@pytest.mark.parametrize("side,cash", [("BUY", "-51.75"), ("SELL", "48.25")])
def test_v2_exact_amounts_and_fees_are_collateral(side, cash):
    result = parse_execution_receipts([receipt(side)], order_hash=ORDER, token_id="123", side=side,
                                      owner_address=OWNER, expected_shares=100)
    assert result.shares == Decimal(100)
    assert result.gross_collateral == Decimal(50)
    assert result.fee_collateral == Decimal("1.75")
    assert result.net_cashflow == Decimal(cash)


@pytest.mark.parametrize("mutation", ["failed", "unmined", "wrong_exchange", "wrong_order", "wrong_owner", "wrong_token", "wrong_side", "removed", "truncated"])
def test_invalid_or_unrelated_receipts_cannot_supply_accounting(mutation):
    raw = receipt()
    log = raw["logs"][0]
    if mutation == "failed": raw["status"] = "0x0"
    elif mutation == "unmined": raw["blockHash"] = None
    elif mutation == "wrong_exchange": log["address"] = OWNER
    elif mutation == "wrong_order": log["topics"][1] = "0x"+"99"*32
    elif mutation == "wrong_owner": log["topics"][2] = "0x"+"0"*24+"99"*20
    elif mutation == "wrong_token": log["data"] = log["data"][:66]+f"{124:064x}"+log["data"][130:]
    elif mutation == "wrong_side": log["data"] = "0x"+f"{1:064x}"+log["data"][66:]
    elif mutation == "removed": log["removed"] = True
    elif mutation == "truncated": log["data"] = log["data"][:-64]
    assert parse_execution_receipts([raw], order_hash=ORDER, token_id="123", side="BUY", owner_address=OWNER) is None


def test_duplicate_logs_are_counted_once_but_conflicts_are_rejected():
    raw = receipt()
    raw["logs"].append(deepcopy(raw["logs"][0]))
    result = parse_execution_receipts([raw, deepcopy(raw)], order_hash=ORDER, token_id="123", side="BUY")
    assert result.shares == 100 and result.net_cashflow == Decimal("-51.75")
    raw["logs"][1]["data"] = raw["logs"][1]["data"][:258]+f"{1_000_000:064x}"+raw["logs"][1]["data"][322:]
    assert parse_execution_receipts([raw], order_hash=ORDER, token_id="123", side="BUY") is None


def test_expected_total_prevents_partial_transaction_list_being_final():
    assert parse_execution_receipts([receipt()], order_hash=ORDER, token_id="123", side="BUY", expected_shares=200) is None
    two = [receipt(), receipt(tx="0x"+"66"*32)]
    result = parse_execution_receipts(two, order_hash=ORDER, token_id="123", side="BUY", expected_shares=200)
    assert result.shares == 200 and result.net_cashflow == Decimal("-103.50")


def test_fetch_checks_chain_and_receipt_identity_without_order_submission():
    calls = []
    def post(url, *, json, timeout):
        calls.append(json["method"])
        value = "0x89" if json["method"] == "eth_chainId" else receipt()
        return SimpleNamespace(raise_for_status=lambda: None, json=lambda: {"result": value})
    result = fetch_execution_receipts(order_hash=ORDER, token_id="123", side="BUY", transaction_hashes=[TX],
        rpc_urls=["https://rpc.test"], owner_address=OWNER, expected_shares=100, session=SimpleNamespace(post=post))
    assert result.net_cashflow == Decimal("-51.75")
    assert calls == ["eth_chainId", "eth_getTransactionReceipt"]


def test_unavailable_or_wrong_chain_never_invents_fees():
    def post(url, *, json, timeout):
        return SimpleNamespace(raise_for_status=lambda: None, json=lambda: {"result": "0x1"})
    assert fetch_execution_receipts(order_hash=ORDER, token_id="123", side="BUY", transaction_hashes=[TX],
        rpc_urls=["https://rpc.test"], session=SimpleNamespace(post=post)) is None
    assert fetch_execution_receipts(order_hash=ORDER, token_id="123", side="BUY", transaction_hashes=[], rpc_urls=["https://rpc.test"]) is None
