"""Actual V2 execution amounts from mined OrderFilled receipt logs.

ABI: Polymarket/ctf-exchange-v2, src/exchange/mixins/Events.sol.
V2 charges fees in collateral on both sides: buys pay gross+fee while sells
receive gross-fee. This reader never estimates fees from a market schedule.
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Optional

import requests
from eth_utils import keccak

V2_EXCHANGES = frozenset({
    "0xe111180000d2663c0091e4f400237545b87b996b",
    "0xe2222d279d744050d28e00520010520000310f59",
})
ORDER_FILLED_TOPIC = "0x" + keccak(text="OrderFilled(bytes32,address,address,uint8,uint256,uint256,uint256,uint256,bytes32,bytes32)").hex()
_SCALE = Decimal(1_000_000)


def _hex(value, size: int) -> Optional[str]:
    if isinstance(value, bytes):
        value = "0x" + value.hex()
    if not isinstance(value, str) or len(value) != 2 + size * 2 or not value.startswith("0x"):
        return None
    try:
        int(value[2:], 16)
    except ValueError:
        return None
    return value.lower()


def _integer(value) -> int:
    return int(value, 16) if isinstance(value, str) and value.startswith("0x") else int(value)


@dataclass(frozen=True)
class ExecutionReceiptTotals:
    order_hash: str
    token_id: str
    side: str
    shares: Decimal
    gross_collateral: Decimal
    fee_collateral: Decimal
    net_cashflow: Decimal
    transaction_hashes: tuple[str, ...]


def parse_execution_receipts(
    receipts: list[dict], *, order_hash: str, token_id: str, side: str,
    owner_address: Optional[str] = None, expected_shares=None,
) -> Optional[ExecutionReceiptTotals]:
    """Sum only matching canonical V2 logs, deduplicated by tx/log index.

    Every supplied transaction must contain this order's fill. A caller with
    the complete CLOB matched quantity should pass expected_shares so a
    partial transaction list cannot be mistaken for complete cash accounting.
    """
    order_hash = _hex(order_hash, 32)
    side = str(side).upper()
    owner = _hex(owner_address, 20) if owner_address is not None else None
    if not order_hash or side not in ("BUY", "SELL") or (owner_address is not None and owner is None):
        return None
    try:
        token = int(token_id)
        if token < 0:
            return None
    except (TypeError, ValueError):
        return None
    expected_side = 0 if side == "BUY" else 1
    shares_raw = gross_raw = fee_raw = 0
    seen_logs: dict[tuple[str, int], tuple] = {}
    txs: list[str] = []
    if not receipts:
        return None
    for receipt in receipts:
        try:
            tx = _hex(receipt.get("transactionHash"), 32)
            if not tx or _integer(receipt.get("status")) != 1 or not _hex(receipt.get("blockHash"), 32):
                return None
            logs = receipt.get("logs")
            if not isinstance(logs, list):
                return None
            matching = False
            for log in logs:
                if str(log.get("address", "")).lower() not in V2_EXCHANGES:
                    continue
                topics = log.get("topics") or []
                if len(topics) != 4 or _hex(topics[0], 32) != ORDER_FILLED_TOPIC or _hex(topics[1], 32) != order_hash:
                    continue
                if log.get("removed") or (_hex(log.get("transactionHash"), 32) not in (None, tx)):
                    return None
                maker_topic = _hex(topics[2], 32)
                taker_topic = _hex(topics[3], 32)
                if not maker_topic or not taker_topic or maker_topic[2:26] != "0" * 24 or taker_topic[2:26] != "0" * 24:
                    return None
                if owner is not None and "0x" + maker_topic[-40:] != owner:
                    return None
                data = _hex(log.get("data"), 224)
                if data is None:
                    return None
                words = tuple(int(data[2+i*64:2+(i+1)*64], 16) for i in range(7))
                log_side, log_token, making, taking, fee, _builder, _metadata = words
                if log_side != expected_side or log_token != token:
                    return None
                gross, shares = (making, taking) if side == "BUY" else (taking, making)
                if not (shares > 0 and 0 < gross <= shares and 0 <= fee <= gross):
                    return None
                index = _integer(log.get("logIndex"))
                if index < 0:
                    return None
                key = (tx, index)
                signature = (str(log["address"]).lower(), tuple(str(t).lower() for t in topics), words)
                matching = True
                if key in seen_logs:
                    if seen_logs[key] != signature:
                        return None
                    continue
                seen_logs[key] = signature
                shares_raw += shares
                gross_raw += gross
                fee_raw += fee
            if not matching:
                return None
            if tx not in txs:
                txs.append(tx)
        except (AttributeError, TypeError, ValueError, OverflowError):
            return None
    shares = Decimal(shares_raw) / _SCALE
    if expected_shares is not None:
        try:
            expected = Decimal(str(expected_shares))
            if not expected.is_finite() or expected <= 0 or abs(shares - expected) > Decimal("0.000001"):
                return None
        except (TypeError, ValueError, ArithmeticError):
            return None
    gross, fee = Decimal(gross_raw) / _SCALE, Decimal(fee_raw) / _SCALE
    cash = -(gross + fee) if side == "BUY" else gross - fee
    return ExecutionReceiptTotals(order_hash, str(token), side, shares, gross, fee, cash, tuple(txs))


def fetch_execution_receipts(
    *, order_hash: str, token_id: str, side: str, transaction_hashes,
    rpc_urls, owner_address: Optional[str] = None, expected_shares=None,
    timeout_seconds: float = 4.0, request_timeout_seconds: float = 1.0, session=None,
) -> Optional[ExecutionReceiptTotals]:
    """Bounded read-only Polygon receipt lookup; incomplete evidence returns None."""
    if not transaction_hashes or not rpc_urls:
        return None
    hashes = tuple(dict.fromkeys(_hex(value, 32) for value in transaction_hashes))
    if None in hashes or len(hashes) > 8 or not _hex(order_hash, 32):
        return None
    budget = min(10.0, max(0.0, float(timeout_seconds)))
    deadline = time.monotonic() + budget
    client = session or requests
    for rpc_url in tuple(rpc_urls)[:3]:
        if not str(rpc_url).startswith(("https://", "http://")):
            continue
        def rpc(method, params):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return None
            limit = min(max(0.05, request_timeout_seconds), remaining)
            response = client.post(rpc_url, json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
                                   timeout=(limit / 2, limit / 2))
            response.raise_for_status()
            raw = response.json()
            return raw.get("result") if isinstance(raw, dict) and not raw.get("error") else None
        try:
            if rpc("eth_chainId", []) != "0x89":
                continue
            receipts = []
            for tx_hash in hashes:
                receipt = rpc("eth_getTransactionReceipt", [tx_hash])
                if not isinstance(receipt, dict) or _hex(receipt.get("transactionHash"), 32) != tx_hash:
                    break
                receipts.append(receipt)
            if len(receipts) != len(hashes):
                continue
            result = parse_execution_receipts(receipts, order_hash=order_hash, token_id=token_id,
                side=side, owner_address=owner_address, expected_shares=expected_shares)
            if result is not None:
                return result
        except (requests.RequestException, ValueError, TypeError, ArithmeticError):
            pass
        if time.monotonic() >= deadline:
            break
    return None
