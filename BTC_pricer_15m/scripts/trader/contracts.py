"""Market payoff, fee and resolution semantics shared by all trading engines.

Oracle prices are measurements; a market's resolved token payout is the
accounting authority. The TWAP forecast approximates a continuous trailing
average and does not reproduce Chainlink's unpublished sampling rules.
"""
from __future__ import annotations

import json
import math
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, ROUND_CEILING
from typing import Optional
from urllib.parse import quote, urlparse

import requests
from eth_utils import keccak

TWAP_PAYOFF = "chainlink_twap_60s"
AUTHORITATIVE_PAYOUT_SOURCES = frozenset({"polymarket_resolved_outcome", "conditional_tokens_payout"})
# Polymarket's documented Polygon mainnet CTF deployment.
CONDITIONAL_TOKENS_ADDRESS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"


def _array(raw) -> list:
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except (ValueError, TypeError):
            return []
    return list(raw) if isinstance(raw, (list, tuple)) else []


def fee_schedule_valid(schedule: Optional[dict]) -> bool:
    if not isinstance(schedule, dict) or not isinstance(schedule.get("enabled"), bool):
        return False
    if not schedule["enabled"]:
        return True
    try:
        return (
            math.isfinite(float(schedule["rate"]))
            and 0.0 <= float(schedule["rate"]) <= 1.0
            and float(schedule["exponent"]) == 1.0
            and schedule.get("takerOnly") is True
        )
    except (KeyError, TypeError, ValueError):
        return False


def fee_for_fill(price: float, shares: float, schedule: dict, *, is_maker: bool = False) -> float:
    """Fee in collateral units for one price level (current exponent-1 curve).

    Round upward at the documented 1e-5 collateral precision for paper costs.
    This bounds unspecified exchange rounding conservatively by <1e-5/fill;
    live accounting must use actual net execution amounts, never this estimate.
    """
    if not fee_schedule_valid(schedule):
        raise ValueError("unknown or unsupported market fee schedule")
    if not (math.isfinite(price) and 0.0 < price < 1.0 and math.isfinite(shares) and shares >= 0.0):
        raise ValueError("invalid fill price or quantity")
    if not schedule["enabled"] or is_maker:
        return 0.0
    p, q = Decimal(str(price)), Decimal(str(shares))
    fee = q * Decimal(str(schedule["rate"])) * p * (1 - p)
    return float(fee.quantize(Decimal("0.00001"), rounding=ROUND_CEILING))


@dataclass(frozen=True)
class MarketContract:
    payoff_kind: str = "unknown"
    averaging_window_seconds: int = 0
    condition_id: str = ""
    outcome_token_ids: dict = field(default_factory=dict)
    resolution_source: str = ""
    fee_schedule: Optional[dict] = None
    supported: bool = False
    reason: str = "missing_contract_metadata"
    outcome_indices: dict[str, int] = field(default_factory=dict)

    @property
    def has_binary_slots(self) -> bool:
        return (set(self.outcome_token_ids) == {"UP", "DOWN"}
                and len(set(self.outcome_token_ids.values())) == 2
                and set(self.outcome_indices) == {"UP", "DOWN"}
                and all(type(index) is int for index in self.outcome_indices.values())
                and set(self.outcome_indices.values()) == {0, 1})

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, raw: Optional[dict]) -> "MarketContract":
        if not isinstance(raw, dict):
            return cls()
        return cls(**{key: raw[key] for key in cls.__dataclass_fields__ if key in raw})


def parse_market_contract(market: dict) -> MarketContract:
    """Parse explicit Gamma market semantics; never infer payoff from a slug."""
    source = str(market.get("resolutionSource") or "")
    parsed = urlparse(source)
    kind, window = "unknown", 0
    if parsed.hostname == "data.chain.link":
        path = parsed.path.rstrip("/")
        if path == "/streams/btc-usd-twap-60s-streams":
            kind, window = TWAP_PAYOFF, 60
        elif path == "/streams/btc-usd":
            kind = "terminal_spot"
    outcomes = _array(market.get("outcomes"))
    tokens = {
        str(outcome).upper(): str(token)
        for outcome, token in zip(outcomes, _array(market.get("clobTokenIds")))
    }
    indices = {str(outcome).upper(): index for index, outcome in enumerate(outcomes)}
    enabled = market.get("feesEnabled")
    raw_fee = market.get("feeSchedule") or {}
    fee = {"enabled": enabled, **{key: raw_fee.get(key) for key in ("rate", "exponent", "takerOnly")}}
    condition_id = str(market.get("conditionId") or "")
    reason = ""
    if kind == "unknown":
        reason = "unsupported_resolution_source"
    elif not condition_id or len(outcomes) != 2 or set(tokens) != {"UP", "DOWN"} or len(set(tokens.values())) != 2:
        reason = "invalid_outcome_token_mapping"
    elif not fee_schedule_valid(fee):
        reason = "unsupported_fee_schedule"
    return MarketContract(kind, window, condition_id, tokens, source, fee, not reason, reason, indices)


def fetch_market_contract(slug: str, *, session=None, timeout: float = 3.0) -> Optional[MarketContract]:
    """Resolve an inherited pending market's identity before any settlement.

    An old market can lack today's fee/model support and still have a known
    token payout. Missing or mismatched identity must remain pending.
    """
    if not slug:
        return None
    client = session or requests
    try:
        response = client.get("https://gamma-api.polymarket.com/markets/slug/" + quote(str(slug), safe=""), timeout=timeout)
        response.raise_for_status()
        raw = response.json()
        if not isinstance(raw, dict) or raw.get("slug") != slug:
            return None
        contract = parse_market_contract(raw)
        if (not contract.condition_id or set(contract.outcome_token_ids) != {"UP", "DOWN"}
                or len(set(contract.outcome_token_ids.values())) != 2
                or any(not token or token == "None" for token in contract.outcome_token_ids.values())):
            return None
        return contract
    except (requests.RequestException, ValueError, TypeError, AttributeError):
        return None


@dataclass(frozen=True)
class MarketResolution:
    condition_id: str
    payout_by_token: dict[str, float]
    source: str
    observed_at: str
    details: dict = field(default_factory=dict)


def resolution_from_clob(raw: dict, contract: MarketContract) -> Optional[MarketResolution]:
    """Accept only a closed, uniquely resolved binary market with exact tokens."""
    if raw.get("closed") is not True or raw.get("condition_id") != contract.condition_id:
        return None
    tokens = raw.get("tokens")
    if not isinstance(tokens, list) or len(tokens) != 2:
        return None
    by_id = {str(token.get("token_id")): token for token in tokens if isinstance(token, dict)}
    if set(by_id) != set(contract.outcome_token_ids.values()):
        return None
    if any(not isinstance(token.get("winner"), bool) for token in by_id.values()):
        return None
    if sum(token["winner"] for token in by_id.values()) != 1:
        return None  # unresolved, conflicting, or non-binary payout: retain pending
    return MarketResolution(
        contract.condition_id,
        {token_id: float(token["winner"]) for token_id, token in by_id.items()},
        "polymarket_resolved_outcome", datetime.now(timezone.utc).isoformat(),
    )


def fetch_conditional_tokens_payout(contract: MarketContract, *, rpc_urls, session=None, timeout: float = 3.0) -> Optional[MarketResolution]:
    """Read the immutable resolved CTF payout vector in one RPC batch.

    The outcome-slot index comes from Gamma's paired outcome/token arrays;
    JSON dictionary order is never a substitute. Fractional/unsupported
    payouts remain pending until the accounting engine can represent them.
    """
    if not contract.has_binary_slots or not rpc_urls:
        return None
    condition = contract.condition_id
    try:
        if not isinstance(condition, str) or len(condition) != 66 or not condition.startswith("0x"):
            return None
        int(condition[2:], 16)
    except ValueError:
        return None
    deadline = time.monotonic() + max(0.0, min(float(timeout), 8.0))
    client = session or requests
    def calldata(signature, index=None):
        encoded = keccak(text=signature)[:4].hex() + condition[2:]
        return "0x" + encoded + (f"{index:064x}" if index is not None else "")
    calls = [
        {"jsonrpc": "2.0", "id": 1, "method": "eth_chainId", "params": []},
    ]
    for call_id, signature, index in ((2, "getOutcomeSlotCount(bytes32)", None),
            (3, "payoutDenominator(bytes32)", None), (4, "payoutNumerators(bytes32,uint256)", 0),
            (5, "payoutNumerators(bytes32,uint256)", 1)):
        calls.append({"jsonrpc": "2.0", "id": call_id, "method": "eth_call",
                      "params": [{"to": CONDITIONAL_TOKENS_ADDRESS, "data": calldata(signature, index)}, "latest"]})
    for rpc_url in tuple(rpc_urls)[:3]:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        try:
            response = client.post(rpc_url, json=calls, timeout=min(1.0, remaining))
            response.raise_for_status()
            raw = response.json()
            if not isinstance(raw, list) or len(raw) != len(calls):
                continue
            results = {item["id"]: item["result"] for item in raw if isinstance(item, dict) and not item.get("error")}
            if set(results) != {1, 2, 3, 4, 5} or results[1] != "0x89":
                continue
            words = []
            for call_id in (2, 3, 4, 5):
                word = results[call_id]
                if not isinstance(word, str) or len(word) != 66 or not word.startswith("0x"):
                    raise ValueError("malformed CTF uint256 result")
                words.append(int(word[2:], 16))
            slots, denominator, numerator0, numerator1 = words
            numerators = [numerator0, numerator1]
            if (slots != 2 or denominator <= 0 or sum(numerators) != denominator
                    or any(value not in (0, denominator) for value in numerators)):
                continue
            return MarketResolution(
                condition,
                {token: float(numerators[contract.outcome_indices[side]] // denominator)
                 for side, token in contract.outcome_token_ids.items()},
                "conditional_tokens_payout", datetime.now(timezone.utc).isoformat(),
                {"chain_id": 137, "contract_address": CONDITIONAL_TOKENS_ADDRESS,
                 "payout_denominator": denominator, "payout_numerators": numerators,
                 "outcome_indices": dict(contract.outcome_indices)},
            )
        except (requests.RequestException, ValueError, TypeError, KeyError, AttributeError):
            continue
    return None


def fetch_market_resolution(contract: MarketContract, *, session=None, timeout: float = 8.0, rpc_urls=()) -> Optional[MarketResolution]:
    if not contract.condition_id or not contract.outcome_token_ids:
        return None
    client = session or requests
    started = time.monotonic()
    try:
        response = client.get("https://clob.polymarket.com/markets/" + contract.condition_id,
                              timeout=min(0.75, timeout) if rpc_urls else timeout)
        response.raise_for_status()
        resolution = resolution_from_clob(response.json(), contract)
        if resolution is not None:
            return resolution
    except (requests.RequestException, ValueError, TypeError, AttributeError):
        pass
    return fetch_conditional_tokens_payout(contract, rpc_urls=rpc_urls, session=session,
                                           timeout=max(0.0, timeout-(time.monotonic()-started)))


def payout_for_position(payouts, token_id: str, direction) -> Optional[float]:
    if not isinstance(payouts, dict):
        return None
    side = str(getattr(direction, "value", direction)).upper()
    raw = payouts.get(token_id, payouts.get(side))
    try:
        value = float(raw)
        return value if math.isfinite(value) and 0.0 <= value <= 1.0 else None
    except (TypeError, ValueError):
        return None


def entry_inputs_valid(tick, now: Optional[datetime] = None) -> tuple[bool, str]:
    """One entry predicate for grid and live; legacy in-memory fixtures remain usable."""
    if not getattr(tick, "contract_supported", True):
        return False, getattr(tick, "contract_reason", "") or "unsupported_contract"
    pa, pb = getattr(tick, "prob_above", None), getattr(tick, "prob_below", None)
    try:
        if not (math.isfinite(pa) and math.isfinite(pb) and 0 <= pa <= 1 and 0 <= pb <= 1 and abs(pa + pb - 1) <= 1e-6):
            return False, "invalid_probability"
    except TypeError:
        return False, "invalid_probability"
    if getattr(tick, "payoff_kind", "legacy") == "legacy":
        return True, ""
    if getattr(tick, "payoff_kind", "unknown") not in (TWAP_PAYOFF, "terminal_spot"):
        return False, "unsupported_payoff"
    if getattr(tick, "payoff_kind", "unknown") == TWAP_PAYOFF and getattr(tick, "barrier_source", "") != "polymarket":
        return False, "unconfirmed_market_threshold"
    if not fee_schedule_valid(getattr(tick, "fee_schedule", None)):
        return False, "unsupported_fee_schedule"
    now = now or datetime.now(timezone.utc)
    limit = min(2.0, float(getattr(tick, "probability_max_age_s", 2.0)))
    for field_name, reason in (("probability_evaluated_at", "stale_probability"), ("probability_spot_observed_at", "stale_probability_spot")):
        observed = getattr(tick, field_name, None)
        try:
            age = (now - observed).total_seconds()
        except (TypeError, AttributeError):
            return False, reason
        if not 0.0 <= age <= limit:
            return False, reason
    return True, ""
