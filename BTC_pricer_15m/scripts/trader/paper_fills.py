"""CLOB V2 paper fills: collateral fees charged per executed price level."""

import math
from decimal import Decimal

from .contracts import fee_for_fill, fee_schedule_valid
from .order_book import MutableOrderBook


def buy_with_fees(order_book, budget: float, schedule: dict, *, consume: bool = False) -> tuple[float, float, float, float]:
    """Return shares, gross VWAP, notional, fees with q*p + fee <= budget."""
    if not fee_schedule_valid(schedule):
        raise ValueError("unknown or unsupported market fee schedule")
    if not math.isfinite(budget) or budget < 0:
        raise ValueError("invalid buy budget")
    remaining = Decimal(str(budget))
    fees = shares = notional = Decimal(0)
    rate = Decimal(str(schedule.get("rate", 0.0))) if schedule.get("enabled") else Decimal(0)
    for raw_price, raw_size in order_book.asks:
        price, size = float(raw_price), float(raw_size)
        if not (math.isfinite(price) and 0 < price < 1 and math.isfinite(size) and size >= 0):
            raise ValueError("invalid ask level")
        price_d, size_d = Decimal(str(price)), Decimal(str(size))
        unit_cost = price_d * (1 + rate * (1 - price_d))
        quantity = min(size_d, max(remaining, Decimal(0)) / unit_cost)
        fee = Decimal(str(fee_for_fill(price, float(quantity), schedule)))
        if quantity * price_d + fee > remaining:
            quantity = max(Decimal(0), min(quantity, (remaining - fee) / price_d))
            fee = Decimal(str(fee_for_fill(price, float(quantity), schedule)))
        fees += fee
        shares += quantity
        notional += quantity * price_d
        remaining -= quantity * price_d + fee
        if remaining <= Decimal("0.00001"):
            break
    if consume and isinstance(order_book, MutableOrderBook) and notional > 0:
        order_book.simulate_market_buy(float(notional), consume=True)
    return float(shares), float(notional / shares) if shares > 0 else 0.0, float(notional), float(fees)


def sell_fee_by_level(order_book, shares: float, schedule: dict) -> float:
    if not fee_schedule_valid(schedule):
        raise ValueError("unknown or unsupported market fee schedule")
    remaining, fees = Decimal(str(shares)), Decimal(0)
    for raw_price, raw_size in order_book.bids:
        price, size = float(raw_price), float(raw_size)
        if not (math.isfinite(price) and 0 < price < 1 and math.isfinite(size) and size >= 0):
            raise ValueError("invalid bid level")
        quantity = min(Decimal(str(size)), remaining)
        fees += Decimal(str(fee_for_fill(price, float(quantity), schedule)))
        remaining -= quantity
        if remaining <= 1e-9:
            break
    return float(fees)
