"""Shared BTC price sanity bounds."""

from __future__ import annotations

BTC_PRICE_MIN: float = 10_000
BTC_PRICE_MAX: float = 1_000_000


def is_sane_btc_price(price) -> bool:
    """Return True for plausible BTC/USD prices."""
    return isinstance(price, (int, float)) and BTC_PRICE_MIN < price < BTC_PRICE_MAX
