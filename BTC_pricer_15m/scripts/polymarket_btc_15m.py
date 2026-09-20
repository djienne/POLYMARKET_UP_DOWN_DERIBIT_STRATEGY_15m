#!/usr/bin/env python3
"""Find and fetch 15-minute Bitcoin Up/Down markets from Polymarket."""

import json
import logging
import os
import re
import requests
import sys
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from btc_pricer.utils.price_sanity import (
    BTC_PRICE_MAX,
    BTC_PRICE_MIN,
    is_sane_btc_price,
)
from scripts.trader.chainlink_feed import CHAINLINK_SUBSCRIPTION

GAMMA_URL = "https://gamma-api.polymarket.com"
BINANCE_URL = "https://api.binance.com/api/v3"
COINBASE_DATA_URL = "https://api.coinbase.com/v2"
COINBASE_EXCHANGE_URL = "https://api.exchange.coinbase.com"
_SESSION = requests.Session()
_CURRENT_SPOT_LOCK = threading.Lock()
_CURRENT_SPOT_CHAINLINK = None
_CURRENT_SPOT_PROVIDER = None

ET = ZoneInfo("America/New_York")

# Title pattern: "Bitcoin Up or Down - April 12, 10:30AM-10:45AM ET"
TITLE_TIME_RE = re.compile(
    r"(\d{1,2}:\d{2}\s*[APap][Mm])\s*-\s*(\d{1,2}:\d{2}\s*[APap][Mm])\s+ET",
)
TITLE_DATE_RE = re.compile(
    r"Bitcoin Up or Down\s*-\s*(\w+)\s+(\d{1,2})",
    re.IGNORECASE,
)


def _get_binance_rest_current_btc_price():
    """Fetch current BTC/USDT price from Binance REST. Returns None if insane."""
    url = f"{BINANCE_URL}/ticker/price"
    response = _SESSION.get(url, params={"symbol": "BTCUSDT"}, timeout=10)
    response.raise_for_status()
    price = float(response.json().get("price", 0))
    if not _is_sane_btc_price(price):
        logging.getLogger("price_feed").warning(f"Binance spot insane: {price}")
        return None
    return price


def _get_current_spot_provider():
    """Lazily start the process-local RTDS-first current spot provider."""
    global _CURRENT_SPOT_CHAINLINK, _CURRENT_SPOT_PROVIDER
    with _CURRENT_SPOT_LOCK:
        if _CURRENT_SPOT_PROVIDER is not None:
            return _CURRENT_SPOT_PROVIDER

        from scripts.trader.chainlink_feed import ChainlinkPriceFeed
        from scripts.trader.spot_aggregator import SpotAggregator

        _CURRENT_SPOT_CHAINLINK = ChainlinkPriceFeed()
        _CURRENT_SPOT_CHAINLINK.start()
        _CURRENT_SPOT_PROVIDER = SpotAggregator(
            chainlink_feed=_CURRENT_SPOT_CHAINLINK,
            rest_fallback_fn=_get_binance_rest_current_btc_price,
        )
        _CURRENT_SPOT_PROVIDER.start()
        return _CURRENT_SPOT_PROVIDER


def get_current_btc_price():
    """Return current BTC spot from the shared Redis feed when configured.

    Legacy process-local fallbacks remain only for non-Redis/manual contexts.
    Docker trading stacks set ``BTC_SPOT_REDIS_URL`` so current spot comes from
    the single shared spot daemon.
    """
    redis_configured = bool(os.environ.get("BTC_SPOT_REDIS_URL") or os.environ.get("REDIS_URL"))
    try:
        from scripts.trader.spot_ipc import get_shared_spot_price, redis_url_from_env
        redis_configured = bool(redis_url_from_env())
        shared = get_shared_spot_price()
        if shared is not None:
            return shared
        if redis_configured:
            return None
    except Exception as exc:
        logging.getLogger("price_feed").debug("shared redis spot failed: %s", exc)
        if redis_configured:
            return None

    try:
        provider = _get_current_spot_provider()
        result = provider.get_aggregated_spot()
        if result is not None:
            return result[0]
    except Exception as exc:
        logging.getLogger("price_feed").debug("RTDS-first spot provider failed: %s", exc)
    return _get_binance_rest_current_btc_price()


def get_coinbase_exchange_ticker_price(product_id: str = "BTC-USD"):
    """Fetch current BTC spot from Coinbase Exchange public ticker."""
    url = f"{COINBASE_EXCHANGE_URL}/products/{product_id}/ticker"
    response = _SESSION.get(url, timeout=10)
    response.raise_for_status()
    price = float(response.json().get("price", 0))
    if not _is_sane_btc_price(price):
        logging.getLogger("price_feed").warning(f"Coinbase Exchange spot insane: {price}")
        return None
    return price


def get_coinbase_spot_price(currency_pair: str = "BTC-USD"):
    """Fetch current BTC spot from Coinbase public Prices API."""
    url = f"{COINBASE_DATA_URL}/prices/{currency_pair}/spot"
    response = _SESSION.get(url, timeout=10)
    response.raise_for_status()
    payload = response.json().get("data", {})
    price = float(payload.get("amount", 0))
    if not _is_sane_btc_price(price):
        logging.getLogger("price_feed").warning(f"Coinbase spot insane: {price}")
        return None
    return price


def get_binance_price_at(timestamp_utc):
    """Fetch BTC/USDT open price for the 1-minute candle that begins at
    ``timestamp_utc``. This is the price *at* the timestamp — the value we
    want for window-start barrier capture.

    The kline at ``startTime=t`` covers ``[t, t+60s)`` with index [1]=open
    (price at t) and [4]=close (price at t+60s minus one tick). We use
    [1] so the returned price aligns with the requested boundary; using
    [4] would offset by ~59 s.

    Fallback if Chainlink fails.
    """
    url = f"{BINANCE_URL}/klines"
    start_time_ms = int(timestamp_utc.timestamp() * 1000)
    params = {
        "symbol": "BTCUSDT",
        "interval": "1m",
        "startTime": start_time_ms,
        "limit": 1,
    }
    response = _SESSION.get(url, params=params, timeout=10)
    response.raise_for_status()
    data = response.json()
    if data:
        price = float(data[0][1])  # open price (price AT startTime)
        if not _is_sane_btc_price(price):
            _price_log.warning(f"Binance kline insane: {price}")
            return None
        return price
    return None


_price_log = logging.getLogger("price_feed")

# BTC price sanity bounds — reject anything outside this range
_BTC_MIN = BTC_PRICE_MIN
_BTC_MAX = BTC_PRICE_MAX


def _is_sane_btc_price(price):
    """Check if a BTC price is within reasonable bounds."""
    return is_sane_btc_price(price)


def _fetch_chainlink_ws(subscribe_msg, parse_fn, timeout=10):
    """Connect to Polymarket WebSocket, subscribe, and parse messages.

    Handles asyncio.run() safely (creates new loop if needed).
    Returns whatever parse_fn returns, or None on failure.
    """
    import asyncio
    import websockets

    async def _inner():
        uri = "wss://ws-live-data.polymarket.com"
        async with websockets.connect(uri, close_timeout=5, open_timeout=10) as ws:
            await ws.send(json.dumps(subscribe_msg))
            deadline = asyncio.get_running_loop().time() + timeout
            while True:
                remaining = deadline - asyncio.get_running_loop().time()
                if remaining <= 0:
                    return None
                msg = await asyncio.wait_for(ws.recv(), timeout=remaining)
                if not msg or not msg.strip():
                    continue
                data = json.loads(msg)
                result = parse_fn(data)
                if result is not None:
                    return result
        return None

    try:
        # Handle case where event loop is already running
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop and loop.is_running():
            # Can't use asyncio.run() inside an existing loop
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as pool:
                return pool.submit(asyncio.run, _inner()).result(timeout=20)
        else:
            return asyncio.run(_inner())
    except Exception as e:
        _price_log.warning(f"WebSocket fetch failed: {e}")
        return None


_CHAINLINK_SUB = CHAINLINK_SUBSCRIPTION
_CHAINLINK_SNAPSHOT_SUB = {
    "action": "subscribe",
    "subscriptions": [
        {
            "topic": "crypto_prices_chainlink",
            "type": "*",
            "filters": '{"symbol":"btc/usd"}',
        }
    ],
}


def get_chainlink_btc_price():
    """Get current Chainlink BTC/USD price from Polymarket's WebSocket.

    Returns:
        float price or None on failure.
    """
    def _parse(data):
        if data.get("topic") not in (None, "crypto_prices_chainlink", "crypto_prices"):
            return None
        payload = data.get("payload", {})
        payload_symbol = str(payload.get("symbol") or "").lower()
        # Format 1: initial snapshot with data[] array
        price_data = payload.get("data", [])
        if price_data:
            for point in reversed(price_data):
                point_symbol = str(point.get("symbol") or payload_symbol or "").lower()
                if point_symbol and point_symbol != "btc/usd":
                    continue
                val = point.get("value")
                if _is_sane_btc_price(val):
                    return val
        # Format 2: live update with value directly in payload
        if "value" in payload and _is_sane_btc_price(payload["value"]):
            if payload_symbol != "btc/usd":
                return None
            return payload["value"]
        return None

    return _fetch_chainlink_ws(_CHAINLINK_SUB, _parse)


def get_chainlink_price_at(timestamp_utc):
    """Get Chainlink BTC/USD price at a specific timestamp.

    Connects to Polymarket WebSocket, receives recent price history,
    finds the closest price to the target timestamp, and cross-validates
    against Binance. Falls back to Binance kline if WebSocket fails.

    Args:
        timestamp_utc: Target datetime (UTC).

    Returns:
        float price or None.
    """
    target_ms = int(timestamp_utc.timestamp() * 1000)

    def _parse(data):
        payload = data.get("payload", {})
        payload_symbol = str(payload.get("symbol") or "").lower()
        # Format 1: initial snapshot with data[] array
        price_data = payload.get("data", [])
        # Format 2: live update with value directly in payload
        if not price_data and "value" in payload and "timestamp" in payload:
            if payload_symbol != "btc/usd":
                return None
            price_data = [payload]
        if not price_data:
            return None
        # Filter to only valid data points
        valid = []
        for point in price_data:
            point_symbol = str(point.get("symbol") or payload_symbol or "").lower()
            if point_symbol and point_symbol != "btc/usd":
                continue
            if (
                "timestamp" in point and "value" in point
                and _is_sane_btc_price(point["value"])
            ):
                valid.append(point)
        if not valid:
            return None
        best = min(valid, key=lambda p: abs(p["timestamp"] - target_ms))
        diff_s = abs(best["timestamp"] - target_ms) / 1000
        if diff_s > 60:  # Must be within 60 seconds of target
            _price_log.warning(f"Closest Chainlink point is {diff_s:.0f}s from target — too stale")
            return None
        return best["value"], diff_s

    chainlink_price = None
    result = _fetch_chainlink_ws(_CHAINLINK_SNAPSHOT_SUB, _parse)
    if result is not None:
        chainlink_price, diff_s = result
        _price_log.debug(f"Chainlink price at target: ${chainlink_price:,.2f} ({diff_s:.0f}s offset)")

    # Also get Binance for cross-validation
    binance_price = None
    try:
        binance_price = get_binance_price_at(timestamp_utc)
    except Exception as e:
        _price_log.warning(f"Binance kline fetch failed: {e}")

    # Cross-validate if both available
    if chainlink_price and binance_price:
        diff_pct = abs(chainlink_price - binance_price) / binance_price * 100
        if diff_pct > 0.5:  # >0.5% divergence is suspicious
            _price_log.warning(
                f"Chainlink/Binance divergence: ${chainlink_price:,.2f} vs ${binance_price:,.2f} "
                f"({diff_pct:.2f}%) — using Chainlink (resolution source)"
            )
        return chainlink_price  # Chainlink is the resolution source

    if chainlink_price:
        return chainlink_price

    if binance_price and _is_sane_btc_price(binance_price):
        _price_log.warning(f"Using Binance fallback: ${binance_price:,.2f}")
        return binance_price

    _price_log.error("Both Chainlink and Binance failed to return barrier price")
    return None


def _quarter_hour_floor(dt):
    """Round datetime down to nearest 15-minute boundary (:00/:15/:30/:45)."""
    minute = (dt.minute // 15) * 15
    return dt.replace(minute=minute, second=0, microsecond=0)


def _quarter_hour_ceil(dt):
    """Round datetime up to next 15-minute boundary."""
    floored = _quarter_hour_floor(dt)
    if floored == dt:
        return dt
    return floored + timedelta(minutes=15)


def slug_for_timestamp(ts_utc):
    """Build the Gamma API slug for a 15-min market given window start UTC."""
    epoch = int(ts_utc.timestamp())
    return f"btc-updown-15m-{epoch}"


def fetch_event_by_slug(slug):
    """Fetch a single event from Gamma API by slug."""
    try:
        resp = _SESSION.get(
            f"{GAMMA_URL}/events", params={"slug": slug}, timeout=10
        )
        resp.raise_for_status()
        data = resp.json()
        events = data if isinstance(data, list) else [data] if isinstance(data, dict) else []
        for e in events:
            if e.get("slug") == slug:
                return e
    except Exception as exc:
        _price_log.debug("fetch_event_by_slug failed for %s: %s", slug, exc)
    return None


def find_active_15m_market():
    """Find the current active 15-minute BTC Up/Down market.

    Strategy: compute 15-minute boundaries around now and try slugs.
    Markets are created ahead of time, so nearby windows should already exist.
    We only return a market once it has actually started.

    Returns:
        Event dict or None.
    """
    now_utc = datetime.now(timezone.utc)

    # Try nearby windows, but only accept windows that are active right now.
    # Offset 0 (the window containing `now`) is the active market in the
    # normal case, so probe it first; the -15 window has always ended and
    # used to burn a guaranteed-wasted HTTP round-trip every scan.
    base = _quarter_hour_floor(now_utc)
    for offset_min in (0, 15, -15, 30, 45, 60):
        window_start = base + timedelta(minutes=offset_min)
        slug = slug_for_timestamp(window_start)
        event = fetch_event_by_slug(slug)
        if not event or event.get("closed", False):
            continue

        market = extract_market_data(event)
        start_dt = market.get("window_start_utc")
        end_dt = market.get("end_date_utc")
        if start_dt is None or end_dt is None:
            continue
        if start_dt <= now_utc < end_dt:
            return event
    return None


def parse_window_times_from_title(title, reference_et=None):
    """Extract window start and end times from a 15-min market title.

    Title format: "Bitcoin Up or Down - April 12, 10:30AM-10:45AM ET"

    Returns:
        (start_utc, end_utc) or (None, None)
    """
    date_match = TITLE_DATE_RE.search(title)
    time_match = TITLE_TIME_RE.search(title)
    if not date_match or not time_match:
        return None, None

    month_name = date_match.group(1)
    day = int(date_match.group(2))
    start_str = time_match.group(1).strip()
    end_str = time_match.group(2).strip()

    if reference_et is None:
        reference_et = datetime.now(ET)
    elif reference_et.tzinfo is None:
        reference_et = reference_et.replace(tzinfo=ET)
    else:
        reference_et = reference_et.astimezone(ET)

    months = {
        "January": 1, "February": 2, "March": 3, "April": 4,
        "May": 5, "June": 6, "July": 7, "August": 8,
        "September": 9, "October": 10, "November": 11, "December": 12,
    }
    month = months.get(month_name)
    if not month:
        return None, None

    def _parse_time(s, base_date):
        """Parse '10:30AM' or '2:00 PM' into a datetime."""
        s = s.replace(" ", "").upper()
        for fmt in ("%I:%M%p", "%H:%M%p"):
            try:
                t = datetime.strptime(s, fmt)
                return base_date.replace(hour=t.hour, minute=t.minute, second=0, microsecond=0)
            except ValueError:
                continue
        return None

    candidates = []
    for year in (reference_et.year - 1, reference_et.year, reference_et.year + 1):
        base = datetime(year, month, day, tzinfo=ET)
        start_et = _parse_time(start_str, base)
        end_et = _parse_time(end_str, base)
        if not start_et or not end_et:
            continue
        if end_et <= start_et:
            end_et += timedelta(days=1)
        distance = abs((start_et - reference_et).total_seconds())
        candidates.append((distance, start_et, end_et))

    if not candidates:
        return None, None

    _, start_et, end_et = min(candidates, key=lambda item: item[0])
    return start_et.astimezone(timezone.utc), end_et.astimezone(timezone.utc)


def extract_market_data(event):
    """Extract structured data from a 15-min market event.

    Returns dict with: market_id, title, slug, end_date_utc,
    window_start_utc, window_end_utc, prob_up, prob_down, token_ids.
    """
    title = event.get("title", "")
    slug = event.get("slug", "")
    end_str = event.get("endDate", "")
    end_utc = None
    if end_str:
        try:
            end_utc = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
        except ValueError:
            pass

    # Use startTime from API (exact window open) as primary source
    # Fall back to title regex parsing
    window_start = None
    start_time_str = event.get("startTime")
    if start_time_str:
        try:
            window_start = datetime.fromisoformat(start_time_str.replace("Z", "+00:00"))
        except ValueError:
            pass
    parsed_window_end = None
    if window_start is None:
        reference_et = end_utc.astimezone(ET) if end_utc else None
        window_start, parsed_window_end = parse_window_times_from_title(title, reference_et=reference_et)
    window_end = end_utc or parsed_window_end  # endDate = window end

    # Extract outcomes and prices
    prob_up = None
    prob_down = None
    token_ids = {}

    markets = event.get("markets", [])
    if markets:
        m = markets[0]
        outcomes = m.get("outcomes", [])
        outcome_prices = m.get("outcomePrices", [])
        clob_ids = m.get("clobTokenIds", [])

        if isinstance(outcomes, str):
            try:
                outcomes = json.loads(outcomes)
            except json.JSONDecodeError:
                outcomes = ["Up", "Down"]
        if isinstance(outcome_prices, str):
            try:
                outcome_prices = json.loads(outcome_prices)
            except json.JSONDecodeError:
                outcome_prices = []
        if isinstance(clob_ids, str):
            try:
                clob_ids = json.loads(clob_ids)
            except json.JSONDecodeError:
                clob_ids = []

        for i, outcome in enumerate(outcomes):
            key = outcome.upper()
            if i < len(outcome_prices):
                try:
                    price = float(outcome_prices[i])
                    if key == "UP":
                        prob_up = price
                    elif key == "DOWN":
                        prob_down = price
                except (ValueError, TypeError):
                    pass
            if i < len(clob_ids):
                token_ids[key] = clob_ids[i]

    from scripts.trader.contracts import parse_market_contract
    contract = parse_market_contract(markets[0] if markets else {})
    return {
        "market_id": slug,
        "title": title,
        "slug": slug,
        "end_date_utc": end_utc,
        "window_start_utc": window_start,
        "window_end_utc": window_end,
        "prob_up": prob_up,
        "prob_down": prob_down,
        "token_ids": token_ids,
        "contract_metadata": contract.to_dict(),
    }


def discover_markets(n=6):
    """Discovery mode: find and print upcoming 15-min markets."""
    now_utc = datetime.now(timezone.utc)
    base = _quarter_hour_floor(now_utc)

    found = []
    for offset_min in range(-30, 120, 15):
        window_start = base + timedelta(minutes=offset_min)
        slug = slug_for_timestamp(window_start)
        event = fetch_event_by_slug(slug)
        if event:
            data = extract_market_data(event)
            closed = event.get("closed", False)
            data["closed"] = closed
            found.append(data)
            if len(found) >= n:
                break

    return found


def main():
    """Discovery CLI."""
    markets = discover_markets(n=10)
    if not markets:
        print("No 15-minute markets found.")
        return

    now_utc = datetime.now(timezone.utc)
    print(f"Found {len(markets)} 15-minute BTC markets:\n")
    for m in markets:
        status = "CLOSED" if m["closed"] else "ACTIVE"
        remaining = ""
        if m["end_date_utc"] and not m["closed"]:
            secs = (m["end_date_utc"] - now_utc).total_seconds()
            remaining = f" ({int(secs // 60)}m {int(secs % 60)}s left)"
        prob = ""
        if m["prob_up"] is not None:
            prob = f"  UP={m['prob_up']:.1%}  DOWN={m['prob_down']:.1%}"
        print(f"  [{status}] {m['title']}{remaining}")
        print(f"    Slug: {m['slug']}{prob}")
        print()


if __name__ == "__main__":
    main()
