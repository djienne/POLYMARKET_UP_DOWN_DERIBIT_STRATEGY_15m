"""CLOB API interaction for order book data."""

import csv
import logging
import os
import tempfile
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Optional
import requests

CLOB_URL = "https://clob.polymarket.com"
log = logging.getLogger(__name__)

# Rolling window of last N request durations (ms) — written to
# results/.clob_latency_ms so the dashboard can show the real, warm latency
# the live trader actually experiences (not a fresh-connection probe).
_LATENCY_WINDOW: deque[float] = deque(maxlen=12)
PROJECT_ROOT = Path(__file__).resolve().parents[2]
_LATENCY_PATH = PROJECT_ROOT / "results" / ".clob_latency_ms"


def _record_clob_latency(dt_ms: float) -> None:
    _LATENCY_WINDOW.append(dt_ms)
    if len(_LATENCY_WINDOW) < 2:
        return
    med = median(_LATENCY_WINDOW)
    # Atomic write so the dashboard can't read a half-written file.
    try:
        _LATENCY_PATH.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(
            dir=str(_LATENCY_PATH.parent), suffix=".tmp", prefix=".clob_"
        )
        try:
            os.chmod(tmp, 0o644)
        except OSError:
            pass
        with os.fdopen(fd, "w") as f:
            # "<median_ms> <samples> <epoch>"
            f.write(f"{med:.1f} {len(_LATENCY_WINDOW)} {int(time.time())}\n")
        Path(tmp).replace(_LATENCY_PATH)
    except OSError:
        pass


def log_orderbook_to_csv(
    token_id: str,
    best_bid: float,
    best_ask: float,
    spread: float,
    bid_depth: int,
    ask_depth: int,
    csv_path: Path,
    sanity_ok: bool,
    reference_price: Optional[float] = None,
) -> None:
    """Append orderbook snapshot to CSV file.

    Args:
        token_id: The CLOB token ID.
        best_bid: Highest bid price.
        best_ask: Lowest ask price.
        spread: best_ask - best_bid.
        bid_depth: Number of bid levels.
        ask_depth: Number of ask levels.
        csv_path: Path to CSV file.
        sanity_ok: True if best_bid < best_ask (valid spread).
        reference_price: Reference price for this market (for backtest join key).
    """
    file_exists = csv_path.exists()

    row = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "token_id": token_id,
        "best_bid": best_bid,
        "best_ask": best_ask,
        "spread": spread,
        "spread_pct": (spread / best_ask * 100) if best_ask > 0 else 0,
        "bid_depth": bid_depth,
        "ask_depth": ask_depth,
        "sanity_ok": sanity_ok,
        "reference_price": reference_price if reference_price is not None else "",
    }

    with open(csv_path, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=row.keys())
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)


@dataclass
class OrderBookSnapshot:
    """Snapshot of order book at a point in time."""

    token_id: str
    best_bid: float
    best_ask: float
    bids: list[tuple[float, float]]  # (price, size) sorted desc by price
    asks: list[tuple[float, float]]  # (price, size) sorted asc by price
    timestamp: str


@dataclass
class MutableOrderBook:
    """Mutable order book used for shared-capacity paper fills."""

    token_id: str
    bids: list[list[float]]  # [price, size] sorted desc by price
    asks: list[list[float]]  # [price, size] sorted asc by price
    timestamp: str = ""

    @classmethod
    def from_snapshot(cls, snapshot: Optional[OrderBookSnapshot]) -> Optional["MutableOrderBook"]:
        """Create a mutable copy of an immutable snapshot."""
        if snapshot is None:
            return None
        return cls(
            token_id=snapshot.token_id,
            bids=[[price, size] for price, size in snapshot.bids],
            asks=[[price, size] for price, size in snapshot.asks],
            timestamp=snapshot.timestamp,
        )

    @property
    def best_bid(self) -> float:
        return self.bids[0][0] if self.bids else 0.0

    @property
    def best_ask(self) -> float:
        return self.asks[0][0] if self.asks else 0.0

    def simulate_market_buy(self, usd_amount: float, consume: bool = False) -> tuple[float, float, float]:
        """Simulate or consume a market buy against asks."""
        total_shares, avg_fill_price, total_cost = _walk_buy_levels(
            self.asks,
            usd_amount,
            consume=consume,
        )
        if consume:
            self.asks = [level for level in self.asks if level[1] > 1e-9]
        return total_shares, avg_fill_price, total_cost

    def simulate_market_sell(self, shares: float, consume: bool = False) -> tuple[float, float, float]:
        """Simulate or consume a market sell against bids."""
        total_proceeds, avg_fill_price, shares_sold = _walk_sell_levels(
            self.bids,
            shares,
            consume=consume,
        )
        if consume:
            self.bids = [level for level in self.bids if level[1] > 1e-9]
        return total_proceeds, avg_fill_price, shares_sold


def _walk_buy_levels(
    levels,
    usd_amount: float,
    *,
    consume: bool = False,
) -> tuple[float, float, float]:
    """Walk ask levels for a market buy.

    Returns ``(shares_bought, avg_fill_price, total_cost)``. Mutable books pass
    list levels and set ``consume=True``; immutable snapshots pass tuple levels.
    """
    if not levels:
        return 0.0, 0.0, 0.0

    remaining_usd = usd_amount
    total_shares = 0.0
    total_cost = 0.0

    for level in levels:
        if remaining_usd <= 0:
            break

        price, size = level
        cost_for_all = size * price

        if cost_for_all <= remaining_usd:
            total_shares += size
            total_cost += cost_for_all
            remaining_usd -= cost_for_all
            if consume:
                level[1] = 0.0
        else:
            shares_can_buy = remaining_usd / price
            total_shares += shares_can_buy
            total_cost += remaining_usd
            if consume:
                level[1] = max(0.0, size - shares_can_buy)
            remaining_usd = 0.0

    avg_fill_price = total_cost / total_shares if total_shares > 0 else 0.0
    return total_shares, avg_fill_price, total_cost


def _walk_sell_levels(
    levels,
    shares: float,
    *,
    consume: bool = False,
) -> tuple[float, float, float]:
    """Walk bid levels for a market sell.

    Returns ``(proceeds, avg_fill_price, shares_sold)``.
    """
    if not levels:
        return 0.0, 0.0, 0.0

    remaining_shares = shares
    total_proceeds = 0.0

    for level in levels:
        if remaining_shares <= 0:
            break

        price, size = level
        if size <= remaining_shares:
            total_proceeds += size * price
            remaining_shares -= size
            if consume:
                level[1] = 0.0
        else:
            total_proceeds += remaining_shares * price
            if consume:
                level[1] = max(0.0, size - remaining_shares)
            remaining_shares = 0.0

    shares_sold = shares - remaining_shares
    avg_fill_price = total_proceeds / shares_sold if shares_sold > 0 else 0.0
    return total_proceeds, avg_fill_price, shares_sold


def clone_order_book(snapshot: Optional[OrderBookSnapshot]) -> Optional[OrderBookSnapshot]:
    """Deep copy a snapshot so tests and callers can mutate safely."""
    if snapshot is None:
        return None
    return OrderBookSnapshot(
        token_id=snapshot.token_id,
        best_bid=snapshot.best_bid,
        best_ask=snapshot.best_ask,
        bids=list(snapshot.bids),
        asks=list(snapshot.asks),
        timestamp=snapshot.timestamp,
    )


class OrderBookClient:
    """Client for fetching CLOB order book data."""

    def __init__(self, timeout: int = 10):
        self.timeout = timeout
        # Persistent Session → urllib3 connection pool reuses TCP+TLS across
        # calls (keep-alive), so latency after the first call reflects the
        # warm-connection cost only. This is what the live trader actually
        # sees on its 5-second CLOB polls.
        self._session = requests.Session()

    def fetch_order_book(
        self,
        token_id: str,
        log_csv: bool = False,
        csv_path: Optional[Path] = None,
    ) -> Optional[OrderBookSnapshot]:
        """Fetch order book for a token.

        Args:
            token_id: The CLOB token ID
            log_csv: Whether to log best bid/ask to CSV.
            csv_path: Path to CSV file (required if log_csv=True).

        Returns:
            OrderBookSnapshot or None if fetch fails
        """
        url = f"{CLOB_URL}/book"
        params = {"token_id": token_id}

        try:
            _t0 = time.perf_counter()
            response = self._session.get(url, params=params, timeout=self.timeout)
            _dt_ms = (time.perf_counter() - _t0) * 1000.0
            response.raise_for_status()
            data = response.json()
            _record_clob_latency(_dt_ms)

            # Parse bids and asks
            bids = []
            for bid in data.get("bids", []):
                price = float(bid.get("price", 0))
                size = float(bid.get("size", 0))
                if price > 0 and size > 0:
                    bids.append((price, size))

            asks = []
            for ask in data.get("asks", []):
                price = float(ask.get("price", 0))
                size = float(ask.get("size", 0))
                if price > 0 and size > 0:
                    asks.append((price, size))

            # Sort: bids descending (best bid first), asks ascending (best ask first)
            bids.sort(key=lambda x: x[0], reverse=True)
            asks.sort(key=lambda x: x[0])

            best_bid = bids[0][0] if bids else 0.0
            best_ask = asks[0][0] if asks else 0.0

            # Sanity check: best_bid < best_ask (spread > 0)
            sanity_ok = best_bid < best_ask if (best_bid > 0 and best_ask > 0) else True
            spread = best_ask - best_bid if (best_bid > 0 and best_ask > 0) else 0.0

            # Log to CSV if requested
            if log_csv and csv_path:
                log_orderbook_to_csv(
                    token_id=token_id,
                    best_bid=best_bid,
                    best_ask=best_ask,
                    spread=spread,
                    bid_depth=len(bids),
                    ask_depth=len(asks),
                    csv_path=csv_path,
                    sanity_ok=sanity_ok,
                )

            return OrderBookSnapshot(
                token_id=token_id,
                best_bid=best_bid,
                best_ask=best_ask,
                bids=bids,
                asks=asks,
                timestamp=data.get("timestamp", ""),
            )

        except requests.RequestException as e:
            log.warning("Error fetching order book: %s", e)
            return None
        except (KeyError, ValueError) as e:
            log.warning("Error parsing order book: %s", e)
            return None

    def simulate_market_buy(
        self, order_book: OrderBookSnapshot, usd_amount: float
    ) -> tuple[float, float, float]:
        """Simulate a market buy by walking through asks.

        Args:
            order_book: Current order book snapshot
            usd_amount: Amount in USD to spend

        Returns:
            (shares_bought, avg_fill_price, total_cost)
        """
        return _walk_buy_levels(order_book.asks, usd_amount)

    def simulate_market_sell(
        self, order_book: OrderBookSnapshot, shares: float
    ) -> tuple[float, float, float]:
        """Simulate a market sell by walking through bids.

        Args:
            order_book: Current order book snapshot
            shares: Number of shares to sell

        Returns:
            (proceeds, avg_fill_price, shares_sold)
        """
        return _walk_sell_levels(order_book.bids, shares)
