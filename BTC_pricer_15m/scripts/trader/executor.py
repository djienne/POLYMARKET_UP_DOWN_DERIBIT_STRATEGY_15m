"""Abstract executor interface for trading operations."""

import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP
from typing import Callable, Optional


@dataclass
class OrderResult:
    """Result of an order execution."""

    success: bool
    order_id: Optional[str] = None
    shares_filled: float = 0.0
    avg_fill_price: float = 0.0
    total_cost: float = 0.0
    error_message: Optional[str] = None
    ambiguous: bool = False
    metadata: dict = field(default_factory=dict)
    # Only not_submitted/rejected establish that another buy may be attempted.
    submission_state: str = ""

    @property
    def proceeds(self) -> float:
        """Calculate proceeds from a sell (shares * price)."""
        if self.metadata.get("market_fees_applied"):
            return self.total_cost
        return self.shares_filled * self.avg_fill_price


@dataclass
class CancelResult:
    """Structured result of an order cancellation attempt."""

    success: bool
    terminal_absent: bool = False
    retryable: bool = False
    reason: str = ""
    raw_response: Optional[object] = None


# ---------------------------------------------------------------------------
# Polymarket precision: per-market rounding config (mirrors upstream
# py_clob_client.order_builder.builder.ROUNDING_CONFIG).
#
# Polymarket's CLOB rejects orders whose fields exceed these decimal limits.
# The limits are determined by the market's tick_size (price increment).
# Look up via py_clob_client.client.ClobClient.get_tick_size(token_id).
#
#   price  = max decimals on the price (matches tick_size).
#   size   = max decimals on the share size (taker_amount for BUY).
#   amount = max decimals on the USDC maker_amount (i.e. price × size).
#
# For BTC up/down 15-min markets (tick=0.01) → price=2, size=2, amount=4.
# An earlier hard-coded rounding to size=4 caused order rejections — see
# plan tingly-foraging-pebble.md.
# ---------------------------------------------------------------------------

ROUNDING_CONFIG: dict[str, dict[str, int]] = {
    "0.1":    {"price": 1, "size": 2, "amount": 3},
    "0.01":   {"price": 2, "size": 2, "amount": 4},
    "0.001":  {"price": 3, "size": 2, "amount": 5},
    "0.0001": {"price": 4, "size": 2, "amount": 6},
}

# Legacy default used only by callers that haven't been migrated. New code
# must call rounding helpers with an explicit tick_size from
# client.get_tick_size(token_id).
DEFAULT_TICK_SIZE: str = "0.01"

PRICE_QUANT = Decimal("0.01")  # legacy alias; prefer quantize_price(tick_size)
SHARE_QUANT = Decimal("0.0001")  # legacy alias; prefer quantize_size(tick_size)
COST_QUANT = Decimal("0.01")  # legacy alias; prefer quantize_amount(tick_size)


def _quant(decimals: int) -> Decimal:
    """Return Decimal('0.001...') with the specified number of decimal places."""
    if decimals < 0:
        raise ValueError(f"decimals must be >= 0, got {decimals}")
    if decimals == 0:
        return Decimal("1")
    return Decimal("1").scaleb(-decimals)


def _round_config(tick_size: Optional[str]) -> dict[str, int]:
    if tick_size is None:
        tick_size = DEFAULT_TICK_SIZE
    cfg = ROUNDING_CONFIG.get(str(tick_size))
    if cfg is None:
        raise ValueError(
            f"unknown tick_size {tick_size!r}; supported: "
            f"{sorted(ROUNDING_CONFIG.keys())}"
        )
    return cfg


def quantize_price(price: float, tick_size: Optional[str] = None) -> Decimal:
    cfg = _round_config(tick_size)
    return Decimal(str(price)).quantize(_quant(cfg["price"]), rounding=ROUND_HALF_UP)


def quantize_size(shares: float, tick_size: Optional[str] = None) -> Decimal:
    cfg = _round_config(tick_size)
    return Decimal(str(shares)).quantize(_quant(cfg["size"]), rounding=ROUND_DOWN)


def normalize_limit_buy(
    price: float,
    shares: float,
    tick_size: Optional[str] = None,
) -> tuple[float, float, float]:
    """Normalize a Polymarket limit buy to valid CLOB precision.

    BUY orders must keep the USDC maker amount and outcome-token taker
    amount within Polymarket's per-market decimal limits (see
    ``ROUNDING_CONFIG``). Rounding to a fixed number of decimals is not
    sufficient — the *product* ``price × shares`` (the maker_amount) must
    also fit within ``cfg["amount"]`` decimals. We decrement shares one
    size-quantum at a time until the product is exactly representable.

    ``tick_size`` is the market's CLOB tick (e.g. ``"0.01"``); fetch via
    ``client.get_tick_size(token_id)``. Defaults to ``"0.01"`` for
    backwards compatibility with callers that haven't been migrated.

    Returns (price, shares, max_cost), with shares floored so the order
    never exceeds the requested size.
    """
    cfg = _round_config(tick_size)
    price_q = _quant(cfg["price"])
    size_q = _quant(cfg["size"])
    amount_q = _quant(cfg["amount"])

    price_dec = Decimal(str(price)).quantize(price_q, rounding=ROUND_HALF_UP)
    shares_dec = Decimal(str(shares)).quantize(size_q, rounding=ROUND_DOWN)

    while shares_dec > 0:
        max_cost = price_dec * shares_dec
        if max_cost == max_cost.quantize(amount_q):
            return float(price_dec), float(shares_dec), float(max_cost)
        shares_dec -= size_q

    return float(price_dec), 0.0, 0.0


class Executor(ABC):
    """Abstract base class for trade execution.

    Provides a common interface for both paper trading
    and live trading executors.
    """

    @abstractmethod
    def buy_market(self, token_id: str, usd_amount: float) -> OrderResult:
        """Execute a market buy order.

        Args:
            token_id: The CLOB token ID to buy.
            usd_amount: Amount in USD to spend.

        Returns:
            OrderResult with fill details or error.
        """
        pass

    @abstractmethod
    def sell_market(self, token_id: str, shares: float) -> OrderResult:
        """Execute a market sell order.

        Args:
            token_id: The CLOB token ID to sell.
            shares: Number of shares to sell.

        Returns:
            OrderResult with fill details or error.
        """
        pass

    def buy_limit(
        self,
        token_id: str,
        price: float,
        shares: float,
        order_type: str = "GTC",
        wait_for_fill_seconds: Optional[float] = None,
    ) -> OrderResult:
        """Place a limit buy order.

        Implementors may override this. The default keeps older test stubs and
        simple executors working by falling back to a market buy for the
        requested notional. ``wait_for_fill_seconds`` is honoured by the live
        executor (poll-and-cancel-residual semantics for IOC-like entries);
        paper / fallback implementations ignore it.
        """
        return self.buy_market(token_id, price * shares)

    @abstractmethod
    def get_best_bid(self, token_id: str) -> Optional[float]:
        """Get the current best bid price for a token.

        Args:
            token_id: The CLOB token ID.

        Returns:
            Best bid price or None if unavailable.
        """
        pass

    @abstractmethod
    def get_best_ask(self, token_id: str) -> Optional[float]:
        """Get the current best ask price for a token.

        Args:
            token_id: The CLOB token ID.

        Returns:
            Best ask price or None if unavailable.
        """
        pass

    def get_mid_price(self, token_id: str) -> Optional[float]:
        """Get the mid price (average of best bid and ask) for a token.

        Args:
            token_id: The CLOB token ID.

        Returns:
            Mid price, or best bid/ask if only one side available, or None.
        """
        bid = self.get_best_bid(token_id)
        ask = self.get_best_ask(token_id)
        if bid is not None and ask is not None:
            return (bid + ask) / 2
        return bid or ask or None

    @abstractmethod
    def sell_limit(
        self,
        token_id: str,
        price: float,
        shares: float,
        order_type: str = "GTC",
    ) -> OrderResult:
        """Place a limit sell order.

        Args:
            token_id: The CLOB token ID to sell.
            price: Limit price per share.
            shares: Number of shares to sell.
            order_type: Order type - "GTC", "FOK", or "GTD".

        Returns:
            OrderResult with order_id.
        """
        pass

    @abstractmethod
    def get_order_status(self, order_id: str) -> Optional[dict]:
        """Get the status of an order.

        Args:
            order_id: The order ID to check.

        Returns:
            Dict with order status including:
            - status: 'open', 'matched', 'cancelled'
            - size_matched: amount filled
            - original_size: total order size
            - price: order price
            Or None if unavailable.
        """
        pass

    @abstractmethod
    def cancel_order(self, order_id: str) -> bool:
        """Cancel an open order.

        Args:
            order_id: The order ID to cancel.

        Returns:
            True if cancelled successfully.
        """
        pass

    def cancel_order_detailed(self, order_id: str, retries: int = 3) -> CancelResult:
        """Cancel an order, preserving a reason for settlement-specific logic."""
        try:
            success = self.cancel_order(order_id)
        except Exception as exc:
            return CancelResult(
                success=False,
                retryable=True,
                reason=str(exc),
            )
        return CancelResult(
            success=success,
            retryable=not success,
            reason="" if success else "cancel_order_failed",
        )

    @property
    @abstractmethod
    def is_live(self) -> bool:
        """Return True if this executor trades with real money."""
        pass

    @abstractmethod
    def get_position(self, token_id: str, strict: bool = False) -> float:
        """Get shares held for a token.

        Args:
            token_id: The CLOB token ID.
            strict: If True, raise error on failure instead of returning 0.

        Returns:
            Number of shares held (0.0 if none or error).

        Raises:
            RuntimeError: If strict=True and position cannot be determined.
        """
        pass

    def get_trades(self, token_id: str, since_timestamp: Optional[float] = None) -> list[dict]:
        """Get trade history for a token.

        Args:
            token_id: The CLOB token ID.
            since_timestamp: Only return trades after this Unix timestamp.

        Returns:
            List of trade dicts with keys:
            - id: Trade ID
            - taker_order_id: Order ID of the taker
            - size: Number of shares
            - price: Fill price
            - side: BUY or SELL
            - match_time: ISO timestamp of match
        """
        return []

    @abstractmethod
    def get_open_orders(self) -> list:
        """Get all open orders.

        Returns:
            List of open order dicts with keys:
            - order_id: Order ID
            - token_id: Token ID
            - side: BUY or SELL
            - price: Order price
            - size: Original size
            - filled: Amount filled
        """
        pass

    @abstractmethod
    def cancel_all_orders(self) -> bool:
        """Cancel all open orders.

        Returns:
            True if cancelled successfully.
        """
        pass

    def get_usdc_balance(self) -> Optional[float]:
        """Get USDC balance. Override in subclasses.

        Returns:
            USDC balance in dollars, or None if query failed.
        """
        return None

    def wait_for_position(
        self,
        token_id: str,
        target: float,
        comparison: str = "gte",
        timeout_seconds: float = 30.0,
    ) -> float:
        """Wait for position to reach target shares.

        Uses exponential backoff to poll position until target is met.

        Args:
            token_id: Token ID to check.
            target: Target share count.
            comparison: "gte" (>=), "lte" (<=), or "eq" (==).
            timeout_seconds: Maximum wait time.

        Returns:
            Final position when target met.

        Raises:
            TimeoutError: If target not met within timeout.
        """
        comparators: dict[str, Callable[[float, float], bool]] = {
            "gte": lambda pos, t: pos >= t,
            "lte": lambda pos, t: pos <= t,
            "eq": lambda pos, t: abs(pos - t) < 0.01,
        }
        compare_fn = comparators.get(comparison, comparators["gte"])

        start = time.time()
        delay = 1.0

        while time.time() - start < timeout_seconds:
            position = self.get_position(token_id)
            if compare_fn(position, target):
                return position
            time.sleep(delay)
            delay = min(delay * 1.5, 5.0)

        final_pos = self.get_position(token_id)
        raise TimeoutError(
            f"Position {final_pos:.4f} did not reach target {target} "
            f"({comparison}) within {timeout_seconds}s"
        )

    def wait_for_order_on_book(
        self,
        order_id: str,
        timeout_seconds: float = 15.0,
        verify_live: bool = True,
    ) -> Optional[dict]:
        """Wait for order to appear on book and optionally verify LIVE status.

        Uses exponential backoff to poll open orders until order is found.

        Args:
            order_id: Order ID to check.
            timeout_seconds: Maximum wait time.
            verify_live: If True, verify order status is LIVE.

        Returns:
            Order status dict if verify_live=True, else True.

        Raises:
            TimeoutError: If order not found within timeout.
            RuntimeError: If order found but not LIVE (when verify_live=True).
        """
        start = time.time()
        delay = 0.5

        while time.time() - start < timeout_seconds:
            open_orders = self.get_open_orders()
            order_ids = [o["order_id"] for o in open_orders]

            if order_id in order_ids:
                if verify_live:
                    # Get order status to verify it's LIVE
                    status = self.get_order_status(order_id)
                    if status:
                        order_status = status.get("status", "").lower()
                        # Accept various representations of "live" status
                        if order_status in ("live", "open", "active"):
                            return status
                        elif order_status in ("matched", "cancelled", "expired"):
                            raise RuntimeError(
                                f"Order {order_id} not live: status={order_status}"
                            )
                        # If status is unknown but order is in open orders, treat as live
                        return status
                return {"order_id": order_id, "status": "live"}

            time.sleep(delay)
            delay = min(delay * 1.5, 3.0)

        raise TimeoutError(
            f"Order {order_id} not found on book within {timeout_seconds}s"
        )

    def validate_position_readable(self, token_id: str) -> float:
        """Validate that position can be read for a token.

        Performs two consecutive reads and ensures they're consistent.

        Args:
            token_id: Token ID to check position for.

        Returns:
            The position value if readable.

        Raises:
            RuntimeError: If position cannot be determined or reads are inconsistent.
        """
        try:
            position1 = self.get_position(token_id, strict=True)
            time.sleep(0.5)  # Brief delay between reads
            position2 = self.get_position(token_id, strict=True)

            # Check for consistent reads (allow small variance)
            if abs(position1 - position2) > 0.001:
                raise RuntimeError(
                    f"Inconsistent position reads: {position1} vs {position2}"
                )
            return position1
        except RuntimeError:
            raise
        except Exception as e:
            raise RuntimeError(f"Position detection failed: {e}")
