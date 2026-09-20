"""Paper trading executor implementation."""

from typing import TYPE_CHECKING, Optional

from btc_pricer.constants import MIN_FILL_RATIO

from .executor import Executor, OrderResult, normalize_limit_buy, quantize_size
from .order_book import OrderBookClient
from .contracts import fee_for_fill, fee_schedule_valid
from .paper_fills import buy_with_fees, sell_fee_by_level

if TYPE_CHECKING:
    from .config import TraderConfig


class PaperExecutor(Executor):
    """Paper trading executor that simulates order fills.

    Uses the existing OrderBookClient to simulate market orders
    by walking through the order book without placing real trades.
    """

    def __init__(
        self,
        order_book_client: Optional[OrderBookClient] = None,
        config: Optional["TraderConfig"] = None,
    ):
        """Initialize the paper executor.

        Args:
            order_book_client: Client for fetching order book data.
                If None, creates a new client.
            config: Configuration for logging options. If None, logging is disabled.
        """
        self._order_book_client = order_book_client or OrderBookClient()
        self._config = config
        # Simulated order tracking for limit orders
        self._simulated_orders: dict[str, dict] = {}
        self._simulated_positions: dict[str, float] = {}
        self._next_order_id = 1
        self._fee_schedule: Optional[dict] = None
        self._position_fee_schedules: dict[str, dict] = {}

    def set_fee_schedule(self, schedule: Optional[dict], token_id: Optional[str] = None) -> None:
        """Set validated market context; retain it per held token across rollovers."""
        if schedule is not None and not fee_schedule_valid(schedule):
            raise ValueError("unknown or unsupported market fee schedule")
        if token_id is None:
            self._fee_schedule = dict(schedule) if schedule is not None else None
        elif schedule is None:
            self._position_fee_schedules.pop(token_id, None)
        else:
            self._position_fee_schedules[token_id] = dict(schedule)

    def _fee_metadata(self, fee: float, notional: float, schedule: Optional[dict]) -> dict:
        if schedule is None:
            return {}
        return {"market_fees_applied": True, "fee_usd": fee, "fill_notional": notional, "fee_schedule": dict(schedule)}

    def _should_log_orderbook(self) -> bool:
        """Check if orderbook logging is enabled."""
        return self._config is not None and self._config.log_orderbook

    def buy_market(self, token_id: str, usd_amount: float) -> OrderResult:
        """Simulate a market buy by walking through asks.

        Args:
            token_id: The CLOB token ID to buy.
            usd_amount: Amount in USD to spend.

        Returns:
            OrderResult with simulated fill details.
        """
        order_book = self._order_book_client.fetch_order_book(
            token_id,
            log_csv=self._should_log_orderbook(),
            csv_path=self._config.orderbook_file if self._config else None,
        )
        if not order_book:
            return OrderResult(
                success=False,
                error_message="Could not fetch order book",
            )

        # Round USD to 2 decimals (matches LiveExecutor)
        usd_amount = round(usd_amount, 2)

        fee = 0.0
        if self._fee_schedule is not None:
            shares, avg_price, notional, fee = buy_with_fees(order_book, usd_amount, self._fee_schedule)
            total_cost = notional + fee
        else:
            shares, avg_price, total_cost = self._order_book_client.simulate_market_buy(order_book, usd_amount)
            notional = total_cost

        if shares <= 0:
            return OrderResult(
                success=False,
                error_message="No liquidity available",
            )

        # Reject partial fills to match live FOK semantics
        if total_cost < usd_amount * MIN_FILL_RATIO:
            return OrderResult(
                success=False,
                error_message=f"Insufficient liquidity (filled ${total_cost:.2f} of ${usd_amount:.2f})",
            )

        # Track simulated position
        self._simulated_positions[token_id] = (
            self._simulated_positions.get(token_id, 0.0) + shares
        )
        if self._fee_schedule is not None:
            self._position_fee_schedules[token_id] = dict(self._fee_schedule)

        return OrderResult(
            success=True,
            order_id=None,  # No real order ID for simulated trades
            shares_filled=shares,
            avg_fill_price=avg_price,
            total_cost=total_cost,
            metadata=self._fee_metadata(fee, notional, self._fee_schedule),
        )

    def buy_limit(
        self,
        token_id: str,
        price: float,
        shares: float,
        order_type: str = "GTC",
        wait_for_fill_seconds: Optional[float] = None,
    ) -> OrderResult:
        """Simulate a limit buy order.

        FOK buys walk only asks at or below the limit price and require the
        full normalized share amount to be immediately fillable. This mirrors
        the live entry path, including the max-price cap.
        ``wait_for_fill_seconds`` is accepted for API parity with the live
        executor but is a no-op here (paper fills are deterministic).
        """
        price, shares, _ = normalize_limit_buy(price, shares)

        if shares < 5.0:
            return OrderResult(
                success=False,
                error_message=f"Limit buy size below minimum after precision normalization: {shares:.4f}",
            )

        order_type = order_type.upper()
        # GTC + wait_for_fill_seconds: IOC-like semantics — walk asks at-or-
        # below limit price ONCE and report fills. This mirrors live's new
        # entry path (cancel residual after timeout). Pure GTC without a
        # wait window keeps the legacy "register pending order" behavior.
        if order_type != "FOK" and not (wait_for_fill_seconds and wait_for_fill_seconds > 0):
            order_id = f"paper_order_{self._next_order_id}"
            self._next_order_id += 1
            self._simulated_orders[order_id] = {
                "status": "open",
                "size_matched": 0.0,
                "original_size": shares,
                "price": price,
                "side": "BUY",
                "token_id": token_id,
            }
            return OrderResult(
                success=True,
                order_id=order_id,
                shares_filled=0.0,
                avg_fill_price=price,
                total_cost=0.0,
            )

        order_book = self._order_book_client.fetch_order_book(
            token_id,
            log_csv=self._should_log_orderbook(),
            csv_path=self._config.orderbook_file if self._config else None,
        )
        if not order_book or not order_book.asks:
            return OrderResult(
                success=False,
                error_message="No asks available in order book",
            )

        remaining = shares
        total_cost = 0.0
        fee = 0.0
        for ask_price, ask_size in sorted(order_book.asks, key=lambda level: level[0]):
            if ask_price > price:
                break
            take = min(remaining, ask_size)
            total_cost += take * ask_price
            if self._fee_schedule is not None:
                fee += fee_for_fill(ask_price, take, self._fee_schedule)
            remaining -= take
            if remaining <= 1e-9:
                break

        # FOK requires full fill; partial → reject. GTC+wait accepts
        # whatever filled (cancel residual semantics). When NOTHING filled,
        # both return failure.
        filled = shares - remaining
        if order_type == "FOK" and remaining > 1e-9:
            return OrderResult(
                success=False,
                error_message=f"Limit FOK not filled at <=${price:.2f}",
            )
        if filled <= 1e-9:
            return OrderResult(
                success=False,
                error_message=f"Limit GTC: no asks at <=${price:.2f}",
            )

        avg_price = total_cost / filled if filled > 0 else 0.0
        self._simulated_positions[token_id] = (
            self._simulated_positions.get(token_id, 0.0) + filled
        )
        if self._fee_schedule is not None:
            self._position_fee_schedules[token_id] = dict(self._fee_schedule)

        return OrderResult(
            success=True,
            order_id=None,
            shares_filled=filled,
            avg_fill_price=avg_price,
            total_cost=total_cost + fee,
            metadata=self._fee_metadata(fee, total_cost, self._fee_schedule),
        )

    def sell_market(self, token_id: str, shares: float) -> OrderResult:
        """Simulate a market sell by walking through bids.

        Args:
            token_id: The CLOB token ID to sell.
            shares: Number of shares to sell.

        Returns:
            OrderResult with simulated fill details.
        """
        # Floor to the exchange size quantum; rounding upward invents inventory.
        shares = float(quantize_size(shares))

        order_book = self._order_book_client.fetch_order_book(
            token_id,
            log_csv=self._should_log_orderbook(),
            csv_path=self._config.orderbook_file if self._config else None,
        )
        if not order_book:
            return OrderResult(
                success=False,
                error_message="Could not fetch order book",
            )

        proceeds, avg_price, shares_sold = self._order_book_client.simulate_market_sell(
            order_book, shares
        )
        schedule = self._position_fee_schedules.get(token_id, self._fee_schedule)
        fee = sell_fee_by_level(order_book, shares_sold, schedule) if schedule is not None else 0.0

        if proceeds <= 0:
            return OrderResult(
                success=False,
                error_message="No liquidity available",
            )

        # Reject partial fills to match live FOK semantics
        if shares_sold < shares - 1e-8:
            return OrderResult(
                success=False,
                error_message=f"Insufficient liquidity (sold {shares_sold:.2f} of {shares:.2f} shares)",
            )

        # Track simulated position
        self._simulated_positions[token_id] = max(
            0.0, self._simulated_positions.get(token_id, 0.0) - shares_sold
        )
        if self._simulated_positions[token_id] <= 1e-9:
            self._position_fee_schedules.pop(token_id, None)

        return OrderResult(
            success=True,
            order_id=None,
            shares_filled=shares_sold,
            avg_fill_price=avg_price,
            total_cost=proceeds - fee,  # Net collateral proceeds after actual per-level fee model.
            metadata=self._fee_metadata(fee, proceeds, schedule),
        )

    def get_best_bid(self, token_id: str) -> Optional[float]:
        """Get the current best bid price.

        Args:
            token_id: The CLOB token ID.

        Returns:
            Best bid price or None if unavailable.
        """
        order_book = self._order_book_client.fetch_order_book(
            token_id,
            log_csv=self._should_log_orderbook(),
            csv_path=self._config.orderbook_file if self._config else None,
        )
        if order_book and order_book.best_bid > 0:
            return order_book.best_bid
        return None

    def get_best_ask(self, token_id: str) -> Optional[float]:
        """Get the current best ask price.

        Args:
            token_id: The CLOB token ID.

        Returns:
            Best ask price or None if unavailable.
        """
        order_book = self._order_book_client.fetch_order_book(
            token_id,
            log_csv=self._should_log_orderbook(),
            csv_path=self._config.orderbook_file if self._config else None,
        )
        if order_book and order_book.best_ask > 0:
            return order_book.best_ask
        return None

    def sell_limit(
        self,
        token_id: str,
        price: float,
        shares: float,
        order_type: str = "GTC",
    ) -> OrderResult:
        """Simulate limit sell order placement.

        Creates a simulated order that can be tracked via get_order_status().

        Args:
            token_id: The CLOB token ID to sell.
            price: Limit price per share.
            shares: Number of shares to sell.
            order_type: Order type (ignored in simulation).

        Returns:
            OrderResult with simulated order_id.
        """
        # Round to 2 decimals (matches LiveExecutor)
        price = round(price, 2)
        shares = float(quantize_size(shares))

        # Enforce Polymarket minimum for limit orders
        if shares < 5.0:
            return OrderResult(
                success=False,
                error_message="Minimum 5 shares for limit orders",
            )

        order_id = f"paper_order_{self._next_order_id}"
        self._next_order_id += 1

        self._simulated_orders[order_id] = {
            "status": "open",
            "size_matched": 0.0,
            "original_size": shares,
            "price": price,
            "side": "SELL",
            "token_id": token_id,
        }

        return OrderResult(
            success=True,
            order_id=order_id,
            shares_filled=0.0,
            avg_fill_price=price,
            total_cost=0.0,
        )

    def get_order_status(self, order_id: str) -> Optional[dict]:
        """Get simulated order status.

        Args:
            order_id: The order ID to check.

        Returns:
            Dict with order status or None if not found.
        """
        return self._simulated_orders.get(order_id)

    def cancel_order(self, order_id: str) -> bool:
        """Cancel a simulated order.

        Args:
            order_id: The order ID to cancel.

        Returns:
            True if cancelled, False if not found.
        """
        if order_id in self._simulated_orders:
            self._simulated_orders[order_id]["status"] = "cancelled"
            return True
        return False

    def simulate_order_fill(self, order_id: str, fill_amount: Optional[float] = None) -> bool:
        """Simulate a fill on a limit order (for testing).

        Args:
            order_id: The order ID to fill.
            fill_amount: Amount to fill. If None, fills completely.

        Returns:
            True if order was filled/partially filled.
        """
        if order_id not in self._simulated_orders:
            return False

        order = self._simulated_orders[order_id]
        if order["status"] != "open":
            return False

        original_size = order["original_size"]
        if fill_amount is None:
            fill_amount = original_size

        old_matched = order["size_matched"]
        new_matched = min(old_matched + fill_amount, original_size)
        order["size_matched"] = new_matched
        delta = new_matched - old_matched
        token_id = order["token_id"]
        if order["side"] == "SELL":
            self._simulated_positions[token_id] = max(0.0, self._simulated_positions.get(token_id, 0.0) - delta)
        else:
            self._simulated_positions[token_id] = self._simulated_positions.get(token_id, 0.0) + delta

        if new_matched >= original_size:
            order["status"] = "matched"

        return True

    @property
    def is_live(self) -> bool:
        """Return False - this is paper trading."""
        return False

    def get_position(self, token_id: str, strict: bool = False) -> float:
        """Return simulated position from internal state.

        Args:
            token_id: The CLOB token ID.
            strict: Ignored in paper mode (always returns a value).

        Returns:
            Number of simulated shares held.
        """
        return self._simulated_positions.get(token_id, 0.0)

    def set_position(self, token_id: str, shares: float) -> None:
        """Set simulated position (for testing).

        Args:
            token_id: The CLOB token ID.
            shares: Number of shares to set.
        """
        self._simulated_positions[token_id] = shares

    def get_open_orders(self) -> list:
        """Return simulated open orders.

        Returns:
            List of open order dicts.
        """
        result = []
        for order_id, order in self._simulated_orders.items():
            if order.get("status") == "open":
                result.append({
                    "order_id": order_id,
                    "token_id": order.get("token_id", ""),
                    "side": order.get("side", ""),
                    "price": order.get("price", 0.0),
                    "size": order.get("original_size", 0.0),
                    "filled": order.get("size_matched", 0.0),
                })
        return result

    def cancel_all_orders(self) -> bool:
        """Cancel all simulated orders.

        Returns:
            True always (simulation).
        """
        for order_id in list(self._simulated_orders.keys()):
            if self._simulated_orders[order_id].get("status") == "open":
                self._simulated_orders[order_id]["status"] = "cancelled"
        return True
