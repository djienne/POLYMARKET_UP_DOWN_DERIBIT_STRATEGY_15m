"""Live trading executor using py-clob-client-v2."""

import logging
import os
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Callable, Optional

from dotenv import load_dotenv

from .config import DEFAULT_POLYGON_RPC_URLS
from .executor import CancelResult, Executor, OrderResult, normalize_limit_buy
from .order_book import log_orderbook_to_csv
from ._polymarket import create_clob_client

if TYPE_CHECKING:
    from .config import TraderConfig

# Load environment variables from .env file
load_dotenv()

log = logging.getLogger(__name__)


def _emit(*args, sep: str = " ", end: str = "\n", flush: bool = False) -> None:
    """Route live executor operational output through logging."""
    message = sep.join(str(arg) for arg in args)
    if end and end != "\n":
        message += end
    message = message.strip("\n")
    if not message:
        return
    for raw_line in message.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        upper = line.upper()
        if "CRITICAL" in upper:
            log.critical(line)
        elif "[ERROR]" in upper or upper.startswith("ERROR"):
            log.error(line)
        elif "[WARN]" in upper or "WARNING" in upper or "FAILED" in upper:
            log.warning(line)
        else:
            log.info(line)


@dataclass(frozen=True)
class CancelAllResult:
    """Structured result for batch order cancellation."""

    success: bool
    canceled: list[str] = field(default_factory=list)
    not_canceled: dict[str, str] = field(default_factory=dict)
    error: str = ""
    raw_response: object = None


class LiveExecutor(Executor):
    """Live trading executor using Polymarket CLOB API.

    Uses py-clob-client-v2 to place real orders. Entry limit buys use
    GTC plus a short wait/cancel window; market exits still use FOK.
    """

    def __init__(
        self,
        private_key: Optional[str] = None,
        funder: Optional[str] = None,
        signature_type: int = 1,
        max_slippage_pct: float = 0.02,
        config: Optional["TraderConfig"] = None,
        delayed_order_timeout_seconds: Optional[float] = None,
    ):
        """Initialize the live executor.

        Args:
            private_key: Polymarket private key. If None, reads from
                POLYMARKET_PRIVATE_KEY environment variable.
            funder: Funder address for Magic/browser wallets. If None,
                reads from POLYMARKET_FUNDER or ADDRESS environment variable.
            signature_type: Signature type (0=EOA, 1=Magic, 2=Browser).
            max_slippage_pct: Maximum allowed slippage (default 2%).
            config: Configuration for logging options. If None, logging is disabled.
            delayed_order_timeout_seconds: Override for _poll_delayed_order
                timeout. If None, falls back to config.delayed_order_timeout_seconds
                when available, else 30s (legacy default).
        """
        self._private_key = private_key or os.getenv("POLYMARKET_PRIVATE_KEY")
        self._funder = funder or os.getenv("POLYMARKET_FUNDER") or os.getenv("ADDRESS")
        self._signature_type = signature_type
        self._max_slippage_pct = max_slippage_pct
        self._config = config
        if delayed_order_timeout_seconds is not None:
            self._delayed_order_timeout_seconds = float(delayed_order_timeout_seconds)
        elif config is not None and hasattr(config, "delayed_order_timeout_seconds"):
            self._delayed_order_timeout_seconds = float(config.delayed_order_timeout_seconds)
        else:
            self._delayed_order_timeout_seconds = 30.0
        self._post_cancel_reconcile_seconds = float(
            getattr(config, "post_cancel_reconcile_seconds", 3.0)
        )
        self._post_cancel_reconcile_poll_seconds = float(
            getattr(config, "post_cancel_reconcile_poll_seconds", 0.5)
        )
        self._post_cancel_ambiguous_on_unreadable_balance = bool(
            getattr(config, "post_cancel_ambiguous_on_unreadable_balance", True)
        )
        self._client = None
        self._initialized = False
        self._entry_intent_handler: Optional[Callable[[dict], None]] = None

    def set_entry_intent_handler(self, handler: Optional[Callable[[dict], None]]) -> None:
        """Install the trader's durable, before-submit intent writer."""
        self._entry_intent_handler = handler

    def _signed_order_id(self, signed_order, options) -> str:
        """Compute the venue's EIP-712 order hash without retaining a signature."""
        from py_clob_client_v2.config import get_contract_config
        from py_clob_client_v2.order_utils import ExchangeOrderBuilderV1, ExchangeOrderBuilderV2

        signer = self._client.signer
        chain_id = signer.get_chain_id()
        contracts = get_contract_config(chain_id)
        version_two = hasattr(signed_order, "timestamp")
        if version_two:
            address = contracts.neg_risk_exchange_v2 if options.neg_risk else contracts.exchange_v2
            builder = ExchangeOrderBuilderV2(address, chain_id, signer)
        else:
            address = contracts.neg_risk_exchange if options.neg_risk else contracts.exchange
            builder = ExchangeOrderBuilderV1(address, chain_id, signer)
        return builder.build_order_hash(builder.build_order_typed_data(signed_order))

    def _post_signed_buy(self, args, order_type, lifecycle: dict, *, market_order: bool = False):
        """Persist a recoverable identity before crossing the uncertain POST boundary.

        A response timeout is not evidence of rejection. Only explicit venue
        rejection or a failure before POST permits another independently signed buy.
        """
        options = self._order_options(args.token_id)
        if options.neg_risk is None:
            options.neg_risk = self._client.get_neg_risk(args.token_id)
        creator = self._client.create_market_order if market_order else self._client.create_order
        signed = creator(args, options=options)
        order_hash = self._signed_order_id(signed, options)
        lifecycle.update({"submitted_order_hash": order_hash, "submission_state": "not_submitted"})
        handler = self._entry_intent_handler
        if self._config is not None and handler is None:
            raise RuntimeError("Live buy requires a durable entry intent handler")
        if handler is not None:
            handler(dict(lifecycle, submission_state="submitting"))
        lifecycle["submission_state"] = "uncertain"
        response = self._client.post_order(signed, order_type=order_type)
        if not isinstance(response, dict):
            raise ValueError("Unrecognized order response after submission")
        status = str(response.get("status") or "").lower()
        accepted = self._order_id_from_response(response) or status in {
            "matched", "live", "open", "active", "delayed", "pending", "unmatched",
        }
        if accepted:
            lifecycle["submission_state"] = "accepted"
            if not self._order_id_from_response(response):
                response = dict(response, orderID=order_hash)
                lifecycle["order_id_source"] = "signed_hash"
        elif response.get("success") is False or status in {"rejected", "failed"}:
            lifecycle["submission_state"] = "rejected"
        return response

    def _ensure_client(self) -> None:
        """Lazily initialize the CLOB client."""
        if self._initialized:
            return

        try:
            ctx = create_clob_client(
                private_key=self._private_key,
                funder=self._funder,
                signature_type=self._signature_type,
                allow_legacy=False,
            )
        except ImportError:
            raise ImportError(
                "py-clob-client-v2 is required for live trading. "
                "Install with: pip install py-clob-client-v2==1.0.0"
            )
        self._client = ctx.client
        self._funder = ctx.funder or self._funder
        self._initialized = True

    @staticmethod
    def _field(obj, *names: str, default=None):
        """Read a field from either dict-style or object-style SDK responses."""
        if isinstance(obj, dict):
            for name in names:
                if name in obj:
                    return obj.get(name)
            return default
        for name in names:
            if hasattr(obj, name):
                return getattr(obj, name)
        return default

    @classmethod
    def _order_id_from_response(cls, response) -> Optional[str]:
        order_id = cls._field(response, "orderID", "order_id", "id", default=None)
        return str(order_id) if order_id else None

    @classmethod
    def _book_prices(cls, book, side: str) -> list[float]:
        rows = cls._field(book, side, default=[]) or []
        prices = []
        for row in rows:
            price = cls._field(row, "price", default=None)
            if price is not None:
                prices.append(float(price))
        return sorted(prices, reverse=(side == "bids"))

    def _resolve_tick_size(self, token_id: str) -> str:
        """Look up the market's CLOB tick_size for precision rounding.

        Calls upstream ``client.get_tick_size`` (15 min cache). Falls back
        to ``"0.01"`` on failure — the most common tick for Polymarket
        binary markets and a safe default that exceeds upstream's
        rounding for narrower-tick markets.
        """
        try:
            ts = self._client.get_tick_size(token_id)
            if ts:
                return str(ts)
        except Exception as exc:
            log.debug("get_tick_size failed for token %s; using 0.01: %s", token_id, exc)
        return "0.01"

    def _order_options(self, token_id: str):
        """Build V2 order options with market tick/neg-risk metadata."""
        from py_clob_client_v2 import PartialCreateOrderOptions

        tick_size = self._resolve_tick_size(token_id)
        neg_risk = None
        try:
            neg_risk = self._client.get_neg_risk(token_id)
        except Exception as exc:
            log.debug("get_neg_risk failed for token %s; omitting option: %s", token_id, exc)
            neg_risk = None
        return PartialCreateOrderOptions(tick_size=tick_size, neg_risk=neg_risk)

    def _wait_for_position_at_least(
        self,
        token_id: str,
        min_shares: float,
        timeout_seconds: float = 12.0,
    ) -> tuple[bool, Optional[float]]:
        """Wait for CLOB conditional-token balance to reflect a matched buy."""
        import time

        deadline = time.time() + timeout_seconds
        last_balance: Optional[float] = None
        while time.time() < deadline:
            try:
                last_balance = self.get_position(token_id, strict=True)
                if last_balance + 1e-6 >= min_shares:
                    return True, last_balance
            except RuntimeError as exc:
                log.debug("position-at-least poll failed for token %s: %s", token_id, exc)
            time.sleep(0.5)
        return False, last_balance

    def _wait_for_position_at_most(
        self,
        token_id: str,
        max_shares: float,
        timeout_seconds: float = 12.0,
    ) -> tuple[bool, Optional[float]]:
        """Wait for CLOB conditional-token balance to reflect a matched sell."""
        import time

        deadline = time.time() + timeout_seconds
        last_balance: Optional[float] = None
        while time.time() < deadline:
            try:
                last_balance = self.get_position(token_id, strict=True)
                if last_balance <= max_shares + 1e-6:
                    return True, last_balance
            except RuntimeError as exc:
                log.debug("position-at-most poll failed for token %s: %s", token_id, exc)
            time.sleep(0.5)
        return False, last_balance

    def _confirmed_buy_result(
        self,
        token_id: str,
        order_id: Optional[str],
        shares: float,
        avg_price: float,
        cost: float,
        metadata: Optional[dict] = None,
    ) -> OrderResult:
        """Return buy success only after the matched shares are balance-visible."""
        metadata = dict(metadata or {})
        pre_balance = self._safe_float(metadata.get("pre_balance"), 0.0)
        ok, observed = self._wait_for_position_at_least(
            token_id,
            max(0.0, pre_balance + shares - 0.01),
        )
        metadata["balance_visible"] = str(bool(ok))
        metadata["observed_balance"] = "" if observed is None else f"{float(observed):.4f}"
        if not ok:
            return OrderResult(
                success=False,
                order_id=order_id,
                error_message=(
                    "Matched buy order, but conditional-token balance is not "
                    f"visible yet (expected {shares:.4f}, observed {observed})"
                ),
                ambiguous=True,
                metadata=metadata,
            )
        receipt = self.get_execution_cashflow(order_id, token_id, "BUY", shares, time.time() - 300)
        if receipt is not None:
            avg_price = float(receipt.gross_collateral / receipt.shares)
            cost = -float(receipt.net_cashflow)
            metadata.update({"cost_basis_source": "v2_receipt", "fee_collateral": float(receipt.fee_collateral)})
        else:
            metadata["cost_basis_source"] = "order_status"
        return OrderResult(
            success=True,
            order_id=order_id,
            shares_filled=shares,
            avg_fill_price=avg_price,
            total_cost=cost,
            metadata=metadata,
            submission_state="accepted",
        )

    def _confirmed_sell_result(
        self,
        token_id: str,
        order_id: Optional[str],
        pre_balance: Optional[float],
        shares: float,
        avg_price: float,
        proceeds: float,
    ) -> OrderResult:
        """Return sell success after the matched sell is reflected in balance."""
        if pre_balance is not None:
            expected_remaining = max(0.0, pre_balance - shares)
            ok, observed = self._wait_for_position_at_most(token_id, expected_remaining + 0.01)
            if not ok:
                return OrderResult(
                    success=False,
                    order_id=order_id,
                    error_message=(
                        "Matched sell order, but conditional-token balance has "
                        f"not updated yet (expected <= {expected_remaining:.4f}, "
                        f"observed {observed})"
                    ),
                    ambiguous=True,
                )
        return OrderResult(
            success=True,
            order_id=order_id,
            shares_filled=shares,
            avg_fill_price=avg_price,
            total_cost=proceeds,
        )

    def _should_log_orderbook(self) -> bool:
        """Check if orderbook logging is enabled."""
        return self._config is not None and self._config.log_orderbook

    def _log_orderbook(
        self,
        token_id: str,
        bids: list[float],
        asks: list[float],
    ) -> None:
        """Log orderbook best bid/ask to CSV if enabled.

        Args:
            token_id: The CLOB token ID.
            bids: List of bid prices (sorted descending).
            asks: List of ask prices (sorted ascending).
        """
        if not self._should_log_orderbook() or not self._config:
            return

        best_bid = bids[0] if bids else 0.0
        best_ask = asks[0] if asks else 0.0
        spread = best_ask - best_bid if (best_bid > 0 and best_ask > 0) else 0.0
        sanity_ok = best_bid < best_ask if (best_bid > 0 and best_ask > 0) else True

        log_orderbook_to_csv(
            token_id=token_id,
            best_bid=best_bid,
            best_ask=best_ask,
            spread=spread,
            bid_depth=len(bids),
            ask_depth=len(asks),
            csv_path=self._config.orderbook_file,
            sanity_ok=sanity_ok,
        )

    def _poll_delayed_order(
        self,
        order_id: str,
        timeout_seconds: Optional[float] = None,
    ) -> Optional[dict]:
        """Poll a delayed order until resolved (matched/cancelled/expired) or timeout."""
        import time
        if not order_id:
            return None
        if timeout_seconds is None:
            timeout_seconds = self._delayed_order_timeout_seconds
        start = time.time()
        delay = 1.0
        while time.time() - start < timeout_seconds:
            status = self.get_order_status(order_id)
            if status is None:
                time.sleep(delay)
                delay = min(delay * 1.5, 5.0)
                continue
            resolved = status.get("status", "").lower()
            if resolved == "matched":
                return status
            elif resolved in ("cancelled", "expired"):
                return status
            time.sleep(delay)
            delay = min(delay * 1.5, 5.0)
        return None

    @staticmethod
    def _safe_float(value: object, default: float = 0.0) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    def _post_cancel_buy_reconcile(
        self,
        *,
        order_id: str,
        token_id: str,
        fallback_price: float,
        previous_size_matched: float = 0.0,
        requested_shares: float = 0.0,
        pre_balance: Optional[float] = None,
        started_at: Optional[float] = None,
        metadata: Optional[dict] = None,
        reason_prefix: str = "Buy order cancellation",
    ) -> Optional[OrderResult]:
        """After cancel success, wait briefly for late fills or stale status.

        Returns an OrderResult when a fill is discovered or the cancel remains
        ambiguous; returns None only when the order appears cleanly unfilled.
        """
        import time as _time

        metadata = dict(metadata or {})
        seconds = max(0.0, float(self._post_cancel_reconcile_seconds))
        poll = max(0.01, float(self._post_cancel_reconcile_poll_seconds))
        deadline = _time.monotonic() + seconds
        since_ts = started_at if started_at is not None else _time.time() - max(seconds, 1.0)
        last_status: dict = {}
        last_balance: Optional[float] = None
        balance_error = ""
        active_after_cancel = False
        attempts = 0
        size_matched = max(0.0, float(previous_size_matched or 0.0))
        fill_price = float(fallback_price or 0.0)
        observed_trade_shares = 0.0
        observed_trade_price = 0.0

        while True:
            attempts += 1
            status = self.get_order_status(order_id) or {}
            if status:
                last_status = status
                status_size = self._safe_float(status.get("size_matched"), size_matched)
                size_matched = max(size_matched, status_size)
                fill_price = self._safe_float(status.get("price"), fill_price)
                status_state = str(status.get("status") or "").lower()
                if status_state in {"live", "open", "active", "pending", "delayed", "unconfirmed"}:
                    active_after_cancel = True

            try:
                open_orders = self.get_open_orders(strict=True)
            except RuntimeError:
                open_orders = []
                active_after_cancel = True
            for order in open_orders:
                if str(order.get("order_id") or "") == order_id:
                    active_after_cancel = True
                    order_filled = self._safe_float(order.get("filled"), 0.0)
                    size_matched = max(size_matched, order_filled)
                    fill_price = self._safe_float(order.get("price"), fill_price)

            try:
                last_balance = self.get_position(token_id, strict=True)
            except RuntimeError as exc:
                balance_error = str(exc)

            for trade in self.get_trades(token_id, since_timestamp=since_ts):
                side = str(trade.get("side") or "").upper()
                trade_order_ids = {
                    str(trade.get("order_id") or ""),
                    str(trade.get("taker_order_id") or ""),
                }
                if side and side != "BUY":
                    continue
                if order_id not in trade_order_ids and any(trade_order_ids):
                    continue
                trade_size = self._safe_float(trade.get("size"), 0.0)
                if trade_size > observed_trade_shares:
                    observed_trade_shares = trade_size
                    observed_trade_price = self._safe_float(trade.get("price"), observed_trade_price)

            balance_delta = 0.0
            if last_balance is not None:
                if pre_balance is None:
                    balance_delta = last_balance
                else:
                    balance_delta = max(0.0, last_balance - pre_balance)

            observed_shares = max(size_matched, balance_delta, observed_trade_shares)
            if observed_shares > 0.01:
                if active_after_cancel:
                    return OrderResult(
                        success=False, order_id=order_id, shares_filled=observed_shares,
                        avg_fill_price=fill_price, total_cost=observed_shares * fill_price,
                        ambiguous=True, submission_state="uncertain",
                        error_message="Partial buy fill remains pending until residual cancellation is verified",
                        metadata=metadata | {"final_reason": "post_cancel_status_active"},
                    )
                final_price = fill_price or observed_trade_price or fallback_price
                final_cost = observed_shares * final_price
                metadata.update({
                    "post_cancel_reconcile_attempts": str(attempts),
                    "post_cancel_observed_shares": f"{float(observed_shares):.4f}",
                    "post_cancel_balance": "" if last_balance is None else f"{float(last_balance):.4f}",
                    "post_cancel_balance_delta": f"{float(balance_delta):.4f}",
                    "post_cancel_status": str(last_status.get("status") or ""),
                    "final_reason": "post_cancel_fill_detected",
                })
                return self._confirmed_buy_result(
                    token_id,
                    order_id,
                    observed_shares,
                    final_price,
                    final_cost,
                    metadata=metadata,
                )

            if _time.monotonic() >= deadline:
                break
            _time.sleep(poll)

        metadata.update({
            "post_cancel_reconcile_attempts": str(attempts),
            "post_cancel_balance": "" if last_balance is None else f"{float(last_balance):.4f}",
            "post_cancel_status": str(last_status.get("status") or ""),
        })
        if active_after_cancel:
            return OrderResult(
                success=False,
                order_id=order_id,
                shares_filled=size_matched,
                avg_fill_price=fill_price,
                total_cost=size_matched * fill_price,
                ambiguous=True,
                error_message=(
                    f"{reason_prefix} remained ambiguous after {seconds:.1f}s; "
                    "buy order may still be live"
                ),
                metadata=metadata | {"final_reason": "post_cancel_status_active"},
            )
        if balance_error and self._post_cancel_ambiguous_on_unreadable_balance:
            return OrderResult(
                success=False,
                order_id=order_id,
                shares_filled=size_matched,
                avg_fill_price=fill_price,
                total_cost=size_matched * fill_price,
                ambiguous=True,
                error_message=(
                    f"{reason_prefix} could not confirm token balance after cancel: "
                    f"{balance_error}; buy order may have filled"
                ),
                metadata=metadata | {
                    "final_reason": "post_cancel_balance_unreadable",
                    "post_cancel_balance_error": balance_error[:160],
                },
            )
        return None

    def _fail_unresolved_buy(
        self,
        order_id: Optional[str],
        reason: str,
        context: str,
        resolved_status: Optional[str] = None,
        *,
        token_id: Optional[str] = None,
        fallback_price: float = 0.0,
        requested_shares: float = 0.0,
        pre_balance: Optional[float] = None,
        started_at: Optional[float] = None,
        metadata: Optional[dict] = None,
    ) -> OrderResult:
        """Cancel an unresolved buy before reporting failure."""
        if resolved_status in ("cancelled", "expired"):
            return OrderResult(
                success=False,
                order_id=None,
                error_message=reason,
            )

        ambiguous_order_id = None
        if order_id:
            cancelled = self.cancel_order(order_id)
            if cancelled:
                _emit(f"[INFO] Cancelled unresolved {context} buy {order_id}")
                if token_id:
                    reconciled = self._post_cancel_buy_reconcile(
                        order_id=order_id,
                        token_id=token_id,
                        fallback_price=fallback_price,
                        requested_shares=requested_shares,
                        pre_balance=pre_balance,
                        started_at=started_at,
                        metadata=metadata,
                        reason_prefix=f"Unresolved {context} buy cancel",
                    )
                    if reconciled is not None:
                        return reconciled
            else:
                _emit(f"[WARN] Failed to cancel unresolved {context} buy {order_id} - may fill later")
                ambiguous_order_id = order_id
                reason = f"{reason}; buy order may still be live"

        return OrderResult(
            success=False,
            order_id=ambiguous_order_id,
            error_message=reason,
            ambiguous=ambiguous_order_id is not None,
        )

    def _fail_unresolved_sell(
        self,
        order_id: Optional[str],
        reason: str,
        context: str,
        resolved_status: Optional[str] = None,
    ) -> OrderResult:
        """Cancel an unresolved sell before reporting failure."""
        if resolved_status in ("cancelled", "expired"):
            return OrderResult(
                success=False,
                order_id=None,
                error_message=reason,
            )

        ambiguous_order_id = None
        if order_id:
            cancelled = self.cancel_order(order_id)
            if cancelled:
                _emit(f"[INFO] Cancelled unresolved {context} sell {order_id}")
            else:
                _emit(f"[WARN] Failed to cancel unresolved {context} sell {order_id} - may still fill later")
                ambiguous_order_id = order_id
                reason = f"{reason}; sell order may still be live"

        return OrderResult(
            success=False,
            order_id=ambiguous_order_id,
            error_message=reason,
            ambiguous=ambiguous_order_id is not None,
        )

    def verify_connection(self) -> tuple[bool, str]:
        """Verify connection to Polymarket API.

        Returns:
            Tuple of (success, message).
        """
        try:
            self._ensure_client()
            # Test connection by getting server time or a simple endpoint
            result = self._client.get_ok()
            if result == "OK":
                return True, "Connection verified"
            return True, f"Server responded: {result}"
        except Exception as e:
            return False, f"Connection failed: {e}"

    def buy_market(self, token_id: str, usd_amount: float) -> OrderResult:
        """Execute a market buy order.

        Uses create_market_order with FOK for immediate execution.

        Args:
            token_id: The CLOB token ID to buy.
            usd_amount: Amount in USD to spend.

        Returns:
            OrderResult with fill details or error.
        """
        lifecycle = {"submission_state": "not_submitted"}
        try:
            self._ensure_client()
            from py_clob_client_v2 import MarketOrderArgs, OrderType, Side

            # Get current order book to estimate fill price
            book = self._client.get_order_book(token_id)
            if not book or not self._field(book, "asks", default=[]):
                return OrderResult(
                    success=False,
                    error_message="No asks available in order book",
                )

            # Get best ask for price estimate (sort ascending to get lowest)
            asks = self._book_prices(book, "asks")
            bids = self._book_prices(book, "bids")
            best_ask = asks[0]

            # Log orderbook
            self._log_orderbook(token_id, bids, asks)

            # Round USD amount to 2 decimals (API requirement for maker amount)
            rounded_amount = round(usd_amount, 2)
            try:
                pre_balance = self.get_position(token_id, strict=True)
            except RuntimeError as exc:
                log.debug("pre-buy balance read failed for token %s: %s", token_id, exc)
                pre_balance = None

            # Create market order (BUY side)
            market_args = MarketOrderArgs(
                token_id=token_id,
                amount=rounded_amount,
                side=Side.BUY,
                order_type=OrderType.FOK,
                user_usdc_balance=self.get_usdc_balance() or 0,
            )

            started_at = time.time()
            lifecycle["pre_balance"] = pre_balance
            response = self._post_signed_buy(market_args, OrderType.FOK, lifecycle, market_order=True)

            # Parse response
            status = str(response.get("status", ""))
            status_l = status.lower()
            order_id = self._order_id_from_response(response)

            if status_l == "delayed":
                _emit(f"[INFO] Buy order delayed, polling for confirmation (ID: {order_id})")
                resolved = self._poll_delayed_order(order_id)
                if resolved and resolved.get("status", "").lower() == "matched":
                    filled_shares = float(resolved.get("size_matched", 0))
                    fill_price = float(resolved.get("price", 0))
                    if filled_shares <= 0:
                        return OrderResult(
                            success=False, order_id=order_id,
                            error_message="Delayed buy matched but size_matched=0",
                        )
                    cost = filled_shares * fill_price if fill_price > 0 else rounded_amount
                    return self._confirmed_buy_result(
                        token_id,
                        order_id,
                        filled_shares,
                        fill_price if fill_price > 0 else best_ask,
                        cost,
                    )
                else:
                    reason = resolved.get("status", "timeout") if resolved else "timeout"
                    return self._fail_unresolved_buy(
                        order_id,
                        f"Delayed buy order not filled: {reason}",
                        "delayed",
                        reason if reason in ("cancelled", "expired") else None,
                        token_id=token_id,
                        fallback_price=best_ask,
                        requested_shares=(rounded_amount / best_ask if best_ask > 0 else 0.0),
                        pre_balance=pre_balance,
                        started_at=started_at,
                    )

            if status_l == "matched":
                # For BUY: takingAmount = shares received, makingAmount = USDC spent
                taking = float(response.get("takingAmount", 0))   # shares received
                making = float(response.get("makingAmount", 0))   # USDC spent

                if taking <= 0:
                    return OrderResult(
                        success=False,
                        order_id=order_id,
                        error_message="Buy matched but takingAmount=0 (no shares received)",
                    )

                shares = taking
                cost = making if making > 0 else rounded_amount
                avg_price = cost / shares

                # Post-fill slippage warning
                if avg_price > best_ask * (1 + self._max_slippage_pct):
                    _emit(f"[WARN] Fill price ${avg_price:.4f} exceeds best_ask "
                          f"${best_ask:.4f} by {((avg_price/best_ask)-1)*100:.1f}% "
                          f"(max slippage {self._max_slippage_pct*100:.0f}%)")

                return self._confirmed_buy_result(token_id, order_id, shares, avg_price, cost)

            elif response.get("success"):
                # Order accepted but not immediately matched — poll like delayed
                _emit(f"[INFO] Buy order accepted (success=true, status={status}), "
                      f"polling for match...")
                resolved = self._poll_delayed_order(order_id)
                if resolved and resolved.get("status", "").lower() == "matched":
                    filled_shares = float(resolved.get("size_matched", 0))
                    fill_price = float(resolved.get("price", 0))
                    if filled_shares <= 0:
                        return OrderResult(
                            success=False, order_id=order_id,
                            error_message="Accepted buy matched but size_matched=0",
                        )
                    cost = filled_shares * fill_price if fill_price > 0 else rounded_amount
                    return self._confirmed_buy_result(
                        token_id,
                        order_id,
                        filled_shares,
                        fill_price if fill_price > 0 else best_ask,
                        cost,
                    )
                else:
                    reason = (resolved.get("status", "timeout")
                              if resolved else "timeout")
                    return self._fail_unresolved_buy(
                        order_id,
                        f"Accepted buy order not matched: {reason}",
                        "accepted",
                        reason if reason in ("cancelled", "expired") else None,
                        token_id=token_id,
                        fallback_price=best_ask,
                        requested_shares=(rounded_amount / best_ask if best_ask > 0 else 0.0),
                        pre_balance=pre_balance,
                        started_at=started_at,
                    )

            else:
                error = (response.get("errorMsg") or response.get("error")
                         or "Order rejected")
                return OrderResult(
                    success=False,
                    error_message=f"Order rejected: {error}",
                    order_id=lifecycle.get("submitted_order_hash") if lifecycle["submission_state"] == "uncertain" else None,
                    ambiguous=lifecycle["submission_state"] == "uncertain",
                    metadata=lifecycle,
                    submission_state=lifecycle["submission_state"],
                )

        except Exception as e:
            return OrderResult(
                success=False,
                error_message=f"Buy order failed: {e}",
                order_id=lifecycle.get("submitted_order_hash") if lifecycle["submission_state"] != "not_submitted" else None,
                ambiguous=lifecycle["submission_state"] in {"uncertain", "accepted"},
                metadata=lifecycle,
                submission_state=lifecycle["submission_state"],
            )

    def sell_market(self, token_id: str, shares: float) -> OrderResult:
        """Execute a market sell order.

        Uses create_market_order with FOK for immediate execution.

        Args:
            token_id: The CLOB token ID to sell.
            shares: Number of shares to sell.

        Returns:
            OrderResult with fill details or error.
        """
        try:
            self._ensure_client()
            from py_clob_client_v2 import MarketOrderArgs, OrderType, Side

            # Get current order book for price estimate
            book = self._client.get_order_book(token_id)
            if not book or not self._field(book, "bids", default=[]):
                return OrderResult(
                    success=False,
                    error_message="No bids available in order book",
                )

            # Get best bid for price estimate
            bids = self._book_prices(book, "bids")
            asks = self._book_prices(book, "asks")
            best_bid = bids[0]

            # Log orderbook
            self._log_orderbook(token_id, bids, asks)

            # Floor shares to 2 decimals (never sell more than owned)
            import math
            rounded_shares = math.floor(shares * 100) / 100
            try:
                pre_balance = self.get_position(token_id, strict=True)
            except Exception as exc:
                log.debug("pre-sell balance read failed for token %s: %s", token_id, exc)
                pre_balance = None

            # Create market order - for SELL, amount is tokens to sell
            market_args = MarketOrderArgs(
                token_id=token_id,
                amount=rounded_shares,
                side=Side.SELL,
                order_type=OrderType.FOK,
            )

            response = self._client.create_and_post_market_order(
                market_args,
                options=self._order_options(token_id),
                order_type=OrderType.FOK,
            )

            # Check if order was matched
            status = str(response.get("status", ""))
            status_l = status.lower()
            order_id = self._order_id_from_response(response)

            if status_l == "delayed":
                _emit(f"[INFO] Sell order delayed, polling for confirmation (ID: {order_id})")
                resolved = self._poll_delayed_order(order_id)
                if resolved and resolved.get("status", "").lower() == "matched":
                    filled_shares = float(resolved.get("size_matched", 0))
                    fill_price = float(resolved.get("price", 0))
                    if filled_shares <= 0:
                        return OrderResult(
                            success=False, order_id=order_id,
                            error_message="Delayed sell matched but size_matched=0",
                        )
                    proceeds = filled_shares * fill_price if fill_price > 0 else rounded_shares * best_bid
                    return self._confirmed_sell_result(
                        token_id,
                        order_id,
                        pre_balance,
                        filled_shares,
                        fill_price if fill_price > 0 else best_bid,
                        proceeds,
                    )
                else:
                    reason = resolved.get("status", "timeout") if resolved else "timeout"
                    return self._fail_unresolved_sell(
                        order_id,
                        f"Delayed sell order not filled: {reason}",
                        "delayed",
                        reason if reason in ("cancelled", "expired") else None,
                    )

            if status_l == "matched":
                # takingAmount is USDC received, makingAmount is shares sold
                taking = float(response.get("takingAmount", 0))
                making = float(response.get("makingAmount", 0))
                if making <= 0 or taking <= 0:
                    return OrderResult(
                        success=False, order_id=order_id,
                        error_message=f"Sell matched but invalid fill data (taking={taking}, making={making})",
                    )
                avg_price = taking / making
                return self._confirmed_sell_result(
                    token_id,
                    order_id,
                    pre_balance,
                    making,
                    avg_price,
                    taking,
                )

            elif response.get("success"):
                # Order accepted but not immediately matched — poll like delayed
                _emit(f"[INFO] Sell order accepted (success=true, status={status}), "
                      f"polling for match...")
                resolved = self._poll_delayed_order(order_id)
                if resolved and resolved.get("status", "").lower() == "matched":
                    filled_shares = float(resolved.get("size_matched", 0))
                    fill_price = float(resolved.get("price", 0))
                    if filled_shares <= 0:
                        return OrderResult(
                            success=False, order_id=order_id,
                            error_message="Accepted sell matched but size_matched=0",
                        )
                    proceeds = filled_shares * fill_price if fill_price > 0 else rounded_shares * best_bid
                    return self._confirmed_sell_result(
                        token_id,
                        order_id,
                        pre_balance,
                        filled_shares,
                        fill_price if fill_price > 0 else best_bid,
                        proceeds,
                    )
                else:
                    reason = (resolved.get("status", "timeout")
                              if resolved else "timeout")
                    return self._fail_unresolved_sell(
                        order_id,
                        f"Accepted sell order not matched: {reason}",
                        "accepted",
                        reason if reason in ("cancelled", "expired") else None,
                    )

            else:
                error = response.get("errorMsg") or response.get("error") or f"Status: {status}"
                return OrderResult(
                    success=False,
                    order_id=order_id,
                    error_message=f"Order not matched: {error}",
                )

        except Exception as e:
            return OrderResult(
                success=False,
                error_message=f"Sell order failed: {e}",
            )

    def get_best_bid(self, token_id: str) -> Optional[float]:
        """Get the current best (highest) bid price.

        Args:
            token_id: The CLOB token ID.

        Returns:
            Best bid price or None if unavailable.
        """
        try:
            self._ensure_client()
            book = self._client.get_order_book(token_id)
            if book and self._field(book, "bids", default=[]):
                # Sort bids descending to get highest (best) bid
                bids = self._book_prices(book, "bids")
                asks = self._book_prices(book, "asks")
                # Log orderbook
                self._log_orderbook(token_id, bids, asks)
                return bids[0] if bids else None
            return None
        except Exception as exc:
            log.warning("get_best_bid failed for token %s: %s", token_id, exc)
            return None

    def get_best_ask(self, token_id: str) -> Optional[float]:
        """Get the current best (lowest) ask price.

        Args:
            token_id: The CLOB token ID.

        Returns:
            Best ask price or None if unavailable.
        """
        try:
            self._ensure_client()
            book = self._client.get_order_book(token_id)
            if book and self._field(book, "asks", default=[]):
                # Sort asks ascending to get lowest (best) ask
                asks = self._book_prices(book, "asks")
                bids = self._book_prices(book, "bids")
                # Log orderbook
                self._log_orderbook(token_id, bids, asks)
                return asks[0] if asks else None
            return None
        except Exception as exc:
            log.warning("get_best_ask failed for token %s: %s", token_id, exc)
            return None

    def buy_limit(
        self,
        token_id: str,
        price: float,
        shares: float,
        order_type: str = "GTC",
        wait_for_fill_seconds: Optional[float] = None,
    ) -> OrderResult:
        """Place a limit buy order.

        Args:
            token_id: The CLOB token ID to buy.
            price: Limit price per share (hard cap — fill can't exceed this).
            shares: Number of shares to buy.
            order_type: "GTC" (default, resting order) or "FOK" (fill-or-kill
                at ≤ price, atomic price-cap; nothing fills if the full size
                isn't available at ≤ price).
            wait_for_fill_seconds: When set with order_type="GTC", poll the
                order status for up to this many seconds and CANCEL the
                residual on timeout. Returns OrderResult populated with the
                actual fills (full or partial). Used for entries where we
                want IOC-like semantics without using FOK (FOK rejects on
                partial liquidity even when partial fill would satisfy us).

        Returns:
            OrderResult. For FOK fills, shares_filled/avg_fill_price/total_cost
            reflect the matched amounts parsed from the response.
        """
        lifecycle = {"submission_state": "not_submitted"}
        try:
            self._ensure_client()
            from py_clob_client_v2 import OrderArgs, OrderType, Side

            tick_size = self._resolve_tick_size(token_id)
            rounded_price, rounded_shares, max_cost = normalize_limit_buy(
                price, shares, tick_size=tick_size,
            )
            lifecycle = {
                "submission_state": "not_submitted",
                "order_type": order_type.upper(),
                "limit_price": f"{float(rounded_price):.4f}",
                "shares_requested": f"{float(rounded_shares):.4f}",
                "max_cost": f"{float(max_cost or 0.0):.4f}",
                "wait_seconds": (
                    f"{float(wait_for_fill_seconds):.3f}"
                    if wait_for_fill_seconds is not None else ""
                ),
            }

            if rounded_shares < 5.0:
                return OrderResult(
                    success=False,
                    error_message=(
                        "Limit buy size below minimum after precision normalization: "
                        f"{rounded_shares:.4f} shares"
                    ),
                    metadata=lifecycle,
                )

            try:
                pre_balance = self.get_position(token_id, strict=True)
            except RuntimeError as exc:
                log.debug("pre-limit-buy balance read failed for token %s: %s", token_id, exc)
                pre_balance = None
            lifecycle["pre_balance"] = "" if pre_balance is None else f"{float(pre_balance):.4f}"

            order_args = OrderArgs(
                token_id=token_id,
                price=rounded_price,
                size=rounded_shares,
                side=Side.BUY,
            )

            type_map = {
                "GTC": OrderType.GTC,
                "FOK": OrderType.FOK,
                "GTD": OrderType.GTD,
            }
            ot = type_map.get(order_type.upper(), OrderType.GTC)

            started_at = time.time()
            response = self._post_signed_buy(order_args, ot, lifecycle)
            status = str(response.get("status", ""))
            status_l = status.lower()
            order_id = self._order_id_from_response(response)
            lifecycle.update({
                "order_id": order_id or "",
                "posted_status": status,
                "response_keys": ",".join(sorted(str(k) for k in response.keys())[:20]),
            })

            if order_type.upper() == "FOK":
                if status_l != "matched":
                    return OrderResult(
                        success=False,
                        order_id=order_id,
                        error_message=(
                            f"Limit FOK not filled at ≤${rounded_price:.2f}: "
                            f"status={status or 'unknown'}"
                        ),
                        ambiguous=status_l in ("live", "open", "active", "delayed", "pending"),
                        metadata=lifecycle,
                    )
                # Matched: parse takingAmount (shares) / makingAmount (USDC)
                # the same way buy_market's matched branch does.
                taking = float(response.get("takingAmount", 0))
                making = float(response.get("makingAmount", 0))
                if taking <= 0:
                    return OrderResult(
                        success=False,
                        order_id=order_id,
                        error_message="FOK matched but takingAmount=0",
                        metadata=lifecycle,
                    )
                cost = making if making > 0 else max_cost or taking * rounded_price
                avg_price = cost / taking
                return self._confirmed_buy_result(
                    token_id,
                    order_id,
                    taking,
                    avg_price,
                    cost,
                    metadata=lifecycle | {
                        "final_reason": "matched",
                        "size_matched": f"{float(taking):.4f}",
                        "avg_fill_price": f"{float(avg_price):.4f}",
                    },
                )

            # GTC path: optionally wait for fill and cancel residual.
            if order_id and wait_for_fill_seconds and wait_for_fill_seconds > 0:
                import time as _time
                deadline = _time.monotonic() + float(wait_for_fill_seconds)
                last_status: Optional[dict] = None
                while _time.monotonic() < deadline:
                    last_status = self.get_order_status(order_id) or last_status
                    if last_status:
                        s = str(last_status.get("status", "")).lower()
                        if s == "matched":
                            break
                        if s in ("cancelled", "canceled", "rejected", "expired", "failed"):
                            break
                        size_matched = float(last_status.get("size_matched") or 0.0)
                        original = float(last_status.get("original_size") or 0.0)
                        if original > 0 and size_matched >= original - 1e-9:
                            break
                    _time.sleep(0.2)

                # If still open with residual, cancel and report what filled.
                final_status = self.get_order_status(order_id) or last_status or {}
                size_matched = float(final_status.get("size_matched") or 0.0)
                original = float(final_status.get("original_size") or rounded_shares)
                final_state = str(final_status.get("status", "")).lower()
                lifecycle.update({
                    "status_before_cancel": final_state,
                    "size_matched_before_cancel": f"{float(size_matched):.4f}",
                    "original_size": f"{float(original):.4f}",
                })
                terminal_states = (
                    "matched", "cancelled", "canceled", "rejected", "expired", "failed",
                )
                if final_state not in terminal_states and size_matched < original - 1e-9:
                    fill_price = float(final_status.get("price") or rounded_price)
                    cost = size_matched * fill_price
                    cancel_error = ""
                    try:
                        cancel_ok = self.cancel_order(order_id)
                    except RuntimeError as exc:
                        cancel_ok = False
                        cancel_error = f": {exc}"
                    lifecycle.update({
                        "cancel_success": str(bool(cancel_ok)),
                        "cancel_error": cancel_error[:160],
                    })
                    if not cancel_ok:
                        return OrderResult(
                            success=False,
                            order_id=order_id,
                            shares_filled=size_matched,
                            avg_fill_price=fill_price,
                            total_cost=cost,
                            ambiguous=True,
                            error_message=(
                                "Limit GTC partially filled, but residual "
                                f"cancel was not confirmed{cancel_error}; "
                                "buy order may still be live"
                            ),
                            metadata=lifecycle | {"final_reason": "cancel_not_confirmed"},
                        )

                    reconciled = self._post_cancel_buy_reconcile(
                        order_id=order_id,
                        token_id=token_id,
                        fallback_price=fill_price,
                        previous_size_matched=size_matched,
                        requested_shares=original,
                        pre_balance=pre_balance,
                        started_at=started_at,
                        metadata=lifecycle,
                        reason_prefix="Limit GTC residual cancel",
                    )
                    if reconciled is not None:
                        return reconciled

                    status_after_cancel = self.get_order_status(order_id) or {}
                    after_state = str(status_after_cancel.get("status", "")).lower()
                    if status_after_cancel.get("size_matched") is not None:
                        size_matched = max(
                            size_matched,
                            float(status_after_cancel.get("size_matched") or 0.0),
                        )
                    if status_after_cancel.get("original_size") is not None:
                        original = float(status_after_cancel.get("original_size") or original)
                    fill_price = float(status_after_cancel.get("price") or fill_price)
                    final_state = after_state
                    final_status = {**final_status, **status_after_cancel}
                    lifecycle.update({
                        "status_after_cancel": after_state,
                        "size_matched_after_cancel": f"{float(size_matched):.4f}",
                    })
                    cost = size_matched * fill_price
                    if final_state not in terminal_states and size_matched < original - 1e-9:
                        return OrderResult(
                            success=False,
                            order_id=order_id,
                            shares_filled=size_matched,
                            avg_fill_price=fill_price,
                            total_cost=cost,
                            ambiguous=True,
                            error_message=(
                                "Limit GTC partially filled, but residual "
                                "order status remained unconfirmed after cancel"
                            ),
                            metadata=lifecycle | {"final_reason": "status_unconfirmed_after_cancel"},
                        )

                if size_matched > 0:
                    fill_price = float(final_status.get("price") or rounded_price)
                    cost = size_matched * fill_price
                    return self._confirmed_buy_result(
                        token_id, order_id, size_matched, fill_price, cost,
                        metadata=lifecycle | {
                            "final_reason": "matched_or_partial_after_cancel",
                            "size_matched": f"{float(size_matched):.4f}",
                            "avg_fill_price": f"{float(fill_price):.4f}",
                        },
                    )
                return OrderResult(
                    success=False,
                    order_id=order_id,
                    error_message=(
                        f"Limit GTC unfilled within {wait_for_fill_seconds:.1f}s "
                        f"and residual cancelled"
                    ),
                    metadata=lifecycle | {"final_reason": "unfilled_timeout"},
                    submission_state="cancelled",
                )

            # GTC fire-and-forget path (legacy): returns with order_id only.
            if order_id:
                return OrderResult(
                    success=True,
                    order_id=order_id,
                    shares_filled=0.0,
                    avg_fill_price=rounded_price,
                    total_cost=0.0,
                    metadata=lifecycle | {"final_reason": "posted_no_wait"},
                )
            error = response.get("errorMsg") or response.get("error") or "Order failed"
            return OrderResult(
                success=False,
                error_message=f"Limit order failed: {error}",
                metadata=lifecycle | {"final_reason": "post_failed", "error": str(error)[:160]},
                order_id=lifecycle.get("submitted_order_hash") if lifecycle["submission_state"] == "uncertain" else None,
                ambiguous=lifecycle["submission_state"] == "uncertain",
                submission_state=lifecycle["submission_state"],
            )

        except Exception as e:
            return OrderResult(
                success=False,
                error_message=f"Limit buy failed: {e}",
                order_id=lifecycle.get("order_id") or (lifecycle.get("submitted_order_hash") if lifecycle["submission_state"] != "not_submitted" else None),
                ambiguous=lifecycle["submission_state"] in {"uncertain", "accepted"},
                metadata=lifecycle | {"final_reason": "exception", "error": str(e)[:160]},
                submission_state=lifecycle["submission_state"],
            )

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
            order_type: Order type - "GTC" (default), "FOK", or "GTD".

        Returns:
            OrderResult with order_id (may not be filled yet for GTC).
        """
        try:
            self._ensure_client()
            from py_clob_client_v2 import OrderArgs, OrderType, Side
            from .executor import quantize_price, quantize_size

            # Per-market precision via tick_size lookup. ROUNDING_CONFIG for
            # tick=0.01 gives price 2dp / size 2dp, but a tick=0.001 or 0.0001
            # market needs 3dp / 4dp price respectively. Hardcoding to 2dp
            # would silently truncate sub-cent prices on those markets.
            tick_size = self._resolve_tick_size(token_id)
            rounded_price = float(quantize_price(price, tick_size=tick_size))
            # Floor shares to avoid selling more than owned (caller may pre-floor).
            # quantize_size uses ROUND_DOWN, matching the existing math.floor
            # semantics for tick=0.01 (size 2dp).
            rounded_shares = float(quantize_size(shares, tick_size=tick_size))
            if rounded_shares < 5.0:
                return OrderResult(
                    success=False,
                    error_message=(
                        "Limit sell size below minimum after precision normalization: "
                        f"{rounded_shares:.4f} shares"
                    ),
                    metadata={
                        "final_reason": "sell_size_below_minimum",
                        "shares_requested": f"{float(shares):.4f}",
                        "rounded_shares": f"{rounded_shares:.4f}",
                    },
                )
            try:
                pre_balance = self.get_position(token_id, strict=True)
            except RuntimeError as exc:
                log.debug("pre-limit-sell balance read failed for token %s: %s", token_id, exc)
                pre_balance = None

            order_args = OrderArgs(
                token_id=token_id,
                price=rounded_price,
                size=rounded_shares,
                side=Side.SELL,
            )

            # Map order type string to enum
            type_map = {
                "GTC": OrderType.GTC,
                "FOK": OrderType.FOK,
                "GTD": OrderType.GTD,
            }
            ot = type_map.get(order_type.upper(), OrderType.GTC)

            response = self._client.create_and_post_order(
                order_args,
                options=self._order_options(token_id),
                order_type=ot,
            )

            status = str(response.get("status", ""))
            status_l = status.lower()
            order_id = self._order_id_from_response(response)

            if order_id:
                # For FOK, check if matched
                if order_type.upper() == "FOK":
                    if status_l != "matched":
                        return OrderResult(
                            success=False,
                            order_id=order_id,
                            error_message=f"FOK order not matched: {status}",
                        )
                    taking = float(response.get("takingAmount", 0))
                    making = float(response.get("makingAmount", 0))
                    if making <= 0 or taking <= 0:
                        return OrderResult(
                            success=False,
                            order_id=order_id,
                            error_message=(
                                "FOK sell matched but invalid fill data "
                                f"(taking={taking}, making={making})"
                            ),
                        )
                    return self._confirmed_sell_result(
                        token_id,
                        order_id,
                        pre_balance,
                        making,
                        taking / making,
                        taking,
                    )
                return OrderResult(
                    success=True,
                    order_id=order_id,
                    shares_filled=0.0,  # Will be filled later for GTC
                    avg_fill_price=rounded_price,
                    total_cost=0.0,
                )
            else:
                error = response.get("errorMsg") or response.get("error") or "Order failed"
                return OrderResult(
                    success=False,
                    error_message=f"Limit order failed: {error}",
                )

        except Exception as e:
            return OrderResult(
                success=False,
                error_message=f"Limit sell failed: {e}",
            )

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
        """
        try:
            self._ensure_client()
            order = self._client.get_order(order_id)
            if order:
                # Handle both object-style and dict-style responses
                if isinstance(order, dict):
                    return {
                        "order_id": order_id,
                        "status": order.get("status", "unknown"),
                        "size_matched": float(order.get("size_matched", 0) or 0),
                        "original_size": float(order.get("original_size", 0) or 0),
                        "price": float(order.get("price", 0) or 0),
                        "side": order.get("side", "unknown"),
                    }
                else:
                    # Object-style response
                    return {
                        "order_id": order_id,
                        "status": getattr(order, 'status', 'unknown') or 'unknown',
                        "size_matched": float(getattr(order, 'size_matched', 0) or 0),
                        "original_size": float(getattr(order, 'original_size', 0) or 0),
                        "price": float(getattr(order, 'price', 0) or 0),
                        "side": getattr(order, 'side', 'unknown') or 'unknown',
                    }
            return None
        except Exception as e:
            _emit(f"[ERROR] get_order_status failed: {e}")
            return None

    @staticmethod
    def _cancel_reason_is_terminal_absent(reason: str) -> bool:
        text = (reason or "").lower()
        return any(
            marker in text
            for marker in (
                "not found",
                "can't be found",
                "cannot be found",
                "already canceled",
                "already cancelled",
                "already matched",
                "already filled",
            )
        )

    def cancel_order_detailed(self, order_id: str, retries: int = 3) -> CancelResult:
        """Cancel an order while preserving terminal/ambiguous failure detail."""
        import time

        self._ensure_client()
        last_reason = "cancel_order_failed"
        last_response = None

        for attempt in range(retries):
            try:
                from py_clob_client_v2 import OrderPayload

                response = self._client.cancel_order(OrderPayload(orderID=order_id))
                last_response = response
                if response is None:
                    last_reason = "empty_response"
                    _emit(f"[WARN] cancel_order got None response (attempt {attempt + 1}/{retries})")
                    if attempt < retries - 1:
                        time.sleep(1.0 * (attempt + 1))
                    continue

                if isinstance(response, dict):
                    canceled = response.get("canceled", [])
                    not_canceled = response.get("not_canceled", {})

                    if order_id in canceled:
                        return CancelResult(success=True, raw_response=response)
                    if order_id in not_canceled:
                        reason = str(not_canceled[order_id])
                        terminal_absent = self._cancel_reason_is_terminal_absent(reason)
                        _emit(f"[WARN] cancel_order: order {order_id} not canceled: {reason}")
                        return CancelResult(
                            success=False,
                            terminal_absent=terminal_absent,
                            retryable=not terminal_absent,
                            reason=reason,
                            raw_response=response,
                        )
                    if not canceled and not not_canceled:
                        if response.get("error") or response.get("errorMsg"):
                            last_reason = str(response.get("error") or response.get("errorMsg"))
                            _emit(f"[WARN] cancel_order: error response: {response}")
                            return CancelResult(
                                success=False,
                                retryable=True,
                                reason=last_reason,
                                raw_response=response,
                            )
                        if response.get("success"):
                            return CancelResult(success=True, raw_response=response)
                        last_reason = f"ambiguous response: {response}"
                        _emit(f"[WARN] cancel_order: ambiguous response: {response}")
                        return CancelResult(
                            success=False,
                            retryable=True,
                            reason=last_reason,
                            raw_response=response,
                        )

                last_reason = f"unexpected_response_type:{type(response).__name__}"
                _emit(f"[WARN] cancel_order: unexpected response type: {type(response)}")
                return CancelResult(
                    success=False,
                    retryable=True,
                    reason=last_reason,
                    raw_response=response,
                )

            except Exception as e:
                last_reason = str(e)
                _emit(f"[ERROR] cancel_order failed (attempt {attempt + 1}/{retries}): {e}")
                if attempt < retries - 1:
                    time.sleep(1.0 * (attempt + 1))

        return CancelResult(
            success=False,
            retryable=True,
            reason=last_reason,
            raw_response=last_response,
        )

    def cancel_order(self, order_id: str, retries: int = 3) -> bool:
        """Cancel an open order.

        Parses the API response to verify the order was actually cancelled.
        Retries on failure with short backoff.

        Args:
            order_id: The order ID to cancel.
            retries: Number of retry attempts (default 3).

        Returns:
            True if cancelled successfully or order already gone.
        """
        import time

        self._ensure_client()

        for attempt in range(retries):
            try:
                from py_clob_client_v2 import OrderPayload
                response = self._client.cancel_order(OrderPayload(orderID=order_id))
                if response is None:
                    _emit(f"[WARN] cancel_order got None response (attempt {attempt + 1}/{retries})")
                    if attempt < retries - 1:
                        time.sleep(1.0 * (attempt + 1))
                    continue

                # Parse canceled/not_canceled fields from API response
                if isinstance(response, dict):
                    canceled = response.get("canceled", [])
                    not_canceled = response.get("not_canceled", {})

                    if order_id in canceled:
                        return True
                    if order_id in not_canceled:
                        reason = not_canceled[order_id]
                        _emit(f"[WARN] cancel_order: order {order_id} not canceled: {reason}")
                        return False
                    # If neither canceled/not_canceled field present
                    if not canceled and not not_canceled:
                        if response.get("error") or response.get("errorMsg"):
                            _emit(f"[WARN] cancel_order: error response: {response}")
                            return False
                        if response.get("success"):
                            return True  # Legacy success response
                        # Truly empty/unknown response — assume failure
                        _emit(f"[WARN] cancel_order: ambiguous response: {response}")
                        return False

                # Non-dict response — assume failure
                _emit(f"[WARN] cancel_order: unexpected response type: {type(response)}")
                return False

            except Exception as e:
                _emit(f"[ERROR] cancel_order failed (attempt {attempt + 1}/{retries}): {e}")
                if attempt < retries - 1:
                    time.sleep(1.0 * (attempt + 1))

        return False

    @staticmethod
    def _normalize_cancel_all_response(response: object) -> CancelAllResult:
        if response is None:
            return CancelAllResult(
                success=False,
                error="empty_cancel_all_response",
                raw_response=response,
            )
        if not isinstance(response, dict):
            return CancelAllResult(
                success=False,
                error=f"unexpected_response_type:{type(response).__name__}",
                raw_response=response,
            )

        raw_canceled = response.get("canceled")
        if raw_canceled is None:
            canceled: list[str] = []
        elif isinstance(raw_canceled, dict):
            canceled = [str(order_id) for order_id in raw_canceled.keys()]
        elif isinstance(raw_canceled, (list, tuple, set)):
            canceled = [str(order_id) for order_id in raw_canceled]
        else:
            canceled = [str(raw_canceled)]

        raw_not_canceled = response.get("not_canceled") or {}
        if isinstance(raw_not_canceled, dict):
            not_canceled = {
                str(order_id): str(reason)
                for order_id, reason in raw_not_canceled.items()
            }
        else:
            not_canceled = {"unknown": str(raw_not_canceled)}

        error = str(response.get("error") or response.get("errorMsg") or "")
        if not_canceled:
            return CancelAllResult(
                success=False,
                canceled=canceled,
                not_canceled=not_canceled,
                error=error or "orders_not_canceled",
                raw_response=response,
            )
        if error:
            return CancelAllResult(
                success=False,
                canceled=canceled,
                error=error,
                raw_response=response,
            )
        if raw_canceled is not None or response.get("success"):
            return CancelAllResult(
                success=True,
                canceled=canceled,
                raw_response=response,
            )
        return CancelAllResult(
            success=False,
            canceled=canceled,
            error="ambiguous_cancel_all_response",
            raw_response=response,
        )

    def cancel_all_orders_detailed(self) -> CancelAllResult:
        """Cancel all open orders and retain venue cancellation details."""
        try:
            self._ensure_client()
            response = self._client.cancel_all()
            result = self._normalize_cancel_all_response(response)
            if result.not_canceled:
                _emit(f"[WARN] cancel_all_orders: not canceled: {result.not_canceled}")
            elif not result.success:
                _emit(f"[WARN] cancel_all_orders: {result.error}: {response}")
            return result
        except Exception as e:
            _emit(f"[ERROR] cancel_all_orders failed: {e}")
            return CancelAllResult(success=False, error=str(e))

    def cancel_all_orders(self) -> bool:
        """Cancel all open orders.

        Returns:
            True if cancelled successfully.
        """
        return self.cancel_all_orders_detailed().success

    def get_open_orders(self, strict: bool = False) -> list:
        """Get all open orders.

        Returns:
            List of open order dicts.
        """
        try:
            self._ensure_client()
            from py_clob_client_v2 import OpenOrderParams

            orders = self._client.get_open_orders(OpenOrderParams())
            result = []
            for o in orders:
                # Handle both attribute and dict-style access
                if hasattr(o, 'id'):
                    order_id = self._field(o, "id", "order_id", "orderID", default="")
                    token_id = self._field(o, "asset_id", "token_id", "market", default="")
                    side = self._field(o, "side", default="")
                    price = float(self._field(o, "price", default=0) or 0)
                    size = float(self._field(o, "original_size", "size", default=0) or 0)
                    filled = float(self._field(o, "size_matched", "filled", default=0) or 0)
                elif isinstance(o, dict):
                    order_id = o.get('id') or o.get('order_id') or o.get('orderID') or ''
                    token_id = o.get('asset_id') or o.get('token_id') or o.get('market') or ''
                    side = o.get('side', '')
                    price = float(o.get('price', 0) or 0)
                    size = float(o.get('original_size') or o.get('size') or 0)
                    filled = float(o.get('size_matched') or o.get('filled') or 0)
                else:
                    continue
                result.append({
                    "order_id": order_id,
                    "token_id": token_id,
                    "side": side,
                    "price": price,
                    "size": size,
                    "filled": filled,
                })
            return result
        except Exception as e:
            if strict:
                raise RuntimeError("Could not verify open orders") from e
            _emit(f"[ERROR] get_open_orders failed: {e}")
            return []

    def get_position(self, token_id: str, strict: bool = False) -> float:
        """Get actual shares held for a token.

        Uses the CLOB client's get_balance_allowance for accurate balances.
        Falls back to data API if CLOB client unavailable.

        Args:
            token_id: The CLOB token ID.
            strict: If True, raise error on failure instead of returning 0.

        Returns:
            Number of shares held (0.0 if none or error).

        Raises:
            RuntimeError: If strict=True and position cannot be determined.
        """
        import requests

        error_msgs = []
        clob_failed = False

        # Try CLOB client first (more accurate)
        try:
            self._ensure_client()
            from py_clob_client_v2 import AssetType, BalanceAllowanceParams

            params = BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=token_id)
            result = self._client.get_balance_allowance(params)
            if result and "balance" in result:
                # Balance is in raw units, divide by 1e6 for shares
                raw_balance = int(result["balance"])
                return raw_balance / 1e6
        except Exception as e:
            error_msgs.append(f"CLOB: {e}")
            clob_failed = True

        # Fallback to data API
        try:
            if not self._funder:
                error_msgs.append("Data API: no funder address")
            else:
                url = f"https://data-api.polymarket.com/positions?user={self._funder}"
                resp = requests.get(url, timeout=10)
                if resp.status_code == 200:
                    positions = resp.json()
                    for p in positions:
                        if p.get("asset") == token_id:
                            return float(p.get("size", 0))
                    # Position not found in data API
                    if clob_failed:
                        # Don't trust "not found" when CLOB also failed (stale data risk)
                        error_msgs.append("Data API: position not found but CLOB also failed (stale?)")
                    else:
                        return 0.0  # Both CLOB and data API agree: no position
                else:
                    error_msgs.append(f"Data API: status {resp.status_code}")
        except (requests.RequestException, TypeError, ValueError) as e:
            error_msgs.append(f"Data API: {e}")

        # Both methods failed
        if strict:
            raise RuntimeError(f"Could not determine position: {'; '.join(error_msgs)}")
        return 0.0

    def get_trades(self, token_id: str, since_timestamp: Optional[float] = None) -> list[dict]:
        """Get trade history for a token from Polymarket.

        Uses the py_clob_client_v2 get_trades API to fetch fills.

        Args:
            token_id: The CLOB token ID.
            since_timestamp: Only return trades after this Unix timestamp.

        Returns:
            List of trade dicts with fill details.
        """
        try:
            self._ensure_client()
            from py_clob_client_v2 import TradeParams

            params = TradeParams(
                maker_address=self._funder,
                asset_id=token_id,
            )
            if since_timestamp is not None:
                params.after = int(since_timestamp)

            raw_trades = self._client.get_trades(params)

            result = []
            for t in raw_trades:
                if isinstance(t, dict):
                    trade_dict = {
                        "id": t.get("id", ""),
                        "taker_order_id": t.get("taker_order_id", ""),
                        "size": float(t.get("size", 0)),
                        "price": float(t.get("price", 0)),
                        "side": t.get("side", ""),
                        "match_time": t.get("match_time", ""),
                        "transaction_hash": t.get("transaction_hash", ""),
                    }
                else:
                    trade_dict = {
                        "id": getattr(t, "id", ""),
                        "taker_order_id": getattr(t, "taker_order_id", ""),
                        "size": float(getattr(t, "size", 0)),
                        "price": float(getattr(t, "price", 0)),
                        "side": getattr(t, "side", ""),
                        "match_time": getattr(t, "match_time", ""),
                        "transaction_hash": getattr(t, "transaction_hash", ""),
                    }
                result.append(trade_dict)

            return result

        except Exception as e:
            _emit(f"[ERROR] get_trades failed: {e}")
            return []

    def _our_uuid(self) -> Optional[str]:
        """Cache + return our Polymarket owner UUID.

        Derived from the top-level `owner` field of any TAKER trade we
        participated in — Polymarket uses that UUID in
        ``maker_orders[i].owner`` when we are a maker, which is how we
        identify OUR leg of a maker fill.
        """
        if getattr(self, "_cached_uuid", None):
            return self._cached_uuid
        try:
            self._ensure_client()
            from py_clob_client_v2 import TradeParams
            params = TradeParams(maker_address=self._funder) if self._funder else TradeParams()
            raw = self._client.get_trades(params)
            for t in raw:
                d = t if isinstance(t, dict) else t.__dict__
                if str(d.get("trader_side", "")).upper() == "TAKER":
                    uuid = str(d.get("owner", "") or "") or None
                    if uuid:
                        self._cached_uuid = uuid
                        return uuid
        except Exception as e:
            _emit(f"[WARN] _our_uuid lookup failed: {e}")
        return None

    def _our_portion(self, trade: dict, our_uuid: Optional[str]) -> Optional[dict]:
        """Return (size, price, asset_id, side) from our perspective.

        For a TAKER trade the top-level fields describe our action
        directly. For a MAKER trade the top-level ``size`` is the full
        match across all makers; we need to find our specific entry in
        ``maker_orders`` to get OUR matched amount + price + side.
        Returns None if this trade isn't ours (shouldn't happen since
        /data/trades filters by our wallet, but defensive).
        """
        trader_side = str(trade.get("trader_side", "TAKER")).upper()
        if trader_side == "MAKER":
            if not our_uuid:
                return None
            for mo in (trade.get("maker_orders") or []):
                if str(mo.get("owner", "")) == our_uuid:
                    try:
                        return {
                            "order_id": str(mo.get("order_id") or ""),
                            "size": float(mo.get("matched_amount", 0) or 0),
                            "price": float(mo.get("price", 0) or 0),
                            "asset_id": str(mo.get("asset_id", "")),
                            "side": str(mo.get("side", "")).upper(),
                        }
                    except (TypeError, ValueError):
                        return None
            return None
        try:
            return {
                "size": float(trade.get("size", 0) or 0),
                "price": float(trade.get("price", 0) or 0),
                "asset_id": str(trade.get("asset_id", "")),
                "side": str(trade.get("side", "")).upper(),
            }
        except (TypeError, ValueError):
            return None

    def get_execution_cashflow(self, order_id: str, token_id: str, side: str,
                               target_shares: float, since_timestamp: float,
                               raw_trades=None):
        """Read matched V2 receipt amounts; return None rather than estimate fees."""
        from .execution_receipts import fetch_execution_receipts
        if not order_id or not str(order_id).startswith("0x") or len(str(order_id)) != 66:
            return None
        key = (order_id, token_id, side.upper(), float(target_shares))
        cache = getattr(self, "_execution_receipt_cache", {})
        if key in cache:
            return cache[key]
        try:
            if raw_trades is None:
                self._ensure_client()
                from py_clob_client_v2 import TradeParams
                params = TradeParams(maker_address=self._funder, asset_id=token_id)
                params.after = int(since_timestamp)
                raw_trades = self._client.get_trades(params)
            hashes = []
            for row in raw_trades or []:
                row = row if isinstance(row, dict) else vars(row)
                if str(row.get("status") or "").upper() not in {"CONFIRMED", "MINED"}:
                    continue
                order_ids = {str(row.get("taker_order_id") or ""), str(row.get("order_id") or "")}
                order_ids.update(str(m.get("order_id") or "") for m in row.get("maker_orders", []) or [])
                if order_id in order_ids and row.get("transaction_hash"):
                    hashes.append(row["transaction_hash"])
            if not hashes:
                return None
            owner = self._funder or self._client.signer.address()
            totals = fetch_execution_receipts(
                order_hash=order_id, token_id=token_id, side=side,
                transaction_hashes=hashes, owner_address=owner,
                expected_shares=target_shares,
                rpc_urls=getattr(self._config, "polygon_rpc_urls", DEFAULT_POLYGON_RPC_URLS),
            )
            if totals is not None:
                if len(cache) >= 256:
                    cache.pop(next(iter(cache)))
                cache[key] = totals
                self._execution_receipt_cache = cache
            return totals
        except Exception as exc:
            log.debug("Execution receipt not yet available for %s: %s", order_id, type(exc).__name__)
            return None

    def get_proceeds_from_trades(
        self,
        order_id: Optional[str],
        token_id: Optional[str],
        since_timestamp: float,
        target_shares: float,
    ) -> Optional[float]:
        """Sum actual USDC received from SELL fills for this position.

        Queries /data/trades, filters to SELL fills on ``token_id`` after
        ``since_timestamp`` when a token is provided, handles both TAKER and MAKER cases (the
        common GTC TP sell is a MAKER fill where ``trade.size`` is the
        total match and our portion lives in ``maker_orders[owner]``).

        If ``order_id`` is provided, prefers fills where
        ``taker_order_id`` matches (exact — highest confidence). If none
        match exactly (e.g. we were maker so our order_id isn't the
        taker), falls back to matching SELL fills whose cumulative
        size fits within ``target_shares * 1.05``.

        Retries with 0s/2s/4s/8s backoff while the Polymarket indexer
        catches up. Returns None if the history is still empty after
        all retries — caller falls back to planned proceeds.
        """
        self.last_proceeds_source = ""
        self.last_proceeds_receipt = None
        try:
            self._ensure_client()
            from py_clob_client_v2 import TradeParams

            our_uuid = self._our_uuid()
            delays = [0, 2, 4, 8]
            raw_trades: list = []
            for delay in delays:
                if delay > 0:
                    time.sleep(delay)
                params = TradeParams(maker_address=self._funder)
                if token_id:
                    params.asset_id = token_id
                params.after = int(since_timestamp)
                raw_trades = self._client.get_trades(params)
                if raw_trades:
                    break

            if not raw_trades:
                _emit(f"  [WARN] get_proceeds_from_trades: no trades returned after {len(delays)} attempts")
                return None

            if order_id and token_id:
                receipt = self.get_execution_cashflow(order_id, token_id, "SELL", target_shares, since_timestamp, raw_trades)
                if receipt is not None:
                    self.last_proceeds_source = "v2_receipt"
                    self.last_proceeds_receipt = receipt
                    return float(receipt.net_cashflow)

            normalized: list[dict] = []
            for t in raw_trades:
                d = t if isinstance(t, dict) else t.__dict__
                if str(d.get("status") or "CONFIRMED").upper() != "CONFIRMED":
                    continue
                # Confirmed maker fills have zero exchange fee. A taker's
                # size*price omits its V2 collateral fee and is not net cash.
                if str(d.get("trader_side") or "TAKER").upper() != "MAKER":
                    continue
                our = self._our_portion(d, our_uuid)
                if our is None:
                    continue
                if our["side"] != "SELL":
                    continue
                if token_id and our["asset_id"] != token_id:
                    continue
                normalized.append({
                    "taker_order_id": str(our.get("order_id") or d.get("taker_order_id", "")),
                    "size": our["size"],
                    "price": our["price"],
                    "match_time": str(d.get("match_time", "")),
                })

            # Exact match on order_id (when we were taker on the sell).
            if order_id:
                exact = [x for x in normalized if x["taker_order_id"] == order_id]
                if exact:
                    if abs(sum(x["size"] for x in exact) - target_shares) > 1e-6:
                        return None
                    proceeds = sum(x["size"] * x["price"] for x in exact)
                    _emit(f"  [INFO] get_proceeds_from_trades: order_id exact match, "
                          f"{len(exact)} fill(s), proceeds=${proceeds:.4f}")
                    self.last_proceeds_source = "polymarket_history"
                    return round(proceeds, 6)
                return None

            if not normalized:
                _emit("  [WARN] get_proceeds_from_trades: no SELL fills found")
                return None

            # No exact match (typical for MAKER GTC TPs) — accumulate
            # SELL fills chronologically, capped at target_shares * 1.05.
            if token_id is None:
                _emit(
                    "  [WARN] get_proceeds_from_trades: no exact order match and no token_id; "
                    "refusing fuzzy SELL match"
                )
                return None

            normalized.sort(key=lambda x: x["match_time"])
            cap = target_shares * 1.05
            selected: list[dict] = []
            acc = 0.0
            for f in normalized:
                if acc + f["size"] > cap:
                    if f["size"] > cap:
                        continue
                    break
                selected.append(f)
                acc += f["size"]

            if not selected or abs(acc - target_shares) > 1e-6:
                _emit("  [WARN] get_proceeds_from_trades: cap-filter rejected all fills; target_shares={:.4f}".format(target_shares))
                return None

            proceeds = sum(f["size"] * f["price"] for f in selected)
            self.last_proceeds_source = "polymarket_history"
            _emit(f"  [INFO] get_proceeds_from_trades: {len(selected)} SELL fill(s), "
                  f"{acc:.4f} shares for ${proceeds:.4f} (target {target_shares:.4f})")
            return round(proceeds, 6)

        except Exception as e:
            _emit(f"  [WARN] get_proceeds_from_trades failed: {e}")
            return None

    @property
    def is_live(self) -> bool:
        """Return True - this trades with real money."""
        return True

    def get_usdc_balance(self) -> Optional[float]:
        """Get actual Polymarket collateral balance.

        Prefers CLOB V2 collateral balance, then falls back to pUSD on-chain
        balance for the funder address.

        Returns:
            Collateral balance in dollars, or None if query failed.
        """
        try:
            import requests

            if not self._funder:
                return None

            try:
                self._ensure_client()
                from py_clob_client_v2 import AssetType, BalanceAllowanceParams
                params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
                result = self._client.get_balance_allowance(params)
                if result and "balance" in result:
                    return int(result["balance"]) / 1e6
            except Exception as exc:
                log.warning("get_usdc_balance: CLOB collateral query failed: %s", exc)

            # Query Polygon RPC for collateral balance.
            # 2026-04-28: Polymarket migrated collateral from USDC.e/USDC to pUSD
            # (Polymarket USD), 1:1 backed by USDC. Legacy USDC contracts now
            # return 0 for migrated wallets.
            COLLATERAL_CONTRACTS = [
                "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB",  # pUSD
            ]
            rpc_urls = tuple(getattr(self._config, "polygon_rpc_urls", ()) or ())
            if not rpc_urls:
                rpc_urls = DEFAULT_POLYGON_RPC_URLS

            total_balance = 0.0
            contracts_succeeded = 0
            contracts_total = len(COLLATERAL_CONTRACTS)

            for usdc_addr in COLLATERAL_CONTRACTS:
                # ERC20 balanceOf(address) selector: 0x70a08231
                data = f"0x70a08231000000000000000000000000{self._funder[2:].lower()}"

                payload = {
                    "jsonrpc": "2.0",
                    "method": "eth_call",
                    "params": [{"to": usdc_addr, "data": data}, "latest"],
                    "id": 1,
                }

                for rpc_url in rpc_urls:
                    try:
                        resp = requests.post(rpc_url, json=payload, timeout=10)
                        body = resp.json()
                        if "error" in body:
                            log.warning("get_usdc_balance: RPC %s returned %s", rpc_url, body["error"])
                            continue
                        result = body.get("result", "0x0")
                        balance_raw = int(result, 16)
                        total_balance += balance_raw / 1e6
                        contracts_succeeded += 1
                        break  # Got balance for this contract, move to next
                    except (requests.RequestException, TypeError, ValueError) as exc:
                        log.warning("get_usdc_balance: RPC %s failed: %s", rpc_url, exc)
                        continue

            if contracts_succeeded < contracts_total:
                log.warning(
                    "get_usdc_balance: %s/%s contract queries failed, returning None",
                    contracts_total - contracts_succeeded,
                    contracts_total,
                )
                return None
            return total_balance

        except Exception as e:
            log.error("get_usdc_balance failed: %s", e)
            return None
