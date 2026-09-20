"""Pre-trade risk gates for single-instance trading.

The trading loop keeps orchestration state in ``SingleTrader`` for now, but
entry safety checks live here so they can be tested and changed without
touching order execution or settlement code.
"""

from __future__ import annotations

import time
from typing import Callable, Optional

from btc_pricer.edge import has_edge

from .config import TraderConfig
from .order_book import OrderBookClient
from .position import MicroPosition as Position


RiskEventLogger = Callable[..., None]


class RiskEngine:
    """Pre-trade gates for live and paper entries."""

    def __init__(
        self,
        config: TraderConfig,
        order_book_client: OrderBookClient | None,
        *,
        log_fn: Callable[[str], None] | None = None,
        risk_event_logger: RiskEventLogger | None = None,
    ) -> None:
        self.config = config
        self.order_book_client = order_book_client
        self._log = log_fn or (lambda message: None)
        self._log_risk_event = risk_event_logger or (lambda **kwargs: None)
        self.last_entry_recheck_details: dict = {}
        self.last_spot_drift_details: dict = {}

    def check_concurrency(
        self,
        open_positions: list[Position],
        *,
        is_live: bool,
    ) -> tuple[bool, str]:
        """Reject if the concurrent-position cap or TP-confirmation gate is hit."""
        max_concurrent = getattr(self.config, "max_concurrent_positions", 1)
        if len(open_positions) >= max_concurrent:
            return False, f"open_positions={len(open_positions)}>=max={max_concurrent}"

        require_tp = getattr(self.config, "require_tp_confirmed_before_new_entry", True)
        if is_live and require_tp:
            for pos in open_positions:
                if not getattr(pos, "tp_order_id", None):
                    return False, f"position {pos.id} has no TP order_id"
        return True, ""

    def check_slippage(
        self,
        token_id: str,
        usd_amount: float,
    ) -> Optional[tuple[float, float, float]]:
        """Walk asks and return ``(slippage_pct, best_ask_depth_usd, best_ask)``."""
        if self.order_book_client is None:
            return None
        try:
            book = self.order_book_client.fetch_order_book(token_id)
        except Exception as exc:
            self._log(f"Slippage check: fetch_order_book failed: {exc}")
            return None
        if book is None or not book.asks:
            return None

        shares, avg_price, _ = self.order_book_client.simulate_market_buy(book, usd_amount)
        if shares <= 0 or avg_price <= 0:
            return None

        best_ask_price, best_ask_size = book.asks[0]
        best_ask_depth_usd = best_ask_price * best_ask_size
        slip_pct = (avg_price / best_ask_price) - 1.0 if best_ask_price > 0 else 0.0
        return slip_pct, best_ask_depth_usd, best_ask_price

    def simulate_entry_recheck(
        self,
        token_id: str,
        usd_amount: float,
        opportunity: dict,
    ) -> Optional[float]:
        """Mirror dry-run's current+delayed book walk before live entry."""
        self.last_entry_recheck_details = {
            "token_id": token_id,
            "required_usd": f"{float(usd_amount):.2f}",
            "max_entry_price": f"{float(self.config.max_entry_price):.4f}",
        }
        mirror_controlled = bool(opportunity.get("mirror_controlled_entry"))
        if mirror_controlled:
            self.last_entry_recheck_details["mirror_controlled_entry"] = "1"
        if self.order_book_client is None:
            self.last_entry_recheck_details["reason"] = "no_order_book_client"
            return None

        def simulate_once(label: str) -> Optional[float]:
            book = self.order_book_client.fetch_order_book(token_id)
            if book is None or not book.asks:
                self.last_entry_recheck_details[f"{label}_reason"] = "book_unavailable"
                return None
            shares, avg_price, total_cost = self.order_book_client.simulate_market_buy(
                book,
                usd_amount,
            )
            self.last_entry_recheck_details[f"{label}_avg_price"] = (
                f"{float(avg_price):.4f}" if avg_price else ""
            )
            self.last_entry_recheck_details[f"{label}_fillable_usd"] = f"{float(total_cost):.2f}"
            self.last_entry_recheck_details[f"{label}_shares"] = f"{float(shares):.4f}"
            if shares <= 0 or avg_price <= 0 or total_cost < usd_amount * 0.99:
                self.last_entry_recheck_details[f"{label}_reason"] = "insufficient_fillable_notional"
                return None
            return avg_price

        first_avg = simulate_once("first")
        if first_avg is None:
            self._log("Entry blocked: current book cannot fill intended notional")
            self.last_entry_recheck_details["reason"] = "current_book_no_fill"
            return None

        delay_s = 0.0 if mirror_controlled else max(
            0.0,
            getattr(self.config, "live_recheck_delay_seconds", 0.0),
        )
        self.last_entry_recheck_details["delay_s"] = f"{delay_s:.3f}"
        if delay_s > 0:
            time.sleep(delay_s)
        second_avg = simulate_once("second")
        if second_avg is None:
            self._log("Entry blocked: delayed book cannot fill intended notional")
            self.last_entry_recheck_details["reason"] = "delayed_book_no_fill"
            return None

        entry_price = max(first_avg, second_avg)
        self.last_entry_recheck_details["rechecked_price"] = f"{float(entry_price):.4f}"
        if entry_price > self.config.max_entry_price:
            self._log(
                f"Entry blocked: delayed executable price ${entry_price:.4f} "
                f"> max ${self.config.max_entry_price:.2f}"
            )
            self.last_entry_recheck_details["reason"] = "price_above_max_entry"
            return None

        direction = opportunity.get("direction", "UP")
        model_prob = opportunity.get("model_prob")
        if direction == "UP":
            alpha = self.config.edge_alpha_up
            floor = self.config.edge_floor_up
        else:
            alpha = self.config.edge_alpha_down
            floor = self.config.edge_floor_down
        if model_prob is not None and not has_edge(model_prob, entry_price, alpha, floor):
            self.last_entry_recheck_details["model_prob"] = f"{float(model_prob):.4f}"
            if mirror_controlled:
                self.last_entry_recheck_details["mirror_edge_check"] = "bypassed"
                self.last_entry_recheck_details["reason"] = "accepted_mirror_edge_bypassed"
                return entry_price
            self._log(
                f"Entry blocked: delayed executable price ${entry_price:.4f} "
                "no longer has edge"
            )
            self.last_entry_recheck_details["reason"] = "edge_lost_after_recheck"
            return None
        self.last_entry_recheck_details["reason"] = "accepted"
        return entry_price

    @staticmethod
    def spot_side(spot_price: float, reference_price: float, buffer_usd: float) -> str:
        if spot_price >= reference_price + buffer_usd:
            return "UP"
        if spot_price <= reference_price - buffer_usd:
            return "DOWN"
        return "NEAR"

    def check_spot_drift(
        self,
        opportunity: dict,
        reference_price: float,
        *,
        is_live: bool,
        get_btc_price: Callable[[], Optional[float]],
    ) -> bool:
        """Reject live entries whose fresh spot crossed away from the tick side."""
        self.last_spot_drift_details = {
            "enabled": str(bool(getattr(self.config, "live_entry_spot_recheck_enabled", True))),
            "is_live_gate": str(bool(is_live)),
            "reference_price": f"{float(reference_price):.2f}" if reference_price else "",
        }
        if not is_live:
            self.last_spot_drift_details["reason"] = "not_live_gate"
            return True
        if not getattr(self.config, "live_entry_spot_recheck_enabled", True):
            self.last_spot_drift_details["reason"] = "disabled"
            return True

        direction = opportunity.get("direction")
        if direction not in ("UP", "DOWN") or not reference_price:
            self.last_spot_drift_details["reason"] = "missing_direction_or_reference"
            return True
        tick = opportunity.get("tick")
        tick_spot = getattr(tick, "spot_price", None) if tick is not None else None
        if tick_spot is None:
            self.last_spot_drift_details["reason"] = "missing_tick_spot"
            return True

        buffer_usd = max(
            0.0,
            float(getattr(self.config, "live_entry_spot_recheck_buffer_usd", 0.0) or 0.0),
        )
        try:
            tick_side = self.spot_side(float(tick_spot), float(reference_price), buffer_usd)
        except (TypeError, ValueError):
            self.last_spot_drift_details["reason"] = "invalid_tick_spot"
            return True
        self.last_spot_drift_details.update({
            "direction": direction,
            "tick_spot": f"{float(tick_spot):.2f}",
            "buffer_usd": f"{buffer_usd:.2f}",
            "tick_side": tick_side,
        })

        try:
            fresh_spot = get_btc_price()
        except Exception as exc:
            self._log(f"Entry blocked: fresh spot recheck failed ({exc})")
            self._log_risk_event(
                kind="entry_rejected_spot_recheck_unavailable",
                details={"direction": direction, "reference_price": f"{reference_price:.2f}"},
            )
            self.last_spot_drift_details["reason"] = "fresh_spot_fetch_failed"
            self.last_spot_drift_details["error"] = str(exc)[:160]
            return False
        if fresh_spot is None:
            self._log("Entry blocked: fresh spot recheck returned no price")
            self._log_risk_event(
                kind="entry_rejected_spot_recheck_unavailable",
                details={"direction": direction, "reference_price": f"{reference_price:.2f}"},
            )
            self.last_spot_drift_details["reason"] = "fresh_spot_unavailable"
            return False

        fresh_side = self.spot_side(float(fresh_spot), float(reference_price), buffer_usd)
        self.last_spot_drift_details.update({
            "fresh_spot": f"{float(fresh_spot):.2f}",
            "fresh_side": fresh_side,
        })
        if tick_side == direction and fresh_side != direction:
            self._log(
                f"Entry blocked: fresh spot crossed away from {direction} "
                f"(tick_spot=${float(tick_spot):,.2f}, fresh_spot=${float(fresh_spot):,.2f}, "
                f"barrier=${float(reference_price):,.2f}, buffer=${buffer_usd:.2f})"
            )
            self._log_risk_event(
                kind="entry_rejected_spot_side_flip",
                details={
                    "direction": direction,
                    "tick_spot": f"{float(tick_spot):.2f}",
                    "fresh_spot": f"{float(fresh_spot):.2f}",
                    "reference_price": f"{float(reference_price):.2f}",
                    "buffer_usd": f"{buffer_usd:.2f}",
                    "tick_side": tick_side,
                    "fresh_side": fresh_side,
                },
            )
            self.last_spot_drift_details["reason"] = "spot_side_flip"
            return False
        self.last_spot_drift_details["reason"] = "accepted"
        return True
