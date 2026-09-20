"""Settlement service for expired single-trader positions."""

from __future__ import annotations

import json
import math
from datetime import datetime, timedelta, timezone
from typing import Callable, Optional

from scripts.polymarket_btc_15m import get_binance_price_at, get_chainlink_price_at

from .config import TraderConfig
from .contracts import AUTHORITATIVE_PAYOUT_SOURCES, MarketContract, fetch_market_resolution, payout_for_position
from .data_feed import TickData
from .executor import CancelResult, Executor
from .market_orchestrator import MarketOrchestrator
from .position import Direction, MicroPosition as Position, Trade, TradeResult
from .state import StateManager

ClosePositionCallback = Callable[..., None]


class SettlementService:
    """Resolve expired positions using settlement files and fallback price feeds."""

    _EXIT_FILLED_STATUSES = {"matched", "filled"}
    _INACTIVE_ORDER_STATUSES = {"cancelled", "canceled", "expired", "rejected", "failed"}
    _ACTIVE_ORDER_STATUSES = {"live", "open", "active", "delayed", "pending"}

    def __init__(
        self,
        config: TraderConfig,
        executor: Executor,
        state: StateManager,
        *,
        log_fn: Callable[[str], None] | None = None,
        close_tp_fn: ClosePositionCallback | None = None,
        close_trail_fn: ClosePositionCallback | None = None,
        remember_recently_settled_fn: Callable[[str], None] | None = None,
        market_orchestrator: MarketOrchestrator | None = None,
        chainlink_price_at_fn: Callable[[datetime], Optional[float]] = get_chainlink_price_at,
        binance_price_at_fn: Callable[[datetime], Optional[float]] = get_binance_price_at,
    ) -> None:
        self.config = config
        self.executor = executor
        self.state = state
        self._log = log_fn or (lambda message: None)
        self._close_tp = close_tp_fn
        self._close_trail = close_trail_fn
        self._remember_recently_settled = (
            remember_recently_settled_fn or (lambda token_id: None)
        )
        self._market_orchestrator = market_orchestrator
        self._chainlink_price_at = chainlink_price_at_fn
        self._binance_price_at = binance_price_at_fn
        self._resolution_cache: dict = {}

    def _position_contract(self, position: Position) -> Optional[MarketContract]:
        raw = getattr(position, "contract_metadata", None)
        if self._market_orchestrator is not None:
            payload = self._market_orchestrator.read_market(position.market_id) or {}
            stored = payload.get("contract_metadata")
            if stored and (not raw or (stored.get("condition_id") == raw.get("condition_id")
                    and (MarketContract.from_dict(stored).has_binary_slots or not MarketContract.from_dict(raw).has_binary_slots))):
                raw = stored  # includes recovered slot mapping after a deployment
        if raw:
            return MarketContract.from_dict(raw)
        if getattr(position, "payoff_kind", "legacy") not in ("", "legacy"):
            return MarketContract()  # explicit modern position with missing metadata
        return None

    def resolve_position_payout(self, position: Position, contract: MarketContract) -> tuple[Optional[float], str]:
        payload = None
        if self._market_orchestrator is not None:
            payload = self._market_orchestrator.read_market(position.market_id)
        else:
            path = self.state.state_file.parent / "settlements" / f"{position.market_id}.json"
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                pass
        if (payload and payload.get("settlement_status") == "finalized"
                and payload.get("settlement_source") in AUTHORITATIVE_PAYOUT_SOURCES
                and payload.get("condition_id") == contract.condition_id):
            payout = payout_for_position(payload.get("settlement_payouts"), position.token_id, position.direction)
            if payout in (0.0, 1.0):
                return payout, payload["settlement_source"]
        now = datetime.now(timezone.utc).timestamp()
        cached_at, cached_resolution = self._resolution_cache.get(contract.condition_id, (0.0, None))
        if now - cached_at >= 10.0:
            cached_resolution = fetch_market_resolution(contract, timeout=3.0,
                rpc_urls=tuple(getattr(self.config, "polygon_rpc_urls", ()) or ()))
            self._resolution_cache[contract.condition_id] = (now, cached_resolution)
        if cached_resolution is not None:
            if self._market_orchestrator is not None:
                self._market_orchestrator.finalize_payout(position.market_id, cached_resolution)
            return payout_for_position(cached_resolution.payout_by_token, position.token_id, position.direction), cached_resolution.source
        return None, "authoritative_payout_pending"

    @staticmethod
    def _order_status_name(status: Optional[dict]) -> str:
        return (status or {}).get("status", "").lower()

    def _close_filled_exit_order(
        self,
        position: Position,
        attr: str,
        status: dict,
        result: TradeResult,
    ) -> bool:
        exit_price = status.get(
            "price",
            position.tp_price if attr == "tp_order_id" else position.trail_order_price,
        )
        if result == TradeResult.TP_FILLED:
            if self._close_tp is None:
                return False
            self._close_tp(position, exit_price)
        else:
            if self._close_trail is None:
                return False
            self._close_trail(position, exit_price)
        return True

    def _cancel_order_for_settlement(self, order_id: str) -> CancelResult:
        detailed_cancel = getattr(self.executor, "cancel_order_detailed", None)
        if callable(detailed_cancel):
            return detailed_cancel(order_id)
        try:
            success = self.executor.cancel_order(order_id)
        except Exception as exc:
            return CancelResult(success=False, retryable=True, reason=str(exc))
        return CancelResult(
            success=success,
            retryable=not success,
            reason="" if success else "cancel_order_failed",
        )

    @staticmethod
    def market_end_time_from_id(market_id: str) -> Optional[datetime]:
        """Derive the 15m market end time from btc-updown-15m-{start_epoch}."""
        try:
            start_epoch = int(str(market_id).rsplit("-", 1)[1])
        except (IndexError, TypeError, ValueError):
            return None
        return datetime.fromtimestamp(start_epoch, tz=timezone.utc) + timedelta(seconds=900)

    def mark_pending(self, position: Position, reason: str) -> None:
        """Keep a live position blocked until its own market can be resolved."""
        position.pending_settlement = True
        position.pending_settlement_market_id = (
            position.pending_settlement_market_id or position.market_id
        )
        position.pending_settlement_end_time = (
            position.pending_settlement_end_time
            or self.market_end_time_from_id(position.market_id)
        )
        position.pending_settlement_reason = reason
        self.state.save()

    def has_pending(self) -> bool:
        return any(getattr(pos, "pending_settlement", False) for pos in self.state.open_positions)

    def resolve_settlement_file(self, position: Position) -> tuple[Optional[float], str]:
        market_id = position.pending_settlement_market_id or position.market_id
        if not market_id:
            return None, "settlement_pending"
        payload = None
        if self._market_orchestrator is not None:
            try:
                payload = self._market_orchestrator.read_market(market_id)
            except (OSError, json.JSONDecodeError) as exc:
                self._log(f"Settlement lookup failed for {market_id}: {exc}")
                return None, "settlement_pending"
        else:
            path = self.state.state_file.parent / "settlements" / f"{market_id}.json"
            if not path.exists():
                return None, "settlement_pending"
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                self._log(f"Settlement file lookup failed for {market_id}: {exc}")
                return None, "settlement_pending"
        if payload is None:
            return None, "settlement_pending"
        settlement_source = payload.get("settlement_source")
        settlement_price = payload.get("settlement_price")
        settlement_status = str(payload.get("settlement_status") or "")
        if settlement_price is None and settlement_status == "abandoned":
            return None, "settlement_abandoned"
        if settlement_price is None or settlement_source not in ("chainlink", "polymarket"):
            return None, "settlement_pending"
        try:
            value = float(settlement_price)
            if not math.isfinite(value) or value <= 0:
                return None, "settlement_pending"
            return value, f"{settlement_source}_settlement"
        except (TypeError, ValueError):
            return None, "settlement_pending"

    def resolve_final_spot(self, position: Position) -> tuple[Optional[float], str]:
        """Resolve the final BTC price for the position's own 15m market."""
        if self._position_contract(position) is not None:
            return None, "authoritative_payout_pending"
        end_utc = (
            position.pending_settlement_end_time
            or self.market_end_time_from_id(position.market_id)
        )
        if end_utc is None:
            return None, ""
        settlement_price, settlement_source = self.resolve_settlement_file(position)
        if settlement_price is not None:
            return settlement_price, settlement_source
        settlement_abandoned = settlement_source == "settlement_abandoned"
        wait_seconds = float(getattr(self.config, "settlement_wait_seconds", 20) or 0)
        if wait_seconds > 0 and not settlement_abandoned:
            wait_until = end_utc + timedelta(seconds=wait_seconds)
            if datetime.now(timezone.utc) < wait_until:
                return None, "settlement_pending"
        try:
            final_spot = self._chainlink_price_at(end_utc)
            if final_spot:
                return final_spot, "chainlink_end"
        except Exception as exc:
            self._log(f"Chainlink settlement lookup failed for {position.market_id}: {exc}")
        try:
            final_spot = self._binance_price_at(end_utc)
            if final_spot:
                return final_spot, "binance_end"
        except Exception as exc:
            self._log(f"Binance settlement lookup failed for {position.market_id}: {exc}")
        return None, "settlement_abandoned" if settlement_abandoned else ""

    def confirm_live_orders_inactive_for_resolution(self, position: Position) -> bool:
        """Cancel/verify live exit orders before booking resolution payout."""
        for attr, result in (
            ("tp_order_id", TradeResult.TP_FILLED),
            ("trail_order_id", TradeResult.TRAILING_STOP),
        ):
            order_id = getattr(position, attr, None)
            if not order_id:
                continue
            status = self.executor.get_order_status(order_id)
            status_name = self._order_status_name(status)
            if status_name in self._EXIT_FILLED_STATUSES:
                self._close_filled_exit_order(position, attr, status or {}, result)
                return False
            if status_name not in self._INACTIVE_ORDER_STATUSES:
                cancel_result = self._cancel_order_for_settlement(order_id)
                if not cancel_result.success and not cancel_result.terminal_absent:
                    reason = f": {cancel_result.reason}" if cancel_result.reason else ""
                    self._log(
                        f"Warning: failed to cancel {attr} {order_id} "
                        f"before settlement{reason}"
                    )
                    return False
                status_after = self.executor.get_order_status(order_id)
                status_after_name = self._order_status_name(status_after)
                if status_after_name in self._EXIT_FILLED_STATUSES:
                    self._close_filled_exit_order(
                        position, attr, status_after or {}, result
                    )
                    return False
                if status_after_name in self._ACTIVE_ORDER_STATUSES:
                    self._log(f"Warning: {attr} {order_id} still {status_after_name} after cancel")
                    return False
        try:
            onchain_shares = self.executor.get_position(
                position.token_id, strict=True
            )
        except RuntimeError as exc:
            self._log(f"Warning: position unreadable before settlement: {exc}")
            return False
        local_shares = position.shares
        # Compare on-chain to local. If the position was already closed via
        # a manual sale, partial TP fill, or stale recovery state, local
        # position.shares may be greater than the actual balance; booking
        # local_shares * payout would inflate P&L on a position the wallet
        # no longer holds. Tolerance is asymmetric: on-chain >= local proceeds.
        tolerance = max(0.5, local_shares * 0.05)
        if onchain_shares + tolerance < local_shares:
            self._log(
                f"Warning: settlement guard - on-chain {onchain_shares:.4f} "
                f"shares materially less than local position.shares "
                f"{local_shares:.4f} (tolerance {tolerance:.4f}). "
                f"Refusing settlement."
            )
            self.state.log_risk_event(
                kind="settlement_blocked_balance_mismatch",
                details={
                    "token_id": position.token_id,
                    "onchain_shares": f"{onchain_shares:.4f}",
                    "local_shares": f"{local_shares:.4f}",
                    "tolerance": f"{tolerance:.4f}",
                },
            )
            return False
        return True

    def settle_position_at_final(
        self,
        position: Position,
        final_price: Optional[float],
        final_source: str = "",
        *, payout: Optional[float] = None,
    ) -> None:
        """Record resolution for one position using that position's own market."""
        barrier = position.reference_price
        if not barrier and payout is None:
            self._log("Could not get barrier for settlement")
            return

        if payout is not None:
            if payout not in (0.0, 1.0):
                raise ValueError("unsupported non-binary resolution payout")
            won = payout == 1.0
        else:
            if final_price is None:
                raise ValueError("missing legacy settlement price")
            if not math.isfinite(final_price) or final_price <= 0:
                raise ValueError("invalid legacy settlement price")
            is_up = position.direction == Direction.UP
            up_won = final_price >= barrier
            won = (is_up and up_won) or (not is_up and not up_won)
        if won:
            exit_price = 1.0
            result = TradeResult.WIN_EXPIRY
        else:
            exit_price = 0.0
            result = TradeResult.LOSS_EXPIRY

        proceeds = position.shares * exit_price
        pnl = proceeds - position.cost_basis
        pnl_pct = (pnl / position.cost_basis) * 100 if position.cost_basis > 0 else 0

        position.pending_settlement = False
        position.pending_settlement_reason = ""

        trade = Trade(
            id=position.id,
            direction=position.direction,
            shares=position.shares,
            entry_price=position.entry_price,
            exit_price=exit_price,
            cost_basis=position.cost_basis,
            proceeds=proceeds,
            pnl=pnl,
            pnl_pct=pnl_pct,
            result=result,
            opened_at=position.opened_at,
            market_title=position.market_title,
            reference_price=position.reference_price,
            final_btc_price=final_price,
            entry_context=position.entry_context,
            token_id=position.token_id,
            proceeds_source="resolution",
        )

        self.state.close_position(position, trade)

        if self.executor.is_live:
            self._remember_recently_settled(position.token_id)
            real_balance = self.executor.get_usdc_balance()
            if real_balance is not None and real_balance > 0:
                self.state.current_capital = real_balance

        self.state.log_equity_event(
            event=result.value,
            direction=position.direction.value,
            entry_price=position.entry_price,
            exit_price=exit_price,
            shares=position.shares,
            pnl=pnl,
        )

        win_loss = "WIN" if result == TradeResult.WIN_EXPIRY else "LOSS"
        source_text = f" [{final_source}]" if final_source else ""
        print(f"\n>>> EXPIRY {win_loss}: {position.direction.value}{source_text} <<<")
        if final_price is not None:
            print(f"  Final BTC: ${final_price:,.2f}")
        print(f"  Barrier:   ${barrier:,.2f}")
        print(f"  P&L:       ${pnl:+.2f} ({pnl_pct:+.1f}%)")

    def attempt_settlement(
        self,
        position: Position,
        final_price: Optional[float] = None,
        final_source: str = "",
    ) -> bool:
        """Try to settle a live position; leave it pending on ambiguity."""
        contract = self._position_contract(position)
        if contract is not None:
            payout, payout_source = self.resolve_position_payout(position, contract)
            if payout is None:
                self.mark_pending(position, payout_source)
                return False
            if not self.confirm_live_orders_inactive_for_resolution(position):
                if position in self.state.open_positions:
                    self.mark_pending(position, "orders_or_position_ambiguous")
                return False
            if position in self.state.open_positions:
                self.settle_position_at_final(position, None, payout_source, payout=payout)
                self.state.save()
            return True
        if final_price is None:
            final_price, final_source = self.resolve_final_spot(position)
        if final_price is None:
            self.mark_pending(position, final_source or "final_spot_unavailable")
            return False
        if not self.confirm_live_orders_inactive_for_resolution(position):
            if position in self.state.open_positions:
                self.mark_pending(position, "orders_or_position_ambiguous")
            return False
        if position not in self.state.open_positions:
            return True
        self.settle_position_at_final(position, final_price, final_source)
        self.state.save()
        return True

    def retry_pending(self) -> None:
        """Retry live settlements that were deliberately blocked earlier."""
        if not self.executor.is_live:
            return
        for position in self.state.open_positions[:]:
            if getattr(position, "pending_settlement", False):
                self.attempt_settlement(position)

    def settle_positions(self, tick: TickData) -> None:
        """Settle all open positions at market expiry."""
        if not self.state.open_positions:
            return

        try:
            for position in self.state.open_positions[:]:
                if self.executor.is_live:
                    if position.market_id and position.market_id != tick.market_id:
                        self.attempt_settlement(position)
                        continue
                    self.attempt_settlement(
                        position,
                        tick.final_spot,
                        tick.final_spot_source,
                    )
                else:
                    if getattr(tick, "contract_metadata", None) is not None:
                        payout = payout_for_position(tick.settlement_payouts, position.token_id, position.direction)
                        if payout is None:
                            self.mark_pending(position, "authoritative_payout_pending")
                            continue
                        self.settle_position_at_final(position, None, tick.settlement_source, payout=payout)
                        continue
                    if not tick.final_spot:
                        self._log("Could not get final price for settlement")
                        return
                    if not position.reference_price:
                        position.reference_price = tick.barrier
                    self.settle_position_at_final(
                        position,
                        tick.final_spot,
                        tick.final_spot_source,
                    )

            self.state.save()

        except Exception as e:
            timestamp = datetime.now().strftime("%H:%M:%S")
            print(f"[{timestamp}] [ERROR] Error handling expiry: {e}")
            import traceback

            print(f"[{timestamp}] [ERROR] Traceback: {traceback.format_exc()}")
