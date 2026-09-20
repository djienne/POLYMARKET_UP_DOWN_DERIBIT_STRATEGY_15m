"""Lightweight paper trading instance for 15-min grid search.

Each PaperInstance processes shared ticks and cached venue-availability
checks. Entry costs walk executable depth with market fees; expiry waits
for the position's resolved token payout.
"""

import copy
import json
import logging
import math
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from typing import Optional

from btc_pricer.constants import MAX_CONTRACT_PRICE, MIN_FILL_RATIO, MIN_ORDER_USD
from btc_pricer.edge import has_edge

from scripts.trader.config import GridConfig, InstanceParams
from scripts.trader.contracts import entry_inputs_valid, fee_for_fill, fee_schedule_valid, payout_for_position
from scripts.trader.data_feed import TickData
from scripts.trader.grid_diagnostics import GridEntryDiagnostic
from scripts.trader.order_book import MutableOrderBook, OrderBookClient, OrderBookSnapshot
from scripts.trader.paper_fills import buy_with_fees as _simulate_buy_with_fees, sell_fee_by_level as _sell_fee_by_level
from scripts.trader import availability
from scripts.trader.clob_health import taker_outage_active
from scripts.trader.polymarket_status import clob_reads_healthy, is_polymarket_operational
from scripts.trader.position import (
    Direction, MicroPosition, TradeEvent, TradeResult,
)

log = logging.getLogger(__name__)

# Shared simulator for immutable snapshots.
_ob_client = OrderBookClient()


def _simulate_market_buy(order_book, usd_amount: float, consume_liquidity: bool = False):
    """Simulate a market buy for immutable or mutable books."""
    if not order_book:
        return 0.0, 0.0, 0.0
    if isinstance(order_book, MutableOrderBook):
        return order_book.simulate_market_buy(usd_amount, consume=consume_liquidity)
    return _ob_client.simulate_market_buy(order_book, usd_amount)


def _simulate_market_sell(order_book, shares: float, consume_liquidity: bool = False):
    """Simulate a market sell for immutable or mutable books."""
    if not order_book:
        return 0.0, 0.0, 0.0
    if isinstance(order_book, MutableOrderBook):
        return order_book.simulate_market_sell(shares, consume=consume_liquidity)
    return _ob_client.simulate_market_sell(order_book, shares)


BookLike = Optional[OrderBookSnapshot | MutableOrderBook]


def _limit_sell_quantity(order_book, limit_price: float, shares: float, *, consume: bool = False) -> float:
    """Only bids at or above a maker limit can establish an executable quantity."""
    remaining = shares
    for level in order_book.bids:
        price, quantity = float(level[0]), float(level[1])
        if not (math.isfinite(price) and 0 < price <= 1 and math.isfinite(quantity) and quantity >= 0):
            return 0.0
        if price < limit_price:
            break
        filled = min(max(quantity, 0.0), remaining)
        if consume and isinstance(order_book, MutableOrderBook):
            level[1] -= filled
        remaining -= filled
        if remaining <= 1e-9:
            break
    if consume and isinstance(order_book, MutableOrderBook):
        order_book.bids = [level for level in order_book.bids if level[1] > 1e-9]
    return shares - remaining


@dataclass
class _EntryPlan:
    """A validated, side-effect-free paper entry, ready to stage or commit.

    Produced by ``_compute_entry_plan`` (no capital debit, no liquidity
    consumption, no event). The caller decides whether to stage it for the
    confirmation hold or commit it immediately.
    """

    direction: Direction
    token_id: str
    order_usd: float
    shares: float
    entry_price: float
    total_cost: float
    cost_basis: float
    tp_price: float
    sl_price: float
    effective_tp_pct: float
    model_prob: float
    poly_prob: float
    alpha: float
    floor: float
    current_order_book: BookLike = None
    delayed_order_book: BookLike = None
    current_tuple: tuple = (None, None, None)
    delayed_tuple: tuple = (None, None, None)
    fee_schedule: Optional[dict] = None
    fee_usd: float = 0.0


@dataclass
class StagedEntry:
    """An entry decision held for ``grid_entry_confirm_seconds`` before commit.

    The grid records a paper fill instantly, but the live trader needs a few
    seconds to POST a real order and discover a 425 / post-only taker outage.
    Holding the fill lets the live trader's reactive CLOB-health flag land first,
    so the grid drops entries live could not place (dry-run / live parity).

    Intentionally NOT persisted in ``to_snapshot``/``load_snapshot``: a staged
    entry that survives a restart is discarded (fail toward not-trading the
    unconfirmed entry), matching the existing "no phantom on restart" posture.
    """

    market_id: str
    staged_at: datetime
    plan: _EntryPlan
    signal: Optional[dict] = None


@dataclass
class PaperInstance:
    """One paper trading instance with a fixed parameter set."""

    id: int
    params: InstanceParams
    shared_config: GridConfig

    # Capital
    capital: float = 0.0
    total_pnl: float = 0.0

    # Position (at most 1 per market)
    position: Optional[MicroPosition] = None
    last_tp_sl_time: Optional[datetime] = None

    # Stats
    trade_pnls: list = field(default_factory=list)
    pnl_sum: float = 0.0
    pnl_sum_sq: float = 0.0
    pnl_count: int = 0
    pnl_moments_complete: bool = True
    realized_peak: float = 0.0
    max_drawdown: float = 0.0
    max_drawdown_pct: float = 0.0
    drawdown_history_complete: bool = True
    last_close_at: Optional[datetime] = None
    last_close_result: str = ""
    wins: int = 0
    losses: int = 0
    trades_count: int = 0

    # Markets whose expiry we've already emitted an event for. Persisted in
    # the snapshot so a crash between CSV flush and state save doesn't cause
    # a phantom force-LOSS on restart (fix #1).
    settled_market_ids: set = field(default_factory=set)

    # Last market_id this instance ENTERED a position in. Used to enforce a
    # one-position-per-15m-window rule: after an exit, no re-entry in the same
    # market regardless of grace period or remaining tradeable time.
    last_entered_market_id: Optional[str] = None

    # In-flight entry held for grid_entry_confirm_seconds before commit (see
    # StagedEntry). Never persisted: discarded on restart, cleared on market
    # rollover, and dropped if a CLOB taker outage is discovered during the hold.
    staged_entry: Optional[StagedEntry] = None

    def __post_init__(self):
        if self.capital == 0.0:
            self.capital = self.shared_config.starting_capital
        if self.realized_peak == 0.0:
            self.realized_peak = self.shared_config.starting_capital

    # -- Snapshot persistence --

    def to_snapshot(self) -> dict:
        """Serialize instance state for JSON persistence."""
        pos = None
        if self.position:
            pos = {
                "direction": self.position.direction.value,
                "token_id": self.position.token_id,
                "entry_price": self.position.entry_price,
                "shares": self.position.shares,
                "cost_basis": self.position.cost_basis,
                "tp_price": self.position.tp_price,
                "stop_loss_price": self.position.stop_loss_price,
                "opened_at": self.position.opened_at.isoformat(),
                "market_id": self.position.market_id,
                "peak_price": self.position.peak_price,
                "reference_price": self.position.reference_price,
                "barrier_source": self.position.barrier_source,
                "effective_tp_pct": self.position.effective_tp_pct,
                "fee_schedule": dict(self.position.fee_schedule) if self.position.fee_schedule is not None else None,
                "decision_at": self.position.decision_at.isoformat() if self.position.decision_at else None,
                "condition_id": self.position.condition_id,
                "payoff_kind": self.position.payoff_kind,
                "averaging_window_seconds": self.position.averaging_window_seconds,
                "contract_metadata": copy.deepcopy(self.position.contract_metadata),
            }
        return {
            "version": 1,
            "experiment_id": getattr(self.shared_config, "experiment_id", "legacy"),
            "params": {
                "alpha_up": float(self.params.alpha_up),
                "alpha_down": float(self.params.alpha_down),
                "floor_up": float(self.params.floor_up),
                "floor_down": float(self.params.floor_down),
                "tp_percentage": float(self.params.tp_percentage),
                "stop_loss_pct": float(self.params.stop_loss_pct),
            },
            "capital": self.capital,
            "total_pnl": self.total_pnl,
            "wins": self.wins,
            "losses": self.losses,
            "trades_count": self.trades_count,
            "trade_pnls": list(self.trade_pnls),
            "pnl_sum": self.pnl_sum,
            "pnl_sum_sq": self.pnl_sum_sq,
            "pnl_count": self.pnl_count,
            "pnl_moments_complete": self.pnl_moments_complete,
            "realized_peak": self.realized_peak,
            "max_drawdown": self.max_drawdown,
            "max_drawdown_pct": self.max_drawdown_pct,
            "drawdown_history_complete": self.drawdown_history_complete,
            "last_close_at": self.last_close_at.isoformat() if self.last_close_at else None,
            "last_close_result": self.last_close_result,
            "position": pos,
            "last_tp_sl_time": self.last_tp_sl_time.isoformat() if self.last_tp_sl_time else None,
            "settled_market_ids": sorted(self.settled_market_ids),
            "last_entered_market_id": self.last_entered_market_id,
        }

    def load_snapshot(self, snap: dict):
        """Restore instance state from a snapshot dict."""
        if not isinstance(snap, dict):
            log.warning("inst#%s: ignoring invalid snapshot payload", self.id)
            return

        try:
            self.capital = float(snap.get("capital", self.capital))
            self.total_pnl = float(snap.get("total_pnl", self.total_pnl))
            self.wins = int(snap.get("wins", self.wins))
            self.losses = int(snap.get("losses", self.losses))
            self.trades_count = int(snap.get("trades_count", self.trades_count))
            self.restore_pnl_history(
                list(snap.get("trade_pnls") or []),
                total_pnl=float(snap.get("pnl_sum", snap.get("total_pnl", self.total_pnl))),
                pnl_sum_sq=float(snap["pnl_sum_sq"]) if "pnl_sum_sq" in snap else None,
                pnl_count=int(snap.get("pnl_count", self.trades_count)),
            )
            self.pnl_moments_complete = bool(snap.get("pnl_moments_complete", self.pnl_moments_complete))
            if all(name in snap for name in ("realized_peak", "max_drawdown", "max_drawdown_pct")):
                self.realized_peak = float(snap["realized_peak"])
                self.max_drawdown = float(snap["max_drawdown"])
                self.max_drawdown_pct = float(snap["max_drawdown_pct"])
                self.drawdown_history_complete = bool(snap.get("drawdown_history_complete", True))
            close_at = snap.get("last_close_at")
            self.last_close_at = datetime.fromisoformat(close_at) if close_at else None
            self.last_close_result = str(snap.get("last_close_result") or "")
            tp_sl = snap.get("last_tp_sl_time")
            self.last_tp_sl_time = datetime.fromisoformat(tp_sl) if tp_sl else None
            self.settled_market_ids = set(snap.get("settled_market_ids") or [])
            self._prune_settled_market_ids()
            self.last_entered_market_id = snap.get("last_entered_market_id")
        except (TypeError, ValueError) as exc:
            log.warning("inst#%s: ignoring corrupt snapshot fields: %s", self.id, exc)
            return

        pos_data = snap.get("position")
        if not pos_data:
            self.position = None
            return

        try:
            entry_price = float(pos_data["entry_price"])
            self.position = MicroPosition(
                direction=Direction(pos_data["direction"]),
                token_id=pos_data.get("token_id", ""),
                entry_price=entry_price,
                shares=float(pos_data["shares"]),
                cost_basis=float(pos_data["cost_basis"]),
                tp_price=float(pos_data["tp_price"]),
                stop_loss_price=float(pos_data.get("stop_loss_price", 0.0)),
                opened_at=datetime.fromisoformat(pos_data["opened_at"]),
                market_id=pos_data["market_id"],
                peak_price=float(pos_data.get("peak_price", entry_price)),
                reference_price=float(pos_data.get("reference_price", 0.0)),
                barrier_source=pos_data.get("barrier_source", ""),
                effective_tp_pct=float(pos_data.get("effective_tp_pct", 0.0)),
                fee_schedule=pos_data.get("fee_schedule"),
                decision_at=datetime.fromisoformat(pos_data["decision_at"]) if pos_data.get("decision_at") else None,
                condition_id=str(pos_data.get("condition_id") or ""),
                payoff_kind=str(pos_data.get("payoff_kind") or "legacy"),
                averaging_window_seconds=int(pos_data.get("averaging_window_seconds") or 0),
                contract_metadata=dict(pos_data.get("contract_metadata") or {}),
            )
        except (KeyError, TypeError, ValueError) as exc:
            log.warning("inst#%s: dropping unrestorable snapshot position: %s", self.id, exc)
            self.position = None

    def _max_trade_pnls(self) -> int:
        return max(1, int(getattr(self.shared_config, "max_trade_pnls", 1000) or 1000))

    def _max_settled_market_ids(self) -> int:
        return max(1, int(getattr(self.shared_config, "max_settled_market_ids", 1000) or 1000))

    def _record_closed_pnl(self, pnl: float) -> None:
        self.pnl_sum += pnl
        self.pnl_sum_sq += pnl * pnl
        self.pnl_count += 1
        realized_equity = self.shared_config.starting_capital + self.pnl_sum
        self.realized_peak = max(self.realized_peak, realized_equity)
        if self.realized_peak > 0:
            drawdown = self.realized_peak - realized_equity
            self.max_drawdown = max(self.max_drawdown, drawdown)
            self.max_drawdown_pct = max(self.max_drawdown_pct, 100 * drawdown / self.realized_peak)
        self.trade_pnls.append(pnl)
        max_pnls = self._max_trade_pnls()
        if len(self.trade_pnls) > max_pnls:
            del self.trade_pnls[: len(self.trade_pnls) - max_pnls]

    def restore_pnl_history(
        self,
        pnls: list[float],
        *,
        total_pnl: float | None = None,
        pnl_sum_sq: float | None = None,
        pnl_count: int | None = None,
    ) -> None:
        """Restore bounded recent PnL history plus unbounded running stats."""
        max_pnls = self._max_trade_pnls()
        values = [float(p) for p in pnls]
        self.trade_pnls = values[-max_pnls:]
        self.pnl_sum = float(total_pnl if total_pnl is not None else sum(values))
        self.pnl_sum_sq = float(
            pnl_sum_sq if pnl_sum_sq is not None and pnl_sum_sq > 0 else sum(p * p for p in values)
        )
        self.pnl_count = int(pnl_count if pnl_count is not None else len(values))
        self.pnl_moments_complete = self.pnl_count == len(values) or pnl_sum_sq is not None
        self.realized_peak = self.shared_config.starting_capital
        self.max_drawdown = self.max_drawdown_pct = 0.0
        self.drawdown_history_complete = self.pnl_count == len(values)
        if self.drawdown_history_complete:
            equity = self.shared_config.starting_capital
            for pnl in values:
                equity += pnl
                self.realized_peak = max(self.realized_peak, equity)
                if self.realized_peak > 0:
                    drawdown = self.realized_peak - equity
                    self.max_drawdown = max(self.max_drawdown, drawdown)
                    self.max_drawdown_pct = max(self.max_drawdown_pct, 100 * drawdown / self.realized_peak)

    def _remember_settled_market_id(self, market_id: str) -> None:
        if not market_id:
            return
        self.settled_market_ids.add(market_id)
        self._prune_settled_market_ids()

    def _prune_settled_market_ids(self) -> None:
        max_ids = self._max_settled_market_ids()
        if len(self.settled_market_ids) <= max_ids:
            return
        self.settled_market_ids = set(sorted(self.settled_market_ids)[-max_ids:])

    # -- Main tick handler --

    def on_tick(
        self,
        tick: TickData,
        diagnostics: Optional[list[GridEntryDiagnostic]] = None,
    ) -> list[TradeEvent]:
        """Process one tick. Returns trade events (entry/exit) if any."""
        events = []

        transition_event = self.handle_market_transition(tick)
        if transition_event:
            events.append(transition_event)
            return events

        if tick.settlement_pending or tick.is_expired or tick.spot_price <= 0:
            return events
        if self.position is not None and self.position.market_id != tick.market_id:
            return events

        if self.position:
            exit_event = self.try_exit(
                tick,
                tick.up_order_book,
                tick.up_order_book_delayed,
                tick.down_order_book,
                tick.down_order_book_delayed,
            )
            if exit_event:
                events.append(exit_event)
                return events

        if not self.position:
            entry_event = self.try_entry(
                tick,
                tick.up_order_book,
                tick.up_order_book_delayed,
                tick.down_order_book,
                tick.down_order_book_delayed,
                diagnostics=diagnostics,
            )
            if entry_event:
                events.append(entry_event)

        return events

    def handle_market_transition(self, tick: TickData) -> Optional[TradeEvent]:
        """Handle expiry and market rollover before normal trading."""
        # A staged entry never crosses a window boundary: drop it if the market
        # rolled over before its confirmation hold elapsed (we never finalize an
        # entry into a market that is expiring/changed). _check_staged_entry has a
        # same-market backstop, but this fires first in on_tick.
        if self.staged_entry is not None and (
            tick.is_new_market or self.staged_entry.market_id != tick.market_id
        ):
            self.staged_entry = None

        if tick.is_expired and self.position:
            return self._settle_expiry(tick)

        if tick.is_new_market and self.position and self.position.market_id != tick.market_id:
            # Fix #1: if we already emitted an expiry event for this market
            # (crash between CSV flush and state save), clear the position
            # silently to avoid duplicating the settlement row.
            if self.position.market_id in self.settled_market_ids:
                log.info(
                    f"inst#{self.id}: clearing already-settled position from "
                    f"{self.position.market_id} (idempotent restart)"
                )
                self.position = None
                return None

            # The main loop retries this position's own finalized payout
            # record. A market rollover does not refund unresolved exposure.
            return None

        return None

    # -- Entry logic --

    @staticmethod
    def _decision_time(tick: TickData) -> datetime:
        return datetime.now(timezone.utc) if getattr(tick, "payoff_kind", "legacy") != "legacy" else tick.timestamp

    def _final_entry_guard(self, tick, signal, diagnostics) -> bool:
        """Recheck age and window after blocking probes, immediately before action."""
        now = self._decision_time(tick)
        valid, reason = entry_inputs_valid(tick, now=now)
        if valid and getattr(tick, "payoff_kind", "legacy") != "legacy":
            try:
                remaining = (tick.market_end_time - now).total_seconds()
                duration = float(tick.window_duration_seconds)
                elapsed = duration - remaining
                if not math.isfinite(remaining) or not math.isfinite(duration) or duration <= 0:
                    reason = "invalid_market_window"
                elif remaining <= 0:
                    reason = "market_expired"
                elif remaining < self.shared_config.no_trade_last_seconds:
                    reason = "blocked_last_window"
                elif elapsed < self.shared_config.no_trade_first_seconds:
                    reason = "blocked_first_window"
            except (AttributeError, TypeError, ValueError):
                reason = "invalid_market_window"
        if reason:
            self._append_entry_diagnostic(diagnostics, tick, signal, rejection_reason=reason)
            return False
        return valid

    @staticmethod
    def _signal_float(signal: Optional[dict], key: str) -> Optional[float]:
        if not signal:
            return None
        raw = signal.get(key)
        if raw in (None, ""):
            return None
        try:
            return float(raw)
        except (TypeError, ValueError):
            return None

    def _matching_entry_signal(self, tick: TickData) -> Optional[dict]:
        index = getattr(tick, "entry_signal_index", None)
        if index is not None:
            return index.get(str(self.id))
        # Legacy scan for ticks built without the index (older producers).
        for sig in tick.entry_signals or ():
            if str(sig.get("instance_id")) == str(self.id):
                return sig
        return None

    def _append_entry_diagnostic(
        self,
        diagnostics: Optional[list[GridEntryDiagnostic]],
        tick: TickData,
        signal: Optional[dict],
        *,
        entered: bool = False,
        direction: str = "",
        rejection_reason: str = "",
        rejection_details: Optional[dict] = None,
        capital_before: Optional[float] = None,
        order_size_usd: Optional[float] = None,
        current_avg_price: Optional[float] = None,
        current_fillable_usd: Optional[float] = None,
        current_shares: Optional[float] = None,
        delayed_avg_price: Optional[float] = None,
        delayed_fillable_usd: Optional[float] = None,
        delayed_shares: Optional[float] = None,
        selected_entry_price: Optional[float] = None,
    ) -> None:
        if diagnostics is None or signal is None:
            return
        cfg = self.shared_config
        p = self.params
        if not direction:
            direction = str(signal.get("direction", "") or "")
        details = json.dumps(rejection_details, sort_keys=True) if rejection_details else ""
        diagnostics.append(
            GridEntryDiagnostic(
                timestamp=tick.timestamp,
                market_id=tick.market_id,
                instance_id=self.id,
                entered=entered,
                direction=direction,
                alpha_up=p.alpha_up,
                alpha_down=p.alpha_down,
                floor_up=p.floor_up,
                floor_down=p.floor_down,
                tp_pct=p.tp_percentage,
                sl_pct=p.stop_loss_pct,
                capital_before=self.capital if capital_before is None else capital_before,
                order_size_usd=order_size_usd,
                signal_price=self._signal_float(signal, "executable_price"),
                signal_model_prob=self._signal_float(signal, "model_prob"),
                signal_required_prob=self._signal_float(signal, "required_prob"),
                signal_edge_ratio=self._signal_float(signal, "edge_ratio"),
                current_avg_price=current_avg_price,
                current_fillable_usd=current_fillable_usd,
                current_shares=current_shares,
                delayed_avg_price=delayed_avg_price,
                delayed_fillable_usd=delayed_fillable_usd,
                delayed_shares=delayed_shares,
                selected_entry_price=selected_entry_price,
                max_entry_price=cfg.max_entry_price,
                execution_max_entry_price=cfg.execution_max_entry_price(),
                paper_delayed_book_slippage=cfg.paper_delayed_book_slippage,
                rejection_reason=rejection_reason,
                rejection_details=details,
            )
        )

    def try_entry(
        self,
        tick: TickData,
        up_order_book: BookLike,
        up_order_book_delayed: BookLike,
        down_order_book: BookLike,
        down_order_book_delayed: BookLike,
        consume_liquidity: bool = False,
        diagnostics: Optional[list[GridEntryDiagnostic]] = None,
    ) -> Optional[TradeEvent]:
        """Check if we should enter a position."""
        cfg = self.shared_config

        p = self.params
        signal = self._matching_entry_signal(tick)
        if self.staged_entry is not None and signal is None:
            signal = self.staged_entry.signal

        def _skip(reason: str, **details) -> Optional[TradeEvent]:
            if self.staged_entry is not None:
                staged_reason = {
                    "clob_taker_outage": "staged_dropped_clob_outage",
                    "blocked_last_window": "staged_dropped_last_window",
                }.get(reason, f"staged_dropped_{reason}")
                return self._drop_staged(tick, staged_reason, diagnostics, **details)
            self._append_entry_diagnostic(
                diagnostics,
                tick,
                signal,
                rejection_reason=reason,
                rejection_details=details or None,
            )
            return None

        if self.position or tick.settlement_pending:
            return _skip("has_open_position" if self.position else "settlement_pending")
        valid, reason = entry_inputs_valid(tick)
        if not valid:
            return _skip(reason)
        if tick.is_expired:
            return _skip("market_expired")

        # One-position-per-window rule: refuse re-entry in a market we've
        # already opened a position in this window, even after a TP/SL exit.
        if (
            tick.market_id
            and tick.market_id == self.last_entered_market_id
        ):
            return _skip("already_entered_market", market_id=tick.market_id)

        if not tick.barrier_confirmed:
            return _skip("barrier_unconfirmed", barrier_source=tick.barrier_source)
        # Skip entries while Polymarket is undergoing maintenance — quotes go
        # stale or vanish, so any "edge" computed against stale prob_up/prob_down
        # is unreliable.
        if not is_polymarket_operational():
            return _skip("polymarket_down")
        # CLOB taker-health gate: skip while the live trader's shared flag marks
        # a taker outage (post-only mode / 425 / transport failure on a real
        # order POST, or its read-only probe finding the market not accepting
        # orders). The grid only reads the flag — no network, no orders — so the
        # dry-run leaderboard stops "winning" trades the live trader could not
        # actually place. Self-expiring flag => fail-open if live isn't writing.
        if getattr(cfg, "clob_health_gate_enabled", True):
            outage, clob_reason, until = taker_outage_active(
                getattr(cfg, "clob_health_shared_path", None)
                or "results/15m_clob_health.json"
            )
            if outage:
                return _skip(
                    "clob_taker_outage",
                    clob_reason=clob_reason or "",
                    unhealthy_until=until.isoformat() if until else "",
                )
            # Also probe directly (read-only) so the grid still gates if the live
            # trader — the flag's only writer — is stopped. Cached, so the
            # grid tick triggers at most one network refresh per TTL.
            reads_ok, reads_reason = clob_reads_healthy()
            if not reads_ok:
                return _skip("clob_unavailable", clob_reason=reads_reason or "")
        # Optional second gate: consult the normalized availability ledger
        # (confirmed intervals only), controlled by availability_gate_enabled,
        # fail-open on a missing/corrupt ledger, cached read so all instances
        # share one parse per TTL. Additive to the hot-flag gate above.
        if getattr(cfg, "availability_gate_enabled", False):
            led_active, led_reason, _iv = availability.untradable_at(
                tick.timestamp,
                market_id=tick.market_id,
                ledger_path=getattr(cfg, "availability_ledger_path", None)
                or "results/availability_ledger.csv",
                min_confidence=getattr(cfg, "availability_gate_min_confidence", "confirmed")
                or "confirmed",
            )
            if led_active:
                return _skip("availability_ledger_outage", clob_reason=led_reason or "")
        if tick.elapsed_seconds < cfg.no_trade_first_seconds:
            return _skip(
                "blocked_first_window",
                elapsed_seconds=f"{tick.elapsed_seconds:.3f}",
                no_trade_first_seconds=cfg.no_trade_first_seconds,
            )
        if tick.time_remaining_seconds < cfg.no_trade_last_seconds:
            return _skip(
                "blocked_last_window",
                remaining_seconds=f"{tick.time_remaining_seconds:.3f}",
                no_trade_last_seconds=cfg.no_trade_last_seconds,
            )

        if self.last_tp_sl_time:
            since = (tick.timestamp - self.last_tp_sl_time).total_seconds()
            if since < cfg.grace_period_seconds:
                return _skip(
                    "grace_period",
                    seconds_since_exit=f"{since:.3f}",
                    grace_period_seconds=cfg.grace_period_seconds,
                )

        if self.staged_entry is not None:
            return self._check_staged_entry(
                tick, up_order_book, up_order_book_delayed, down_order_book, down_order_book_delayed,
                consume_liquidity=consume_liquidity, diagnostics=diagnostics,
            )

        if signal is None:
            return None

        dir_str = signal.get("direction", "")
        if dir_str not in ("UP", "DOWN"):
            return _skip("invalid_signal_direction", direction=dir_str)
        signal_price = self._signal_float(signal, "executable_price")
        if signal_price is not None and signal_price > cfg.max_entry_price:
            return _skip(
                "signal_price_above_max_entry",
                signal_price=f"{signal_price:.4f}",
                max_entry_price=f"{cfg.max_entry_price:.4f}",
            )
        direction = Direction.UP if dir_str == "UP" else Direction.DOWN
        if direction == Direction.UP:
            current_book = up_order_book
            delayed_book = up_order_book_delayed or up_order_book
            poly_prob = tick.poly_prob_up
            token_id = tick.up_token_id
        else:
            current_book = down_order_book
            delayed_book = down_order_book_delayed or down_order_book
            poly_prob = tick.poly_prob_down
            token_id = tick.down_token_id
        if current_book is None or current_book.best_ask <= 0 or not current_book.asks:
            return _skip("book_unavailable", side="current", direction=dir_str)
        return self._execute_entry(
            tick, direction, poly_prob, signal.get("model_prob", 0),
            current_book, delayed_book, token_id,
            p.alpha_up if direction == Direction.UP else p.alpha_down,
            p.floor_up if direction == Direction.UP else p.floor_down,
            signal=signal,
            diagnostics=diagnostics,
            consume_liquidity=consume_liquidity,
        )

    def _compute_entry_plan(
        self,
        tick: TickData,
        direction: Direction,
        poly_prob: float,
        model_prob: float,
        current_order_book: BookLike,
        delayed_order_book: BookLike,
        token_id: str,
        alpha: float = 1.0,
        floor: float = 0.5,
        signal: Optional[dict] = None,
        diagnostics: Optional[list[GridEntryDiagnostic]] = None,
    ) -> Optional[_EntryPlan]:
        """Validate + price a paper entry against the current/delayed books.

        Pure and side-effect-free: returns a fully-validated ``_EntryPlan`` (no
        capital debit, no liquidity consumption, no trade event) or ``None`` with
        a rejection diagnostic. The caller (``_execute_entry``) decides whether to
        stage the plan for the confirmation hold or commit it immediately.
        """
        cfg = self.shared_config
        p = self.params

        capital_before = self.capital
        order_usd = round(self.capital * cfg.order_size_pct, 2)
        execution_cap = cfg.execution_max_entry_price()
        direction_str = direction.value

        def _diag(
            reason: str = "",
            *,
            entered: bool = False,
            details: Optional[dict] = None,
            current: tuple = (None, None, None),
            delayed: tuple = (None, None, None),
            selected_entry_price: Optional[float] = None,
        ) -> None:
            self._append_entry_diagnostic(
                diagnostics,
                tick,
                signal,
                entered=entered,
                direction=direction_str,
                rejection_reason=reason,
                rejection_details=details,
                capital_before=capital_before,
                order_size_usd=order_usd,
                current_shares=current[0],
                current_avg_price=current[1],
                current_fillable_usd=current[2],
                delayed_shares=delayed[0],
                delayed_avg_price=delayed[1],
                delayed_fillable_usd=delayed[2],
                selected_entry_price=selected_entry_price,
            )

        if order_usd < MIN_ORDER_USD:
            _diag(
                "order_size_below_minimum",
                details={"order_size_usd": f"{order_usd:.2f}", "min_order_usd": MIN_ORDER_USD},
            )
            return None
        if not current_order_book or not current_order_book.asks:
            _diag("book_unavailable", details={"side": "current"})
            return None
        if not delayed_order_book or not delayed_order_book.asks:
            _diag("book_unavailable", details={"side": "delayed"})
            return None

        if not isinstance(model_prob, (float, int)) or not math.isfinite(model_prob) or not 0 <= model_prob <= 1:
            _diag("invalid_probability")
            return None

        fee_schedule = getattr(tick, "fee_schedule", None)
        current_fee = delayed_fee = 0.0
        if fee_schedule is not None:
            if not fee_schedule_valid(fee_schedule):
                _diag("unsupported_fee_schedule")
                return None
            try:
                shares, avg_price, total_cost, current_fee = _simulate_buy_with_fees(current_order_book, order_usd, fee_schedule)
                d_shares, d_avg, d_cost, delayed_fee = _simulate_buy_with_fees(delayed_order_book, order_usd, fee_schedule)
            except ValueError as exc:
                _diag("invalid_fee_or_book", details={"reason": str(exc)})
                return None
        else:
            shares, avg_price, total_cost = _simulate_market_buy(current_order_book, order_usd)
            d_shares, d_avg, d_cost = _simulate_market_buy(delayed_order_book, order_usd)
        current_tuple = (shares, avg_price, total_cost + current_fee)
        if shares <= 0 or total_cost + current_fee < order_usd * MIN_FILL_RATIO:
            _diag(
                "current_fill_insufficient",
                details={
                    "required_usd": f"{order_usd:.2f}",
                    "min_fill_ratio": f"{MIN_FILL_RATIO:.4f}",
                },
                current=current_tuple,
            )
            return None

        delayed_tuple = (d_shares, d_avg, d_cost + delayed_fee)
        if d_shares <= 0 or d_cost + delayed_fee < order_usd * MIN_FILL_RATIO:
            _diag(
                "delayed_fill_insufficient",
                details={
                    "required_usd": f"{order_usd:.2f}",
                    "min_fill_ratio": f"{MIN_FILL_RATIO:.4f}",
                },
                current=current_tuple,
                delayed=delayed_tuple,
            )
            return None

        fee_usd = current_fee
        if (d_cost + delayed_fee) / d_shares > (total_cost + current_fee) / shares:
            shares = d_shares
            avg_price = d_avg
            total_cost = d_cost
            fee_usd = delayed_fee

        entry_price = avg_price
        cost_basis = total_cost + fee_usd if fee_schedule is not None else total_cost * (1 + cfg.friction_pct)
        executable_unit_cost = cost_basis / shares if fee_schedule is not None else entry_price
        if not has_edge(model_prob, executable_unit_cost, alpha, floor):
            _diag(
                "edge_lost_after_vwap",
                details={
                    "model_prob": f"{float(model_prob):.4f}",
                    "entry_price": f"{entry_price:.4f}",
                    "unit_cost_after_fees": f"{executable_unit_cost:.4f}",
                    "alpha": f"{alpha:.4f}",
                    "floor": f"{floor:.4f}",
                },
                current=current_tuple,
                delayed=delayed_tuple,
                selected_entry_price=entry_price,
            )
            return None
        if entry_price > execution_cap:
            _diag(
                "execution_price_above_slippage_cap",
                details={
                    "entry_price": f"{entry_price:.4f}",
                    "execution_max_entry_price": f"{execution_cap:.4f}",
                    "max_entry_price": f"{cfg.max_entry_price:.4f}",
                    "paper_delayed_book_slippage": f"{cfg.paper_delayed_book_slippage:.4f}",
                },
                current=current_tuple,
                delayed=delayed_tuple,
                selected_entry_price=entry_price,
            )
            return None

        tp_price = min(entry_price * (1 + p.tp_percentage), MAX_CONTRACT_PRICE)
        if tp_price <= entry_price:
            _diag(
                "tp_invalid",
                details={"entry_price": f"{entry_price:.4f}", "tp_price": f"{tp_price:.4f}"},
                current=current_tuple,
                delayed=delayed_tuple,
                selected_entry_price=entry_price,
            )
            return None

        # Effective TP after the 0.99 clip — diverges from p.tp_percentage for
        # high entries. Logged on the trade event so the leaderboard can see
        # what the grid actually tested.
        effective_tp_pct = (tp_price - entry_price) / entry_price

        if cost_basis > self.capital + 1e-9:
            log.warning(
                "inst#%s: skipping entry because cost basis %.2f exceeds capital %.2f",
                self.id,
                cost_basis,
                self.capital,
            )
            _diag(
                "cost_exceeds_capital",
                details={"cost_basis": f"{cost_basis:.2f}", "capital": f"{self.capital:.2f}"},
                current=current_tuple,
                delayed=delayed_tuple,
                selected_entry_price=entry_price,
            )
            return None

        sl_price = 0.0
        if p.stop_loss_pct > 0:
            sl_price = entry_price * (1 - p.stop_loss_pct)

        return _EntryPlan(
            direction=direction,
            token_id=token_id,
            order_usd=order_usd,
            shares=shares,
            entry_price=entry_price,
            total_cost=total_cost,
            cost_basis=cost_basis,
            tp_price=tp_price,
            sl_price=sl_price,
            effective_tp_pct=effective_tp_pct,
            model_prob=float(model_prob),
            poly_prob=poly_prob,
            alpha=alpha,
            floor=floor,
            current_order_book=current_order_book,
            delayed_order_book=delayed_order_book,
            current_tuple=current_tuple,
            delayed_tuple=delayed_tuple,
            fee_schedule=dict(fee_schedule) if fee_schedule is not None else None,
            fee_usd=fee_usd,
        )

    def _execute_entry(
        self,
        tick: TickData,
        direction: Direction,
        poly_prob: float,
        model_prob: float,
        current_order_book: BookLike,
        delayed_order_book: BookLike,
        token_id: str,
        alpha: float = 1.0,
        floor: float = 0.5,
        signal: Optional[dict] = None,
        diagnostics: Optional[list[GridEntryDiagnostic]] = None,
        consume_liquidity: bool = False,
    ) -> Optional[TradeEvent]:
        """Compute an entry plan, then stage it (confirm hold) or commit now."""
        plan = self._compute_entry_plan(
            tick, direction, poly_prob, model_prob,
            current_order_book, delayed_order_book, token_id,
            alpha, floor, signal=signal, diagnostics=diagnostics,
        )
        if plan is None:
            return None
        if not self._final_entry_guard(tick, signal, diagnostics):
            return None
        confirm = float(getattr(self.shared_config, "grid_entry_confirm_seconds", 0.0) or 0.0)
        if confirm > 0:
            self._stage_entry(tick, plan, signal=signal, diagnostics=diagnostics)
            return None
        return self._commit_entry_plan(
            tick, plan,
            consume_current=current_order_book,
            consume_delayed=delayed_order_book,
            consume_liquidity=consume_liquidity,
            record_timestamp=tick.timestamp,
            signal=signal,
            diagnostics=diagnostics,
        )

    def _stage_entry(
        self,
        tick: TickData,
        plan: _EntryPlan,
        *,
        signal: Optional[dict] = None,
        diagnostics: Optional[list[GridEntryDiagnostic]] = None,
    ) -> None:
        """Hold a validated entry for the confirmation window before committing."""
        if not self._final_entry_guard(tick, signal, diagnostics):
            return
        self.staged_entry = StagedEntry(
            market_id=tick.market_id,
            staged_at=self._decision_time(tick),
            plan=plan,
            signal=dict(signal) if signal is not None else None,
        )
        confirm = float(getattr(self.shared_config, "grid_entry_confirm_seconds", 0.0) or 0.0)
        self._append_entry_diagnostic(
            diagnostics, tick, signal,
            entered=False,
            direction=plan.direction.value,
            rejection_reason="entry_staged",
            rejection_details={"confirm_seconds": f"{confirm:.3f}"},
            order_size_usd=plan.order_usd,
            current_shares=plan.current_tuple[0],
            current_avg_price=plan.current_tuple[1],
            current_fillable_usd=plan.current_tuple[2],
            delayed_shares=plan.delayed_tuple[0],
            delayed_avg_price=plan.delayed_tuple[1],
            delayed_fillable_usd=plan.delayed_tuple[2],
            selected_entry_price=plan.entry_price,
        )

    def _drop_staged(
        self,
        tick: TickData,
        reason: str,
        diagnostics: Optional[list[GridEntryDiagnostic]],
        **details,
    ) -> None:
        """Abandon a staged entry, recording why (no capital/position touched)."""
        staged = self.staged_entry
        self.staged_entry = None
        if staged is not None:
            self._append_entry_diagnostic(
                diagnostics, tick, staged.signal,
                entered=False,
                direction=staged.plan.direction.value,
                rejection_reason=reason,
                rejection_details=details or None,
                selected_entry_price=staged.plan.entry_price,
            )
        return None

    def _check_staged_entry(
        self,
        tick: TickData,
        up_order_book: BookLike,
        up_order_book_delayed: BookLike,
        down_order_book: BookLike,
        down_order_book_delayed: BookLike,
        *,
        consume_liquidity: bool = False,
        diagnostics: Optional[list[GridEntryDiagnostic]] = None,
    ) -> Optional[TradeEvent]:
        """Finalize a staged entry: commit after the hold, or drop it.

        Drops the entry if the market rolled over, if the CLOB taker-health flag
        went unhealthy during the hold (the live trader's 425 verdict landing),
        if the hold pushed us into the late-window cutoff, or if the entry is no
        longer viable on the finalize tick's books. Otherwise records the more
        conservative (worse-priced) of the stage-time and finalize-time fills.
        """
        staged = self.staged_entry
        if staged is None:
            return None
        cfg = self.shared_config

        # 1. Market rollover backstop (handle_market_transition clears first).
        if not tick.market_id or tick.market_id != staged.market_id:
            return self._drop_staged(
                tick, "staged_dropped_market_rollover", diagnostics,
                staged_market_id=staged.market_id, market_id=tick.market_id or "",
            )

        # 2. Confirmation hold still in progress — keep waiting.
        confirm = float(getattr(cfg, "grid_entry_confirm_seconds", 0.0) or 0.0)
        elapsed = (self._decision_time(tick) - staged.staged_at).total_seconds()
        if elapsed < confirm:
            return None

        # 3. CLOB taker outage discovered during the hold (live trader's verdict).
        if getattr(cfg, "clob_health_gate_enabled", True):
            outage, clob_reason, until = taker_outage_active(
                getattr(cfg, "clob_health_shared_path", None)
                or "results/15m_clob_health.json",
                now=tick.timestamp,
            )
            if outage:
                return self._drop_staged(
                    tick, "staged_dropped_clob_outage", diagnostics,
                    clob_reason=clob_reason or "",
                    unhealthy_until=until.isoformat() if until else "",
                )

        # 4. Late-window guard: the hold may have crossed the cutoff.
        if tick.time_remaining_seconds < cfg.no_trade_last_seconds:
            return self._drop_staged(
                tick, "staged_dropped_last_window", diagnostics,
                remaining_seconds=f"{tick.time_remaining_seconds:.3f}",
                no_trade_last_seconds=cfg.no_trade_last_seconds,
            )

        # 5. Re-price on the finalize tick's books; drop if no longer viable.
        direction = staged.plan.direction
        if direction == Direction.UP:
            current_book = up_order_book
            delayed_book = up_order_book_delayed or up_order_book
            poly_prob = tick.poly_prob_up
        else:
            current_book = down_order_book
            delayed_book = down_order_book_delayed or down_order_book
            poly_prob = tick.poly_prob_down

        # Every source must supply a valid current probability; never resurrect
        # the stage-time probability when fresh evaluation fails or equals zero.
        model_prob_for_finalize = tick.prob_above if direction == Direction.UP else tick.prob_below
        if not math.isfinite(model_prob_for_finalize) or not 0 <= model_prob_for_finalize <= 1:
            return self._drop_staged(tick, "staged_invalid_probability", diagnostics)
        if current_book is None or current_book.best_ask > cfg.max_entry_price:
            return self._drop_staged(tick, "staged_signal_price_above_max_entry", diagnostics)
        if staged.plan.fee_schedule != getattr(tick, "fee_schedule", None):
            return self._drop_staged(tick, "staged_fee_schedule_changed", diagnostics)

        finalize_plan = self._compute_entry_plan(
            tick, direction, poly_prob, model_prob_for_finalize,
            current_book, delayed_book, staged.plan.token_id,
            staged.plan.alpha, staged.plan.floor,
            signal=staged.signal, diagnostics=diagnostics,
        )
        if finalize_plan is None:
            # _compute_entry_plan already logged the specific rejection reason.
            self.staged_entry = None
            return None

        # Record the more conservative (worse / higher-priced) observed fill, but
        # consume the live finalize-tick depth for the order budget.
        chosen = max((staged.plan, finalize_plan), key=lambda plan: plan.cost_basis / plan.shares)
        chosen_unit_cost = chosen.cost_basis / chosen.shares if chosen.fee_schedule is not None else chosen.entry_price
        if not has_edge(model_prob_for_finalize, chosen_unit_cost, chosen.alpha, chosen.floor):
            return self._drop_staged(
                tick, "staged_edge_lost_at_selected_price", diagnostics,
                model_prob=model_prob_for_finalize, selected_price=chosen.entry_price,
                unit_cost_after_fees=chosen_unit_cost,
            )
        if chosen.cost_basis > self.capital + 1e-9:
            return self._drop_staged(tick, "staged_cost_exceeds_capital", diagnostics)
        chosen = replace(chosen, model_prob=model_prob_for_finalize, poly_prob=poly_prob)
        self.staged_entry = None
        return self._commit_entry_plan(
            tick, chosen,
            consume_current=finalize_plan.current_order_book,
            consume_delayed=finalize_plan.delayed_order_book,
            consume_liquidity=consume_liquidity,
            record_timestamp=tick.timestamp,
            decision_timestamp=staged.staged_at,
            signal=staged.signal,
            diagnostics=diagnostics,
        )

    def _commit_entry_plan(
        self,
        tick: TickData,
        plan: _EntryPlan,
        *,
        consume_current: BookLike,
        consume_delayed: BookLike,
        consume_liquidity: bool,
        record_timestamp: datetime,
        decision_timestamp: Optional[datetime] = None,
        signal: Optional[dict] = None,
        diagnostics: Optional[list[GridEntryDiagnostic]] = None,
    ) -> Optional[TradeEvent]:
        """Commit a validated plan: consume depth, open position, debit capital."""
        if not self._final_entry_guard(tick, signal, diagnostics):
            return None
        record_timestamp = self._decision_time(tick)
        if consume_liquidity:
            if plan.fee_schedule is None:
                _simulate_market_buy(consume_current, plan.order_usd, consume_liquidity=True)
                if consume_delayed is not consume_current:
                    _simulate_market_buy(consume_delayed, plan.order_usd, consume_liquidity=True)
            else:
                _simulate_buy_with_fees(consume_current, plan.order_usd, plan.fee_schedule, consume=True)
                if consume_delayed is not consume_current:
                    _simulate_buy_with_fees(consume_delayed, plan.order_usd, plan.fee_schedule, consume=True)

        capital_before = self.capital
        self.position = MicroPosition(
            direction=plan.direction,
            token_id=plan.token_id,
            entry_price=plan.entry_price,
            shares=plan.shares,
            cost_basis=plan.cost_basis,
            tp_price=plan.tp_price,
            stop_loss_price=plan.sl_price,
            opened_at=record_timestamp,
            market_id=tick.market_id,
            peak_price=plan.entry_price,
            reference_price=tick.barrier,
            barrier_source=tick.barrier_source,
            effective_tp_pct=plan.effective_tp_pct,
            fee_schedule=plan.fee_schedule,
            decision_at=decision_timestamp or record_timestamp,
            condition_id=str(getattr(tick, "condition_id", "") or ""),
            payoff_kind=str(getattr(tick, "payoff_kind", "legacy") or "legacy"),
            averaging_window_seconds=int(getattr(tick, "averaging_window_seconds", 0) or 0),
            contract_metadata={
                "experiment_id": getattr(self.shared_config, "experiment_id", "legacy"),
                "condition_id": str(getattr(tick, "condition_id", "") or ""),
                "payoff_kind": str(getattr(tick, "payoff_kind", "legacy") or "legacy"),
                "averaging_window_seconds": int(getattr(tick, "averaging_window_seconds", 0) or 0),
                "fee_schedule": plan.fee_schedule,
                "outcome_token_ids": {"UP": tick.up_token_id, "DOWN": tick.down_token_id},
            },
        )
        self.last_entered_market_id = tick.market_id
        self.capital -= plan.cost_basis
        if self.capital < 0:
            log.warning(
                "inst#%s: capital went negative after entry (%.2f); clamping to zero",
                self.id,
                self.capital,
            )
            self.capital = 0.0

        self._append_entry_diagnostic(
            diagnostics, tick, signal,
            entered=True,
            direction=plan.direction.value,
            capital_before=capital_before,
            order_size_usd=plan.order_usd,
            current_shares=plan.current_tuple[0],
            current_avg_price=plan.current_tuple[1],
            current_fillable_usd=plan.current_tuple[2],
            delayed_shares=plan.delayed_tuple[0],
            delayed_avg_price=plan.delayed_tuple[1],
            delayed_fillable_usd=plan.delayed_tuple[2],
            selected_entry_price=plan.entry_price,
        )

        return TradeEvent(
            instance_id=self.id,
            timestamp=record_timestamp,
            event="ENTRY",
            direction=plan.direction.value,
            market_id=tick.market_id,
            entry_price=plan.entry_price,
            shares=plan.shares,
            capital=self.capital,
            model_prob=plan.model_prob,
            poly_prob=plan.poly_prob,
            spot_price=tick.spot_price,
            barrier=tick.barrier,
            effective_tp_pct=plan.effective_tp_pct,
            decision_at=decision_timestamp or record_timestamp,
            cost_basis=plan.cost_basis,
            fee_schedule=plan.fee_schedule,
            contract_metadata=dict(self.position.contract_metadata),
            experiment_id=getattr(self.shared_config, "experiment_id", "legacy"),
        )

    # -- Exit logic --

    def try_exit(
        self,
        tick: TickData,
        up_order_book: BookLike,
        up_order_book_delayed: BookLike,
        down_order_book: BookLike,
        down_order_book_delayed: BookLike,
        consume_liquidity: bool = False,
    ) -> Optional[TradeEvent]:
        """Check TP and SL with shared current/delayed book validation."""
        cfg = self.shared_config
        pos = self.position
        if not pos or tick.settlement_pending or pos.market_id != tick.market_id:
            return None

        if pos.direction == Direction.UP:
            current_book = up_order_book
            delayed_book = up_order_book_delayed or up_order_book
        else:
            current_book = down_order_book
            delayed_book = down_order_book_delayed or down_order_book

        if not current_book or current_book.best_bid <= 0:
            return None
        if not delayed_book or delayed_book.best_bid <= 0:
            return None

        current_bid = current_book.best_bid
        if current_bid > pos.peak_price:
            pos.peak_price = current_bid

        if current_bid >= pos.tp_price:
            # Maker semantics: a resting GTC sell at tp_price fills at
            # tp_price regardless of how far the taker bid jumped past it.
            # We do NOT VWAP the bid book — that would credit the grid with
            # windfall upside a real pre-placed limit order can't earn.
            # But we DO consume bid depth under capacity_aware: in live,
            # multiple maker GTCs at tp_price compete FIFO for the same
            # taker volume; a 5-share taker bid fills at most 5 shares of
            # total maker depth. Without consumption, every grid instance
            # at the same tp_price credits a full fill against the same
            # observed bid depth.
            current_sold = _limit_sell_quantity(current_book, pos.tp_price, pos.shares)
            delayed_sold = _limit_sell_quantity(delayed_book, pos.tp_price, pos.shares)
            if min(current_sold, delayed_sold) < pos.shares - 1e-8:
                # Independent parameter experiments each need their own fully
                # executable quantity. Capacity mode additionally shares it.
                return None
            if consume_liquidity:
                _limit_sell_quantity(current_book, pos.tp_price, pos.shares, consume=True)
                if delayed_book is not current_book:
                    _limit_sell_quantity(delayed_book, pos.tp_price, pos.shares, consume=True)
            return self._close_position(tick, pos.tp_price, TradeResult.TP_FILLED)

        sl_allowed = tick.time_remaining_seconds > cfg.no_trade_last_seconds
        if sl_allowed and pos.stop_loss_price > 0 and current_bid <= pos.stop_loss_price:
            # Live SL trigger: a market sell against the bid book once the
            # mid crosses stop_loss_price (single_trader._execute_stop_loss_live
            # at 2619, close_position_stop_loss at 2601). Actual fill is the
            # VWAP of the bid book at that moment — strictly worse than
            # stop_loss_price on a gap below the stop. Booking the trigger
            # price overstates strategies with stop_loss_pct>0 on the grid
            # leaderboard, biasing live param selection toward strategies
            # whose true loss profile is hidden.
            sl_fill = self._vwap_sell_pessimistic(
                current_book, delayed_book, pos, consume_liquidity=consume_liquidity,
            )
            if sl_fill is None:
                # Bid depth insufficient on either current or delayed book
                # to fill the position. Live's FOK market sell would
                # also struggle here; defer SL to the next tick and let
                # try_exit re-evaluate on a fresher book.
                return None
            return self._close_position(tick, sl_fill[0], TradeResult.STOP_LOSS, fee_usd=sl_fill[1])

        return None

    def _vwap_sell_pessimistic(
        self,
        current_order_book: BookLike,
        delayed_order_book: BookLike,
        pos: MicroPosition,
        consume_liquidity: bool = False,
    ) -> Optional[tuple[float, float | None]]:
        """VWAP sell using the current and delayed shared books."""
        proceeds, avg_price, shares_sold = _simulate_market_sell(current_order_book, pos.shares)
        if shares_sold < pos.shares - 1e-8:
            return None

        d_proceeds, d_avg, d_sold = _simulate_market_sell(delayed_order_book, pos.shares)
        if d_sold < pos.shares - 1e-8:
            return None

        current_fee = delayed_fee = None
        if pos.fee_schedule is not None:
            try:
                current_fee = _sell_fee_by_level(current_order_book, pos.shares, pos.fee_schedule)
                delayed_fee = _sell_fee_by_level(delayed_order_book, pos.shares, pos.fee_schedule)
            except ValueError:
                return None
        if d_proceeds - (delayed_fee or 0.0) < proceeds - (current_fee or 0.0):
            avg_price = d_avg
            current_fee = delayed_fee

        if consume_liquidity:
            _simulate_market_sell(current_order_book, pos.shares, consume_liquidity=True)
            if delayed_order_book is not current_order_book:
                _simulate_market_sell(delayed_order_book, pos.shares, consume_liquidity=True)

        return avg_price, current_fee

    def _settle_expiry(self, tick: TickData) -> Optional[TradeEvent]:
        """Use the position's resolved payout; invalid/unknown outcomes stay open."""
        pos = self.position
        if tick.market_id != pos.market_id:
            return None
        payouts = getattr(tick, "settlement_payouts", None)
        if payouts is not None:
            payout = payout_for_position(payouts, pos.token_id, pos.direction)
            if payout is None:
                return None
            self._remember_settled_market_id(pos.market_id)
            result = TradeResult.WIN_EXPIRY if payout > 0 else TradeResult.LOSS_EXPIRY
            return self._close_position(tick, payout, result)
        if getattr(tick, "payoff_kind", "legacy") != "legacy" or pos.payoff_kind != "legacy":
            return None
        final_spot = tick.final_spot if tick.final_spot is not None else tick.spot_price
        barrier = pos.reference_price or tick.barrier
        if not (math.isfinite(final_spot) and final_spot > 0 and math.isfinite(barrier) and barrier > 0):
            return None

        # Fix #1: mark this market as settled before we close, so a crash
        # between CSV flush and snapshot save can't cause a duplicate event
        # on restart. Add here (not inside _close_position) because only
        # expiry closes are restart-sensitive.
        self._remember_settled_market_id(pos.market_id)

        up_won = final_spot >= barrier

        if (pos.direction == Direction.UP and up_won) or \
           (pos.direction == Direction.DOWN and not up_won):
            if self.id == 0:
                log.info(
                    f"  Expiry WIN: inst#{self.id} {pos.direction.value} "
                    f"spot=${final_spot:,.0f} barrier=${barrier:,.0f} "
                    f"[{tick.final_spot_source or 'unknown'}]"
                )
            return self._close_position(tick, 1.0, TradeResult.WIN_EXPIRY)

        if self.id == 0:
            log.info(
                f"  Expiry LOSS: inst#{self.id} {pos.direction.value} "
                f"spot=${final_spot:,.0f} barrier=${barrier:,.0f} "
                f"[{tick.final_spot_source or 'unknown'}]"
            )
        return self._close_position(tick, 0.0, TradeResult.LOSS_EXPIRY)

    def _close_position(self, tick, exit_price, result, *, fee_usd: float | None = None):
        """Close current position and record P&L."""
        pos = self.position
        cfg = self.shared_config

        proceeds = pos.shares * exit_price
        if result not in (TradeResult.WIN_EXPIRY, TradeResult.LOSS_EXPIRY):
            if pos.fee_schedule is not None:
                if fee_usd is None:
                    fee_usd = fee_for_fill(exit_price, pos.shares, pos.fee_schedule, is_maker=result == TradeResult.TP_FILLED)
                proceeds -= fee_usd
            else:
                proceeds *= (1 - cfg.friction_pct)

        pnl = proceeds - pos.cost_basis
        pnl_pct = (pnl / pos.cost_basis * 100) if pos.cost_basis > 0 else 0.0

        if result in (TradeResult.TP_FILLED, TradeResult.STOP_LOSS):
            self.last_tp_sl_time = tick.timestamp

        self.capital += proceeds
        self.total_pnl += pnl
        self.trades_count += 1
        self._record_closed_pnl(pnl)
        self.last_close_at = tick.timestamp
        self.last_close_result = result.value

        if pnl >= 0:
            self.wins += 1
        else:
            self.losses += 1

        # Settlement source is only meaningful for expiry-resolved rows.
        settlement_source = ""
        event_spot_price = tick.spot_price
        if result in (TradeResult.WIN_EXPIRY, TradeResult.LOSS_EXPIRY):
            settlement_source = getattr(tick, "settlement_source", "") or tick.final_spot_source
            event_spot_price = tick.final_spot if tick.final_spot is not None else tick.spot_price

        event = TradeEvent(
            instance_id=self.id,
            timestamp=tick.timestamp,
            event=result.value,
            direction=pos.direction.value,
            market_id=pos.market_id,
            entry_price=pos.entry_price,
            exit_price=exit_price,
            shares=pos.shares,
            pnl=pnl,
            pnl_pct=pnl_pct,
            capital=self.capital,
            spot_price=event_spot_price,
            barrier=pos.reference_price or tick.barrier,
            settlement_source=settlement_source,
            effective_tp_pct=pos.effective_tp_pct,
            decision_at=pos.decision_at,
            cost_basis=pos.cost_basis,
            fee_schedule=pos.fee_schedule,
            contract_metadata=dict(pos.contract_metadata),
            experiment_id=getattr(self.shared_config, "experiment_id", "legacy"),
        )

        self.position = None
        return event
