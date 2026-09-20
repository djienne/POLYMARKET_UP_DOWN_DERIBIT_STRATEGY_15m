"""Position and trade event models for 15-min grid search and single trader."""

import csv
import io
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Optional


class Direction(str, Enum):
    UP = "UP"
    DOWN = "DOWN"


class TradeResult(str, Enum):
    TP_FILLED = "TP_FILLED"
    TP_SIGNAL_MARKET_EXIT = "TP_SIGNAL_MARKET_EXIT"
    WIN_EXPIRY = "WIN_EXPIRY"
    LOSS_EXPIRY = "LOSS_EXPIRY"
    STOP_LOSS = "STOP_LOSS"
    TRAILING_STOP = "TRAILING_STOP"
    MANUAL_CLOSE = "MANUAL_CLOSE"
    # Emitted when a restart spans a market boundary and the old market's
    # outcome is unknown — neutral close (no PnL, no win/loss count).
    UNRESOLVED_RESTART = "UNRESOLVED_RESTART"
    # Live-only: a TP_FILLED record was retracted because the post-exit
    # on-chain balance check found shares still in the wallet (i.e. the
    # CLOB order_status was lying). The phantom row is rewritten in
    # place; the position is re-marked pending_settlement so the
    # eventual WIN/LOSS_EXPIRY produces the correct PnL.
    VOIDED_PHANTOM_TP = "VOIDED_PHANTOM_TP"
    # Live-only: position was closed on chain (balance dropped) but
    # neither /data/trades nor order_status produced a verified
    # proceeds value. Recorded with proceeds=0 as a placeholder; the
    # background polymarket_history_reconciler will upgrade this row
    # to the exact final value once /data/trades indexing catches up.
    UNRESOLVED_PROCEEDS = "UNRESOLVED_PROCEEDS"


@dataclass
class MicroPosition:
    """Open position for paper grid instance or single trader.

    Core fields (used by grid PaperInstance):
        direction, token_id, entry_price, shares, cost_basis, tp_price,
        stop_loss_price, opened_at, market_id, peak_price

    Extended fields (used by SingleTrader, all defaulted for backward compat):
        id, order_id, tp_order_id, trail_order_id, trail_order_price,
        pending_sl_order_id, market_title, reference_price, entry_context
    """
    direction: Direction
    token_id: str            # CLOB token ID for order book lookups
    entry_price: float
    shares: float
    cost_basis: float
    tp_price: float
    stop_loss_price: float  # 0 = disabled
    opened_at: datetime
    market_id: str
    peak_price: float = 0.0  # Highest bid seen (for trailing stop)
    # Source of barrier at window start — "chainlink", "binance", or "spot_fallback".
    # Carried into TradeEvent so leaderboard can filter by resolution source.
    barrier_source: str = ""
    # (tp_price - entry_price) / entry_price after the 0.99 clip. Makes the
    # tp_percentage grid axis auditable post-hoc.
    effective_tp_pct: float = 0.0

    # Extended fields for single trader (live/paper)
    id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    order_id: Optional[str] = None       # Live buy order ID
    tp_order_id: Optional[str] = None    # GTC limit sell for TP
    tp_placed_at: Optional[datetime] = None  # When TP order was confirmed on CLOB
    trail_order_id: Optional[str] = None  # Trail stop GTC sell order ID
    trail_order_price: float = 0.0       # Price of trail order on book
    pending_sl_order_id: Optional[str] = None  # Fallback SL limit order
    market_title: str = ""
    reference_price: float = 0.0         # Barrier price for this market
    entry_context: Optional[dict] = None
    pending_settlement: bool = False
    pending_settlement_market_id: str = ""
    pending_settlement_end_time: Optional[datetime] = None
    pending_settlement_reason: str = ""
    opened_from_mirror_signal_id: str = ""
    pending_dry_tp_signal_id: str = ""
    dry_tp_seen_at: Optional[datetime] = None
    dry_tp_market_exit_deadline: Optional[datetime] = None
    condition_id: str = ""
    payoff_kind: str = "legacy"
    averaging_window_seconds: int = 0
    contract_metadata: dict = field(default_factory=dict)
    fee_schedule: Optional[dict] = None
    decision_at: Optional[datetime] = None
    last_exit_order_id: Optional[str] = None

    def to_dict(self) -> dict:
        """Serialize to dictionary for JSON state persistence."""
        return {
            "id": self.id,
            "direction": self.direction.value,
            "token_id": self.token_id,
            "entry_price": self.entry_price,
            "shares": self.shares,
            "cost_basis": self.cost_basis,
            "tp_price": self.tp_price,
            "stop_loss_price": self.stop_loss_price,
            "opened_at": self.opened_at.isoformat(),
            "market_id": self.market_id,
            "peak_price": self.peak_price,
            "barrier_source": self.barrier_source,
            "effective_tp_pct": self.effective_tp_pct,
            "condition_id": self.condition_id,
            "payoff_kind": self.payoff_kind,
            "averaging_window_seconds": self.averaging_window_seconds,
            "contract_metadata": self.contract_metadata,
            "fee_schedule": self.fee_schedule,
            "decision_at": self.decision_at.isoformat() if self.decision_at else None,
            "last_exit_order_id": self.last_exit_order_id,
            "order_id": self.order_id,
            "tp_order_id": self.tp_order_id,
            "tp_placed_at": self.tp_placed_at.isoformat() if self.tp_placed_at else None,
            "trail_order_id": self.trail_order_id,
            "trail_order_price": self.trail_order_price,
            "pending_sl_order_id": self.pending_sl_order_id,
            "market_title": self.market_title,
            "reference_price": self.reference_price,
            "entry_context": self.entry_context,
            "pending_settlement": self.pending_settlement,
            "pending_settlement_market_id": self.pending_settlement_market_id,
            "pending_settlement_end_time": (
                self.pending_settlement_end_time.isoformat()
                if self.pending_settlement_end_time else None
            ),
            "pending_settlement_reason": self.pending_settlement_reason,
            "opened_from_mirror_signal_id": self.opened_from_mirror_signal_id,
            "pending_dry_tp_signal_id": self.pending_dry_tp_signal_id,
            "dry_tp_seen_at": self.dry_tp_seen_at.isoformat() if self.dry_tp_seen_at else None,
            "dry_tp_market_exit_deadline": (
                self.dry_tp_market_exit_deadline.isoformat()
                if self.dry_tp_market_exit_deadline else None
            ),
        }

    @classmethod
    def from_dict(cls, data: dict) -> "MicroPosition":
        """Deserialize from dictionary."""
        return cls(
            direction=Direction(data["direction"]),
            token_id=data.get("token_id", ""),
            entry_price=data["entry_price"],
            shares=data["shares"],
            cost_basis=data["cost_basis"],
            tp_price=data["tp_price"],
            stop_loss_price=data.get("stop_loss_price", 0.0),
            opened_at=datetime.fromisoformat(data["opened_at"]),
            market_id=data.get("market_id", ""),
            peak_price=data.get("peak_price", data.get("entry_price", 0.0)),
            barrier_source=data.get("barrier_source", ""),
            effective_tp_pct=data.get("effective_tp_pct", 0.0),
            condition_id=str(data.get("condition_id") or ""),
            payoff_kind=str(data.get("payoff_kind") or "legacy"),
            averaging_window_seconds=int(data.get("averaging_window_seconds") or 0),
            contract_metadata=dict(data.get("contract_metadata") or {}),
            fee_schedule=data.get("fee_schedule"),
            decision_at=datetime.fromisoformat(data["decision_at"]) if data.get("decision_at") else None,
            last_exit_order_id=data.get("last_exit_order_id"),
            id=data.get("id", str(uuid.uuid4())[:8]),
            order_id=data.get("order_id"),
            tp_order_id=data.get("tp_order_id"),
            tp_placed_at=(
                datetime.fromisoformat(data["tp_placed_at"])
                if data.get("tp_placed_at") else None
            ),
            trail_order_id=data.get("trail_order_id"),
            trail_order_price=data.get("trail_order_price", 0.0),
            pending_sl_order_id=data.get("pending_sl_order_id"),
            market_title=data.get("market_title", ""),
            reference_price=data.get("reference_price", 0.0),
            entry_context=data.get("entry_context"),
            pending_settlement=data.get("pending_settlement", False),
            pending_settlement_market_id=data.get("pending_settlement_market_id", ""),
            pending_settlement_end_time=(
                datetime.fromisoformat(data["pending_settlement_end_time"])
                if data.get("pending_settlement_end_time") else None
            ),
            pending_settlement_reason=data.get("pending_settlement_reason", ""),
            opened_from_mirror_signal_id=data.get("opened_from_mirror_signal_id", ""),
            pending_dry_tp_signal_id=data.get("pending_dry_tp_signal_id", ""),
            dry_tp_seen_at=(
                datetime.fromisoformat(data["dry_tp_seen_at"])
                if data.get("dry_tp_seen_at") else None
            ),
            dry_tp_market_exit_deadline=(
                datetime.fromisoformat(data["dry_tp_market_exit_deadline"])
                if data.get("dry_tp_market_exit_deadline") else None
            ),
        )


@dataclass
class Trade:
    """A completed trade record for single trader state persistence."""

    id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    direction: Direction = Direction.UP
    shares: float = 0.0
    entry_price: float = 0.0
    exit_price: float = 0.0
    cost_basis: float = 0.0
    proceeds: float = 0.0
    pnl: float = 0.0
    pnl_pct: float = 0.0
    result: TradeResult = TradeResult.TP_FILLED
    opened_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    closed_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    market_title: str = ""
    reference_price: float = 0.0
    final_btc_price: Optional[float] = None
    entry_context: Optional[dict] = None
    # Order IDs from the CLOB fill response — persisted so future
    # reconciliation against Polymarket /data/trades can join by
    # taker_order_id directly instead of fuzzy (time, size) matching.
    entry_order_id: Optional[str] = None
    exit_order_id: Optional[str] = None
    token_id: str = ""
    # Where the proceeds value came from. "polymarket_history" = exact
    # USDC delta from /data/trades (post-fee final). "on-chain" is an
    # accepted legacy alias. "order_status" = tentative
    # value computed at fill time as size_matched × matched_price; the
    # background history reconciler upgrades these to "polymarket_history"
    # once Polymarket indexes the sub-trades. "resolution" = expiry
    # payout (already exact). "" = legacy / pre-tracking. The reconciler
    # uses this to know which rows are still tentative and need refining.
    proceeds_source: str = ""

    def to_dict(self) -> dict:
        """Serialize to dictionary."""
        return {
            "id": self.id,
            "direction": self.direction.value,
            "shares": self.shares,
            "entry_price": self.entry_price,
            "exit_price": self.exit_price,
            "cost_basis": self.cost_basis,
            "proceeds": self.proceeds,
            "pnl": self.pnl,
            "pnl_pct": self.pnl_pct,
            "result": self.result.value,
            "opened_at": self.opened_at.isoformat(),
            "closed_at": self.closed_at.isoformat(),
            "market_title": self.market_title,
            "reference_price": self.reference_price,
            "final_btc_price": self.final_btc_price,
            "entry_context": self.entry_context,
            "entry_order_id": self.entry_order_id,
            "exit_order_id": self.exit_order_id,
            "token_id": self.token_id,
            "proceeds_source": self.proceeds_source,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Trade":
        """Deserialize from dictionary."""
        return cls(
            id=data["id"],
            direction=Direction(data["direction"]),
            shares=data["shares"],
            entry_price=data["entry_price"],
            exit_price=data["exit_price"],
            cost_basis=data["cost_basis"],
            proceeds=data["proceeds"],
            pnl=data["pnl"],
            pnl_pct=data["pnl_pct"],
            result=TradeResult(data["result"]),
            opened_at=datetime.fromisoformat(data["opened_at"]),
            closed_at=datetime.fromisoformat(data["closed_at"]),
            market_title=data.get("market_title", ""),
            reference_price=data.get("reference_price", 0.0),
            final_btc_price=data.get("final_btc_price"),
            entry_context=data.get("entry_context"),
            entry_order_id=data.get("entry_order_id"),
            exit_order_id=data.get("exit_order_id"),
            token_id=data.get("token_id", ""),
            proceeds_source=data.get("proceeds_source", ""),
        )

    def _ctx(self, key: str) -> str:
        """Extract entry_context value as string, or empty."""
        if not self.entry_context:
            return ""
        val = self.entry_context.get(key)
        return str(val) if val is not None else ""

    def to_csv_row(self, mode: str = "") -> str:
        """Format as CSV row."""
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow([
            self.id, self.direction.value, f"{self.shares:.6f}",
            f"{self.entry_price:.4f}", f"{self.exit_price:.4f}",
            f"{self.cost_basis:.2f}", f"{self.proceeds:.2f}",
            f"{self.pnl:.2f}", f"{self.pnl_pct:.2f}%",
            self.result.value, self.opened_at.isoformat(), self.closed_at.isoformat(),
            self.market_title,
            f"{self.reference_price:.2f}",
            self.final_btc_price if self.final_btc_price else "",
            mode,
            self._ctx("model_prob"),
            self._ctx("poly_price"),
            self._ctx("edge_ratio"),
            self._ctx("spot_price"),
            self._ctx("model_used"),
            self._ctx("ttm_hours"),
            self._ctx("required_prob"),
            self.entry_order_id or "",
            self.exit_order_id or "",
            self.token_id or "",
            self.proceeds_source,
        ])
        return output.getvalue().rstrip("\r\n")

    @staticmethod
    def csv_header() -> str:
        """Return CSV header row."""
        return (
            "id,direction,shares,entry_price,exit_price,"
            "cost_basis,proceeds,pnl,pnl_pct,result,"
            "opened_at,closed_at,market_title,reference_price,final_btc_price,mode,"
            "model_prob,poly_price,edge_ratio,spot_price,model_used,ttm_hours,required_prob,"
            "entry_order_id,exit_order_id,token_id,proceeds_source"
        )


@dataclass
class TradeEvent:
    """A trade event (entry or exit) for grid search logging."""
    instance_id: int
    timestamp: datetime
    event: str               # ENTRY, TP_FILLED, WIN_EXPIRY, LOSS_EXPIRY, STOP_LOSS
    direction: str           # UP or DOWN
    market_id: str
    entry_price: float = 0.0
    exit_price: float = 0.0
    shares: float = 0.0
    pnl: float = 0.0
    pnl_pct: float = 0.0
    capital: float = 0.0
    model_prob: float = 0.0
    poly_prob: float = 0.0
    spot_price: float = 0.0
    barrier: float = 0.0
    liquidity_mode: str = ""
    # Source that produced tick.final_spot for this expiry (populated on
    # WIN_EXPIRY / LOSS_EXPIRY / UNRESOLVED_RESTART; empty otherwise).
    settlement_source: str = ""
    # Effective TP gain after the 0.99 clip. Constant for entry + matching exit.
    effective_tp_pct: float = 0.0
    decision_at: Optional[datetime] = None
    cost_basis: Optional[float] = None
    fee_schedule: Optional[dict] = None
    contract_metadata: dict = field(default_factory=dict)
    experiment_id: str = "legacy"
