"""Refine explicitly tentative execution cash flows when receipts arrive.

Legacy untagged entry costs are left untouched. CSV replacement preserves
the previous row in the existing reconciliation audit. A fresh wallet read
anchors current cash so accounting corrections do not charge fees twice.
"""
from __future__ import annotations

import math
from typing import TYPE_CHECKING, Optional

from .position import Trade

if TYPE_CHECKING:
    from .single_trader import SingleTrader


# Sources whose proceeds value is tentative and worth re-querying.
TENTATIVE_SOURCES = frozenset({"order_status", "planned"})

# Sources that are already authoritative — never overwrite.
FINAL_SOURCES = frozenset({"on-chain", "polymarket_history", "resolution", "v2_receipt"})


def _refresh_cash(trader: "SingleTrader", correction: float) -> None:
    balance_fn = getattr(trader.executor, "get_usdc_balance", None)
    try:
        balance = balance_fn() if callable(balance_fn) else None
        if balance is not None and math.isfinite(float(balance)) and float(balance) >= 0:
            trader.state.current_capital = float(balance)
            return
    except Exception:
        pass
    trader.state.current_capital += correction


def _rewrite_succeeded(trader: "SingleTrader", original) -> bool:
    return original is not None or (getattr(trader.state, "_sqlite_store", None) is not None
                                   and not getattr(trader.config, "export_legacy_files", True))


def reconcile_pending_entry_costs(trader: "SingleTrader", *, lookback: int = 50, max_attempts: int = 2) -> int:
    """Refine new tagged BUY costs; bounded calls and rotating closed backlog.

    Partial exits/residual positions receive their proportional share of the
    original fee-inclusive buy cost, using persisted entry_fill_shares.
    """
    lookup = getattr(trader.executor, "get_execution_cashflow", None)
    if not trader.executor.is_live or not callable(lookup):
        return 0
    open_items = [(item, False) for item in list(trader.state.open_positions)]
    closed = list(trader.state.closed_positions[-max(0, lookback):]) if lookback > 0 else []
    if closed:
        cursor = int(getattr(trader, "_entry_cost_reconcile_cursor", 0)) % len(closed)
        closed = closed[cursor:] + closed[:cursor]
        trader._entry_cost_reconcile_cursor = (cursor + 1) % len(closed)
    candidates = open_items + [(item, True) for item in closed]
    attempts = upgraded = 0
    for item, is_closed in candidates:
        context = dict(getattr(item, "entry_context", None) or {})
        if str(context.get("cost_basis_source") or "").lower() not in TENTATIVE_SOURCES:
            continue
        order_id = getattr(item, "entry_order_id", None) or getattr(item, "order_id", None)
        token_id = getattr(item, "token_id", "")
        if not order_id or not token_id:
            continue
        if attempts >= max(0, max_attempts):
            break
        attempts += 1
        try:
            entry_shares = float(context.get("entry_fill_shares", item.shares))
            if not math.isfinite(entry_shares) or entry_shares <= 0 or not 0 < item.shares <= entry_shares + 1e-6:
                continue
            receipt = lookup(order_id=order_id, token_id=token_id, side="BUY", target_shares=entry_shares,
                             since_timestamp=item.opened_at.timestamp())
            if receipt is None:
                continue
            actual = -float(receipt.net_cashflow) * item.shares / entry_shares
            if not math.isfinite(actual) or actual <= 0:
                continue
        except Exception as exc:
            print(f"[INFO] entry cost receipt unavailable for {item.id}: {exc}; retrying later")
            continue
        old_cost = float(item.cost_basis)
        new_pnl = float(item.proceeds) - actual if is_closed else None
        new_pnl_pct = new_pnl / actual * 100 if is_closed else None
        if is_closed:
            original = trader._rewrite_live_trade_row(item.id, {
                "cost_basis": f"{actual:.2f}", "pnl": f"{new_pnl:.2f}", "pnl_pct": f"{new_pnl_pct:.2f}%",
            }, audit_suffix="reconciled_audit", audit_label="ENTRY_COST")
            if not _rewrite_succeeded(trader, original):
                continue  # do not finalize a tag whose expected CSV rewrite failed
        context["cost_basis_source"] = "v2_receipt"
        context["entry_fee_collateral"] = float(receipt.fee_collateral) * item.shares / entry_shares
        context["entry_fill_shares"] = entry_shares
        previous_context = item.entry_context
        previous_cash, previous_total = trader.state.current_capital, trader.state.total_pnl
        previous_pnl = (item.pnl, item.pnl_pct) if is_closed else None
        correction = old_cost - actual
        try:
            item.entry_context, item.cost_basis = context, actual
            if is_closed:
                old_pnl = float(item.pnl)
                item.pnl, item.pnl_pct = new_pnl, new_pnl_pct
                trader.state.total_pnl += new_pnl - old_pnl
            _refresh_cash(trader, correction)
            # StateManager saves its snapshot, positions and closed-trade
            # table together in one SQLite transaction, then exports JSON.
            trader.state.save()
        except Exception:
            item.entry_context, item.cost_basis = previous_context, old_cost
            trader.state.current_capital, trader.state.total_pnl = previous_cash, previous_total
            if is_closed:
                item.pnl, item.pnl_pct = previous_pnl
            raise
        try:
            trader.state.log_risk_event(kind="entry_cost_refined", details={
                "position_id": item.id, "old_cost": old_cost, "cost_basis": actual,
                "fee_collateral": context["entry_fee_collateral"], "source": "v2_receipt",
            })
        except Exception:
            pass  # accounting is already persisted; an audit I/O failure must not repeat the adjustment
        upgraded += 1
    return upgraded


def _is_tentative(trade: Trade) -> bool:
    """Return True if the trade's proceeds value should be re-checked
    against Polymarket history. Pre-tracking rows (proceeds_source = "")
    are not re-checked here — they predate this code path entirely
    and the operator handles them with the void_phantom CLI."""
    src = (getattr(trade, "proceeds_source", "") or "").lower()
    return src in TENTATIVE_SOURCES


def reconcile_pending_proceeds(
    trader: "SingleTrader",
    *,
    epsilon: float = 0.01,
    lookback: int = 50,
) -> int:
    """Re-query /data/trades for the most recent tentative trade rows
    and upgrade them to source="polymarket_history" when the exact
    value differs by more than ``epsilon``.

    Returns the number of rows upgraded.
    """
    if not trader.executor.is_live:
        return 0

    closed = list(trader.state.closed_positions[-lookback:])
    upgraded = 0
    for trade in closed:
        if not _is_tentative(trade):
            continue
        # Skip rows without an exit_order_id — we can't disambiguate
        # which on-chain trades belong to this position. The void_phantom
        # CLI still works for these via slower fuzzy matching.
        if not trade.exit_order_id:
            continue
        try:
            actual = trader.executor.get_proceeds_from_trades(
                order_id=trade.exit_order_id,
                token_id=_token_id_for(trader, trade),
                since_timestamp=trade.opened_at.timestamp(),
                target_shares=trade.shares,
            )
        except Exception as exc:
            print(
                f"[INFO] history_reconciler: get_proceeds_from_trades raised "
                f"for {trade.id} ({exc}); will retry next pass"
            )
            continue
        if actual is None or actual <= 0:
            # /data/trades still hasn't indexed; try again next pass.
            continue
        source = str(getattr(trader.executor, "last_proceeds_source", "polymarket_history") or "").lower()
        if source not in FINAL_SOURCES:
            continue
        if math.isclose(actual, trade.proceeds, abs_tol=epsilon):
            # Already accurate — just upgrade the source tag without
            # rewriting visible columns, so a future pass doesn't keep
            # re-checking forever.
            # Preserve even sub-cent verified amounts in state; CSV remains
            # displayed to cents. An identical amount only upgrades metadata.
            if actual != trade.proceeds:
                upgraded += int(_apply_history_upgrade(trader, trade, actual, source=source))
                continue
            original = trader._rewrite_live_trade_row(trade.id, {"proceeds_source": source},
                audit_suffix="reconciled_audit", audit_label="RECONCILE")
            if not _rewrite_succeeded(trader, original):
                continue
            previous_source = trade.proceeds_source
            trade.proceeds_source = source
            try:
                trader.state.save()
            except Exception:
                trade.proceeds_source = previous_source
                raise
            continue
        upgraded += int(_apply_history_upgrade(trader, trade, actual, source=source))
    return upgraded


def _token_id_for(trader: "SingleTrader", trade: Trade) -> Optional[str]:
    """Return the persisted token id for token-scoped history matching."""
    token_id = getattr(trade, "token_id", "") or ""
    return token_id or None


def _apply_history_upgrade(
    trader: "SingleTrader", trade: Trade, actual_proceeds: float,
    *, source: str = "polymarket_history",
) -> bool:
    """Update the in-memory Trade + atomically rewrite its CSV row +
    log the delta to the reconciliation audit. Mutates Trade in place;
    the caller's state.save() (called outside) persists the new
    proceeds_source."""
    old_proceeds = float(trade.proceeds)
    old_pnl = float(trade.pnl)
    new_pnl = float(actual_proceeds) - trade.cost_basis
    new_pnl_pct = new_pnl / trade.cost_basis * 100 if trade.cost_basis > 0 else 0.0
    new_exit_price = trade.exit_price
    if trade.shares > 0 and source != "v2_receipt":
        new_exit_price = float(actual_proceeds) / trade.shares
    elif source == "v2_receipt":
        receipt = getattr(trader.executor, "last_proceeds_receipt", None)
        if receipt is not None and float(receipt.shares) > 0:
            new_exit_price = float(receipt.gross_collateral) / float(receipt.shares)
    delta = actual_proceeds - old_proceeds

    # Atomically rewrite the visible CSV columns so the dashboard
    # picks up the corrected values via its in-place modification
    # detection (no F5 needed).
    updates = {
        "exit_price": f"{new_exit_price:.4f}",
        "proceeds": f"{actual_proceeds:.2f}",
        "pnl": f"{new_pnl:.2f}",
        "pnl_pct": f"{new_pnl_pct:.2f}%",
        "proceeds_source": source,
    }
    original = trader._rewrite_live_trade_row(
        trade.id,
        updates,
        audit_suffix="reconciled_audit",
        audit_label="RECONCILE",
    )
    if not _rewrite_succeeded(trader, original):
        return False
    previous = (trade.proceeds, trade.pnl, trade.pnl_pct, trade.exit_price, trade.proceeds_source)
    previous_cash, previous_total = trader.state.current_capital, trader.state.total_pnl
    try:
        trade.proceeds, trade.pnl, trade.pnl_pct = float(actual_proceeds), new_pnl, new_pnl_pct
        trade.exit_price, trade.proceeds_source = new_exit_price, source
        _refresh_cash(trader, delta)
        trader.state.total_pnl += delta
        trader.state.save()
    except Exception:
        trade.proceeds, trade.pnl, trade.pnl_pct, trade.exit_price, trade.proceeds_source = previous
        trader.state.current_capital, trader.state.total_pnl = previous_cash, previous_total
        raise
    print(
        f"[RECONCILE] {trade.id}: proceeds {old_proceeds:.4f} -> "
        f"{trade.proceeds:.4f} (Δ={delta:+.4f}); pnl {old_pnl:+.4f} -> "
        f"{trade.pnl:+.4f}"
    )
    return True
