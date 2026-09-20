"""Live wallet/local-state reconciliation facade for SingleTrader."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Callable, Optional


class ReconciliationService:
    """Focused facade for live reconciliation and orphan detection."""

    def __init__(
        self,
        *,
        reconcile_positions_fn: Callable[[], None],
        detect_orphans_fn: Callable[[], None],
        reconcile_pending_entry_fn: Callable[[], None],
        maybe_run_history_reconciler_fn: Callable[[], None],
        interval_seconds_fn: Callable[[], float],
        get_last_reconciliation_fn: Callable[[], Optional[datetime]],
        set_last_reconciliation_fn: Callable[[datetime], None],
    ) -> None:
        self._reconcile_positions = reconcile_positions_fn
        self._detect_orphans = detect_orphans_fn
        self._reconcile_pending_entry = reconcile_pending_entry_fn
        self._maybe_run_history_reconciler = maybe_run_history_reconciler_fn
        self._interval_seconds = interval_seconds_fn
        self._get_last_reconciliation = get_last_reconciliation_fn
        self._set_last_reconciliation = set_last_reconciliation_fn

    def reconcile_positions(self, *, mark_now: bool = False) -> None:
        self._reconcile_positions()
        if mark_now:
            self.mark_reconciled_now()

    def detect_orphans(self) -> None:
        self._detect_orphans()

    def reconcile_pending_entry(self) -> None:
        self._reconcile_pending_entry()

    def maybe_run_history_reconciler(self) -> None:
        self._maybe_run_history_reconciler()

    def mark_reconciled_now(self) -> None:
        self._set_last_reconciliation(datetime.now(timezone.utc))

    def should_run_periodic(self) -> bool:
        last = self._get_last_reconciliation()
        if last is None:
            return True
        elapsed = (datetime.now(timezone.utc) - last).total_seconds()
        return elapsed >= self._interval_seconds()
