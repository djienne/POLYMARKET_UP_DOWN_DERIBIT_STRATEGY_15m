"""Entry evaluation/execution facade for SingleTrader."""

from __future__ import annotations

from typing import Any, Callable, Optional

from .data_feed import TickData


class OrderExecutionService:
    """Focused facade for edge evaluation, diagnostics, and entry execution."""

    def __init__(
        self,
        *,
        evaluate_edge_fn: Callable[[TickData], Optional[dict]],
        execute_entry_fn: Callable[[dict], Any],
        log_live_tick_diagnostics_fn: Callable[..., None],
    ) -> None:
        self._evaluate_edge = evaluate_edge_fn
        self._execute_entry = execute_entry_fn
        self._log_live_tick_diagnostics = log_live_tick_diagnostics_fn

    def evaluate_edge(self, tick: TickData) -> Optional[dict]:
        return self._evaluate_edge(tick)

    def execute_entry(self, opportunity: dict) -> Any:
        return self._execute_entry(opportunity)

    def log_live_tick_diagnostics(self, tick: TickData, **kwargs) -> None:
        self._log_live_tick_diagnostics(tick, **kwargs)
