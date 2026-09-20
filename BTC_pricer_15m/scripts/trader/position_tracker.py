"""Position lifecycle facade for SingleTrader.

This module is intentionally a compatibility adapter for the first extraction
step: the high-risk TP/trail/SL implementations still live in ``SingleTrader``,
but the tick loop now depends on this focused interface. That lets follow-up
patches move method bodies one by one without changing orchestration again.
"""

from __future__ import annotations

from typing import Callable

from .position import MicroPosition as Position


class PositionTracker:
    """Focused facade for open-position valuation and exit checks."""

    def __init__(
        self,
        *,
        check_fills_fn: Callable[[], None],
        verify_tp_orders_fn: Callable[[], None],
        get_position_value_fn: Callable[[Position], tuple[float, float]],
    ) -> None:
        self._check_fills = check_fills_fn
        self._verify_tp_orders = verify_tp_orders_fn
        self._get_position_value = get_position_value_fn

    def check_fills(self) -> None:
        self._check_fills()

    def verify_tp_orders(self) -> None:
        self._verify_tp_orders()

    def get_position_value(self, position: Position) -> tuple[float, float]:
        return self._get_position_value(position)
