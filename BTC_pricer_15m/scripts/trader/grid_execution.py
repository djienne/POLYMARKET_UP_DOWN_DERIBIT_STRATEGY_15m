"""Dispatch helpers for the grid search execution modes."""

from __future__ import annotations

import hashlib
import random

from scripts.trader.config import GridConfig
from scripts.trader.data_feed import TickData
from scripts.trader.grid_diagnostics import GridEntryDiagnostic
from scripts.trader.order_book import MutableOrderBook
from scripts.trader.position import TradeEvent
from scripts.trader.trader import PaperInstance


def _phase_instances(
    instances: list[PaperInstance],
    tick: TickData,
    phase: str,
    capacity_seed: int,
    predicate=None,
) -> list[PaperInstance]:
    pool = [inst for inst in instances if predicate(inst)] if predicate else list(instances)
    key = f"{capacity_seed}:{tick.market_id}:{tick.timestamp.isoformat()}:{phase}"
    seed = int(hashlib.sha256(key.encode("utf-8")).hexdigest()[:16], 16)
    rng = random.Random(seed)
    rng.shuffle(pool)
    return pool


def _tag(events: list[TradeEvent], liquidity_mode: str) -> list[TradeEvent]:
    for event in events:
        event.liquidity_mode = liquidity_mode
    return events


def dispatch_grid_tick(
    instances: list[PaperInstance],
    tick: TickData,
    config: GridConfig,
    diagnostics: list[GridEntryDiagnostic] | None = None,
) -> list[TradeEvent]:
    """Dispatch one shared tick for independent or capacity-aware execution."""
    if config.liquidity_mode != "capacity_aware":
        events: list[TradeEvent] = []
        for inst in instances:
            events.extend(inst.on_tick(tick, diagnostics=diagnostics))
        return _tag(events, config.liquidity_mode)

    events: list[TradeEvent] = []

    for inst in _phase_instances(instances, tick, "transition", config.capacity_seed):
        event = inst.handle_market_transition(tick)
        if event:
            events.append(event)

    if tick.settlement_pending or tick.is_expired or tick.spot_price <= 0:
        return _tag(events, config.liquidity_mode)

    up_current = MutableOrderBook.from_snapshot(tick.up_order_book)
    up_delayed = MutableOrderBook.from_snapshot(tick.up_order_book_delayed or tick.up_order_book)
    down_current = MutableOrderBook.from_snapshot(tick.down_order_book)
    down_delayed = MutableOrderBook.from_snapshot(tick.down_order_book_delayed or tick.down_order_book)

    for inst in _phase_instances(
        instances,
        tick,
        "exit",
        config.capacity_seed,
        predicate=lambda item: item.position is not None,
    ):
        event = inst.try_exit(
            tick,
            up_current,
            up_delayed,
            down_current,
            down_delayed,
            consume_liquidity=True,
        )
        if event:
            events.append(event)

    for inst in _phase_instances(
        instances,
        tick,
        "entry",
        config.capacity_seed,
        predicate=lambda item: item.position is None,
    ):
        event = inst.try_entry(
            tick,
            up_current,
            up_delayed,
            down_current,
            down_delayed,
            consume_liquidity=True,
            diagnostics=diagnostics,
        )
        if event:
            events.append(event)

    return _tag(events, config.liquidity_mode)
