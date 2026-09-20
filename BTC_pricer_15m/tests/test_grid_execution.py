"""Tests for delayed fills and capacity-aware grid dispatch."""

from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timedelta, timezone

from btc_pricer.edge import has_edge, required_model_prob
from scripts.trader.config import GridConfig, InstanceParams
from scripts.trader.grid_execution import dispatch_grid_tick
from scripts.trader.order_book import clone_order_book
from scripts.trader.position import Direction, MicroPosition
from scripts.trader.trader import PaperInstance
from tests.conftest import make_order_book, make_tick


def _signal(instance_id, direction, model_prob, market_prob, alpha, floor):
    """Build an entry signal dict matching what calc_daemon produces."""
    return {
        "instance_id": instance_id,
        "direction": direction,
        "edge_ratio": round(model_prob / market_prob, 4) if market_prob > 0 else 0,
        "executable_price": market_prob,
        "required_prob": round(required_model_prob(market_prob, alpha, floor), 4),
        "model_prob": round(model_prob, 4),
    }


def _book(bid: float, ask: float, bid_size: float = 1000.0, ask_size: float = 1000.0):
    return make_order_book(
        best_bid=bid,
        best_ask=ask,
        bid_size=bid_size,
        ask_size=ask_size,
        levels=1,
    )


def _instance(config: GridConfig | None = None):
    cfg = config or GridConfig(starting_capital=1000.0, order_size_pct=0.10)
    params = InstanceParams(
        alpha_up=1.0,
        alpha_down=1.0,
        floor_up=0.45,
        floor_down=0.45,
        tp_percentage=0.15,
        stop_loss_pct=0.20,
    )
    return PaperInstance(id=0, params=params, shared_config=cfg)


def test_entry_uses_worse_delayed_snapshot():
    inst = _instance()
    sig = _signal(0, "UP", 0.85, 0.50, 1.0, 0.45)
    tick = replace(
        make_tick(prob_above=0.85, poly_prob_up=0.50),
        up_order_book=_book(0.48, 0.50, ask_size=500),
        up_order_book_delayed=_book(0.48, 0.60, ask_size=500),
        entry_signals=(sig,),
    )

    events = inst.on_tick(tick)

    assert len(events) == 1
    assert events[0].event == "ENTRY"
    assert round(events[0].entry_price, 4) == 0.60


def test_delayed_snapshot_can_reject_entry():
    inst = _instance()
    sig = _signal(0, "UP", 0.85, 0.50, 1.0, 0.45)
    diagnostics = []
    tick = replace(
        make_tick(prob_above=0.85, poly_prob_up=0.50),
        up_order_book=_book(0.48, 0.50, ask_size=500),
        up_order_book_delayed=_book(0.48, 0.60, ask_size=50),
        entry_signals=(sig,),
    )

    events = inst.on_tick(tick, diagnostics=diagnostics)

    assert events == []
    assert inst.position is None
    assert diagnostics[0].rejection_reason == "delayed_fill_insufficient"


def test_signal_cap_allows_slippage_vwap_to_execution_cap():
    config = GridConfig(
        starting_capital=1000.0,
        order_size_pct=0.10,
        max_entry_price=0.95,
        paper_delayed_book_slippage=0.02,
    )
    inst = _instance(config)
    sig = _signal(0, "UP", 0.99, 0.95, 1.0, 0.45)
    diagnostics = []
    tick = replace(
        make_tick(prob_above=0.99, poly_prob_up=0.95),
        up_order_book=_book(0.94, 0.95, ask_size=500),
        up_order_book_delayed=_book(0.96, 0.97, ask_size=500),
        entry_signals=(sig,),
    )

    events = inst.on_tick(tick, diagnostics=diagnostics)

    assert len(events) == 1
    assert events[0].event == "ENTRY"
    assert round(events[0].entry_price, 4) == 0.97
    assert diagnostics[0].entered is True
    assert round(diagnostics[0].max_entry_price, 4) == 0.95
    assert round(diagnostics[0].execution_max_entry_price, 4) == 0.97


def test_execution_vwap_above_slippage_cap_is_rejected():
    config = GridConfig(
        starting_capital=1000.0,
        order_size_pct=0.10,
        max_entry_price=0.95,
        paper_delayed_book_slippage=0.02,
    )
    inst = _instance(config)
    sig = _signal(0, "UP", 0.999, 0.95, 1.0, 0.45)
    diagnostics = []
    tick = replace(
        make_tick(prob_above=0.999, poly_prob_up=0.95),
        up_order_book=_book(0.94, 0.95, ask_size=500),
        up_order_book_delayed=_book(0.961, 0.971, ask_size=500),
        entry_signals=(sig,),
    )

    events = inst.on_tick(tick, diagnostics=diagnostics)

    assert events == []
    assert inst.position is None
    assert diagnostics[0].rejection_reason == "execution_price_above_slippage_cap"
    assert round(diagnostics[0].selected_entry_price, 4) == 0.971


def test_signal_above_max_entry_is_rejected_before_execution():
    inst = _instance()
    sig = _signal(0, "UP", 0.99, 0.96, 1.0, 0.45)
    diagnostics = []
    tick = replace(
        make_tick(prob_above=0.99, poly_prob_up=0.96),
        up_order_book=_book(0.94, 0.95, ask_size=500),
        up_order_book_delayed=_book(0.94, 0.95, ask_size=500),
        entry_signals=(sig,),
    )

    events = inst.on_tick(tick, diagnostics=diagnostics)

    assert events == []
    assert diagnostics[0].rejection_reason == "signal_price_above_max_entry"


def test_no_signal_does_not_emit_grid_diagnostics():
    config = GridConfig(starting_capital=1000.0, order_size_pct=0.10)
    inst = _instance(config)
    diagnostics = []

    events = dispatch_grid_tick([inst], make_tick(prob_above=0.90), config, diagnostics=diagnostics)

    assert events == []
    assert diagnostics == []


def test_tp_exit_fills_at_tp_price_regardless_of_book_overshoot():
    """Grid TP exit fills at ``pos.tp_price``, mirroring the live trader's
    pre-placed GTC limit. The bid book overshooting past tp_price (e.g. a
    fast rally that gaps the bid up to 0.70/0.72) does NOT credit the grid
    with the windfall — a maker limit at tp_price fills exactly there. The
    delayed snapshot is therefore informational only for TP fills.
    """
    inst = _instance()
    entry_book = _book(0.48, 0.50, ask_size=500)
    sig = _signal(0, "UP", 0.85, 0.50, 1.0, 0.45)
    tick = replace(
        make_tick(prob_above=0.85, poly_prob_up=0.50),
        up_order_book=entry_book,
        up_order_book_delayed=clone_order_book(entry_book),
        entry_signals=(sig,),
    )
    inst.on_tick(tick)
    assert inst.position is not None
    assert inst.position.entry_price == 0.50
    # tp_pct default = 0.15 → tp_price = 0.50 * 1.15 = 0.575
    expected_tp = inst.position.tp_price
    assert round(expected_tp, 4) == 0.575

    tp_tick = replace(
        make_tick(poly_prob_up=0.80),
        up_order_book=_book(0.70, 0.72, bid_size=500),
        up_order_book_delayed=_book(0.62, 0.64, bid_size=500),
    )

    events = inst.on_tick(tp_tick)

    assert len(events) == 1
    assert events[0].event == "TP_FILLED"
    # Exit at tp_price (0.575), NOT at the post-overshoot bid VWAP (0.62 or 0.70).
    assert round(events[0].exit_price, 4) == round(expected_tp, 4)


def test_independent_mode_preserves_parallel_fills():
    config = GridConfig(
        starting_capital=1000.0,
        order_size_pct=0.10,
        liquidity_mode="independent",
    )
    instances = [PaperInstance(id=i, params=_instance(config).params, shared_config=config) for i in range(2)]
    shared_book = _book(0.48, 0.50, ask_size=200)
    sigs = tuple(_signal(i, "UP", 0.90, 0.50, 1.0, 0.45) for i in range(2))
    tick = replace(
        make_tick(prob_above=0.90, poly_prob_up=0.50),
        up_order_book=shared_book,
        up_order_book_delayed=clone_order_book(shared_book),
        entry_signals=sigs,
    )

    events = dispatch_grid_tick(instances, tick, config)

    assert len([event for event in events if event.event == "ENTRY"]) == 2


def test_capacity_aware_mode_depletes_shared_depth():
    config = GridConfig(
        starting_capital=1000.0,
        order_size_pct=0.10,
        liquidity_mode="capacity_aware",
        capacity_seed=7,
    )
    params = _instance(config).params
    instances = [PaperInstance(id=i, params=params, shared_config=config) for i in range(2)]
    shared_book = _book(0.48, 0.50, ask_size=200)
    sigs = tuple(_signal(i, "UP", 0.90, 0.50, 1.0, 0.45) for i in range(2))
    tick = replace(
        make_tick(prob_above=0.90, poly_prob_up=0.50),
        up_order_book=shared_book,
        up_order_book_delayed=clone_order_book(shared_book),
        entry_signals=sigs,
    )

    events = dispatch_grid_tick(instances, tick, config)

    assert len([event for event in events if event.event == "ENTRY"]) == 1


def test_capacity_aware_order_is_seeded_and_reproducible():
    config = GridConfig(
        starting_capital=1000.0,
        order_size_pct=0.10,
        liquidity_mode="capacity_aware",
        capacity_seed=11,
    )
    params = _instance(config).params
    shared_book = _book(0.48, 0.50, ask_size=200)
    sigs = tuple(_signal(i, "UP", 0.90, 0.50, 1.0, 0.45) for i in range(2))
    tick = replace(
        make_tick(prob_above=0.90, poly_prob_up=0.50),
        timestamp=datetime(2026, 4, 15, 12, 0, tzinfo=timezone.utc),
        up_order_book=shared_book,
        up_order_book_delayed=clone_order_book(shared_book),
        entry_signals=sigs,
    )

    run_a = [PaperInstance(id=i, params=params, shared_config=config) for i in range(2)]
    run_b = [PaperInstance(id=i, params=params, shared_config=config) for i in range(2)]

    events_a = dispatch_grid_tick(run_a, deepcopy(tick), config)
    events_b = dispatch_grid_tick(run_b, deepcopy(tick), config)

    assert [event.instance_id for event in events_a] == [event.instance_id for event in events_b]


def test_capacity_aware_exits_happen_before_entries():
    config = GridConfig(
        starting_capital=1000.0,
        order_size_pct=0.10,
        liquidity_mode="capacity_aware",
        capacity_seed=5,
    )
    params = _instance(config).params
    exiting = PaperInstance(id=0, params=params, shared_config=config)
    entering = PaperInstance(id=1, params=params, shared_config=config)
    exiting.position = MicroPosition(
        direction=Direction.UP,
        token_id="test_up_token",
        entry_price=0.50,
        shares=100.0,
        cost_basis=50.0,
        tp_price=0.60,
        stop_loss_price=0.40,
        opened_at=datetime.now(timezone.utc) - timedelta(minutes=1),
        market_id="btc-updown-15m-1776004200",
        peak_price=0.50,
    )

    shared_book = _book(0.65, 0.67, bid_size=1000, ask_size=1000)
    sig = _signal(1, "UP", 0.90, 0.67, 1.0, 0.45)
    tick = replace(
        make_tick(prob_above=0.90, poly_prob_up=0.50),
        up_order_book=shared_book,
        up_order_book_delayed=clone_order_book(shared_book),
        entry_signals=(sig,),
    )

    events = dispatch_grid_tick([exiting, entering], tick, config)

    assert events[0].event == "TP_FILLED"
    assert any(event.event == "ENTRY" for event in events[1:])


# ----------------------------------------------------------------------------
# P2a: stop-loss fills at VWAP of the bid book, matching live's
# market-sell-on-trigger semantics. Booking the trigger price overstates
# strategies with stop_loss_pct>0 on the leaderboard, biasing live param
# selection toward strategies whose true loss profile is hidden.
# ----------------------------------------------------------------------------


def test_stop_loss_fills_at_vwap_not_trigger_price_on_gap():
    """SL trigger at 0.40, but the bid book has gapped to 0.30/0.32.
    Live's market sell would fill at the bid VWAP (~0.30), not at 0.40.
    The grid must credit the actual VWAP fill, not the trigger price."""
    inst = _instance()
    # Open at 0.50 (entry), so SL = 0.50 * (1 - 0.20) = 0.40.
    entry_book = _book(0.48, 0.50, bid_size=500, ask_size=500)
    sig = _signal(0, "UP", 0.85, 0.50, 1.0, 0.45)
    entry_tick = replace(
        make_tick(prob_above=0.85, poly_prob_up=0.50),
        up_order_book=entry_book,
        up_order_book_delayed=clone_order_book(entry_book),
        entry_signals=(sig,),
    )
    inst.on_tick(entry_tick)
    assert inst.position is not None
    assert round(inst.position.stop_loss_price, 4) == 0.40

    # Bid gaps below SL: best bid is 0.30 (well below the 0.40 stop).
    sl_book = _book(0.30, 0.32, bid_size=500, ask_size=500)
    sl_tick = replace(
        make_tick(poly_prob_up=0.30),
        up_order_book=sl_book,
        up_order_book_delayed=clone_order_book(sl_book),
    )
    events = inst.on_tick(sl_tick)

    assert len(events) == 1
    assert events[0].event == "STOP_LOSS"
    # Exit at the bid VWAP (0.30 with deep liquidity), NOT at the trigger
    # price (0.40). On a gap, this is strictly worse than the old behavior.
    assert round(events[0].exit_price, 4) == 0.30, (
        f"SL exit must be VWAP of bid book ({0.30}), not trigger price "
        f"({0.40}); got {events[0].exit_price}"
    )


def test_stop_loss_suppressed_in_closing_window():
    """Inside the final no-trade window, TP/expiry remain but SL is disabled."""
    inst = _instance()
    entry_book = _book(0.48, 0.50, bid_size=500, ask_size=500)
    sig = _signal(0, "UP", 0.85, 0.50, 1.0, 0.45)
    entry_tick = replace(
        make_tick(prob_above=0.85, poly_prob_up=0.50),
        up_order_book=entry_book,
        up_order_book_delayed=clone_order_book(entry_book),
        entry_signals=(sig,),
    )
    inst.on_tick(entry_tick)
    assert inst.position is not None

    sl_book = _book(0.30, 0.32, bid_size=500, ask_size=500)
    sl_tick = replace(
        make_tick(elapsed_seconds=720, time_remaining_seconds=180, poly_prob_up=0.30),
        up_order_book=sl_book,
        up_order_book_delayed=clone_order_book(sl_book),
    )
    events = inst.on_tick(sl_tick)

    assert events == []
    assert inst.position is not None


def test_stop_loss_defers_when_bid_depth_insufficient():
    """If the bid book can't fill ≥99% of position size on either current or
    delayed book, _vwap_sell_pessimistic returns None — the SL doesn't fill
    this tick. Mirrors live's market-sell behavior on a thin book."""
    inst = _instance()
    # Enter with deep book.
    entry_book = _book(0.48, 0.50, bid_size=500, ask_size=500)
    sig = _signal(0, "UP", 0.85, 0.50, 1.0, 0.45)
    inst.on_tick(replace(
        make_tick(prob_above=0.85, poly_prob_up=0.50),
        up_order_book=entry_book,
        up_order_book_delayed=clone_order_book(entry_book),
        entry_signals=(sig,),
    ))
    assert inst.position is not None
    pos_shares = inst.position.shares  # ~$100 / 0.50 = ~200 shares

    # SL trigger but bid depth at 0.30 is only 5 shares — way under 99%.
    thin_book = _book(0.30, 0.32, bid_size=5, ask_size=500)
    sl_tick = replace(
        make_tick(poly_prob_up=0.30),
        up_order_book=thin_book,
        up_order_book_delayed=clone_order_book(thin_book),
    )
    events = inst.on_tick(sl_tick)

    # Insufficient depth → SL deferred, no event this tick.
    assert events == []
    # Position is still open.
    assert inst.position is not None
    assert inst.position.shares == pos_shares


# ----------------------------------------------------------------------------
# P2b: TP exit consumes shared bid depth under capacity_aware mode.
# Maker fill price (tp_price) is preserved.
# ----------------------------------------------------------------------------


def test_tp_consumes_shared_bid_depth_in_capacity_mode():
    """Two instances at the same tp_price share one bid level. The first
    instance fills at tp_price and consumes the bid depth; the second
    instance defers because the consumed book no longer has enough
    bid depth to cover its position."""
    config = GridConfig(
        starting_capital=1000.0,
        order_size_pct=0.10,
        liquidity_mode="capacity_aware",
        capacity_seed=7,
    )
    params = _instance(config).params
    instances = [PaperInstance(id=i, params=params, shared_config=config) for i in range(2)]

    # Pre-load both with positions at entry=0.50 → tp_price=0.575.
    for inst in instances:
        inst.position = MicroPosition(
            direction=Direction.UP,
            token_id="test_up_token",
            entry_price=0.50,
            shares=100.0,
            cost_basis=50.0,
            tp_price=0.575,
            stop_loss_price=0.40,
            opened_at=datetime.now(timezone.utc) - timedelta(minutes=1),
            market_id="btc-updown-15m-1776004200",
            peak_price=0.50,
        )

    # Bid depth: 120 shares @ 0.60 — fits ONE instance's 100-share fill,
    # but not two.
    shared_book = _book(0.60, 0.62, bid_size=120, ask_size=1000)
    tick = replace(
        make_tick(poly_prob_up=0.60),
        up_order_book=shared_book,
        up_order_book_delayed=clone_order_book(shared_book),
    )

    events = dispatch_grid_tick(instances, tick, config)

    tp_events = [e for e in events if e.event == "TP_FILLED"]
    assert len(tp_events) == 1, (
        f"Only one instance should fill at TP when bid depth fits one; "
        f"got {len(tp_events)} TP events"
    )
    # Maker price preserved.
    assert round(tp_events[0].exit_price, 4) == 0.575


def test_tp_in_independent_mode_unaffected_by_consumption():
    """In independent mode (no consume_liquidity), the bid-depth gating
    does not apply. Both instances credit a TP fill simultaneously."""
    config = GridConfig(
        starting_capital=1000.0,
        order_size_pct=0.10,
        liquidity_mode="independent",
    )
    params = _instance(config).params
    instances = [PaperInstance(id=i, params=params, shared_config=config) for i in range(2)]
    for inst in instances:
        inst.position = MicroPosition(
            direction=Direction.UP,
            token_id="test_up_token",
            entry_price=0.50,
            shares=100.0,
            cost_basis=50.0,
            tp_price=0.575,
            stop_loss_price=0.40,
            opened_at=datetime.now(timezone.utc) - timedelta(minutes=1),
            market_id="btc-updown-15m-1776004200",
            peak_price=0.50,
        )

    shared_book = _book(0.60, 0.62, bid_size=120, ask_size=1000)
    tick = replace(
        make_tick(poly_prob_up=0.60),
        up_order_book=shared_book,
        up_order_book_delayed=clone_order_book(shared_book),
    )

    events = dispatch_grid_tick(instances, tick, config)

    tp_events = [e for e in events if e.event == "TP_FILLED"]
    assert len(tp_events) == 2, (
        f"Independent mode skips depth gating — both instances should TP; "
        f"got {len(tp_events)} TP events"
    )
    for e in tp_events:
        assert round(e.exit_price, 4) == 0.575
