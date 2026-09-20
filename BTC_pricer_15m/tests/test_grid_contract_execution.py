from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from scripts.trader import contracts
from scripts.trader import trader as trader_module
from scripts.trader.config import GridConfig, InstanceParams
from scripts.trader.order_book import MutableOrderBook, OrderBookSnapshot
from scripts.trader.trader import PaperInstance
from tests.conftest import make_tick


NOW = datetime(2026, 9, 5, 12, 0, tzinfo=timezone.utc)
FEES = {"enabled": True, "rate": 0.1, "exponent": 1, "takerOnly": True}


@pytest.fixture
def clock(monkeypatch):
    value = {"now": NOW}
    fake = type("Clock", (), {"now": staticmethod(lambda tz=None: value["now"])})
    monkeypatch.setattr(contracts, "datetime", fake)
    monkeypatch.setattr(trader_module, "datetime", fake)
    return value


def book(asks, bids=None):
    bids = bids or [(0.3, 1000)]
    return OrderBookSnapshot("test_up_token", bids[0][0], asks[0][0], bids, asks, NOW.isoformat())


def instance(*, confirm=0, liquidity_mode="independent"):
    cfg = GridConfig(starting_capital=1000, order_size_pct=0.1, grid_entry_confirm_seconds=confirm,
                     liquidity_mode=liquidity_mode)
    return PaperInstance(0, InstanceParams(1, 1, 0.45, 0.45, 0.15, 0.2), cfg)


def tick(up_book, *, when=NOW, probability=0.9, fees=FEES):
    signal = {"instance_id": 0, "direction": "UP", "model_prob": probability,
              "executable_price": up_book.best_ask, "required_prob": 0.45}
    return replace(make_tick(prob_above=probability, elapsed_seconds=400, time_remaining_seconds=500),
                   timestamp=when, up_order_book=up_book, up_order_book_delayed=up_book,
                   entry_signals=(signal,), payoff_kind="terminal_spot", contract_supported=True,
                   condition_id="condition-A", fee_schedule=dict(fees),
                   market_end_time=when + timedelta(seconds=500),
                   probability_evaluated_at=when, probability_spot_observed_at=when)


def test_multilevel_v2_buy_fee_stays_inside_budget_and_keeps_full_shares(clock):
    inst = instance()
    events = inst.on_tick(tick(book([(0.4, 100), (0.6, 1000)])))
    assert [event.event for event in events] == ["ENTRY"]
    # 100 shares at .40 cost40+fee2.40. Remaining57.60 buys92.30768333
    # shares at .60 after fee2.21539 (1e-5 collateral upward rounding).
    assert inst.position.shares == pytest.approx(192.30768333333334)
    assert inst.position.cost_basis == pytest.approx(100)
    assert inst.capital == pytest.approx(900)
    assert events[0].cost_basis == pytest.approx(100)
    assert events[0].fee_schedule == FEES
    assert inst.position.entry_price * inst.position.shares == pytest.approx(95.38461)


def test_stop_loss_deducts_fees_at_each_sell_level(clock):
    inst = instance()
    inst.on_tick(tick(book([(0.4, 100), (0.6, 1000)])))
    exit_book = book([(0.4, 1000)], [(0.3, 100), (0.2, 1000)])
    events = inst.on_tick(tick(exit_book))
    assert len(events) == 1 and events[0].event == "STOP_LOSS"
    # Gross30 + .20*92.30768333, less fee2.10 + fee1.47693.
    expected_proceeds = 30 + 0.2 * 92.30768333333334 - 2.1 - 1.47693
    assert events[0].pnl == pytest.approx(expected_proceeds - 100)
    assert inst.capital == pytest.approx(900 + expected_proceeds)


def test_independent_tp_requires_quantity_above_limit_and_does_not_pay_taker_fee(clock):
    inst = instance()
    inst.on_tick(tick(book([(0.4, 100), (0.6, 1000)])))
    pos = inst.position
    # There is ample total depth, but only one share above the resting limit.
    thin = book([(0.9, 1000)], [(0.8, 1), (0.3, 1000)])
    assert inst.on_tick(tick(thin)) == []
    assert inst.position is pos
    deep = book([(0.9, 1000)], [(0.8, 1000)])
    events = inst.on_tick(tick(deep))
    assert len(events) == 1 and events[0].event == "TP_FILLED"
    assert events[0].exit_price == pos.tp_price
    assert events[0].pnl == pytest.approx(pos.shares * pos.tp_price - pos.cost_basis)


def test_failed_capacity_tp_does_not_consume_other_instances_depth(clock):
    inst = instance(liquidity_mode="capacity_aware")
    inst.on_tick(tick(book([(0.4, 100), (0.6, 1000)])))
    mutable = MutableOrderBook.from_snapshot(book([(0.9, 1000)], [(0.8, 1), (0.3, 1000)]))
    before = [level.copy() for level in mutable.bids]
    event = inst.try_exit(tick(book([(0.9, 1000)], [(0.8, 1)])), mutable, mutable, mutable, mutable,
                          consume_liquidity=True)
    assert event is None
    assert mutable.bids == before


def test_staged_entry_checks_fresh_probability_against_selected_worse_cost(clock):
    inst = instance(confirm=4)
    free = {"enabled": False}
    inst.on_tick(tick(book([(0.6, 1000)]), fees=free))
    assert inst.staged_entry is not None
    clock["now"] += timedelta(seconds=5)
    diagnostics = []
    improved = tick(book([(0.5, 1000)]), when=clock["now"], probability=0.55, fees=free)
    events = inst.on_tick(improved, diagnostics=diagnostics)
    assert events == [] and inst.position is None
    assert inst.staged_entry is None
    assert diagnostics[-1].rejection_reason == "staged_edge_lost_at_selected_price"
    assert inst.capital == 1000


@pytest.mark.parametrize("condition,expected", [
    ("stale", "staged_dropped_stale_probability"),
    ("zero", "edge_lost_after_vwap"),
    ("unconfirmed", "staged_dropped_barrier_unconfirmed"),
    ("unsupported", "staged_dropped_changed_contract"),
])
def test_staged_entry_never_commits_after_a_gate_turns_bad(clock, condition, expected):
    inst = instance(confirm=4)
    inst.on_tick(tick(book([(0.5, 1000)])))
    clock["now"] += timedelta(seconds=5)
    final = tick(book([(0.5, 1000)]), when=clock["now"])
    changes = {
        "stale": {"probability_evaluated_at": NOW},
        "zero": {"prob_above": 0.0, "prob_below": 1.0},
        "unconfirmed": {"barrier_confirmed": False},
        "unsupported": {"contract_supported": False, "contract_reason": "changed_contract"},
    }
    diagnostics = []
    assert inst.on_tick(replace(final, **changes[condition]), diagnostics=diagnostics) == []
    assert inst.position is None and inst.staged_entry is None
    assert diagnostics[-1].rejection_reason == expected


def test_payout_belongs_to_position_market_and_does_not_use_underlying_price(clock):
    inst = instance()
    opening = tick(book([(0.5, 1000)]))
    inst.on_tick(opening)
    cap = inst.capital
    unrelated = replace(opening, market_id="different-market", is_new_market=True, is_expired=True,
                        final_spot=1e10, settlement_payouts={"test_up_token": 1.0})
    assert inst.on_tick(unrelated) == []
    assert inst.position is not None and inst.capital == cap
    own = replace(opening, is_expired=True, final_spot=1e10,
                  settlement_payouts={"test_up_token": 0.0}, settlement_source="polymarket_resolved_outcome")
    events = inst.on_tick(own)
    assert len(events) == 1 and events[0].event == "LOSS_EXPIRY"
    assert events[0].pnl == pytest.approx(-100)
    assert events[0].settlement_source == "polymarket_resolved_outcome"


@pytest.mark.parametrize("delay,remaining,expected", [
    (3, 500, "stale_probability"), (1, 180.5, "blocked_last_window"),
])
@pytest.mark.parametrize("confirm", [0, 4])
def test_entry_rechecks_clock_after_blocking_venue_probe(clock, monkeypatch, delay, remaining, expected, confirm):
    inst = instance(confirm=confirm)
    candidate = tick(book([(0.5, 1000)]))
    if confirm:
        inst.on_tick(candidate)
        assert inst.staged_entry is not None
        clock["now"] += timedelta(seconds=5)
        candidate = tick(book([(0.5, 1000)]), when=clock["now"])
    candidate = replace(candidate, time_remaining_seconds=remaining,
                        market_end_time=clock["now"] + timedelta(seconds=remaining))

    def slow_probe():
        clock["now"] += timedelta(seconds=delay)
        return True, None

    monkeypatch.setattr(trader_module, "clob_reads_healthy", slow_probe)
    diagnostics = []
    assert inst.on_tick(candidate, diagnostics=diagnostics) == []
    assert inst.position is None and inst.capital == 1000
    assert diagnostics[-1].rejection_reason == expected
