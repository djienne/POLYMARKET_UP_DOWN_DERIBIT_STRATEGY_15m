"""Independent cash-accounting checks for the legacy paper execution contract."""

from dataclasses import replace

import pytest

from scripts.trader.config import GridConfig, InstanceParams
from scripts.trader.trader import PaperInstance
from tests.conftest import make_order_book, make_tick


@pytest.mark.parametrize("exit_kind,expected_price,expected_pnl,expected_cash", [
    ("TP_FILLED", .575, 12.85, 1012.85),
    ("STOP_LOSS", .35, -31.70, 968.30),
    ("WIN_EXPIRY", 1.0, 99.0, 1099.0),
])
def test_legacy_paper_cash_conservation_over_entry_and_close(exit_kind, expected_price, expected_pnl, expected_cash):
    config = GridConfig(starting_capital=1000, order_size_pct=.1, friction_pct=.01)
    inst = PaperInstance(0, InstanceParams(1, 1, .45, .45, .15, .2), config)
    entry_book = make_order_book(best_bid=.49, best_ask=.5, bid_size=1000, ask_size=1000, levels=1)
    entry_tick = replace(make_tick(prob_above=.9), up_order_book=entry_book, up_order_book_delayed=entry_book,
                         entry_signals=({"instance_id": 0, "direction": "UP", "model_prob": .9,
                                         "executable_price": .5, "required_prob": .5},))
    entry = inst.on_tick(entry_tick)
    assert [event.event for event in entry] == ["ENTRY"]
    assert inst.position.shares == 200
    assert inst.position.cost_basis == 101
    assert inst.capital == 899

    bid = .70 if exit_kind == "TP_FILLED" else .35
    exit_book = make_order_book(best_bid=bid, best_ask=bid + .02, bid_size=1000, ask_size=1000, levels=1)
    exit_tick = replace(make_tick(prob_above=.8), up_order_book=exit_book, up_order_book_delayed=exit_book)
    if exit_kind == "WIN_EXPIRY":
        exit_tick = replace(exit_tick, is_expired=True, final_spot=83500, barrier=83000)
    closed = inst.on_tick(exit_tick)
    assert [event.event for event in closed] == [exit_kind]
    assert closed[0].exit_price == pytest.approx(expected_price)
    assert closed[0].pnl == pytest.approx(expected_pnl)
    assert inst.capital == pytest.approx(expected_cash)
    assert inst.capital == pytest.approx(1000 + inst.total_pnl)
    assert inst.position is None
