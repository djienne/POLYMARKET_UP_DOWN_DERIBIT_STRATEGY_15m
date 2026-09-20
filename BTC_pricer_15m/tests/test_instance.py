"""Tests for PaperInstance trading logic."""

from dataclasses import replace

from btc_pricer.edge import required_model_prob
from scripts.trader.config import DEFAULT_DRY_RUN_FRICTION_PCT, GridConfig, InstanceParams
from scripts.trader.trader import PaperInstance
from tests.conftest import make_tick

_DEFAULT_ALPHA = 1.3
_DEFAULT_FLOOR = 0.50


def _make_instance(config=None, params=None):
    config = config or GridConfig(starting_capital=1000.0)
    params = params or InstanceParams(
        alpha_up=_DEFAULT_ALPHA, alpha_down=_DEFAULT_ALPHA,
        floor_up=_DEFAULT_FLOOR, floor_down=_DEFAULT_FLOOR,
        tp_percentage=0.15, stop_loss_pct=0.0,
    )
    return PaperInstance(id=0, params=params, shared_config=config)


def _sig(direction, model_prob, market_prob, alpha=_DEFAULT_ALPHA, floor=_DEFAULT_FLOOR, instance_id=0):
    return {
        "instance_id": instance_id,
        "direction": direction,
        "edge_ratio": round(model_prob / market_prob, 4) if market_prob > 0 else 0,
        "executable_price": market_prob,
        "required_prob": round(required_model_prob(market_prob, alpha, floor), 4),
        "model_prob": round(model_prob, 4),
    }


def _tick_with_signal(direction, model_prob, market_prob, **kwargs):
    """make_tick + inject an entry signal for instance 0."""
    tick = make_tick(**kwargs)
    sig = _sig(direction, model_prob, market_prob)
    return replace(tick, entry_signals=(sig,))


class TestNoTradeZones:
    """Entry should be blocked in the first 5 min and last 3 min."""

    def test_no_entry_first_5_minutes(self):
        inst = _make_instance()
        tick = _tick_with_signal("UP", 0.80, 0.50, elapsed_seconds=200, time_remaining_seconds=700,
                                 prob_above=0.80, poly_prob_up=0.50)
        diagnostics = []
        events = inst.on_tick(tick, diagnostics=diagnostics)
        assert len(events) == 0
        assert inst.position is None
        assert diagnostics[0].rejection_reason == "blocked_first_window"

    def test_no_entry_last_3_minutes(self):
        inst = _make_instance()
        tick = _tick_with_signal("UP", 0.80, 0.50, elapsed_seconds=800, time_remaining_seconds=100,
                                 prob_above=0.80, poly_prob_up=0.50)
        diagnostics = []
        events = inst.on_tick(tick, diagnostics=diagnostics)
        assert len(events) == 0
        assert diagnostics[0].rejection_reason == "blocked_last_window"

    def test_entry_in_tradeable_window(self):
        inst = _make_instance()
        tick = _tick_with_signal("UP", 0.80, 0.50,
                                elapsed_seconds=400, time_remaining_seconds=500,
                                prob_above=0.80, poly_prob_up=0.50)
        events = inst.on_tick(tick)
        assert len(events) == 1
        assert events[0].event == "ENTRY"
        assert events[0].direction == "UP"


class TestEdgeCheck:
    """Entry requires the model to show edge over market."""

    def test_no_edge_no_entry(self):
        inst = _make_instance()
        # Model agrees with market — no edge
        tick = _tick_with_signal("UP", 0.50, 0.50, elapsed_seconds=400, time_remaining_seconds=500,
                                 prob_above=0.50, poly_prob_up=0.50)
        diagnostics = []
        events = inst.on_tick(tick, diagnostics=diagnostics)
        assert len(events) == 0
        assert diagnostics[0].rejection_reason == "edge_lost_after_vwap"

    def test_edge_triggers_entry(self):
        inst = _make_instance()
        # Model says 75% but market says 50% — clear edge
        tick = _tick_with_signal("UP", 0.75, 0.50,
                                elapsed_seconds=400, time_remaining_seconds=500,
                                prob_above=0.75, poly_prob_up=0.50,
                                prob_below=0.25, poly_prob_down=0.50)
        events = inst.on_tick(tick)
        assert len(events) == 1
        assert events[0].direction == "UP"

    def test_max_entry_price_guard(self):
        inst = _make_instance()
        # Edge exists but price too high
        tick = _tick_with_signal("UP", 0.98, 0.96, elapsed_seconds=400, time_remaining_seconds=500,
                                 prob_above=0.98, poly_prob_up=0.96)
        diagnostics = []
        events = inst.on_tick(tick, diagnostics=diagnostics)
        assert len(events) == 0
        assert diagnostics[0].rejection_reason == "signal_price_above_max_entry"


class TestTakeProfit:
    """TP should trigger when market price >= tp_price."""

    def test_tp_fills(self):
        inst = _make_instance()
        # Enter
        tick1 = _tick_with_signal("UP", 0.80, 0.50,
                                  elapsed_seconds=400, time_remaining_seconds=500,
                                  prob_above=0.80, poly_prob_up=0.50)
        inst.on_tick(tick1)
        assert inst.position is not None

        entry = inst.position.entry_price
        tp = inst.position.tp_price

        # Price reaches TP
        tick2 = make_tick(elapsed_seconds=500, time_remaining_seconds=400,
                          poly_prob_up=tp + 0.01)
        events = inst.on_tick(tick2)
        assert len(events) == 1
        assert events[0].event == "TP_FILLED"
        assert events[0].pnl > 0
        assert inst.position is None

    def test_tp_fills_in_last_3_minutes(self):
        inst = _make_instance()
        tick1 = _tick_with_signal("UP", 0.80, 0.50,
                                  elapsed_seconds=400, time_remaining_seconds=500,
                                  prob_above=0.80, poly_prob_up=0.50)
        inst.on_tick(tick1)
        assert inst.position is not None
        tp = inst.position.tp_price

        tick2 = make_tick(elapsed_seconds=800, time_remaining_seconds=100,
                          poly_prob_up=tp + 0.01)
        events = inst.on_tick(tick2)

        assert len(events) == 1
        assert events[0].event == "TP_FILLED"
        assert inst.position is None

    def test_down_entry_when_down_edge_exists(self):
        inst = _make_instance()
        tick = _tick_with_signal("DOWN", 0.80, 0.50,
            elapsed_seconds=400,
            time_remaining_seconds=500,
            prob_above=0.20,
            poly_prob_up=0.50,
            prob_below=0.80,
            poly_prob_down=0.50,
        )

        events = inst.on_tick(tick)

        assert len(events) == 1
        assert events[0].event == "ENTRY"
        assert events[0].direction == "DOWN"
        assert inst.position is not None
        assert inst.position.direction.value == "DOWN"

    def test_down_tp_fills(self):
        inst = _make_instance()
        tick1 = _tick_with_signal("DOWN", 0.80, 0.50,
            elapsed_seconds=400,
            time_remaining_seconds=500,
            prob_above=0.20,
            poly_prob_up=0.50,
            prob_below=0.80,
            poly_prob_down=0.50,
        )
        inst.on_tick(tick1)
        assert inst.position is not None
        assert inst.position.direction.value == "DOWN"
        tp = inst.position.tp_price

        tick2 = make_tick(
            elapsed_seconds=500,
            time_remaining_seconds=400,
            poly_prob_down=tp + 0.01,
        )
        events = inst.on_tick(tick2)

        assert len(events) == 1
        assert events[0].event == "TP_FILLED"
        assert events[0].direction == "DOWN"
        assert events[0].pnl > 0
        assert inst.position is None


class TestStopLoss:
    """SL should trigger when market price <= sl_price."""

    def test_stop_loss_fires(self):
        params = InstanceParams(
            alpha_up=1.3, alpha_down=1.3,
            floor_up=0.50, floor_down=0.50,
            tp_percentage=0.15, stop_loss_pct=0.20,
        )
        inst = _make_instance(params=params)

        # Enter
        tick1 = _tick_with_signal("UP", 0.80, 0.50,
                                  elapsed_seconds=400, time_remaining_seconds=500,
                                  prob_above=0.80, poly_prob_up=0.50)
        inst.on_tick(tick1)
        assert inst.position is not None
        sl = inst.position.stop_loss_price

        # Price drops to SL
        tick2 = make_tick(elapsed_seconds=500, time_remaining_seconds=400,
                          poly_prob_up=sl - 0.01)
        events = inst.on_tick(tick2)
        assert len(events) == 1
        assert events[0].event == "STOP_LOSS"
        assert events[0].pnl < 0

    def test_stop_loss_is_suppressed_in_last_3_minutes(self):
        params = InstanceParams(
            alpha_up=1.3, alpha_down=1.3,
            floor_up=0.50, floor_down=0.50,
            tp_percentage=0.15, stop_loss_pct=0.20,
        )
        inst = _make_instance(params=params)

        tick1 = _tick_with_signal("UP", 0.80, 0.50,
                                  elapsed_seconds=400, time_remaining_seconds=500,
                                  prob_above=0.80, poly_prob_up=0.50)
        inst.on_tick(tick1)
        assert inst.position is not None
        sl = inst.position.stop_loss_price

        tick2 = make_tick(elapsed_seconds=720, time_remaining_seconds=180,
                          poly_prob_up=sl - 0.01)
        events = inst.on_tick(tick2)

        assert events == []
        assert inst.position is not None


class TestExpiry:
    """Position settlement at market expiry."""

    def test_win_expiry_up(self):
        inst = _make_instance()
        # Enter UP
        tick1 = _tick_with_signal("UP", 0.80, 0.50,
                                  elapsed_seconds=400, time_remaining_seconds=500,
                                  prob_above=0.80, poly_prob_up=0.50,
                                  barrier=83000.0, spot_price=83100.0)
        inst.on_tick(tick1)
        assert inst.position is not None

        # Expiry: spot above barrier → UP wins
        tick2 = make_tick(is_expired=True, barrier=83000.0,
                          spot_price=83500.0, final_spot=83500.0)
        events = inst.on_tick(tick2)
        assert len(events) == 1
        assert events[0].event == "WIN_EXPIRY"
        assert events[0].exit_price == 1.0

    def test_loss_expiry_up(self):
        inst = _make_instance()
        # Enter UP
        tick1 = _tick_with_signal("UP", 0.80, 0.50,
                                  elapsed_seconds=400, time_remaining_seconds=500,
                                  prob_above=0.80, poly_prob_up=0.50,
                                  barrier=83000.0, spot_price=83100.0)
        inst.on_tick(tick1)

        # Expiry: spot below barrier → UP loses
        tick2 = make_tick(is_expired=True, barrier=83000.0,
                          spot_price=83500.0, final_spot=82500.0)
        events = inst.on_tick(tick2)
        assert len(events) == 1
        assert events[0].event == "LOSS_EXPIRY"
        assert events[0].exit_price == 0.0
        assert events[0].spot_price == 82500.0

    def test_win_expiry_down_reports_final_spot_not_current_spot(self):
        inst = _make_instance()
        # Enter DOWN
        tick1 = _tick_with_signal("DOWN", 0.80, 0.50,
                                  elapsed_seconds=400, time_remaining_seconds=500,
                                  prob_above=0.20, prob_below=0.80,
                                  poly_prob_up=0.50, poly_prob_down=0.50,
                                  barrier=77008.25, spot_price=76811.96)
        inst.on_tick(tick1)
        assert inst.position is not None

        # Expiry: final settlement is below barrier, even if current spot moved above it.
        tick2 = make_tick(is_expired=True, barrier=77008.25,
                          spot_price=77083.25, final_spot=76974.03,
                          final_spot_source="polymarket_settlement")
        events = inst.on_tick(tick2)
        assert len(events) == 1
        assert events[0].event == "WIN_EXPIRY"
        assert events[0].exit_price == 1.0
        assert events[0].spot_price == 76974.03
        assert events[0].barrier == 77008.25


class TestCapitalAccounting:
    """Capital should track correctly across entries and exits."""

    def test_capital_decreases_on_entry(self):
        inst = _make_instance()
        initial = inst.capital
        tick = _tick_with_signal("UP", 0.80, 0.50,
                                elapsed_seconds=400, time_remaining_seconds=500,
                                prob_above=0.80, poly_prob_up=0.50)
        inst.on_tick(tick)
        assert inst.capital < initial

    def test_win_increases_capital(self):
        inst = _make_instance()
        tick1 = _tick_with_signal("UP", 0.80, 0.50,
                                  elapsed_seconds=400, time_remaining_seconds=500,
                                  prob_above=0.80, poly_prob_up=0.50,
                                  barrier=83000.0, spot_price=83100.0)
        inst.on_tick(tick1)
        after_entry = inst.capital

        tick2 = make_tick(is_expired=True, barrier=83000.0,
                          spot_price=84000.0, final_spot=84000.0)
        inst.on_tick(tick2)
        assert inst.capital > after_entry
        assert inst.total_pnl > 0

    def test_entry_rejected_if_friction_would_overdraw_capital(self):
        config = GridConfig(
            starting_capital=1000.0,
            order_size_pct=1.0,
            friction_pct=DEFAULT_DRY_RUN_FRICTION_PCT,
            no_trade_first_seconds=0,
            no_trade_last_seconds=0,
        )
        inst = _make_instance(config=config)
        tick = _tick_with_signal("UP", 0.80, 0.50,
            elapsed_seconds=120,
            time_remaining_seconds=180,
            prob_above=0.80,
            poly_prob_up=0.50,
        )

        diagnostics = []
        events = inst.on_tick(tick, diagnostics=diagnostics)

        assert events == []
        assert inst.position is None
        assert inst.capital == 1000.0
        assert diagnostics[0].rejection_reason == "cost_exceeds_capital"


class TestExpiryEdgeCases:
    def test_zero_underlying_settlement_price_keeps_exposure_pending(self):
        inst = _make_instance()
        tick1 = _tick_with_signal("UP", 0.80, 0.50,
            elapsed_seconds=400,
            time_remaining_seconds=500,
            prob_above=0.80,
            poly_prob_up=0.50,
            barrier=83000.0,
            spot_price=83100.0,
        )
        inst.on_tick(tick1)

        tick2 = make_tick(
            is_expired=True,
            barrier=83000.0,
            spot_price=84000.0,
            final_spot=0.0,
        )
        events = inst.on_tick(tick2)

        assert events == []
        assert inst.position is not None
        assert inst.trades_count == 0
