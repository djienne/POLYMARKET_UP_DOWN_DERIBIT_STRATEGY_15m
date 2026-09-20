import json
from pathlib import Path

import pytest

from app.collector.state_reader import (
    StateReader,
    instance_from_live_raw,
    instance_from_raw,
    position_from_live_raw,
    position_from_raw,
)


def test_instance_stats_derivation():
    raw = {
        "capital": 1200.0,
        "total_pnl": 200.0,
        "wins": 8,
        "losses": 2,
        "trades_count": 10,
        "trade_pnls": [10, -5, 20, -10, 30, -5, 50, -10, 60, 60],
    }
    s = instance_from_raw(773, raw, starting_capital=1000.0)
    assert s.instance_id == 773
    assert s.capital == 1200.0
    assert s.total_pnl == 200.0
    assert s.wins == 8 and s.losses == 2 and s.trades_count == 10
    assert s.win_rate == 80.0
    assert s.pnl_mean_to_sd > 0
    assert s.max_drawdown >= 0


def test_max_drawdown_pct_uses_running_peak():
    raw = {
        "capital": 1100.0,
        "trade_pnls": [1500.0, -1400.0],
    }
    s = instance_from_raw(773, raw, starting_capital=1000.0)
    assert s.max_drawdown == pytest.approx(1400.0)
    assert s.max_drawdown_pct == pytest.approx(56.0, abs=0.01)
    assert s.max_drawdown_pct <= 100.0


def test_dollar_and_percent_drawdown_maxima_can_occur_at_different_peaks():
    # Equity: 1000 -> 2000 -> 1500 -> 4500 -> 3600.
    # The largest dollar loss is 900; the largest fractional loss is 25%.
    s = instance_from_raw(1, {"trade_pnls": [1000, -500, 3000, -900]}, 1000)
    assert s.max_drawdown == 900
    assert s.max_drawdown_pct == 25


def test_pnl_mean_to_sd_is_unscaled_and_uses_complete_moments():
    # P&Ls 4,-2,4: mean 2, sample variance 12. The retained tail is only 4.
    raw = {"trade_pnls": [4], "pnl_sum": 6, "pnl_sum_sq": 36, "pnl_count": 3,
           "pnl_moments_complete": True, "drawdown_history_complete": False}
    s = instance_from_raw(1, raw, 100)
    assert s.pnl_mean_to_sd == pytest.approx(0.5773502691896258)
    assert s.max_drawdown is None
    assert s.max_drawdown_pct is None
    raw["pnl_moments_complete"] = False
    assert instance_from_raw(1, raw, 100).pnl_mean_to_sd is None


def test_instance_stats_respect_explicit_starting_capital():
    raw = {
        "capital": 120.0,
        "trade_pnls": [20.0],
    }
    s = instance_from_raw(773, raw, starting_capital=100.0)
    assert s.starting_capital == 100.0
    assert s.total_pnl == 20.0
    assert s.total_pnl_pct == 20.0


def test_position_from_raw_flat():
    assert position_from_raw({"position": None, "last_tp_sl_time": "2026-04-20T10:00:00+00:00"}).open is None


def test_position_from_raw_open():
    raw = {
        "position": {
            "direction": "UP",
            "entry_price": 0.64,
            "shares": 4.6,
            "tp_price": 0.736,
            "stop_loss_price": 0.0,
            "opened_at": "2026-04-20T14:50:17+00:00",
            "market_id": "btc-updown-15m-1776696300",
            "cost_basis": 3.0,
        },
    }
    ps = position_from_raw(raw)
    assert ps.open is not None
    assert ps.open.direction == "UP"
    assert ps.open.shares == 4.6
    assert ps.open.market_id.startswith("btc-updown-15m")


def test_state_reader_diff_by_mtime(tmp_path: Path):
    p = tmp_path / "state.json"
    p.write_text(json.dumps({"instances": {"773": {"capital": 1100.0, "total_pnl": 100.0, "trade_pnls": []}}}))
    r = StateReader(path_fn=lambda: p)
    assert r.read_if_changed() is True
    assert r.read_if_changed() is False
    stats = r.instance(773)
    assert stats is not None
    assert stats.capital == 1100.0


def test_state_reader_instance_uses_supplied_starting_capital(tmp_path: Path):
    p = tmp_path / "state.json"
    p.write_text(json.dumps({"instances": {"773": {"capital": 110.0, "trade_pnls": [10.0]}}}))
    r = StateReader(path_fn=lambda: p)
    assert r.read_if_changed() is True
    stats = r.instance(773, starting_capital=100.0)
    assert stats is not None
    assert stats.starting_capital == 100.0
    assert stats.total_pnl == 10.0
    assert stats.total_pnl_pct == 10.0


def test_instance_from_live_raw_uses_live_account_starting_capital():
    raw = {
        "capital": {
            "starting": 101.006996,
            "current": 100.99571,
            "total_pnl": 0.0,
        },
        "closed_positions": [
            {"result": "TP_FILLED", "pnl": 0.0},
        ],
        "open_positions": [],
        "last_tp_fill_time": "2026-04-22T07:25:56.446441+00:00",
    }
    s = instance_from_live_raw(100, raw, starting_capital=100.0)
    assert s is not None
    assert s.starting_capital == pytest.approx(101.006996)
    assert s.capital == pytest.approx(100.99571)
    assert s.total_pnl == pytest.approx(0.0)
    assert s.trades_count == 0
    assert s.wins == 0
    assert s.losses == 0


def test_position_from_live_raw_uses_flat_live_schema():
    raw = {
        "open_positions": [
            {
                "direction": "DOWN",
                "entry_price": 0.71,
                "shares": 7.2,
                "cost_basis": 5.112,
                "opened_at": "2026-04-22T09:10:00+00:00",
                "market_id": "btc-updown-15m-1776849300",
            }
        ],
        "last_tp_fill_time": "2026-04-22T07:25:56.446441+00:00",
    }
    ps = position_from_live_raw(raw)
    assert ps.open is not None
    assert ps.open.direction == "DOWN"
    assert ps.open.shares == pytest.approx(7.2)
    assert ps.open.market_id == "btc-updown-15m-1776849300"


def test_state_reader_supports_flat_live_schema(tmp_path: Path):
    p = tmp_path / "15m_live_state.json"
    p.write_text(json.dumps({
        "capital": {
            "starting": 101.006996,
            "current": 100.99571,
            "total_pnl": 0.0,
        },
        "closed_positions": [
            {"result": "TP_FILLED", "pnl": 0.0},
        ],
        "open_positions": [],
        "last_tp_fill_time": "2026-04-22T07:25:56.446441+00:00",
    }))
    r = StateReader(path_fn=lambda: p)
    assert r.read_if_changed() is True
    stats = r.instance(100, starting_capital=100.0)
    assert stats is not None
    assert stats.starting_capital == pytest.approx(101.006996)
    assert r.trade_pnls(100) == []


def test_live_state_counts_non_flat_close_as_trade():
    raw = {
        "capital": {
            "starting": 101.006996,
            "current": 102.006996,
            "total_pnl": 1.0,
        },
        "closed_positions": [
            {
                "result": "TP_FILLED",
                "pnl": 1.0,
                "cost_basis": 5.0,
                "proceeds": 6.0,
            },
        ],
        "open_positions": [],
        "last_tp_fill_time": "2026-04-22T07:25:56.446441+00:00",
    }
    s = instance_from_live_raw(100, raw, starting_capital=100.0)
    assert s is not None
    assert s.trades_count == 1
    assert s.wins == 1
    assert s.losses == 0


def test_live_state_ignores_float_noise_flat_close():
    raw = {
        "capital": {
            "starting": 101.006996,
            "current": 97.643918,
            "total_pnl": -3.0164208210227654,
        },
        "closed_positions": [
            {
                "result": "LOSS_EXPIRY",
                "pnl": -4.965397604895929,
                "cost_basis": 4.965397604895929,
                "proceeds": 0.0,
            },
            {
                "result": "WIN_EXPIRY",
                "pnl": 1.16329669424755,
                "cost_basis": 5.67962230575245,
                "proceeds": 6.842919,
            },
            {
                "result": "TP_FILLED",
                "pnl": 0.7856798796256133,
                "cost_basis": 5.696178800374387,
                "proceeds": 6.48185868,
            },
            {
                "result": "TP_FILLED",
                "pnl": 2.1000000050008794e-07,
                "cost_basis": 5.749998,
                "proceeds": 5.74999821,
            },
        ],
        "open_positions": [],
        "last_tp_fill_time": "2026-04-22T15:43:19.076990+00:00",
    }
    s = instance_from_live_raw(100, raw, starting_capital=100.0)
    assert s is not None
    assert s.trades_count == 3
    assert s.wins == 2
    assert s.losses == 1
