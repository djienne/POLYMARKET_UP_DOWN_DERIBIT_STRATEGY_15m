"""Tests for grid-search leaderboard summaries."""

from types import SimpleNamespace

from scripts.trader.summary import _last_close_per_instance, write_leaderboard_csv


def _instance():
    return SimpleNamespace(
        id=1,
        total_pnl=-1.0,
        trade_pnls=[-1.0],
        shared_config=SimpleNamespace(starting_capital=1000.0),
        wins=0,
        losses=1,
        trades_count=1,
        params=SimpleNamespace(
            alpha_up=2.5,
            alpha_down=1.8,
            floor_up=0.45,
            floor_down=0.65,
            tp_percentage=0.3,
            stop_loss_pct=0.2,
        ),
    )


def test_last_close_per_instance_labels_latest_close(tmp_path):
    trades = tmp_path / "trades.csv"
    trades.write_text(
        "instance_id,timestamp,event,pnl\n"
        "1,2026-05-02T20:00:00+00:00,ENTRY,\n"
        "1,2026-05-02T20:01:00+00:00,TP_FILLED,2.0\n"
        "1,2026-05-02T20:10:00+00:00,STOP_LOSS,-1.0\n"
        "2,2026-05-02T20:02:00+00:00,WIN_EXPIRY,3.0\n"
        "3,2026-05-02T20:03:00+00:00,LOSS_EXPIRY,-4.0\n",
        encoding="utf-8",
    )

    closes = _last_close_per_instance(tmp_path / "leaderboard.csv")

    assert closes[1][1] == "SL loss"
    assert closes[2][1] == "EXP win"
    assert closes[3][1] == "EXP loss"


def test_leaderboard_txt_includes_last_close_label(tmp_path):
    (tmp_path / "trades.csv").write_text(
        "instance_id,timestamp,event,pnl\n"
        "1,2026-05-02T20:10:00+00:00,STOP_LOSS,-1.0\n",
        encoding="utf-8",
    )

    write_leaderboard_csv([_instance()], tmp_path / "leaderboard.csv", liquidity_mode="independent")
    text = (tmp_path / "leaderboard.txt").read_text(encoding="utf-8")

    assert "Last" in text
    assert "SL loss" in text
