from pathlib import Path
import os

from app.collector.leaderboard_reader import LeaderboardReader
from app.collector.leaderboard_redis import _rows_from_payload


HEADER = "rank,instance_id,total_pnl,pnl_mean_to_sd,max_drawdown,max_drawdown_pct,wins,losses,win_rate,trades,alpha_up,alpha_down,floor_up,floor_down,tp_pct,sl_pct,liquidity_mode\n"
ROW = "1,773,12.5,1.2,3.4,5.6,7,1,87.5,8,2.5,1.8,0.45,0.45,0.3,0.0,independent\n"


def test_leaderboard_reader_preserves_last_good_snapshot_on_read_error(tmp_path: Path, monkeypatch):
    p = tmp_path / "leaderboard.csv"
    p.write_text(HEADER + ROW)
    reader = LeaderboardReader(path_fn=lambda: p)
    assert reader.read_if_changed() is True
    assert len(reader.rows) == 1

    from app.collector import leaderboard_reader as mod

    calls = []
    def _boom(_path: Path):
        calls.append(_path)
        raise OSError("sharing violation")

    p.write_text(HEADER + ROW)
    os.utime(p, (p.stat().st_atime, reader._last_mtime + 1))
    monkeypatch.setattr(mod, "parse_leaderboard", _boom)
    assert reader.read_if_changed() is False
    assert len(reader.rows) == 1
    assert reader.row(773) is not None
    assert calls == [p]


def test_leaderboard_reader_can_apply_redis_rows(tmp_path: Path):
    p = tmp_path / "leaderboard.csv"
    reader = LeaderboardReader(path_fn=lambda: p)
    rows = _rows_from_payload({
        "top": [
            {
                "rank": 1,
                "instance_id": 1250,
                "total_pnl": 12.5,
                "pnl_mean_to_sd": 1.2,
                "max_drawdown": 3.4,
                "max_drawdown_pct": 5.6,
                "wins": 7,
                "losses": 1,
                "win_rate": 87.5,
                "trades": 8,
                "params": {
                    "alpha_up": 2.5,
                    "alpha_down": 1.8,
                    "floor_up": 0.45,
                    "floor_down": 0.65,
                    "tp_pct": 0.3,
                    "sl_pct": 0.4,
                },
                "liquidity_mode": "independent",
            }
        ]
    })

    reader.apply_rows(rows)

    assert reader.row(1250) is not None
    assert reader.top(1)[0].params.tp_pct == 0.3
    assert reader.top(1)[0].params.sl_pct == 0.4
