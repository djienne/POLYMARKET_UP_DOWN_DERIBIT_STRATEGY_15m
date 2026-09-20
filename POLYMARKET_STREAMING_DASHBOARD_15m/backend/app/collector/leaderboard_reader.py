from __future__ import annotations

import asyncio
import csv
import logging
from pathlib import Path
from typing import Optional

from ..config import settings
from ..events.bus import bus
from ..models import InstanceParams, LeaderboardRow

log = logging.getLogger(__name__)


def parse_leaderboard(path: Path) -> list[LeaderboardRow]:
    if not path.exists():
        return []
    rows: list[LeaderboardRow] = []
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for r in reader:
            try:
                params = InstanceParams(
                    alpha_up=float(r["alpha_up"]),
                    alpha_down=float(r["alpha_down"]),
                    floor_up=float(r["floor_up"]),
                    floor_down=float(r["floor_down"]),
                    tp_pct=float(r["tp_pct"]),
                    sl_pct=float(r.get("sl_pct", 0.0) or 0.0),
                )
                rows.append(LeaderboardRow(
                    rank=int(r["rank"]),
                    instance_id=int(r["instance_id"]),
                    total_pnl=float(r["total_pnl"]),
                    pnl_mean_to_sd=float(r["pnl_mean_to_sd"]) if r.get("pnl_mean_to_sd") else None,
                    max_drawdown=float(r["max_drawdown"]) if r.get("max_drawdown") else None,
                    max_drawdown_pct=float(r["max_drawdown_pct"]) if r.get("max_drawdown_pct") else None,
                    wins=int(r["wins"]),
                    losses=int(r["losses"]),
                    win_rate=float(r["win_rate"]),
                    trades=int(r["trades"]),
                    params=params,
                    liquidity_mode=r.get("liquidity_mode", "independent"),
                    as_of=r.get("as_of") or None,
                    written_at=r.get("written_at") or None,
                ))
            except (KeyError, ValueError) as e:
                log.warning("bad leaderboard row skipped: %s", e)
    return rows


class LeaderboardReader:
    def __init__(self, path_fn) -> None:
        self._path_fn = path_fn
        self._last_mtime: Optional[float] = None
        self._rows: list[LeaderboardRow] = []
        self._by_instance: dict[int, LeaderboardRow] = {}

    @property
    def path(self) -> Path:
        return self._path_fn()

    def read_if_changed(self) -> bool:
        try:
            st = self.path.stat()
        except FileNotFoundError:
            return False
        if self._last_mtime is not None and st.st_mtime == self._last_mtime:
            return False
        try:
            rows = parse_leaderboard(self.path)
        except OSError as e:
            log.warning("leaderboard read failed: %s", e)
            return False
        self._rows = rows
        self._by_instance = {r.instance_id: r for r in rows}
        self._last_mtime = st.st_mtime
        return True

    def apply_rows(self, rows: list[LeaderboardRow]) -> None:
        self._rows = list(rows)
        self._by_instance = {r.instance_id: r for r in self._rows}

    @property
    def rows(self) -> list[LeaderboardRow]:
        return self._rows

    def row(self, instance_id: int) -> Optional[LeaderboardRow]:
        return self._by_instance.get(instance_id)

    def top(self, n: int = 15) -> list[LeaderboardRow]:
        return self._rows[:n]


async def run_leaderboard_loop(reader: "LeaderboardReader", stop: asyncio.Event) -> None:
    while not stop.is_set():
        try:
            changed = reader.read_if_changed()
        except Exception:  # noqa: BLE001
            log.exception("leaderboard poll error")
            changed = False
        if changed:
            await bus.publish(
                "leaderboard.update",
                {"top": [r.model_dump() for r in reader.top(15)]},
            )
        try:
            await asyncio.wait_for(stop.wait(), timeout=settings.leaderboard_poll_interval_seconds)
        except asyncio.TimeoutError:
            pass
