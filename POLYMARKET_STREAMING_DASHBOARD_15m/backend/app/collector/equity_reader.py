"""Account equity observations exported by the live trader.

Cash plus marked position value is distinct from starting cash plus trade P&L:
fees, deposits, redemptions and reconciliation can change the former. This reader
keeps the recorded observations, without manufacturing account history from P&L.
"""
from __future__ import annotations

import asyncio
import csv
import logging
import math
from pathlib import Path

from ..config import settings
from ..events.bus import bus
from ..time_utils import iso_to_unix

log = logging.getLogger(__name__)


class LiveEquityReader:
    def __init__(self, path_fn) -> None:
        self._path_fn = path_fn
        self._key = None
        self._rows: list[dict] = []

    def read_if_changed(self) -> bool:
        path: Path = self._path_fn()
        try:
            stat = path.stat()
            key = (stat.st_ino, stat.st_size, stat.st_mtime_ns)
            if key == self._key:
                return False
            rows = []
            with path.open(encoding="utf-8", newline="") as file:
                for row in csv.DictReader(file):
                    timestamp = row["timestamp"]
                    values = {name: float(row[name]) for name in ("capital", "equity", "position_value")}
                    if iso_to_unix(timestamp) is None or not all(math.isfinite(v) for v in values.values()):
                        raise ValueError("invalid timestamp or nonfinite account value")
                    rows.append({"t": timestamp, "v": values["equity"], **values})
            after = path.stat()
            if (after.st_ino, after.st_size, after.st_mtime_ns) != key:
                return False
        except FileNotFoundError:
            return False
        except (OSError, ValueError, KeyError, TypeError) as exc:
            log.warning("live equity read failed; retaining last good history: %s", exc)
            return False
        self._rows = rows
        self._key = key
        return True

    def series(self) -> list[dict]:
        return [{"t": row["t"], "v": row["v"]} for row in self._rows]

    def before(self, timestamp: float) -> float | None:
        for row in reversed(self._rows):
            if iso_to_unix(row["t"]) < timestamp:
                return row["v"]
        return None


async def run_equity_loop(reader: LiveEquityReader, stop: asyncio.Event) -> None:
    while not stop.is_set():
        if reader.read_if_changed():
            await bus.publish("equity.update", {"source": "live"})
        try:
            await asyncio.wait_for(stop.wait(), timeout=settings.state_poll_interval_seconds)
        except asyncio.TimeoutError:
            pass
