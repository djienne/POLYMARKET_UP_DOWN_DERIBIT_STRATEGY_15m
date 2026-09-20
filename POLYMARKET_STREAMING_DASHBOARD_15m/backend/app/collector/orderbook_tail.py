from __future__ import annotations

import asyncio
import csv
import io
import logging
from collections import deque
from pathlib import Path
from typing import Deque, Optional

from ..config import settings
from ..events.bus import bus
from ..models import PolymarketPrices

log = logging.getLogger(__name__)

MAX_POINTS = 1000  # enough for a full 15-min window at 1s cadence with headroom


def _f(v) -> Optional[float]:
    try:
        return float(v) if v not in (None, "") else None
    except (TypeError, ValueError):
        return None


def _parse_side(token_id: str) -> Optional[str]:
    if token_id.startswith("UP"):
        return "UP"
    if token_id.startswith("DOWN"):
        return "DOWN"
    return None


class OrderbookTail:
    """Tails the 15m_orderbook.csv — rows alternate UP and DOWN per tick.

    Exposes:
      - .latest  ? merged PolymarketPrices (best_bid/ask from UP side, probs from both)
      - .series(side) ? list of (timestamp_iso, mid_price) for current 15-min market
    """

    def __init__(self, path_fn) -> None:
        self._path_fn = path_fn
        self._header: Optional[list[str]] = None
        self._offset: int = 0
        self._last_size: int = 0
        # Latest UP/DOWN prices
        self._up_bid: Optional[float] = None
        self._up_ask: Optional[float] = None
        self._down_bid: Optional[float] = None
        self._down_ask: Optional[float] = None
        # Rolling histories per side for the current market (reset when
        # token_id changes). Mid is the existing series the chart already
        # renders; bid/ask let the chart draw a band so trade markers
        # (which sit at the actual fill price) lie on the right edge.
        self._series_up: Deque[tuple[str, float]] = deque(maxlen=MAX_POINTS)
        self._series_down: Deque[tuple[str, float]] = deque(maxlen=MAX_POINTS)
        self._series_up_bid: Deque[tuple[str, float]] = deque(maxlen=MAX_POINTS)
        self._series_up_ask: Deque[tuple[str, float]] = deque(maxlen=MAX_POINTS)
        self._series_down_bid: Deque[tuple[str, float]] = deque(maxlen=MAX_POINTS)
        self._series_down_ask: Deque[tuple[str, float]] = deque(maxlen=MAX_POINTS)
        self._cur_up_token: Optional[str] = None
        self._cur_down_token: Optional[str] = None

    @property
    def path(self) -> Path:
        return self._path_fn()

    @property
    def latest(self) -> Optional[PolymarketPrices]:
        up_mid = _mid(self._up_bid, self._up_ask)
        down_mid = _mid(self._down_bid, self._down_ask)
        if up_mid is None and down_mid is None:
            return None
        prob_up = up_mid
        prob_down = down_mid if down_mid is not None else (1.0 - up_mid if up_mid is not None else None)
        return PolymarketPrices(
            best_bid=self._up_bid,
            best_ask=self._up_ask,
            mid=up_mid,
            prob_up=prob_up,
            prob_down=prob_down,
        )

    def series(self, side: str, kind: str = "mid") -> list[dict]:
        if side == "UP":
            dq = {
                "mid": self._series_up,
                "bid": self._series_up_bid,
                "ask": self._series_up_ask,
            }.get(kind, self._series_up)
        else:
            dq = {
                "mid": self._series_down,
                "bid": self._series_down_bid,
                "ask": self._series_down_ask,
            }.get(kind, self._series_down)
        return [{"t": t, "v": v} for (t, v) in dq]

    def seed(self) -> None:
        p = self.path
        if not p.exists():
            return
        try:
            with p.open("r", encoding="utf-8", newline="") as f:
                reader = csv.DictReader(f)
                self._header = reader.fieldnames
                for row in reader:
                    self._apply_row(row)
                self._offset = f.tell()
            self._last_size = p.stat().st_size
        except OSError:
            return

    def _apply_row(self, row: dict) -> None:
        ts = row.get("timestamp", "")
        token = row.get("token_id", "")
        side = _parse_side(token)
        if side is None:
            return
        bid = _f(row.get("best_bid"))
        ask = _f(row.get("best_ask"))
        mid = _mid(bid, ask)
        if side == "UP":
            # Reset history if token changed (new market)
            if token != self._cur_up_token:
                self._cur_up_token = token
                self._series_up.clear()
                self._series_up_bid.clear()
                self._series_up_ask.clear()
            self._up_bid, self._up_ask = bid, ask
            if mid is not None:
                self._series_up.append((ts, mid))
            if bid is not None:
                self._series_up_bid.append((ts, bid))
            if ask is not None:
                self._series_up_ask.append((ts, ask))
        else:
            if token != self._cur_down_token:
                self._cur_down_token = token
                self._series_down.clear()
                self._series_down_bid.clear()
                self._series_down_ask.clear()
            self._down_bid, self._down_ask = bid, ask
            if mid is not None:
                self._series_down.append((ts, mid))
            if bid is not None:
                self._series_down_bid.append((ts, bid))
            if ask is not None:
                self._series_down_ask.append((ts, ask))

    def poll(self) -> bool:
        p = self.path
        if not p.exists():
            return False
        try:
            size = p.stat().st_size
        except OSError:
            return False
        if size < self._offset:
            # Truncation — re-seed
            log.info("orderbook truncated; re-seeding")
            self._offset = 0
            self._series_up.clear()
            self._series_down.clear()
            self._cur_up_token = None
            self._cur_down_token = None
            self.seed()
            return True
        if size == self._last_size:
            return False
        self._last_size = size
        changed = False
        try:
            with p.open("r", encoding="utf-8", newline="") as f:
                if self._header is None:
                    reader = csv.DictReader(f)
                    self._header = reader.fieldnames
                    for row in reader:
                        pass
                    self._offset = f.tell()
                    return False
                f.seek(self._offset)
                remainder = f.read()
                if "\n" not in remainder:
                    return False
                last_nl = remainder.rfind("\n")
                complete = remainder[: last_nl + 1]
                consumed = len(complete.encode("utf-8"))
                self._offset += consumed
                reader = csv.DictReader(io.StringIO(complete), fieldnames=self._header)
                for row in reader:
                    self._apply_row(row)
                    changed = True
        except OSError:
            return False
        return changed


def _mid(bid: Optional[float], ask: Optional[float]) -> Optional[float]:
    if bid is None or ask is None:
        return None
    if bid <= 0 or ask <= 0:
        return None
    return (bid + ask) / 2.0


async def run_orderbook_loop(tail: "OrderbookTail", stop: asyncio.Event) -> None:
    while not stop.is_set():
        try:
            if tail.poll():
                payload = {
                    "prices": tail.latest.model_dump() if tail.latest else None,
                    "series_up": tail.series("UP"),
                    "series_down": tail.series("DOWN"),
                    "series_up_bid": tail.series("UP", "bid"),
                    "series_up_ask": tail.series("UP", "ask"),
                    "series_down_bid": tail.series("DOWN", "bid"),
                    "series_down_ask": tail.series("DOWN", "ask"),
                }
                await bus.publish("orderbook.update", payload)
        except Exception:  # noqa: BLE001
            log.exception("orderbook poll error")
        try:
            await asyncio.wait_for(stop.wait(), timeout=settings.poll_interval_seconds)
        except asyncio.TimeoutError:
            pass
