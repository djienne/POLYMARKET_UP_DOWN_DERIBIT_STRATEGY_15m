"""Structured diagnostics for dry-run grid entry decisions."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


HEADER = [
    "timestamp",
    "market_id",
    "instance_id",
    "entered",
    "direction",
    "alpha_up",
    "alpha_down",
    "floor_up",
    "floor_down",
    "tp_pct",
    "sl_pct",
    "capital_before",
    "order_size_usd",
    "signal_price",
    "signal_model_prob",
    "signal_required_prob",
    "signal_edge_ratio",
    "current_avg_price",
    "current_fillable_usd",
    "current_shares",
    "delayed_avg_price",
    "delayed_fillable_usd",
    "delayed_shares",
    "selected_entry_price",
    "max_entry_price",
    "execution_max_entry_price",
    "paper_delayed_book_slippage",
    "rejection_reason",
    "rejection_details",
    "liquidity_mode",
]


@dataclass
class GridEntryDiagnostic:
    timestamp: datetime
    market_id: str
    instance_id: int
    entered: bool
    direction: str = ""
    alpha_up: float = 0.0
    alpha_down: float = 0.0
    floor_up: float = 0.0
    floor_down: float = 0.0
    tp_pct: float = 0.0
    sl_pct: float = 0.0
    capital_before: float = 0.0
    order_size_usd: Optional[float] = None
    signal_price: Optional[float] = None
    signal_model_prob: Optional[float] = None
    signal_required_prob: Optional[float] = None
    signal_edge_ratio: Optional[float] = None
    current_avg_price: Optional[float] = None
    current_fillable_usd: Optional[float] = None
    current_shares: Optional[float] = None
    delayed_avg_price: Optional[float] = None
    delayed_fillable_usd: Optional[float] = None
    delayed_shares: Optional[float] = None
    selected_entry_price: Optional[float] = None
    max_entry_price: float = 0.0
    execution_max_entry_price: float = 0.0
    paper_delayed_book_slippage: float = 0.0
    rejection_reason: str = ""
    rejection_details: str = ""
    liquidity_mode: str = ""


def _fmt_float(value: Optional[float], places: int = 4) -> str:
    if value is None:
        return ""
    return f"{float(value):.{places}f}"


class GridEntryDiagnosticsLogger:
    """Buffered CSV writer for grid entry accept/reject diagnostics."""

    def __init__(self, path: Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._buffer: list[GridEntryDiagnostic] = []
        self._seen_rejections: set[tuple] = set()
        self._dedupe_market_id: str | None = None
        self._file_initialized = False
        self._reset_current_file()

    def _reset_current_file(self) -> None:
        if self.path.exists() and self.path.stat().st_size > 0:
            stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            rotated = self.path.with_name(f"{self.path.stem}.{stamp}{self.path.suffix}")
            counter = 1
            while rotated.exists():
                rotated = self.path.with_name(
                    f"{self.path.stem}.{stamp}.{counter}{self.path.suffix}"
                )
                counter += 1
            self.path.replace(rotated)
        with self.path.open("w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(HEADER)
        self._file_initialized = True

    @staticmethod
    def _dedupe_key(row: GridEntryDiagnostic) -> tuple:
        selected = (
            round(float(row.selected_entry_price), 4)
            if row.selected_entry_price is not None
            else None
        )
        signal = (
            round(float(row.signal_price), 4)
            if row.signal_price is not None
            else None
        )
        order_size = (
            round(float(row.order_size_usd), 2)
            if row.order_size_usd is not None
            else None
        )
        return (
            row.market_id,
            row.instance_id,
            row.direction,
            row.rejection_reason,
            selected,
            signal,
            order_size,
        )

    def log(self, row: GridEntryDiagnostic, *, liquidity_mode: str = "") -> None:
        if row.market_id != self._dedupe_market_id:
            self._seen_rejections.clear()
            self._dedupe_market_id = row.market_id
        if liquidity_mode and not row.liquidity_mode:
            row.liquidity_mode = liquidity_mode
        if not row.entered:
            if len(self._seen_rejections) >= 16384:
                self._seen_rejections.clear()
            key = self._dedupe_key(row)
            if key in self._seen_rejections:
                return
            self._seen_rejections.add(key)
        self._buffer.append(row)

    def log_many(
        self,
        rows: list[GridEntryDiagnostic],
        *,
        liquidity_mode: str = "",
    ) -> None:
        for row in rows:
            self.log(row, liquidity_mode=liquidity_mode)

    def flush(self) -> None:
        if not self._buffer:
            return
        with self.path.open("a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            for row in self._buffer:
                writer.writerow([
                    row.timestamp.isoformat(),
                    row.market_id,
                    row.instance_id,
                    "1" if row.entered else "0",
                    row.direction,
                    _fmt_float(row.alpha_up),
                    _fmt_float(row.alpha_down),
                    _fmt_float(row.floor_up),
                    _fmt_float(row.floor_down),
                    _fmt_float(row.tp_pct),
                    _fmt_float(row.sl_pct),
                    _fmt_float(row.capital_before, 2),
                    _fmt_float(row.order_size_usd, 2),
                    _fmt_float(row.signal_price),
                    _fmt_float(row.signal_model_prob),
                    _fmt_float(row.signal_required_prob),
                    _fmt_float(row.signal_edge_ratio),
                    _fmt_float(row.current_avg_price),
                    _fmt_float(row.current_fillable_usd, 2),
                    _fmt_float(row.current_shares),
                    _fmt_float(row.delayed_avg_price),
                    _fmt_float(row.delayed_fillable_usd, 2),
                    _fmt_float(row.delayed_shares),
                    _fmt_float(row.selected_entry_price),
                    _fmt_float(row.max_entry_price),
                    _fmt_float(row.execution_max_entry_price),
                    _fmt_float(row.paper_delayed_book_slippage),
                    row.rejection_reason,
                    row.rejection_details,
                    row.liquidity_mode,
                ])
        self._buffer.clear()

    def close(self) -> None:
        self.flush()
