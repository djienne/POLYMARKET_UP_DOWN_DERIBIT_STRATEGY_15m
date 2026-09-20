"""Single CSV trade logger for all paper instances."""

import csv
import json
import logging
from pathlib import Path

from scripts.trader._io import ensure_trailing_newline
from scripts.trader.position import TradeEvent

log = logging.getLogger(__name__)

HEADER = [
    "instance_id", "timestamp", "event", "direction", "market_id",
    "entry_price", "exit_price", "shares", "pnl", "pnl_pct", "capital",
    "model_prob", "poly_prob", "spot_price", "barrier", "liquidity_mode",
    "settlement_source", "effective_tp_pct",
    "decision_at", "cost_basis", "fee_schedule", "contract_metadata", "experiment_id",
]


class TradeLogger:
    """Buffered CSV writer for trade events across all instances."""

    def __init__(self, path: Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._buffer: list[TradeEvent] = []
        # If file exists (recovery), append to it
        self._file_initialized = self.path.exists() and self.path.stat().st_size > 0
        if self._file_initialized:
            try:
                if ensure_trailing_newline(self.path):
                    log.warning(
                        "trade_log: repaired missing trailing newline in %s "
                        "(crash mid-write?)",
                        self.path,
                    )
            except OSError as exc:
                log.warning(
                    "trade_log: newline repair failed for %s: %s", self.path, exc
                )
        self._write_mode_column = not self._file_initialized
        # Only write the fix #2/#5 columns if the existing header has them,
        # or if we're creating a fresh file. Old files without these columns
        # keep their original schema.
        self._write_settlement_columns = not self._file_initialized
        self._extra_columns = HEADER[18:] if not self._file_initialized else []
        if self._file_initialized:
            try:
                with open(self.path, newline="") as f:
                    first_line = f.readline().strip()
                cols = [col.strip() for col in first_line.split(",") if col.strip()]
                self._write_mode_column = "liquidity_mode" in cols
                self._write_settlement_columns = (
                    "settlement_source" in cols and "effective_tp_pct" in cols
                )
                self._extra_columns = [column for column in HEADER[18:] if column in cols]
            except OSError:
                self._write_mode_column = False
                self._write_settlement_columns = False
        missing = [
            col
            for col, present in (
                ("liquidity_mode", self._write_mode_column),
                ("settlement_source", self._write_settlement_columns),
                ("effective_tp_pct", self._write_settlement_columns),
            )
            if not present
        ]
        missing.extend(column for column in HEADER[18:] if column not in self._extra_columns)
        if self._file_initialized and missing:
            log.warning(
                "trades log %s has a legacy header missing %s; new rows will "
                "permanently omit these fields until the file is migrated "
                "(stop the grid, rewrite the header).",
                self.path,
                missing,
            )

    def log(self, event: TradeEvent):
        self._buffer.append(event)

    def flush(self):
        if not self._buffer:
            return

        if len(self._extra_columns) != len(HEADER[18:]) and any(
            ev.decision_at is not None or ev.cost_basis is not None or ev.fee_schedule is not None or ev.contract_metadata
            for ev in self._buffer
        ):
            raise RuntimeError(
                f"{self.path} has a legacy header that cannot preserve execution metadata; "
                "stop the grid and run scripts.trader.cli.migrate_trades_header --apply first"
            )

        mode = "a" if self._file_initialized else "w"
        with open(self.path, mode, newline="") as f:
            writer = csv.writer(f)
            if not self._file_initialized:
                writer.writerow(HEADER)
                self._file_initialized = True
            for ev in self._buffer:
                modern = bool(ev.contract_metadata) or ev.fee_schedule is not None
                quantity_places = 8 if modern else 4
                money_places = 8 if modern else 2
                row = [
                    ev.instance_id,
                    ev.timestamp.isoformat(),
                    ev.event,
                    ev.direction,
                    ev.market_id,
                    f"{ev.entry_price:.4f}" if ev.entry_price is not None else "",
                    f"{ev.exit_price:.4f}" if ev.exit_price is not None else "",
                    f"{ev.shares:.{quantity_places}f}" if ev.shares is not None else "",
                    f"{ev.pnl:.{money_places}f}" if ev.pnl is not None else "",
                    f"{ev.pnl_pct:.2f}" if ev.pnl_pct is not None else "",
                    f"{ev.capital:.{money_places}f}",
                    f"{ev.model_prob:.4f}" if ev.model_prob is not None else "",
                    f"{ev.poly_prob:.4f}" if ev.poly_prob is not None else "",
                    f"{ev.spot_price:.2f}" if ev.spot_price is not None else "",
                    f"{ev.barrier:.2f}" if ev.barrier is not None else "",
                ]
                if self._write_mode_column:
                    row.append(ev.liquidity_mode)
                if self._write_settlement_columns:
                    row.append(ev.settlement_source)
                    row.append(
                        f"{ev.effective_tp_pct:.4f}"
                        if ev.effective_tp_pct is not None
                        else ""
                    )
                extra = {
                    "experiment_id": ev.experiment_id,
                    "decision_at": ev.decision_at.isoformat() if ev.decision_at else "",
                    "cost_basis": f"{ev.cost_basis:.8f}" if ev.cost_basis is not None else "",
                    "fee_schedule": json.dumps(ev.fee_schedule, separators=(",", ":"), sort_keys=True) if ev.fee_schedule is not None else "",
                    "contract_metadata": json.dumps(ev.contract_metadata, separators=(",", ":"), sort_keys=True) if ev.contract_metadata else "",
                }
                row.extend(extra[column] for column in self._extra_columns)
                writer.writerow(row)
        self._buffer.clear()

    def close(self):
        self.flush()
