"""State persistence for the trading bot."""

import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Optional

from .config import TraderConfig
from ._io import atomic_write_json
from .persistence import SqliteStore
from .position import Direction, MicroPosition as Position, Trade, TradeResult


LIVE_TICK_EVAL_HEADER = (
    "timestamp,market_id,model_used,spot_source,elapsed_seconds,remaining_seconds,"
    "spot_price,barrier,barrier_source,barrier_confirmed,skipped_before_eval,"
    "pre_eval_skip_reason,chosen_direction,"
    "up_model_prob,up_poly_prob,up_executable_price,up_required_prob,up_has_edge,"
    "down_model_prob,down_poly_prob,down_executable_price,down_required_prob,down_has_edge,"
    "entry_signal_present,entry_signal_direction,entry_signal_price,"
    "entry_signal_age_s,entry_signal_market_id,latest_signal_stale_age_s,"
    "edge_skip_reason,edge_skip_details,"
    "post_eval_attempted,post_eval_entered,post_eval_rejection_reason,"
    "post_eval_rejection_details,post_eval_capital_source,"
    "post_eval_raw_order_size_usd,post_eval_order_size_usd,"
    "post_eval_capital_before_entry,post_eval_order_id,post_eval_order_ambiguous,"
    "post_eval_token_id,post_eval_rechecked_price,post_eval_order_metadata"
)

MIRROR_SIGNAL_AUDIT_HEADER = (
    "timestamp,stage,status,reason,signal_id,event,market_id,direction,"
    "source_instance_id,stream_id,attempt,signal_age_s,publish_to_receive_s,"
    "receive_to_attempt_s,publish_to_attempt_s,trigger_bid,trigger_ask,"
    "trigger_mid,dry_entry_price,rechecked_price,order_id,live_position_id,details"
)

MIRROR_SIGNAL_TERMINAL_STATUSES = {
    "accepted",
    "rejected",
    "cancelled",
    "diagnostic_only",
    "processed",
}


def _csv_float(value: Optional[float]) -> str:
    if value is None:
        return ""
    try:
        return f"{float(value):.6f}"
    except (TypeError, ValueError):
        return ""


class StateManager:
    """Manages persistence of trading state."""

    VERSION = "1.0"

    def __init__(self, config: TraderConfig):
        self.config = config
        # Use mode-appropriate files (paper vs live)
        self.state_file = config.get_state_file()
        self.trades_file = config.get_trades_file()
        self.mode = "paper" if config.paper_mode else "live"
        self.last_load_error: str = ""
        self._sqlite_store: Optional[SqliteStore] = None
        if getattr(config, "sqlite_enabled", lambda: False)():
            self._sqlite_store = SqliteStore(config.get_sqlite_db_file())

        # State
        self.starting_capital: float = config.get_starting_capital()
        self.current_capital: float = config.get_starting_capital()
        self.total_pnl: float = 0.0
        self.open_positions: list[Position] = []
        self.closed_positions: list[Trade] = []
        self.pending_entry: Optional[dict] = None
        self.last_tp_fill_time: Optional[datetime] = None
        self.current_market: Optional[dict] = None
        self.known_token_ids: set[str] = set()
        self.orphan_scan_cursor: int = 0
        self.mirror_control_enabled: bool = False
        self.mirror_instance_id: Optional[int] = None
        self.mirror_config_hash: str = ""
        self.processed_mirror_signal_ids: list[str] = []
        # Last market_id we've ENTERED a position in (one position per 15m window).
        # Reset only when load() finds a stale value persisted across a restart.
        self.last_entered_market_id: Optional[str] = None
        self.last_updated: datetime = datetime.now(timezone.utc)

        if not self.config.paper_mode:
            self._reset_current_run_logs()

    def _reset_current_run_logs(self) -> None:
        risk_current_path = getattr(self.config, "risk_events_current_file", None)
        live_tick_eval_path = getattr(self.config, "live_tick_eval_file", None)
        mirror_audit_current_path = getattr(self.config, "mirror_signal_audit_current_file", None)
        for path in (risk_current_path, live_tick_eval_path, mirror_audit_current_path):
            if path is None:
                continue
            try:
                self._rotate_current_run_log(path)
            except FileNotFoundError:
                pass
        if risk_current_path is not None:
            risk_current_path.parent.mkdir(parents=True, exist_ok=True)
            risk_current_path.write_text("timestamp,mode,kind,details\n", encoding="utf-8")
        if live_tick_eval_path is not None:
            live_tick_eval_path.parent.mkdir(parents=True, exist_ok=True)
            live_tick_eval_path.write_text(LIVE_TICK_EVAL_HEADER + "\n", encoding="utf-8")
        if mirror_audit_current_path is not None:
            mirror_audit_current_path.parent.mkdir(parents=True, exist_ok=True)
            mirror_audit_current_path.write_text(MIRROR_SIGNAL_AUDIT_HEADER + "\n", encoding="utf-8")

    @staticmethod
    def _rotate_current_run_log(path: Path) -> None:
        if not path.exists():
            return
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        rotated = path.with_name(f"{path.stem}.{stamp}{path.suffix}")
        counter = 1
        while rotated.exists():
            rotated = path.with_name(f"{path.stem}.{stamp}.{counter}{path.suffix}")
            counter += 1
        path.replace(rotated)

    @staticmethod
    def _append_csv_row(path: Path, header: str, row: list[str]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        write_header = not path.exists()
        with open(path, "a", newline="") as f:
            if write_header:
                f.write(header + "\n")
            writer = csv.writer(f)
            writer.writerow(row)

    def load(self) -> bool:
        """Load state from file. Returns True if loaded, False if fresh start."""
        self.last_load_error = ""
        if self._sqlite_store is not None:
            try:
                data = self._sqlite_store.load_state_snapshot(mode=self.mode)
                if data is not None:
                    return self._apply_loaded_data(data)
            except Exception as e:  # noqa: BLE001
                self.last_load_error = f"sqlite_load_failed: {e}"
                print(f"Error loading SQLite state: {e}")
                if not self.config.paper_mode:
                    return False

        if not self.state_file.exists():
            return False

        try:
            with open(self.state_file, "r") as f:
                data = json.load(f)
            return self._apply_loaded_data(data)

        except (json.JSONDecodeError, KeyError, ValueError) as e:
            self.last_load_error = f"state_load_failed: {e}"
            print(f"Error loading state: {e}")
            return False

    def _apply_loaded_data(self, data: dict) -> bool:
        # Version check
        if data.get("version") != self.VERSION:
            self.last_load_error = "state_version_mismatch"
            print(f"State version mismatch, starting fresh")
            return False

        # Load capital
        capital = data.get("capital", {})
        self.starting_capital = capital.get("starting", self.config.get_starting_capital())
        self.current_capital = capital.get("current", self.config.get_starting_capital())
        self.total_pnl = capital.get("total_pnl", 0.0)

        # Load positions
        self.open_positions = [
            Position.from_dict(p) for p in data.get("open_positions", [])
        ]
        self.closed_positions = [
            Trade.from_dict(t) for t in data.get("closed_positions", [])
        ]
        self.pending_entry = data.get("pending_entry")

        # Load timing
        tp_time = data.get("last_tp_fill_time")
        self.last_tp_fill_time = (
            datetime.fromisoformat(tp_time) if tp_time else None
        )

        # Load current market
        self.current_market = data.get("current_market")

        # Load known token IDs (backward compat: default to empty)
        self.known_token_ids = set(data.get("known_token_ids", []))
        self.orphan_scan_cursor = int(data.get("orphan_scan_cursor", 0) or 0)
        mirror = data.get("mirror_control", {}) or {}
        self.mirror_control_enabled = bool(mirror.get("enabled", False))
        raw_instance_id = mirror.get("instance_id")
        try:
            self.mirror_instance_id = int(raw_instance_id) if raw_instance_id is not None else None
        except (TypeError, ValueError):
            self.mirror_instance_id = None
        self.mirror_config_hash = str(mirror.get("config_hash") or "")
        processed = mirror.get("processed_signal_ids", []) or []
        self.processed_mirror_signal_ids = [str(x) for x in processed if x]

        # Last entered market (backward compat: default to None)
        self.last_entered_market_id = data.get("last_entered_market_id")

        last_updated = data.get("last_updated")
        self.last_updated = (
            datetime.fromisoformat(last_updated)
            if last_updated
            else datetime.now(timezone.utc)
        )

        return True

    def save(self) -> None:
        """Save state to file atomically (write to temp, then rename)."""
        self.last_updated = datetime.now(timezone.utc)

        # Ensure directory exists
        self.state_file.parent.mkdir(parents=True, exist_ok=True)

        data = {
            "version": self.VERSION,
            "last_updated": self.last_updated.isoformat(),
            "capital": {
                "starting": self.starting_capital,
                "current": self.current_capital,
                "total_pnl": self.total_pnl,
            },
            "current_market": self.current_market,
            "known_token_ids": sorted(self.known_token_ids),
            "orphan_scan_cursor": self.orphan_scan_cursor,
            "mirror_control": {
                "enabled": self.mirror_control_enabled,
                "instance_id": self.mirror_instance_id,
                "config_hash": self.mirror_config_hash,
                "processed_signal_ids": self.processed_mirror_signal_ids[-1000:],
            },
            "open_positions": [p.to_dict() for p in self.open_positions],
            "closed_positions": [t.to_dict() for t in self.closed_positions],
            "pending_entry": self.pending_entry,
            "last_tp_fill_time": (
                self.last_tp_fill_time.isoformat() if self.last_tp_fill_time else None
            ),
            "last_entered_market_id": self.last_entered_market_id,
        }

        if self._sqlite_store is not None:
            self._sqlite_store.save_state_snapshot(
                mode=self.mode,
                version=self.VERSION,
                payload=data,
                durable=bool(self.pending_entry and self.pending_entry.get("reason") == "order_submission"),
            )
            if not getattr(self.config, "export_legacy_files", True):
                return

        try:
            atomic_write_json(self.state_file, data)
        except OSError as exc:
            if self._sqlite_store is not None:
                self._log_legacy_export_failed("state", exc)
                return
            raise

    def log_trade(self, trade: Trade) -> None:
        """Append trade to CSV log with mode column."""
        self.trades_file.parent.mkdir(parents=True, exist_ok=True)

        mode = self.mode
        if self._sqlite_store is not None:
            self._sqlite_store.upsert_trade(mode=mode, trade=trade)
            if not getattr(self.config, "export_legacy_files", True):
                return
            try:
                self._log_trade_legacy(trade, mode)
            except OSError as exc:
                self._log_legacy_export_failed("trades", exc)
            return

        self._log_trade_legacy(trade, mode)

    def _log_trade_legacy(self, trade: Trade, mode: str) -> None:
        # Write header if file doesn't exist. If it exists but the header
        # is older than the current schema, migrate in place: pad every
        # existing row with empty values for any new trailing columns so
        # DictReader-based consumers (the dashboard) keep parsing cleanly
        # once we start appending rows with the new schema.
        if not self.trades_file.exists():
            with open(self.trades_file, "a", newline="") as f:
                f.write(Trade.csv_header() + "\n")
                f.write(trade.to_csv_row(mode=mode) + "\n")
            return

        expected_header = Trade.csv_header()
        try:
            with open(self.trades_file, "r", newline="", encoding="utf-8") as f:
                first_line = f.readline().rstrip("\r\n")
        except OSError:
            first_line = ""
        if first_line and first_line != expected_header:
            self._migrate_trades_csv_header(expected_header, first_line)

        with open(self.trades_file, "a", newline="") as f:
            f.write(trade.to_csv_row(mode=mode) + "\n")

    def _log_legacy_export_failed(self, target: str, exc: Exception) -> None:
        try:
            self.log_risk_event(
                kind="legacy_export_failed",
                details={"target": target, "error": str(exc)[:160]},
            )
        except Exception:
            pass

    def _migrate_trades_csv_header(self, new_header: str, old_header: str) -> None:
        """Upgrade the trades CSV to the current Trade.csv_header().

        Rewrites existing rows by column name, preserving values when the old
        file had the same columns in a different order and filling newly added
        columns with empty strings. Atomic: write to a tmp file in the same
        dir, then os.replace.
        """
        new_cols = new_header.split(",")
        old_cols = old_header.split(",")
        extra_cols = [c for c in new_cols if c not in old_cols]
        import csv as _csv, os as _os, tempfile as _tempfile
        tmp_fd, tmp_path = _tempfile.mkstemp(
            dir=str(self.trades_file.parent), suffix=".tmp"
        )
        try:
            with open(tmp_fd, "w", newline="", encoding="utf-8") as out:
                out.write(new_header + "\n")
                with open(self.trades_file, "r", newline="", encoding="utf-8") as src:
                    reader = _csv.DictReader(src)
                    writer = _csv.DictWriter(out, fieldnames=new_cols, extrasaction="ignore")
                    for row in reader:
                        for c in extra_cols:
                            row.setdefault(c, "")
                        writer.writerow(row)
            _os.replace(tmp_path, self.trades_file)
        except Exception:
            try:
                _os.unlink(tmp_path)
            except OSError:
                pass
            raise

    def log_equity_event(
        self,
        event: str,
        direction: str = "",
        entry_price: float = 0.0,
        exit_price: float = 0.0,
        shares: float = 0.0,
        pnl: float = 0.0,
        position_value: float = 0.0,
    ) -> None:
        """Append an equity snapshot row on entry/exit events.

        Args:
            event: Event type (ENTRY, TP_FILLED, TRAILING_STOP, STOP_LOSS, etc.)
            direction: UP or DOWN
            entry_price: Entry fill price
            exit_price: Exit fill price (0 for entries)
            shares: Number of shares
            pnl: Realized P&L (0 for entries)
            position_value: Mark-to-market value of open positions
        """
        equity_file = self.config.get_equity_file()
        equity_file.parent.mkdir(parents=True, exist_ok=True)

        equity = self.current_capital + position_value

        if self._sqlite_store is not None:
            self._sqlite_store.append_equity_point(
                mode=self.mode,
                event=event,
                direction=direction,
                entry_price=entry_price,
                exit_price=exit_price,
                shares=shares,
                pnl=pnl,
                capital=self.current_capital,
                equity=equity,
                position_value=position_value,
            )
            if not getattr(self.config, "export_legacy_files", True):
                return
            try:
                self._log_equity_event_legacy(
                    equity_file,
                    event,
                    direction,
                    entry_price,
                    exit_price,
                    shares,
                    pnl,
                    equity,
                    position_value,
                )
            except OSError as exc:
                self._log_legacy_export_failed("equity", exc)
            return

        self._log_equity_event_legacy(
            equity_file,
            event,
            direction,
            entry_price,
            exit_price,
            shares,
            pnl,
            equity,
            position_value,
        )

    def _log_equity_event_legacy(
        self,
        equity_file: Path,
        event: str,
        direction: str,
        entry_price: float,
        exit_price: float,
        shares: float,
        pnl: float,
        equity: float,
        position_value: float,
    ) -> None:
        header = ("timestamp,event,direction,entry_price,exit_price,shares,"
                  "pnl,capital,equity,position_value")
        write_header = not equity_file.exists()

        with open(equity_file, "a", newline="") as f:
            writer = csv.writer(f)
            if write_header:
                f.write(header + "\n")
            writer.writerow([
                datetime.now(timezone.utc).isoformat(),
                event,
                direction,
                f"{entry_price:.4f}",
                f"{exit_price:.4f}",
                f"{shares:.4f}",
                f"{pnl:.2f}",
                f"{self.current_capital:.2f}",
                f"{equity:.2f}",
                f"{position_value:.2f}",
            ])

    def log_risk_event(self, kind: str, details: Optional[dict] = None) -> None:
        """Append a structured row to results/15m_risk_events.csv.

        Captures gate-related events (concurrency rejections, slippage
        rejections, TP verification failures, stale pending cancels, etc.)
        so a bad run can be reviewed from one file.
        """
        path = getattr(self.config, "risk_events_file", None)
        if path is None:
            return

        details = details or {}
        detail_str = ";".join(f"{k}={v}" for k, v in sorted(details.items()))
        mode = "paper" if self.config.paper_mode else "live"

        header = "timestamp,mode,kind,details"
        row = [
            datetime.now(timezone.utc).isoformat(),
            mode,
            kind,
            detail_str,
        ]
        self._append_csv_row(path, header, row)
        current_run_path = getattr(self.config, "risk_events_current_file", None)
        if not self.config.paper_mode and current_run_path is not None:
            self._append_csv_row(current_run_path, header, row)

    def configure_mirror_control(
        self,
        *,
        enabled: bool,
        instance_id: Optional[int],
        config_hash: str,
        reset_processed: bool = False,
    ) -> None:
        self.mirror_control_enabled = bool(enabled)
        self.mirror_instance_id = instance_id
        self.mirror_config_hash = config_hash or ""
        if reset_processed:
            self.processed_mirror_signal_ids = []

    def get_mirror_signal_status(self, signal_id: str) -> Optional[dict]:
        if not signal_id or self._sqlite_store is None:
            return None
        return self._sqlite_store.get_mirror_signal_status(
            mode=self.mode,
            signal_id=signal_id,
        )

    def record_mirror_signal_status(
        self,
        signal_id: str,
        *,
        status: str,
        reason: str = "",
        stream_id: str = "",
        event: str = "",
        market_id: str = "",
        direction: str = "",
        source_instance_id: str = "",
        attempts: Optional[int] = None,
        live_order_id: str = "",
        live_position_id: str = "",
        details: Optional[dict] = None,
        terminal: bool = False,
    ) -> None:
        if not signal_id or self._sqlite_store is None:
            return
        self._sqlite_store.upsert_mirror_signal_status(
            mode=self.mode,
            signal_id=signal_id,
            status=status,
            reason=reason,
            stream_id=stream_id,
            event=event,
            market_id=market_id,
            direction=direction,
            source_instance_id=source_instance_id,
            attempts=attempts,
            live_order_id=live_order_id,
            live_position_id=live_position_id,
            details=details,
            terminal=terminal,
        )

    def has_processed_mirror_signal(self, signal_id: str) -> bool:
        if not signal_id:
            return False
        if signal_id in set(self.processed_mirror_signal_ids):
            return True
        row = self.get_mirror_signal_status(signal_id)
        if row is None:
            return False
        return str(row.get("status") or "") in MIRROR_SIGNAL_TERMINAL_STATUSES

    def mark_mirror_signal_processed(
        self,
        signal_id: str,
        *,
        max_ids: int = 1000,
        status: str = "processed",
        reason: str = "",
        stream_id: str = "",
        event: str = "",
        market_id: str = "",
        direction: str = "",
        source_instance_id: str = "",
        attempts: Optional[int] = None,
        live_order_id: str = "",
        live_position_id: str = "",
        details: Optional[dict] = None,
    ) -> None:
        if not signal_id:
            return
        if signal_id in self.processed_mirror_signal_ids:
            return
        self.processed_mirror_signal_ids.append(signal_id)
        if len(self.processed_mirror_signal_ids) > max_ids:
            self.processed_mirror_signal_ids = self.processed_mirror_signal_ids[-max_ids:]
        self.record_mirror_signal_status(
            signal_id,
            status=status,
            reason=reason,
            stream_id=stream_id,
            event=event,
            market_id=market_id,
            direction=direction,
            source_instance_id=source_instance_id,
            attempts=attempts,
            live_order_id=live_order_id,
            live_position_id=live_position_id,
            details=details,
            terminal=True,
        )

    def log_live_tick_eval(
        self,
        *,
        market_id: str,
        model_used: str,
        spot_source: str,
        elapsed_seconds: float,
        remaining_seconds: float,
        spot_price: float,
        barrier: float,
        barrier_confirmed: bool,
        barrier_source: str,
        skipped_before_eval: bool,
        pre_eval_skip_reason: str,
        chosen_direction: str,
        up_model_prob: float,
        up_poly_prob: float,
        up_executable_price: float,
        up_required_prob: float,
        up_has_edge: bool,
        down_model_prob: float,
        down_poly_prob: float,
        down_executable_price: float,
        down_required_prob: float,
        down_has_edge: bool,
        entry_signal_present: bool = False,
        entry_signal_direction: str = "",
        entry_signal_price: Optional[float] = None,
        entry_signal_age_s: float = -1.0,
        entry_signal_market_id: str = "",
        latest_signal_stale_age_s: float = -1.0,
        edge_skip_reason: str = "",
        edge_skip_details: str = "",
        post_eval_attempted: bool = False,
        post_eval_entered: bool = False,
        post_eval_rejection_reason: str = "",
        post_eval_rejection_details: str = "",
        post_eval_capital_source: str = "",
        post_eval_raw_order_size_usd: Optional[float] = None,
        post_eval_order_size_usd: Optional[float] = None,
        post_eval_capital_before_entry: Optional[float] = None,
        post_eval_order_id: str = "",
        post_eval_order_ambiguous: bool = False,
        post_eval_token_id: str = "",
        post_eval_rechecked_price: Optional[float] = None,
        post_eval_order_metadata: str = "",
    ) -> None:
        if self.config.paper_mode:
            return
        path = getattr(self.config, "live_tick_eval_file", None)
        if path is None:
            return

        row = [
            datetime.now(timezone.utc).isoformat(),
            market_id,
            model_used,
            spot_source,
            f"{elapsed_seconds:.3f}",
            f"{remaining_seconds:.3f}",
            f"{spot_price:.6f}",
            f"{barrier:.6f}",
            barrier_source,
            "1" if barrier_confirmed else "0",
            "1" if skipped_before_eval else "0",
            pre_eval_skip_reason,
            chosen_direction,
            f"{up_model_prob:.6f}",
            f"{up_poly_prob:.6f}",
            f"{up_executable_price:.6f}",
            f"{up_required_prob:.6f}",
            "1" if up_has_edge else "0",
            f"{down_model_prob:.6f}",
            f"{down_poly_prob:.6f}",
            f"{down_executable_price:.6f}",
            f"{down_required_prob:.6f}",
            "1" if down_has_edge else "0",
            "1" if entry_signal_present else "0",
            entry_signal_direction,
            _csv_float(entry_signal_price),
            f"{entry_signal_age_s:.1f}",
            entry_signal_market_id,
            f"{latest_signal_stale_age_s:.1f}",
            edge_skip_reason,
            edge_skip_details,
            "1" if post_eval_attempted else "0",
            "1" if post_eval_entered else "0",
            post_eval_rejection_reason,
            post_eval_rejection_details,
            post_eval_capital_source,
            _csv_float(post_eval_raw_order_size_usd),
            _csv_float(post_eval_order_size_usd),
            _csv_float(post_eval_capital_before_entry),
            post_eval_order_id,
            "1" if post_eval_order_ambiguous else "0",
            post_eval_token_id,
            _csv_float(post_eval_rechecked_price),
            post_eval_order_metadata,
        ]
        self._append_csv_row(path, LIVE_TICK_EVAL_HEADER, row)

    def log_friction_realized(
        self,
        position: Position,
        trade: Trade,
    ) -> None:
        """Append per-trade friction breakdown to results/15m_friction_realized.csv.

        Lets the configured paper buffer be validated against live reality. In live
        mode, `implied_roundtrip_friction_pct` is the gap between raw
        (shares * price) arithmetic and what was actually booked to capital —
        driven by real fill VWAP vs. model reference price.
        """
        path = getattr(self.config, "friction_realized_file", None)
        if path is None:
            return
        path.parent.mkdir(parents=True, exist_ok=True)

        # Skip zero-share edge cases — nothing meaningful to record.
        if position.shares <= 0:
            return

        mode = "paper" if self.config.paper_mode else "live"
        shares = position.shares
        cost_basis_raw = shares * position.entry_price
        cost_basis_booked = position.cost_basis
        proceeds_raw = shares * trade.exit_price
        proceeds_booked = trade.proceeds

        round_trip = (
            (cost_basis_booked - cost_basis_raw) + (proceeds_raw - proceeds_booked)
        )
        implied_pct = round_trip / cost_basis_raw if cost_basis_raw > 1e-6 else 0.0

        header = (
            "timestamp,mode,direction,shares,entry_price,exit_price,"
            "cost_basis_raw,cost_basis_booked,proceeds_raw,proceeds_booked,"
            "pnl_booked,implied_roundtrip_friction_pct,result"
        )
        write_header = not path.exists()
        with open(path, "a", newline="") as f:
            if write_header:
                f.write(header + "\n")
            writer = csv.writer(f)
            writer.writerow([
                datetime.now(timezone.utc).isoformat(),
                mode,
                position.direction.value,
                f"{position.shares:.4f}",
                f"{position.entry_price:.4f}",
                f"{trade.exit_price:.4f}",
                f"{cost_basis_raw:.4f}",
                f"{cost_basis_booked:.4f}",
                f"{proceeds_raw:.4f}",
                f"{proceeds_booked:.4f}",
                f"{trade.pnl:.4f}",
                f"{implied_pct:.6f}",
                trade.result.value,
            ])

    def reset(self) -> None:
        """Reset state to fresh start (deletes state, trades, and equity files)."""
        self.current_capital = self.config.get_starting_capital()
        self.total_pnl = 0.0
        self.open_positions = []
        self.closed_positions = []
        self.pending_entry = None
        self.last_tp_fill_time = None
        self.current_market = None
        self.known_token_ids = set()
        self.orphan_scan_cursor = 0
        self.mirror_control_enabled = False
        self.mirror_instance_id = None
        self.mirror_config_hash = ""
        self.processed_mirror_signal_ids = []
        self.last_updated = datetime.now(timezone.utc)

        # Delete state, trades, and equity files
        for f in [self.state_file, self.trades_file, self.config.get_equity_file()]:
            if f.exists():
                f.unlink()
        if self._sqlite_store is not None:
            self._sqlite_store.delete_all_for_mode(mode=self.mode)

    def recover_from_csv(
        self,
        last_n: Optional[int] = None,
        override_capital: Optional[float] = None,
        preserve_open_positions: bool = False,
    ) -> bool:
        """Rebuild closed_positions and total_pnl from trades CSV.

        Filters trades by current mode (live vs paper) when the CSV has a
        mode column.  Falls back to ``--last N`` when no mode column exists.

        Args:
            last_n: If set, only recover the N most recent matching trades.
            override_capital: If set, use this as current capital instead of
                computing starting_capital + total_pnl.  Useful for live mode
                where the real USDC balance should be used.
            preserve_open_positions: If True, keep live-only open position and
                pending-entry state while rebuilding closed trade history.

        Returns True if recovery succeeded, False otherwise.
        """
        if not self.trades_file.exists():
            print("No trades CSV found to recover from.")
            return False

        # Backup current state file if it exists
        if self.state_file.exists():
            backup_path = self.state_file.with_suffix(".json.bak")
            import shutil
            shutil.copy2(self.state_file, backup_path)
            print(f"Backed up state to {backup_path}")

        current_mode = "paper" if self.config.paper_mode else "live"

        # Read trades from CSV
        trades: list[Trade] = []
        has_mode_column = False

        try:
            with open(self.trades_file, "r", newline="") as f:
                reader = csv.DictReader(f)
                has_mode_column = "mode" in (reader.fieldnames or [])

                for row in reader:
                    # Mode filtering: skip rows that don't match current mode
                    # Backward compat: treat legacy "dry_run" rows as "paper"
                    if has_mode_column:
                        row_mode = (row.get("mode") or "").strip()
                        if row_mode == "dry_run":
                            row_mode = "paper"
                        if row_mode and row_mode != current_mode:
                            continue

                    pnl_pct_str = row.get("pnl_pct", "0")
                    # Strip trailing % if present
                    pnl_pct_str = pnl_pct_str.rstrip("%")

                    # Rebuild entry_context from CSV columns if present
                    entry_context = None
                    ctx_fields = ["model_prob", "poly_price", "edge_ratio",
                                  "spot_price", "model_used", "ttm_hours",
                                  "required_prob"]
                    if any(row.get(f) for f in ctx_fields):
                        entry_context = {}
                        for f in ctx_fields:
                            val = row.get(f, "")
                            if val:
                                try:
                                    entry_context[f] = float(val)
                                except ValueError:
                                    entry_context[f] = val

                    trade = Trade(
                        id=row["id"],
                        direction=Direction(row["direction"]),
                        shares=float(row["shares"]),
                        entry_price=float(row["entry_price"]),
                        exit_price=float(row["exit_price"]),
                        cost_basis=float(row["cost_basis"]),
                        proceeds=float(row["proceeds"]),
                        pnl=float(row["pnl"]),
                        pnl_pct=float(pnl_pct_str),
                        result=TradeResult(row["result"]),
                        opened_at=datetime.fromisoformat(row["opened_at"]),
                        closed_at=datetime.fromisoformat(row["closed_at"]),
                        market_title=row.get("market_title", ""),
                        reference_price=float(row.get("reference_price", row.get("barrier_price", 0))),
                        final_btc_price=(
                            float(row["final_btc_price"])
                            if row.get("final_btc_price")
                            else None
                        ),
                        entry_context=entry_context,
                        entry_order_id=row.get("entry_order_id") or None,
                        exit_order_id=row.get("exit_order_id") or None,
                        token_id=row.get("token_id", ""),
                        proceeds_source=row.get("proceeds_source", ""),
                    )
                    trades.append(trade)
        except Exception as e:
            print(f"Error reading trades CSV: {e}")
            return False

        if not has_mode_column and last_n is None:
            print(
                f"WARNING: CSV has no 'mode' column — recovering all {len(trades)} trades. "
                f"Use --last N to limit."
            )

        # Apply --last N
        if last_n is not None:
            trades = trades[-last_n:]

        total_pnl = sum(t.pnl for t in trades)

        preserved_open_positions = list(self.open_positions)
        preserved_pending_entry = self.pending_entry
        preserved_current_market = self.current_market
        preserved_known_token_ids = set(self.known_token_ids)
        preserved_orphan_scan_cursor = self.orphan_scan_cursor
        preserved_last_entered_market_id = self.last_entered_market_id

        # Rebuild state
        self.closed_positions = trades
        self.total_pnl = total_pnl
        if override_capital is not None:
            self.current_capital = override_capital
        else:
            self.current_capital = self.config.get_starting_capital() + total_pnl
        if preserve_open_positions:
            self.open_positions = preserved_open_positions
            self.pending_entry = preserved_pending_entry
            self.current_market = preserved_current_market
            self.known_token_ids = preserved_known_token_ids
            self.orphan_scan_cursor = preserved_orphan_scan_cursor
            self.last_entered_market_id = preserved_last_entered_market_id
        else:
            self.open_positions = []
            self.pending_entry = None
        self.save()

        filter_info = f" (mode={current_mode})" if has_mode_column else ""
        if last_n is not None:
            filter_info += f" (last {last_n})"
        print(f"Recovered {len(trades)} trades from CSV{filter_info}")
        print(f"Total P&L: ${total_pnl:+.2f}")
        print(f"Capital: ${self.current_capital:.2f}")
        return True

    def has_open_position(self) -> bool:
        """Check if there's an open position."""
        return bool(self.open_positions) or self.pending_entry is not None

    def pending_entry_summary(self) -> dict:
        """Return diagnostic summary for the current pending_entry, or empty dict."""
        pe = self.pending_entry
        if pe is None:
            return {}
        age_s = -1.0
        created = pe.get("created_at")
        if created:
            try:
                ts = datetime.fromisoformat(created)
                age_s = (datetime.now(timezone.utc) - ts).total_seconds()
            except (ValueError, TypeError):
                pass
        return {
            "order_id": pe.get("order_id", ""),
            "token_id": pe.get("token_id", ""),
            "direction": pe.get("direction", ""),
            "market_id": pe.get("market_id", ""),
            "reason": pe.get("reason", ""),
            "age_s": round(age_s, 1),
        }

    def in_grace_period(self) -> bool:
        """Check if we're in the grace period after a TP fill."""
        if self.last_tp_fill_time is None:
            return False

        now = datetime.now(timezone.utc)
        elapsed_secs = (now - self.last_tp_fill_time).total_seconds()
        return elapsed_secs < self.config.grace_period_seconds

    def get_unrealized_pnl(self, price_lookup: Callable[[Position], float]) -> float:
        """Calculate unrealized P&L for all open positions.

        Args:
            price_lookup: Function that takes a Position and returns the current price.

        Returns:
            Total unrealized P&L across all open positions.
        """
        total = 0.0
        for pos in self.open_positions:
            current_price = price_lookup(pos)
            market_value = pos.shares * current_price
            total += market_value - pos.cost_basis
        return total

    def add_position(self, position: Position) -> None:
        """Add a new open position."""
        self.open_positions.append(position)
        self.current_capital -= position.cost_basis

    def close_position(self, position: Position, trade: Trade) -> None:
        """Close a position and record the trade."""
        self.open_positions = [p for p in self.open_positions if p.id != position.id]
        self.closed_positions.append(trade)
        self.current_capital += trade.proceeds
        self.total_pnl += trade.pnl
        self.log_trade(trade)

    def void_last_close_for_position(
        self, position: Position, reason: str = "phantom_tp_fill",
    ) -> Optional[Trade]:
        """Reverse the last close for a position when post-exit verification
        finds the shares are still on chain.

        Pops the most-recent ``Trade`` for ``position.id`` from
        ``closed_positions`` (so subsequent equity / PnL math stops counting
        the phantom fill), refunds ``proceeds`` from ``current_capital``,
        rolls back ``total_pnl``, and re-adds ``position`` to
        ``open_positions`` with ``pending_settlement=True``.

        Returns the popped phantom Trade so callers can rewrite the
        already-flushed CSV row in place.
        """
        phantom: Optional[Trade] = None
        for idx in range(len(self.closed_positions) - 1, -1, -1):
            if self.closed_positions[idx].id == position.id:
                phantom = self.closed_positions.pop(idx)
                break
        if phantom is not None:
            self.current_capital -= phantom.proceeds
            self.total_pnl -= phantom.pnl

        position.pending_settlement = True
        position.pending_settlement_reason = reason
        if not any(p.id == position.id for p in self.open_positions):
            self.open_positions.append(position)
        return phantom

    def reconcile_position(
        self,
        position: Position,
        actual_shares: float,
        estimated_exit_price: float,
        actual_cost_basis: Optional[float] = None,
        trade_result: Optional[TradeResult] = None,
    ) -> None:
        """Reconcile a position against actual Polymarket holdings.

        Handles two cases:
        - Full close (actual_shares <= 0): close position with given trade_result
          (defaults to MANUAL_CLOSE)
        - Shares mismatch: update shares and cost_basis (from trade history or
          proportional scaling). If shares decreased by > 0.01, a Trade record
          is created for the sold portion.

        Args:
            position: The local position to reconcile.
            actual_shares: Actual shares held on Polymarket.
            estimated_exit_price: Best bid or fallback price for P&L estimate.
            actual_cost_basis: If provided, use this as the true cost basis
                instead of proportional scaling. Computed from trade history.
            trade_result: Exit reason for the trade record. Defaults to
                MANUAL_CLOSE if not provided.
        """
        # Guard: if this position was deferred by close_position_tp /
        # _record_exit because /data/trades couldn't price the fill, do
        # NOT overwrite it here with a fabricated proceeds value (the
        # current ``estimated_exit_price`` is typically a stale best_bid
        # or position.entry_price). The background history reconciler
        # will upgrade the row once Polymarket indexing catches up.
        # On 2026-05-01, e312391d hit this exact path: TP truly filled
        # at $0.83 but reconcile_position recorded ``proceeds = shares ×
        # entry_price = $0`` because best_bid was stale on the expired
        # market — wallet truth was +$1.84.
        if (
            getattr(position, "pending_settlement", False)
            and getattr(position, "pending_settlement_reason", "") == "proceeds_unverified"
        ):
            print(
                f"[INFO] reconcile_position: skipping fabricated close for "
                f"{position.id} (pending_settlement=proceeds_unverified — "
                f"history reconciler will resolve)"
            )
            return

        if actual_shares <= 0:
            # Full close — position was sold entirely outside the bot
            proceeds = position.shares * estimated_exit_price
            pnl = proceeds - position.cost_basis
            pnl_pct = (pnl / position.cost_basis) * 100 if position.cost_basis > 0 else 0

            trade = Trade(
                id=position.id,
                direction=position.direction,
                shares=position.shares,
                entry_price=position.entry_price,
                exit_price=estimated_exit_price,
                cost_basis=position.cost_basis,
                proceeds=proceeds,
                pnl=pnl,
                pnl_pct=pnl_pct,
                result=trade_result or TradeResult.MANUAL_CLOSE,
                opened_at=position.opened_at,
                market_title=position.market_title,
                reference_price=position.reference_price,
                token_id=position.token_id,
            )
            self.close_position(position, trade)
        else:
            # Shares mismatch — update position
            shares_sold = position.shares - actual_shares

            if actual_cost_basis is not None:
                # Use real cost basis from trade history
                old_cost = position.cost_basis
                position.shares = actual_shares
                position.cost_basis = actual_cost_basis
                position.entry_price = actual_cost_basis / actual_shares
                sold_cost = old_cost - actual_cost_basis if old_cost > actual_cost_basis else 0
            else:
                # Fallback: scale proportionally (may be inaccurate if extra
                # shares were bought at a different price)
                ratio = actual_shares / position.shares
                sold_cost = position.cost_basis * (1 - ratio)
                position.shares = actual_shares
                position.cost_basis = position.cost_basis * ratio

            # Create a Trade record for the sold portion (partial close)
            if shares_sold > 0.01:
                proceeds = shares_sold * estimated_exit_price
                pnl = proceeds - sold_cost
                pnl_pct = (pnl / sold_cost) * 100 if sold_cost > 0 else 0

                partial_trade = Trade(
                    id=position.id + "-p",
                    direction=position.direction,
                    shares=shares_sold,
                    entry_price=position.entry_price if sold_cost <= 0 else sold_cost / shares_sold,
                    exit_price=estimated_exit_price,
                    cost_basis=sold_cost,
                    proceeds=proceeds,
                    pnl=pnl,
                    pnl_pct=pnl_pct,
                    result=trade_result or TradeResult.MANUAL_CLOSE,
                    opened_at=position.opened_at,
                    market_title=position.market_title,
                    reference_price=position.reference_price,
                    token_id=position.token_id,
                )
                self.closed_positions.append(partial_trade)
                self.current_capital += proceeds
                self.total_pnl += pnl
                self.log_trade(partial_trade)

        self.save()

    def set_market(self, market_data: dict) -> None:
        """Set the current market being tracked."""
        self.current_market = market_data

    def display_status(
        self,
        position_value_lookup: Optional[Callable[[Position], tuple[float, float]]] = None,
        live_equity: Optional[float] = None,
    ) -> None:
        """Print current state summary.

        Args:
            position_value_lookup: Optional function that takes a Position and returns
                (market_value, unrealized_pnl). If not provided, uses cost basis.
            live_equity: If provided (live mode), use as ground-truth equity.
                P&L is computed as live_equity - starting_capital.
        """
        # Calculate position values and unrealized P&L
        total_position_value = 0.0
        total_unrealized_pnl = 0.0
        position_values: dict[str, tuple[float, float]] = {}

        for pos in self.open_positions:
            if position_value_lookup:
                market_value, unrealized = position_value_lookup(pos)
            else:
                market_value, unrealized = pos.cost_basis, 0.0
            total_position_value += market_value
            total_unrealized_pnl += unrealized
            position_values[pos.id] = (market_value, unrealized)

        if live_equity is not None:
            equity = live_equity
            total_pnl = live_equity - self.config.get_starting_capital()
        else:
            equity = self.current_capital + total_position_value
            total_pnl = self.total_pnl + total_unrealized_pnl

        mode = "PAPER" if self.config.paper_mode else "LIVE TRADING"
        print("\n" + "=" * 50)
        print(f"           {mode} STATUS")
        print("=" * 50)
        print(f" Starting Capital: ${self.config.get_starting_capital():.2f}")

        if self.open_positions:
            print(f" Equity:           ${equity:.2f}")
            print(f"   Cash:           ${self.current_capital:.2f}")
            print(f"   Positions:      ${total_position_value:.2f}")
        else:
            print(f" Equity:           ${self.current_capital:.2f}")

        # Format P&L
        total_pnl_str = f"+${total_pnl:.2f}" if total_pnl >= 0 else f"-${abs(total_pnl):.2f}"
        if total_unrealized_pnl != 0:
            unrealized_str = f"+${total_unrealized_pnl:.2f}" if total_unrealized_pnl >= 0 else f"-${abs(total_unrealized_pnl):.2f}"
            print(f" Total P&L:        {total_pnl_str} ({unrealized_str} unrealized)")
        else:
            print(f" Total P&L:        {total_pnl_str}")

        print(f" Open Positions:   {len(self.open_positions)}")
        if self.pending_entry:
            direction = self.pending_entry.get("direction", "UNKNOWN")
            print(f" Pending Entry:    {direction} (order {self.pending_entry.get('order_id', 'unknown')})")
        print(f" Closed Trades:    {len(self.closed_positions)}")
        print(f" Edge Curve:       UP alpha={self.config.edge_alpha_up} floor={self.config.edge_floor_up:.0%} | DOWN alpha={self.config.edge_alpha_down} floor={self.config.edge_floor_down:.0%}")

        if self.in_grace_period():
            elapsed_secs = (
                datetime.now(timezone.utc) - self.last_tp_fill_time
            ).total_seconds()
            remaining_secs = self.config.grace_period_seconds - elapsed_secs
            print(f" Grace Period:     {remaining_secs:.0f}s remaining")

        if self.current_market:
            print("-" * 50)
            print(f" Market: {self.current_market.get('title', 'Unknown')}")
            print(f" Reference: ${self.current_market.get('reference_price', self.current_market.get('barrier_price', 0)):,.2f}")

        if self.open_positions:
            print("-" * 50)
            print(" Open Positions:")
            for pos in self.open_positions:
                market_value, unrealized = position_values.get(pos.id, (pos.cost_basis, 0.0))
                unrealized_str = f"+${unrealized:.2f}" if unrealized >= 0 else f"-${abs(unrealized):.2f}"
                print(f"   {pos.direction.value}: {pos.shares:.4f} @ ${pos.entry_price:.4f}")
                print(f"   TP: ${pos.tp_price:.4f} | Value: ${market_value:.2f} ({unrealized_str})")
                if pos.peak_price > pos.entry_price:
                    print(f"   Peak: ${pos.peak_price:.4f}")

        print("=" * 50 + "\n")
