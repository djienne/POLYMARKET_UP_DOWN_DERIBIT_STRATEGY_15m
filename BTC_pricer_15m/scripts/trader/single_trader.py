"""Single-instance trading logic for 15-min markets (paper and live).

Consumes TickData from SharedDataFeed instead of running subprocesses.
Uses seconds-based timing guards instead of hours.
"""

import csv
import json
import logging
import math
import os
import tempfile
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_DOWN
from pathlib import Path
from typing import Optional

from btc_pricer.edge import has_edge, required_model_prob

from scripts.polymarket_btc_15m import (
    get_binance_price_at,
    get_chainlink_price_at,
    get_current_btc_price,
)

from .config import TraderConfig
from .data_feed import TickData
from .executor import Executor
from .contracts import entry_inputs_valid, fee_for_fill
from .executor_paper import PaperExecutor
from .executor_live import LiveExecutor
from .market_orchestrator import MarketOrchestrator
from .mirror_control import (
    MIRROR_ACTION_EVENTS,
    MirrorTradeSignal,
    RedisMirrorSignalSubscriber,
    mirror_config_hash_from_trader,
    resolve_mirror_instance_from_paths,
)
from .order_execution import OrderExecutionService
from .order_book import OrderBookClient, log_orderbook_to_csv
from .persistence import SqliteStore
from . import clob_health
from .polymarket_status import (
    clob_reads_healthy,
    is_polymarket_operational,
    polymarket_status_label,
)
from .position import Direction, MicroPosition as Position, Trade, TradeResult
from .position_tracker import PositionTracker
from .reconciliation import ReconciliationService
from .risk_engine import RiskEngine
from .settlement import SettlementService
from .state import StateManager

log = logging.getLogger(__name__)


@dataclass
class EntryAttemptResult:
    """Structured result from execute_entry() with rejection diagnostics."""
    position: Optional["Position"] = None
    attempted: bool = False
    rejection_reason: str = ""
    rejection_details: dict = field(default_factory=dict)
    direction: str = ""
    token_id: str = ""
    signal_price: Optional[float] = None
    rechecked_price: Optional[float] = None
    order_size_usd: Optional[float] = None
    raw_order_size_usd: Optional[float] = None
    capital_before_entry: Optional[float] = None
    capital_source: str = ""
    order_id: str = ""
    ambiguous: bool = False
    order_metadata: dict = field(default_factory=dict)

    @property
    def entered(self) -> bool:
        return self.position is not None


_RETRYABLE_MIRROR_ENTRY_ERROR_TOKENS = (
    "post_only_mode",
    "post-only mode",
    "post only mode",
    "status_code=503",
    " 503",
    "service unavailable",
    "undermaintenance",
    "under maintenance",
    "maintenance",
    "temporarily unavailable",
    "timeout",
    "timed out",
    "connection reset",
    "connection aborted",
    "remote end closed",
    "max retries exceeded",
    "rate limit",
    "too many requests",
)

_RETRYABLE_MIRROR_ENTRY_REASONS = {
    "book_unavailable",
    "order_unfilled_timeout",
}

MIRROR_SIGNAL_AUDIT_HEADER = [
    "timestamp",
    "stage",
    "status",
    "reason",
    "signal_id",
    "event",
    "market_id",
    "direction",
    "source_instance_id",
    "stream_id",
    "attempt",
    "signal_age_s",
    "publish_to_receive_s",
    "receive_to_attempt_s",
    "publish_to_attempt_s",
    "trigger_bid",
    "trigger_ask",
    "trigger_mid",
    "dry_entry_price",
    "rechecked_price",
    "order_id",
    "live_position_id",
    "details",
]


def _emit(*args, sep: str = " ", end: str = "\n", flush: bool = False) -> None:
    """Route operational trader output through structured logging.

    Most of this file historically used ``print`` for live runtime output.
    Keeping a small print-compatible shim lets the existing call sites remain
    readable while making container logs level-filterable.
    """
    message = sep.join(str(arg) for arg in args)
    if end and end != "\n":
        message += end
    message = message.strip("\n")
    if not message:
        return

    for raw_line in message.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        upper = line.upper()
        if "CRITICAL" in upper or "MANUAL INTERVENTION" in upper:
            log.critical(line)
        elif "[ERROR]" in upper or upper.startswith("ERROR") or " ERROR:" in upper:
            log.error(line)
        elif "[WARN]" in upper or "WARNING" in upper or "FAILED" in upper or "ABORT" in upper:
            log.warning(line)
        elif "[DEBUG]" in upper:
            log.debug(line)
        else:
            log.info(line)


class ProceedsUnverifiedError(Exception):
    """Raised by ``_record_exit`` when on-chain proceeds cannot be verified
    in live mode and we refuse to fabricate a fill at the limit price.

    Callers should catch this and either retry on a future tick or mark
    the position ``pending_settlement`` so the eventual expiry resolution
    produces the correct PnL. The phantom-fill bug on 2026-04-30
    (trade ``4ca5f67b`` on market 1777591800) was caused by writing a
    profit using ``shares × exit_price`` after a ``NameError`` swallowed
    the proceeds query.
    """


class SingleTrader:
    """Trading logic for 15-min Polymarket Bitcoin Up/Down (paper or live)."""

    _TERMINAL_ORDER_STATUSES = {"cancelled", "canceled", "expired", "rejected", "failed"}
    _NONLIVE_SELL_STATUSES = _TERMINAL_ORDER_STATUSES | {"matched", "filled"}

    def __init__(self, config: TraderConfig, verbose: bool = False):
        self.config = config
        self.verbose = verbose
        self.state = StateManager(config)
        self.order_book_client = OrderBookClient()
        self._start_time: datetime = datetime.now(timezone.utc)
        self._last_heartbeat: Optional[datetime] = None
        self._last_reconciliation: Optional[datetime] = None
        self._last_history_reconcile: Optional[datetime] = None
        self._tp_status_failures: dict[str, int] = {}
        self._tp_backup_failures: dict[str, int] = {}
        self._recently_settled: dict[str, datetime] = {}
        self._last_edge_skip_reason: str = ""
        self._last_edge_skip_details: dict = {}
        self._mirror_instance_id: Optional[int] = None
        self._mirror_config_hash: str = ""
        self._mirror_subscriber: Optional[RedisMirrorSignalSubscriber] = None
        self._mirror_async_subscriber: Optional[RedisMirrorSignalSubscriber] = None
        self._mirror_action_lock = threading.RLock()
        self._mirror_listener_stop = threading.Event()
        self._mirror_listener_thread: Optional[threading.Thread] = None
        self._pending_async_mirror_entries: list[MirrorTradeSignal] = []
        self._mirror_entry_retry_ready_at: dict[str, datetime] = {}
        self._mirror_entry_retry_attempts: dict[str, int] = {}
        self._mirror_signal_received_at: dict[str, datetime] = {}
        self._mirror_signal_stream_ids: dict[str, str] = {}
        self._mirror_signal_received_audited: set[str] = set()
        self._mirror_ack_last_sent_at: dict[tuple[str, str, str], float] = {}
        self._last_mirror_pending_recover_at: Optional[datetime] = None
        self._last_mirror_heartbeat_check_at: Optional[datetime] = None
        self._last_mirror_heartbeat_warn_at: Optional[datetime] = None

        # Track current market for transition detection
        self._current_market_id: Optional[str] = None
        self._last_tick: Optional[TickData] = None
        self._last_tick_received_at: Optional[datetime] = None

        # Initialize executor based on config
        self.executor: Executor = self._create_executor()
        self.risk_engine = self._create_risk_engine()
        self.settlement_service = self._create_settlement_service()
        self.position_tracker = self._create_position_tracker()
        self.order_execution_service = self._create_order_execution_service()
        self.reconciliation_service = self._create_reconciliation_service()
        self._configure_mirror_control()

    def _create_executor(self) -> Executor:
        """Create the appropriate executor based on config."""
        if self.config.paper_mode:
            return PaperExecutor(self.order_book_client, config=self.config)

        # Live trading
        if not self.config.live_trading_enabled:
            raise ValueError(
                "Live trading is disabled. Set live_trading_enabled=true in config "
                "or use paper_mode=true for paper trading."
            )

        executor = LiveExecutor(
            signature_type=self.config.signature_type,
            max_slippage_pct=self.config.max_slippage_pct,
            config=self.config,
        )

        # Verify connection before proceeding
        success, message = executor.verify_connection()
        if not success:
            raise ConnectionError(f"Failed to connect to Polymarket: {message}")

        return executor

    def _create_risk_engine(self) -> RiskEngine:
        risk_event_logger = getattr(self.state, "log_risk_event", None)
        return RiskEngine(
            self.config,
            getattr(self, "order_book_client", None),
            log_fn=self.log,
            risk_event_logger=risk_event_logger,
        )

    def _get_risk_engine(self) -> RiskEngine:
        engine = getattr(self, "risk_engine", None)
        if engine is None:
            engine = self._create_risk_engine()
            self.risk_engine = engine
        return engine

    def _remember_recently_settled(self, token_id: str) -> None:
        self._recently_settled[token_id] = datetime.now(timezone.utc)

    def _create_market_orchestrator(self) -> Optional[MarketOrchestrator]:
        state_file = getattr(self.state, "state_file", None)
        if state_file is None:
            return None
        market_db_path = getattr(self.config, "market_db_path", None)
        sqlite_store = SqliteStore(market_db_path) if market_db_path else None
        return MarketOrchestrator(
            settlements_dir=state_file.parent / "settlements",
            sqlite_store=sqlite_store,
        )

    def _create_settlement_service(self) -> SettlementService:
        return SettlementService(
            self.config,
            self.executor,
            self.state,
            log_fn=self.log,
            close_tp_fn=self.close_position_tp,
            close_trail_fn=self._close_trail_filled,
            remember_recently_settled_fn=self._remember_recently_settled,
            market_orchestrator=self._create_market_orchestrator(),
            chainlink_price_at_fn=get_chainlink_price_at,
            binance_price_at_fn=get_binance_price_at,
        )

    def _get_settlement_service(self) -> SettlementService:
        service = getattr(self, "settlement_service", None)
        if service is None:
            service = self._create_settlement_service()
            self.settlement_service = service
        return service

    def _create_position_tracker(self) -> PositionTracker:
        return PositionTracker(
            check_fills_fn=self.check_tp_fills,
            verify_tp_orders_fn=self._verify_tp_orders,
            get_position_value_fn=self.get_open_position_value,
        )

    def _get_position_tracker(self) -> PositionTracker:
        service = getattr(self, "position_tracker", None)
        if service is None:
            service = self._create_position_tracker()
            self.position_tracker = service
        return service

    def _create_order_execution_service(self) -> OrderExecutionService:
        return OrderExecutionService(
            evaluate_edge_fn=self._evaluate_edge,
            execute_entry_fn=self.execute_entry,
            log_live_tick_diagnostics_fn=self._log_live_tick_diagnostics,
        )

    def _get_order_execution_service(self) -> OrderExecutionService:
        service = getattr(self, "order_execution_service", None)
        if service is None:
            service = self._create_order_execution_service()
            self.order_execution_service = service
        return service

    def _mirror_control_enabled(self) -> bool:
        return bool(
            self.executor.is_live
            and getattr(self.config, "mirror_signal_control_enabled", False)
        )

    def _configure_mirror_control(self) -> None:
        if not self._mirror_control_enabled():
            return
        self._mirror_config_hash = mirror_config_hash_from_trader(self.config)
        self._mirror_instance_id = resolve_mirror_instance_from_paths(
            live_config=self.config,
            requested=getattr(self.config, "mirror_instance_id", "auto"),
        )
        group = f"{getattr(self.config, 'redis_consumer_group', 'live')}.mirror"
        consumer = getattr(self.config, "redis_consumer_name", "live") or "live"
        self._mirror_subscriber = RedisMirrorSignalSubscriber(
            redis_url=self.config.redis_url,
            stream=getattr(self.config, "mirror_signal_stream", "trade.mirror.signals"),
            group=group,
            consumer=consumer,
            ack_stream=getattr(self.config, "mirror_ack_stream", "trade.mirror.acks"),
            ack_maxlen=getattr(self.config, "mirror_ack_stream_maxlen", 100_000),
        )

    def _sync_mirror_state_metadata(self) -> None:
        if not self._mirror_control_enabled():
            self.state.configure_mirror_control(
                enabled=False,
                instance_id=None,
                config_hash="",
            )
            return
        if self._mirror_instance_id is None:
            raise RuntimeError("mirror signal control is enabled but mirror instance was not resolved")
        reset_processed = False
        if self.state.open_positions:
            if (
                self.state.mirror_config_hash
                and self.state.mirror_config_hash != self._mirror_config_hash
            ):
                raise RuntimeError(
                    "mirror config hash changed while a live position is open; "
                    "refusing startup until the position is reconciled"
                )
            if (
                self.state.mirror_instance_id is not None
                and self.state.mirror_instance_id != self._mirror_instance_id
            ):
                raise RuntimeError(
                    "mirror instance changed while a live position is open; "
                    "refusing startup until the position is reconciled"
                )
        else:
            reset_processed = self.state.mirror_config_hash not in ("", self._mirror_config_hash)
        self.state.configure_mirror_control(
            enabled=True,
            instance_id=self._mirror_instance_id,
            config_hash=self._mirror_config_hash,
            reset_processed=reset_processed,
        )
        self.state.log_risk_event(
            kind="mirror_control_resolved",
            details={
                "mirror_instance_id": str(self._mirror_instance_id),
                "mirror_config_hash": self._mirror_config_hash,
                "local_entry_enabled": str(bool(getattr(self.config, "local_entry_enabled", True))).lower(),
            },
        )

    def _ensure_mirror_async_runtime(self) -> None:
        if not hasattr(self, "_mirror_action_lock"):
            self._mirror_action_lock = threading.RLock()
        if not hasattr(self, "_mirror_listener_stop"):
            self._mirror_listener_stop = threading.Event()
        if not hasattr(self, "_pending_async_mirror_entries"):
            self._pending_async_mirror_entries = []
        if not hasattr(self, "_mirror_entry_retry_ready_at"):
            self._mirror_entry_retry_ready_at = {}
        if not hasattr(self, "_mirror_entry_retry_attempts"):
            self._mirror_entry_retry_attempts = {}
        if not hasattr(self, "_mirror_signal_received_at"):
            self._mirror_signal_received_at = {}
        if not hasattr(self, "_mirror_signal_stream_ids"):
            self._mirror_signal_stream_ids = {}
        if not hasattr(self, "_mirror_signal_received_audited"):
            self._mirror_signal_received_audited = set()
        if not hasattr(self, "_mirror_ack_last_sent_at"):
            self._mirror_ack_last_sent_at = {}
        if not hasattr(self, "_last_mirror_pending_recover_at"):
            self._last_mirror_pending_recover_at = None
        if not hasattr(self, "_last_mirror_heartbeat_check_at"):
            self._last_mirror_heartbeat_check_at = None
        if not hasattr(self, "_last_mirror_heartbeat_warn_at"):
            self._last_mirror_heartbeat_warn_at = None
        if not hasattr(self, "_last_tick_received_at"):
            self._last_tick_received_at = None

    def _mirror_async_enabled(self) -> bool:
        return bool(
            self._mirror_control_enabled()
            and getattr(self.config, "mirror_signal_async_enabled", False)
        )

    @staticmethod
    def _mirror_signal_source_timestamp(signal: MirrorTradeSignal) -> Optional[datetime]:
        return signal.publish_timestamp or signal.dry_tick_timestamp

    def _mirror_signal_age_seconds(
        self,
        signal: MirrorTradeSignal,
        *,
        now: Optional[datetime] = None,
    ) -> Optional[float]:
        source_ts = self._mirror_signal_source_timestamp(signal)
        if source_ts is None:
            return None
        now = now or datetime.now(timezone.utc)
        return max(0.0, (now - source_ts).total_seconds())

    @staticmethod
    def _fmt_optional_float(value: Optional[float], digits: int = 3) -> str:
        if value is None:
            return ""
        try:
            return f"{float(value):.{digits}f}"
        except (TypeError, ValueError):
            return ""

    def _mirror_signal_latency_details(
        self,
        signal: MirrorTradeSignal,
        *,
        now: Optional[datetime] = None,
    ) -> dict[str, str]:
        now = now or datetime.now(timezone.utc)
        source_ts = self._mirror_signal_source_timestamp(signal)
        received_at = self._mirror_signal_received_at.get(signal.signal_id)
        details: dict[str, str] = {}
        if source_ts is not None:
            details["signal_age_s"] = f"{max(0.0, (now - source_ts).total_seconds()):.3f}"
            details["publish_to_attempt_s"] = details["signal_age_s"]
            if received_at is not None:
                details["publish_to_receive_s"] = (
                    f"{max(0.0, (received_at - source_ts).total_seconds()):.3f}"
                )
        if received_at is not None:
            details["receive_to_attempt_s"] = f"{max(0.0, (now - received_at).total_seconds()):.3f}"
        if signal.expires_at is not None:
            details["expires_at"] = signal.expires_at.isoformat()
        stream_id = self._mirror_signal_stream_ids.get(signal.signal_id)
        if stream_id:
            details["stream_id"] = stream_id
        return details

    def _append_mirror_audit_row(self, row: list[str]) -> None:
        paths = []
        for attr in ("mirror_signal_audit_file", "mirror_signal_audit_current_file"):
            path = getattr(self.config, attr, None)
            if path is not None and path not in paths:
                paths.append(path)
        for path in paths:
            try:
                path.parent.mkdir(parents=True, exist_ok=True)
                write_header = not path.exists() or path.stat().st_size == 0
                with open(path, "a", newline="", encoding="utf-8") as handle:
                    writer = csv.writer(handle)
                    if write_header:
                        writer.writerow(MIRROR_SIGNAL_AUDIT_HEADER)
                    writer.writerow(row)
            except Exception as exc:  # noqa: BLE001
                _emit(f"[WARN] mirror audit write failed for {path}: {exc}")

    def _log_mirror_signal_audit(
        self,
        signal: MirrorTradeSignal,
        *,
        stage: str,
        status: str = "",
        reason: str = "",
        details: Optional[dict] = None,
        attempt: Optional[int] = None,
        rechecked_price: Optional[float] = None,
        order_id: str = "",
        live_position_id: str = "",
    ) -> None:
        now = datetime.now(timezone.utc)
        latency = self._mirror_signal_latency_details(signal, now=now)
        detail_payload = dict(details or {})
        row = [
            now.isoformat(),
            stage,
            status,
            reason,
            signal.signal_id,
            signal.event,
            signal.market_id,
            signal.direction,
            str(signal.source_instance_id),
            self._mirror_signal_stream_ids.get(signal.signal_id, ""),
            "" if attempt is None else str(attempt),
            latency.get("signal_age_s", ""),
            latency.get("publish_to_receive_s", ""),
            latency.get("receive_to_attempt_s", ""),
            latency.get("publish_to_attempt_s", ""),
            self._fmt_optional_float(signal.trigger_bid, 4),
            self._fmt_optional_float(signal.trigger_ask, 4),
            self._fmt_optional_float(signal.trigger_mid, 4),
            self._fmt_optional_float(signal.dry_entry_price, 4),
            self._fmt_optional_float(rechecked_price, 4),
            order_id or "",
            live_position_id or "",
            json.dumps(detail_payload, sort_keys=True, default=str)[:1500],
        ]
        self._append_mirror_audit_row(row)

    def _note_mirror_signal_received(
        self,
        signal: MirrorTradeSignal,
        *,
        stream_id: str = "",
        source: str = "async",
    ) -> None:
        self._ensure_mirror_async_runtime()
        now = datetime.now(timezone.utc)
        self._mirror_signal_received_at.setdefault(signal.signal_id, now)
        if stream_id:
            self._mirror_signal_stream_ids[signal.signal_id] = stream_id
        if signal.signal_id in self._mirror_signal_received_audited:
            return
        self._mirror_signal_received_audited.add(signal.signal_id)
        self.state.record_mirror_signal_status(
            signal.signal_id,
            status="received",
            reason=source,
            stream_id=stream_id,
            event=signal.event,
            market_id=signal.market_id,
            direction=signal.direction,
            source_instance_id=str(signal.source_instance_id),
            details={"source": source},
        )
        if signal.event == "ENTRY":
            self._log_mirror_signal_audit(
                signal,
                stage="received",
                status="pending",
                reason=source,
            )

    def _fresh_tick_for_mirror_entry(self, signal: MirrorTradeSignal) -> Optional[TickData]:
        tick = getattr(self, "_last_tick", None)
        if tick is None:
            return None
        if signal.market_id and tick.market_id != signal.market_id:
            return None
        received_at = getattr(self, "_last_tick_received_at", None)
        if received_at is None:
            return None
        max_age = float(getattr(self.config, "mirror_signal_entry_tick_max_age_seconds", 5.0) or 0.0)
        if max_age > 0:
            age = (datetime.now(timezone.utc) - received_at).total_seconds()
            if age > max_age:
                return None
        return tick

    def _queue_async_mirror_entry(self, signal: MirrorTradeSignal) -> None:
        self._ensure_mirror_async_runtime()
        if self.state.has_processed_mirror_signal(signal.signal_id):
            self._ack_mirror_signal(signal, "duplicate")
            return
        pending = [
            item for item in self._pending_async_mirror_entries
            if item.signal_id != signal.signal_id
        ]
        pending.append(signal)
        self._pending_async_mirror_entries = pending[-25:]
        self.state.record_mirror_signal_status(
            signal.signal_id,
            status="queued",
            reason="waiting_for_fresh_tick",
            stream_id=self._mirror_signal_stream_ids.get(signal.signal_id, ""),
            event=signal.event,
            market_id=signal.market_id,
            direction=signal.direction,
            source_instance_id=str(signal.source_instance_id),
            details={"pending_count": len(self._pending_async_mirror_entries)},
        )
        self._log_mirror_signal_audit(
            signal,
            stage="queued",
            status="pending",
            reason="waiting_for_fresh_tick",
            details={"pending_count": len(self._pending_async_mirror_entries)},
        )
        self.state.log_risk_event(
            kind="mirror_entry_queued",
            details={
                "signal_id": signal.signal_id,
                "market_id": signal.market_id,
                "pending_count": len(self._pending_async_mirror_entries),
            },
        )

    def _queue_retryable_mirror_entry(
        self,
        signal: MirrorTradeSignal,
        *,
        reason: str,
        details: Optional[dict] = None,
    ) -> None:
        self._ensure_mirror_async_runtime()
        retry_delay = max(
            0.0,
            float(getattr(self.config, "mirror_signal_entry_retry_delay_seconds", 0.5) or 0.0),
        )
        ready_at = datetime.now(timezone.utc) + timedelta(seconds=retry_delay)
        self._mirror_entry_retry_ready_at[signal.signal_id] = ready_at
        attempts = int(self._mirror_entry_retry_attempts.get(signal.signal_id, 0) or 0) + 1
        self._mirror_entry_retry_attempts[signal.signal_id] = attempts
        pending = [
            item for item in self._pending_async_mirror_entries
            if item.signal_id != signal.signal_id
        ]
        pending.append(signal)
        self._pending_async_mirror_entries = pending[-25:]
        audit_details = dict(details or {})
        audit_details.update({
            "attempt": str(attempts),
            "retry_delay_s": f"{retry_delay:.3f}",
            "ready_at": ready_at.isoformat(),
        })
        self.state.record_mirror_signal_status(
            signal.signal_id,
            status="retryable",
            reason=reason,
            stream_id=self._mirror_signal_stream_ids.get(signal.signal_id, ""),
            event=signal.event,
            market_id=signal.market_id,
            direction=signal.direction,
            source_instance_id=str(signal.source_instance_id),
            attempts=attempts,
            details=audit_details,
        )
        self._log_mirror_signal_audit(
            signal,
            stage="retry_queued",
            status="retryable",
            reason=reason,
            details=audit_details,
            attempt=attempts,
        )
        self.state.log_risk_event(
            kind="mirror_entry_retry_queued",
            details={
                "signal_id": signal.signal_id,
                "market_id": signal.market_id,
                "reason": reason,
                "attempt": str(attempts),
                "retry_delay_s": f"{retry_delay:.3f}",
                "ready_at": ready_at.isoformat(),
                "expires_at": signal.expires_at.isoformat() if signal.expires_at else "",
                "details": json.dumps(details or {}, sort_keys=True)[:240],
            },
        )

    def _clear_mirror_entry_retry_state(self, signal_id: str) -> None:
        if not signal_id:
            return
        self._ensure_mirror_async_runtime()
        self._mirror_entry_retry_ready_at.pop(signal_id, None)
        self._mirror_entry_retry_attempts.pop(signal_id, None)
        self._pending_async_mirror_entries = [
            item for item in self._pending_async_mirror_entries
            if item.signal_id != signal_id
        ]

    @staticmethod
    def _mirror_exit_matches_pending_entry(
        exit_signal: MirrorTradeSignal,
        entry_signal: MirrorTradeSignal,
    ) -> bool:
        if entry_signal.event != "ENTRY":
            return False
        if str(entry_signal.source_instance_id) != str(exit_signal.source_instance_id):
            return False
        if str(entry_signal.source_config_hash or "") != str(exit_signal.source_config_hash or ""):
            return False
        if not entry_signal.market_id or not exit_signal.market_id:
            return False
        if entry_signal.market_id != exit_signal.market_id:
            return False
        if not entry_signal.direction or not exit_signal.direction:
            return False
        return entry_signal.direction == exit_signal.direction

    def _cancel_pending_mirror_entry_retries_for_exit(self, signal: MirrorTradeSignal) -> int:
        if signal.event not in {"TP_FILLED", "STOP_LOSS"}:
            return 0
        if self._position_for_mirror_signal(signal) is not None:
            return 0
        self._ensure_mirror_async_runtime()
        matching_entries = [
            pending
            for pending in self._pending_async_mirror_entries
            if self._mirror_exit_matches_pending_entry(signal, pending)
        ]
        for entry_signal in matching_entries:
            self._clear_mirror_entry_retry_state(entry_signal.signal_id)
            self.state.mark_mirror_signal_processed(
                entry_signal.signal_id,
                status="cancelled",
                reason="entry_cancelled_by_dry_exit",
                stream_id=self._mirror_signal_stream_ids.get(entry_signal.signal_id, ""),
                event=entry_signal.event,
                market_id=entry_signal.market_id,
                direction=entry_signal.direction,
                source_instance_id=str(entry_signal.source_instance_id),
                details={"exit_signal_id": signal.signal_id, "exit_event": signal.event},
            )
            self._ack_mirror_delivery(entry_signal)
            self._ack_mirror_signal(
                entry_signal,
                "rejected",
                "entry_cancelled_by_dry_exit",
            )
            self._log_mirror_signal_audit(
                entry_signal,
                stage="cancelled_by_dry_exit",
                status="rejected",
                reason="entry_cancelled_by_dry_exit",
                details={"exit_signal_id": signal.signal_id, "exit_event": signal.event},
            )
            self.state.log_risk_event(
                kind="mirror_entry_retry_cancelled_by_exit",
                details={
                    "entry_signal_id": entry_signal.signal_id,
                    "exit_signal_id": signal.signal_id,
                    "exit_event": signal.event,
                    "market_id": signal.market_id,
                    "direction": signal.direction,
                    "source_instance_id": signal.source_instance_id,
                },
            )
        return len(matching_entries)

    def _process_async_mirror_signal_unlocked(
        self,
        signal: MirrorTradeSignal,
        *,
        stream_id: str = "",
    ) -> None:
        if signal.event == "ENTRY":
            valid, reason = self._validate_mirror_signal(signal)
            if not valid:
                self._process_mirror_signal(signal, None, stream_id=stream_id)
                return
            tick = self._fresh_tick_for_mirror_entry(signal)
            if tick is None:
                self._queue_async_mirror_entry(signal)
                return
            self._process_mirror_signal(signal, tick, stream_id=stream_id)
            return
        self._process_mirror_signal(signal, None, stream_id=stream_id)

    def _process_async_mirror_signal(
        self,
        signal: MirrorTradeSignal,
        *,
        stream_id: str = "",
    ) -> None:
        if not self._mirror_async_enabled():
            return
        self._ensure_mirror_async_runtime()
        start = time.monotonic()
        self._mirror_action_lock.acquire()
        waited = time.monotonic() - start
        try:
            warn_after = float(getattr(self.config, "mirror_signal_lock_warn_seconds", 1.0) or 0.0)
            if warn_after > 0 and waited > warn_after:
                self.state.log_risk_event(
                    kind="mirror_signal_lock_wait",
                    details={
                        "signal_id": signal.signal_id,
                        "event": signal.event,
                        "wait_seconds": f"{waited:.3f}",
                    },
                )
            self._process_async_mirror_signal_unlocked(signal, stream_id=stream_id)
        finally:
            self._mirror_action_lock.release()

    def _drain_pending_async_mirror_entries(self, tick: TickData) -> None:
        self._ensure_mirror_async_runtime()
        if not self._pending_async_mirror_entries:
            return
        remaining: list[MirrorTradeSignal] = []
        now = datetime.now(timezone.utc)
        for signal in self._pending_async_mirror_entries:
            valid, _reason = self._validate_mirror_signal(signal)
            if not valid:
                self._process_mirror_signal(signal, tick)
                continue
            if signal.market_id and signal.market_id != tick.market_id:
                remaining.append(signal)
                continue
            retry_ready_at = self._mirror_entry_retry_ready_at.get(signal.signal_id)
            if retry_ready_at is not None and now < retry_ready_at:
                remaining.append(signal)
                continue
            self._process_mirror_signal(signal, tick)
            if (
                signal.signal_id in self._mirror_entry_retry_ready_at
                and not self.state.has_processed_mirror_signal(signal.signal_id)
            ):
                remaining.append(signal)
        self._pending_async_mirror_entries = remaining

    def _start_mirror_signal_listener(self) -> None:
        if not self._mirror_async_enabled():
            return
        self._ensure_mirror_async_runtime()
        if getattr(self, "_mirror_listener_thread", None) is not None:
            return
        consumer = f"{getattr(self.config, 'redis_consumer_name', 'live') or 'live'}-mirror-async"
        group = f"{getattr(self.config, 'redis_consumer_group', 'live')}.mirror"
        try:
            self._mirror_async_subscriber = RedisMirrorSignalSubscriber(
                redis_url=self.config.redis_url,
                stream=getattr(self.config, "mirror_signal_stream", "trade.mirror.signals"),
                group=group,
                consumer=consumer,
                ack_stream=getattr(self.config, "mirror_ack_stream", "trade.mirror.acks"),
                ack_maxlen=getattr(self.config, "mirror_ack_stream_maxlen", 100_000),
                block_ms=getattr(self.config, "mirror_signal_async_block_ms", 250),
            )
        except Exception as exc:  # noqa: BLE001
            self.state.log_risk_event(
                kind="mirror_signal_listener_failed",
                details={"stage": "start", "error": str(exc)[:160]},
            )
            _emit(f"[WARN] mirror async signal listener failed to start: {exc}")
            return
        self._mirror_listener_stop.clear()
        thread = threading.Thread(
            target=self._mirror_signal_listener_loop,
            name="mirror-signal-listener",
            daemon=True,
        )
        self._mirror_listener_thread = thread
        thread.start()
        _emit("Mirror async signal listener started")

    def _mirror_signal_listener_loop(self) -> None:
        subscriber = getattr(self, "_mirror_async_subscriber", None)
        if subscriber is None:
            return
        block_ms = int(getattr(self.config, "mirror_signal_async_block_ms", 250) or 0)
        while not self._mirror_listener_stop.is_set():
            try:
                messages = subscriber.read_new(block_ms=block_ms, count=50)
                for message_id, signal in messages:
                    if signal is None or self._mirror_listener_stop.is_set():
                        if signal is None:
                            subscriber.ack_message(message_id)
                        continue
                    self._note_mirror_signal_received(signal, stream_id=message_id, source="async")
                    self._process_async_mirror_signal(signal, stream_id=message_id)
            except Exception as exc:  # noqa: BLE001
                self.state.log_risk_event(
                    kind="mirror_signal_listener_failed",
                    details={"stage": "read_or_process", "error": str(exc)[:160]},
                )
                time.sleep(1.0)

    def _stop_mirror_signal_listener(self) -> None:
        self._ensure_mirror_async_runtime()
        self._mirror_listener_stop.set()
        thread = getattr(self, "_mirror_listener_thread", None)
        if thread is not None and thread.is_alive():
            thread.join(timeout=2.0)
        self._mirror_listener_thread = None
        subscriber = getattr(self, "_mirror_async_subscriber", None)
        if subscriber is not None:
            subscriber.close()
        self._mirror_async_subscriber = None

    def _create_reconciliation_service(self) -> ReconciliationService:
        return ReconciliationService(
            reconcile_positions_fn=self._reconcile_positions,
            detect_orphans_fn=self._detect_orphan_positions,
            reconcile_pending_entry_fn=self._reconcile_pending_entry,
            maybe_run_history_reconciler_fn=self._maybe_run_history_reconciler,
            interval_seconds_fn=lambda: self.config.reconciliation_interval_seconds,
            get_last_reconciliation_fn=lambda: self._last_reconciliation,
            set_last_reconciliation_fn=self._set_last_reconciliation,
        )

    def _get_reconciliation_service(self) -> ReconciliationService:
        service = getattr(self, "reconciliation_service", None)
        if service is None:
            service = self._create_reconciliation_service()
            self.reconciliation_service = service
        return service

    def _set_last_reconciliation(self, value: datetime) -> None:
        self._last_reconciliation = value

    def _get_live_equity(self) -> Optional[float]:
        """Compute ground-truth equity from Polymarket (live mode only).

        Returns cash + open position market value, or None if not live
        or if the balance query fails.
        """
        if not self.executor.is_live:
            return None

        cash = self.executor.get_usdc_balance()
        if cash is None:
            return None

        total_open_value = 0.0
        for position in self.state.open_positions:
            market_value, _ = self.get_open_position_value(position)
            total_open_value += market_value

        return cash + total_open_value

    @staticmethod
    def _market_end_time_from_id(market_id: str) -> Optional[datetime]:
        return SettlementService.market_end_time_from_id(market_id)

    def _market_data_from_tick(self, tick: TickData) -> dict:
        return {
            "market_id": tick.market_id,
            "title": tick.market_title,
            "reference_price": tick.barrier,
            "clob_token_ids": {"UP": tick.up_token_id, "DOWN": tick.down_token_id},
        }

    def _sync_current_market_from_tick(self, tick: TickData) -> None:
        if not tick.market_id:
            return
        market_data = self._market_data_from_tick(tick)
        if self.state.current_market != market_data:
            self.state.set_market(market_data)
            self.state.save()
        self._register_market_tokens(market_data)

    def _mark_pending_settlement(self, position: Position, reason: str) -> None:
        self._get_settlement_service().mark_pending(position, reason)

    def _has_pending_settlement(self) -> bool:
        return self._get_settlement_service().has_pending()

    def _resolve_position_settlement_file(self, position: Position) -> tuple[Optional[float], str]:
        return self._get_settlement_service().resolve_settlement_file(position)

    def _resolve_position_final_spot(self, position: Position) -> tuple[Optional[float], str]:
        return self._get_settlement_service().resolve_final_spot(position)

    def _confirm_live_orders_inactive_for_resolution(self, position: Position) -> bool:
        return self._get_settlement_service().confirm_live_orders_inactive_for_resolution(position)

    def _confirm_live_trading(self) -> bool:
        """Prompt user to confirm live trading. Returns True if confirmed."""
        if self.config.paper_mode or not self.config.require_confirmation:
            return True

        _emit("\n" + "=" * 60)
        _emit("WARNING: LIVE TRADING MODE")
        _emit("=" * 60)
        _emit("This will place REAL orders with REAL money on Polymarket.")
        _emit(f"  Order size: {self.config.order_size_pct * 100:.0f}% of capital")
        _emit("  Slippage: No limit (FOK market orders)")
        _emit("=" * 60)
        _emit("\nType CONFIRM to proceed, or anything else to abort: ", end="")

        try:
            response = input().strip()
            if response == "CONFIRM":
                _emit("Live trading confirmed.\n")
                return True
            else:
                _emit("Aborted. Use paper_mode=true for paper trading.")
                return False
        except EOFError:
            # Non-interactive mode (e.g., running in background)
            _emit("\nNon-interactive mode detected. Set require_confirmation=false in config.")
            return False

    def log(self, message: str) -> None:
        """Log a message if verbose mode is enabled."""
        if self.verbose:
            timestamp = datetime.now().strftime("%H:%M:%S")
            _emit(f"[{timestamp}] {message}")

    def _format_uptime(self) -> str:
        """Return human-readable uptime string (e.g., '2h 15m 30s')."""
        elapsed = datetime.now(timezone.utc) - self._start_time
        total_seconds = int(elapsed.total_seconds())

        hours, remainder = divmod(total_seconds, 3600)
        minutes, seconds = divmod(remainder, 60)

        parts = []
        if hours > 0:
            parts.append(f"{hours}h")
        if minutes > 0 or hours > 0:
            parts.append(f"{minutes}m")
        parts.append(f"{seconds}s")

        return " ".join(parts)

    def get_open_position_value(self, position: Position) -> tuple[float, float]:
        """Get current market value and unrealized P&L for a position."""
        order_book = self.order_book_client.fetch_order_book(position.token_id)
        if not order_book or order_book.best_bid <= 0:
            # Fallback to cost basis if can't get price
            return position.cost_basis, 0.0

        market_value = position.shares * order_book.best_bid
        unrealized_pnl = market_value - position.cost_basis
        return market_value, unrealized_pnl

    def _log_orderbook_snapshot(self) -> None:
        """Log orderbook best bid/ask for current market tokens."""
        if not self.config.log_orderbook:
            return

        market = self.state.current_market
        if not market:
            return

        clob_tokens = market.get("clob_token_ids", {})
        for direction, token_id in clob_tokens.items():
            if not token_id:
                continue

            # Skip placeholder/test token IDs (valid CLOB tokens are long numeric strings)
            if len(token_id) < 20:
                continue

            order_book = self.order_book_client.fetch_order_book(token_id)
            if not order_book:
                continue

            best_bid = order_book.best_bid
            best_ask = order_book.best_ask
            spread = best_ask - best_bid if (best_bid > 0 and best_ask > 0) else 0.0
            sanity_ok = best_bid < best_ask if (best_bid > 0 and best_ask > 0) else True

            log_orderbook_to_csv(
                token_id=f"{direction}:{token_id[:8]}",  # Label with direction
                best_bid=best_bid,
                best_ask=best_ask,
                spread=spread,
                bid_depth=len(order_book.bids),
                ask_depth=len(order_book.asks),
                csv_path=self.config.orderbook_file,
                sanity_ok=sanity_ok,
                reference_price=market.get("reference_price"),
            )

    def _verify_tp_orders(self) -> None:
        """Verify TP/trail orders exist for all open positions, place if missing."""
        if not self.executor.is_live:
            return  # Only needed for live trading

        for position in self.state.open_positions:
            # Trail order takes precedence over TP order
            if position.trail_order_id:
                trail_status = self.executor.get_order_status(position.trail_order_id)
                if trail_status is None:
                    self._place_trail_order(position, position.trail_order_price,
                                            reason="trail order not found")
                elif trail_status.get("status", "").lower() in ("cancelled", "expired"):
                    self._place_trail_order(position, position.trail_order_price,
                                            reason=f"trail {trail_status.get('status')}")
                elif trail_status.get("status", "").lower() == "matched":
                    size_matched = trail_status.get("size_matched", 0)
                    if size_matched >= position.shares * 0.99:
                        exit_price = trail_status.get("price", position.trail_order_price)
                        timestamp_now = datetime.now().strftime("%H:%M:%S")
                        _emit(f"\n[{timestamp_now}] TP VERIFY: Detected filled trail order for "
                              f"{position.direction.value} position")
                        self._close_trail_filled(position, exit_price)
                continue

            # Case 1: No TP order ID stored — try to adopt existing sell order first
            if not position.tp_order_id:
                adopted = self._adopt_existing_tp_order(position)
                if not adopted:
                    self._place_backup_tp_order(position, reason="no tp_order_id")
                continue

            # Case 2: TP order ID exists - verify it's actually on the book
            order_status = self.executor.get_order_status(position.tp_order_id)

            if order_status is None:
                # Order not found at all
                self._place_backup_tp_order(position, reason="order not found")
            elif order_status.get("status", "").lower() in ("cancelled", "expired"):
                # Order was cancelled/expired
                self._place_backup_tp_order(position, reason=f"status={order_status.get('status')}")
            elif order_status.get("status", "").lower() == "matched":
                # Order filled - handle immediately instead of deferring
                size_matched = order_status.get("size_matched", 0)
                if size_matched >= position.shares * 0.99:
                    exit_price = order_status.get("price", position.tp_price)
                    timestamp_now = datetime.now().strftime("%H:%M:%S")
                    _emit(f"\n[{timestamp_now}] TP VERIFY: Detected filled TP order for "
                          f"{position.direction.value} position")
                    self.close_position_tp(position, exit_price)
            # Status is 'live' or 'open' - TP order is active, nothing to do

    def _adopt_existing_tp_order(self, position: Position) -> bool:
        """Try to adopt an existing SELL order on the book for this token.

        When the bot restarts without a stored tp_order_id, there may already be
        a TP sell order placed by a previous session.  Adopting it avoids
        a duplicate "not enough balance" failure.

        Validates that the order's size and price are reasonable before adopting:
        - Size must be >= 90% of position shares (allows for rounding)
        - Price must be above entry price (it's a take-profit, not a dump)

        Returns:
            True if an existing order was adopted, False otherwise.
        """
        if not hasattr(self.executor, "get_open_orders"):
            return False

        timestamp = datetime.now().strftime("%H:%M:%S")
        try:
            open_orders = self.executor.get_open_orders()
        except Exception as exc:
            log.debug("TP adopt open-order lookup failed: %s", exc)
            return False

        for order in open_orders:
            if (order.get("token_id") == position.token_id
                    and order.get("side", "").upper() == "SELL"):
                order_id = order.get("order_id", "")
                price = order.get("price", 0.0)
                size = order.get("size", 0.0)

                # Validate size: must cover most of the position
                if size < position.shares * 0.90:
                    _emit(f"\n[{timestamp}] TP ADOPT: Skipping SELL order {order_id} — "
                          f"size {size:.4f} too small for position {position.shares:.4f}")
                    continue

                # Validate price: must be above entry (it's a TP order)
                if price <= position.entry_price:
                    _emit(f"\n[{timestamp}] TP ADOPT: Skipping SELL order {order_id} — "
                          f"price ${price:.4f} <= entry ${position.entry_price:.4f}")
                    continue

                position.tp_order_id = order_id
                position.tp_placed_at = position.tp_placed_at or datetime.now(timezone.utc)
                position.tp_price = price
                self.state.save()
                _emit(f"\n[{timestamp}] TP ADOPT: Found existing SELL order for "
                      f"{position.direction.value} position")
                _emit(f"  Order ID: {order_id}")
                _emit(f"  Price: ${price:.4f} | Size: {size:.4f}")
                return True

        return False

    def _place_backup_tp_order(self, position: Position, reason: str) -> None:
        """Place a backup TP order for a position missing one.

        Cancels any existing TP order first to prevent duplicates.
        Applies cooldown after 3 consecutive failures (retry every 5th heartbeat).

        Args:
            position: The open position needing a TP order.
            reason: Why the backup is needed (for logging).
        """
        timestamp = datetime.now().strftime("%H:%M:%S")

        # Cooldown: after 3 failures, only retry every 5th heartbeat
        if position.payoff_kind != "legacy" and position.shares < 5.0:
            self._mark_pending_settlement(position, "partial_fill_below_exit_minimum")
            return
        fail_count = self._tp_backup_failures.get(position.id, 0)
        if fail_count >= 3:
            # Allow retry every 5th attempt (cooldown ~15 min at 60s heartbeat)
            self._tp_backup_failures[position.id] = fail_count + 1
            if (fail_count - 3) % 5 != 0:
                return
            _emit(f"\n[{timestamp}] BACKUP TP: Retrying after {fail_count} failures (cooldown)")

        _emit(f"\n[{timestamp}] BACKUP TP: Placing TP order ({reason})")
        _emit(f"  Position: {position.direction.value} {position.shares:.4f} shares")
        _emit(f"  TP Price: ${position.tp_price:.4f}")

        # Cancel existing TP order first to prevent double-selling.
        if position.tp_order_id:
            cancelled = self.executor.cancel_order(position.tp_order_id)
            if cancelled:
                _emit(f"  Cancelled old TP order {position.tp_order_id}")
                position.tp_order_id = None
            else:
                status_name = self._order_status_name(position.tp_order_id)
                if status_name not in self._NONLIVE_SELL_STATUSES:
                    self.state.log_risk_event(
                        kind="backup_tp_cancel_unconfirmed",
                        details={
                            "position_id": position.id,
                            "order_id": position.tp_order_id,
                            "token_id": position.token_id,
                            "status_seen": status_name,
                            "reason": reason,
                        },
                    )
                    _emit(
                        f"  ABORT: old TP order {position.tp_order_id} cancel "
                        f"unconfirmed (status={status_name or 'unknown'}); "
                        "not placing replacement"
                    )
                    self.state.save()
                    return
                _emit(f"  Old TP order {position.tp_order_id} is terminal ({status_name or 'unknown'})")
                position.tp_order_id = None

        try:
            live_sell = self._first_open_sell_order_for_token(position.token_id)
        except RuntimeError as exc:
            self.state.log_risk_event(
                kind="backup_tp_duplicate_sell_blocked",
                details={
                    "position_id": position.id,
                    "token_id": position.token_id,
                    "reason": "open_order_check_failed",
                    "error": str(exc)[:160],
                },
            )
            _emit(f"  ABORT: could not verify open SELL orders: {exc}")
            self.state.save()
            return
        if live_sell:
            self.state.log_risk_event(
                kind="backup_tp_duplicate_sell_blocked",
                details={
                    "position_id": position.id,
                    "token_id": position.token_id,
                    "existing_order_id": str(live_sell.get("order_id") or ""),
                    "reason": reason,
                },
            )
            _emit(
                "  ABORT: existing live SELL order "
                f"{live_sell.get('order_id') or ''} found for token; "
                "not placing backup TP"
            )
            self.state.save()
            return

        # Get actual position to determine shares to sell
        try:
            actual_shares = self.executor.get_position(position.token_id, strict=True)
        except RuntimeError as exc:
            self.state.log_risk_event(
                kind="backup_tp_balance_unknown",
                details={
                    "position_id": position.id,
                    "token_id": position.token_id,
                    "reason": reason,
                    "error": str(exc)[:160],
                },
            )
            _emit(f"  ABORT: could not verify live token balance: {exc}")
            self.state.save()
            return

        if actual_shares <= 0.01:
            _emit("  ABORT: no live shares available for backup TP")
            self.state.save()
            return

        # Round down to avoid "not enough balance" errors
        tp_shares = math.floor(actual_shares * 100) / 100
        if tp_shares < getattr(self.config, "min_position_shares", 5.0):
            self.state.log_risk_event(
                kind="backup_tp_balance_unknown",
                details={
                    "position_id": position.id,
                    "token_id": position.token_id,
                    "reason": "shares_below_sell_minimum",
                    "shares": f"{tp_shares:.4f}",
                },
            )
            _emit(f"  ABORT: backup TP shares below sell minimum ({tp_shares:.4f})")
            self.state.save()
            return

        # Cap TP price at $0.99 for binary options
        tp_sell_price = min(position.tp_price, 0.99)
        tp_result = self.executor.sell_limit(position.token_id, tp_sell_price, tp_shares)

        if tp_result.success:
            position.tp_order_id = tp_result.order_id
            position.tp_placed_at = datetime.now(timezone.utc)
            self._tp_backup_failures.pop(position.id, None)  # Reset on success
            self.state.save()
            _emit(f"  SUCCESS: TP order placed (ID: {tp_result.order_id})")
        else:
            # Clear dead TP order ID to prevent verify loop re-triggering on same ID
            position.tp_order_id = None
            self._tp_backup_failures[position.id] = fail_count + 1
            self.state.save()
            _emit(f"  FAILED: {tp_result.error_message}")

    def _order_status_name(self, order_id: str) -> str:
        try:
            status = self.executor.get_order_status(order_id)
        except Exception as exc:
            log.debug("order status lookup failed for %s: %s", order_id, exc)
            return ""
        return str((status or {}).get("status", "") or "").lower()

    def _first_open_sell_order_for_token(self, token_id: str) -> Optional[dict]:
        try:
            orders = self.executor.get_open_orders()
        except Exception as exc:
            raise RuntimeError(f"open-order lookup failed: {exc}") from exc
        for order in orders or []:
            side = str(order.get("side", "") or "").upper()
            order_token = str(order.get("token_id", "") or "")
            if side == "SELL" and order_token == token_id:
                return order
        return None

    def _live_entry_fill_is_viable(self, shares: float, total_cost: float) -> bool:
        min_shares = float(getattr(self.config, "min_position_shares", 0.0) or 0.0)
        min_notional = float(getattr(self.config, "min_order_size_usd", 0.0) or 0.0)
        share_tolerance = 0.01
        notional_tolerance = 0.01
        return (
            shares + share_tolerance + 1e-9 >= min_shares
            and total_cost + notional_tolerance + 1e-9 >= min_notional
        )

    @staticmethod
    def _safe_pending_float(pending: dict, *keys: str) -> float:
        for key in keys:
            value = pending.get(key)
            if value is None or value == "":
                continue
            try:
                return float(value)
            except (TypeError, ValueError):
                continue
        return 0.0

    def _note_manual_pending_market_transition(self, pending: dict) -> None:
        """A new market cannot establish that an uncertain previous order is gone."""
        pending_market_id = str(pending.get("market_id") or "")
        current_market_id = str((self.state.current_market or {}).get("market_id") or "")
        if not pending_market_id or not current_market_id or pending_market_id == current_market_id:
            return
        if pending.get("market_transition_seen") == current_market_id:
            return

        token_id = str(pending.get("token_id") or "")
        if token_id:
            self.state.known_token_ids.add(token_id)
        self.state.log_risk_event(
            kind="manual_pending_entry_preserved_after_market_transition",
            details={
                "order_id": str(pending.get("order_id") or ""),
                "token_id": token_id,
                "pending_market_id": pending_market_id,
                "current_market_id": current_market_id,
                "reason": str(pending.get("reason") or ""),
                "shares": f"{self._safe_pending_float(pending, 'shares', 'shares_filled'):.4f}",
                "total_cost": f"{self._safe_pending_float(pending, 'total_cost'):.4f}",
            },
        )
        pending["market_transition_seen"] = current_market_id
        self.state.save()
        _emit(
            "[WARN] Preserving manual pending entry from previous market "
            f"{pending_market_id}; exposure still requires reconciliation"
        )
    def _reconcile_manual_pending_entry(self, pending: dict) -> None:
        self._note_manual_pending_market_transition(pending)

        if pending.get("reason") != "entry_partial_below_minimum":
            return

        token_id = str(pending.get("token_id") or "")
        if not token_id:
            return

        order_id = pending.get("order_id")
        order_status = None
        if order_id:
            try:
                order_status = self.executor.get_order_status(order_id)
            except Exception as exc:
                log.debug("manual pending entry order status lookup failed for %s: %s", order_id, exc)
                order_status = None

        actual_shares = None
        try:
            actual_shares = self.executor.get_position(token_id, strict=True)
        except RuntimeError:
            actual_shares = None

        if actual_shares is None or actual_shares <= 0.01:
            return
        shares = (
            float(actual_shares)
            if actual_shares is not None and actual_shares > 0.01
            else self._safe_pending_float(pending, "shares", "shares_filled")
        )
        fill_price = self._safe_pending_float(pending, "fill_price", "avg_fill_price")
        total_cost = self._safe_pending_float(pending, "total_cost")
        if total_cost <= 0 and shares > 0 and fill_price > 0:
            total_cost = shares * fill_price

        if shares <= 0.01 or not self._live_entry_fill_is_viable(shares, total_cost):
            return

        pending["manual_intervention_required"] = False
        pending["reason"] = "entry_partial_recovered"
        pending["shares"] = shares
        pending["shares_filled"] = shares
        pending["total_cost"] = total_cost
        if fill_price > 0:
            pending["fill_price"] = fill_price
            pending["avg_fill_price"] = fill_price
        self.state.log_risk_event(
            kind="manual_pending_entry_adopted_after_near_minimum_fill",
            details={
                "order_id": str(order_id or ""),
                "token_id": token_id,
                "market_id": str(pending.get("market_id") or ""),
                "shares": f"{shares:.4f}",
                "total_cost": f"{total_cost:.4f}",
                "min_order_size_usd": f"{float(self.config.min_order_size_usd):.4f}",
            },
        )
        self.state.pending_entry = pending
        self._adopt_pending_entry(pending, order_status, shares)

    def _mark_entry_partial_below_minimum(
        self,
        *,
        order_id: Optional[str],
        token_id: str,
        direction: str,
        market_id: str,
        market_title: str,
        reference_price: float,
        shares: float,
        fill_price: float,
        total_cost: float,
        order_metadata: Optional[dict] = None,
    ) -> None:
        details = {
            "order_id": order_id or "",
            "token_id": token_id,
            "direction": direction,
            "market_id": market_id,
            "shares": f"{shares:.4f}",
            "avg_fill_price": f"{fill_price:.4f}",
            "total_cost": f"{total_cost:.4f}",
            "min_position_shares": f"{float(self.config.min_position_shares):.4f}",
            "min_order_size_usd": f"{float(self.config.min_order_size_usd):.4f}",
        }
        self.state.log_risk_event(kind="entry_partial_below_minimum", details=details)
        self.state.pending_entry = {
            "manual_intervention_required": True,
            "reason": "entry_partial_below_minimum",
            "order_id": order_id,
            "token_id": token_id,
            "direction": direction,
            "market_id": market_id,
            "market_title": market_title,
            "reference_price": reference_price,
            "shares": shares,
            "shares_filled": shares,
            "fill_price": fill_price,
            "avg_fill_price": fill_price,
            "total_cost": total_cost,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "order_metadata": order_metadata or {},
        }
        self.state.known_token_ids.add(token_id)
        self.state.save()

    def _reconcile_positions(self) -> None:
        """Reconcile local positions against actual Polymarket holdings.

        For each open position, checks actual shares on Polymarket:
        - API failure (RuntimeError): skip, don't assume position is gone
        - Position fully gone (0 shares): close as MANUAL_CLOSE, cancel orphaned TP
        - Partial close (shares mismatch): scale position down, re-place TP order
        - Shares match: no action

        Only runs in live mode — paper mode has no external state to reconcile.
        """
        if not self.executor.is_live:
            return

        timestamp = datetime.now().strftime("%H:%M:%S")

        for position in self.state.open_positions[:]:  # iterate over copy
            # Handle pending SL limit orders first
            if position.pending_sl_order_id:
                if self._handle_pending_sl_for_closing_window(position, timestamp=timestamp):
                    continue
                sl_status = self.executor.get_order_status(position.pending_sl_order_id)
                if sl_status and sl_status.get("status", "").lower() == "matched":
                    fill_price = sl_status.get("price", position.stop_loss_price)
                    size_matched = float(sl_status.get("size_matched") or 0.0)
                    sl_proceeds = (
                        size_matched * float(fill_price)
                        if size_matched > 0 and fill_price
                        else None
                    )
                    _emit(f"\n[{timestamp}] RECONCILE: Pending SL order filled for "
                          f"{position.direction.value} position at ${fill_price:.4f}")
                    position.pending_sl_order_id = None
                    try:
                        self._record_exit(
                            position,
                            fill_price,
                            TradeResult.STOP_LOSS,
                            proceeds_override=sl_proceeds,
                        )
                    except ProceedsUnverifiedError as exc:
                        position.pending_settlement = True
                        position.pending_settlement_reason = "proceeds_unverified"
                        self.state.save()
                        _emit(f"[{timestamp}] RECONCILE: SL deferred — proceeds "
                              f"unverified ({exc})")
                    continue
                elif sl_status and sl_status.get("status", "").lower() in ("cancelled", "expired"):
                    _emit(f"\n[{timestamp}] RECONCILE: Pending SL order "
                          f"{sl_status.get('status')} — clearing, SL will re-trigger")
                    position.pending_sl_order_id = None
                    self.state.save()
                # If still live/open, let it continue pending
                continue

            try:
                actual_shares = self.executor.get_position(position.token_id, strict=True)
            except RuntimeError as e:
                _emit(f"[{timestamp}] RECONCILE: API error for {position.direction.value} "
                      f"position, skipping ({e})")
                continue

            # Check for full close
            if actual_shares <= 0:
                # Determine exit reason from order status
                reconcile_result = None  # will default to MANUAL_CLOSE
                exit_price = None

                # Check TP order
                if position.tp_order_id:
                    tp_status = self.executor.get_order_status(position.tp_order_id)
                    if tp_status and tp_status.get("status", "").lower() == "matched":
                        exit_price = tp_status.get("price", None)
                        if exit_price and exit_price > 0:
                            reconcile_result = TradeResult.TP_FILLED
                            _emit(f"\n[{timestamp}] RECONCILE: {position.direction.value} position "
                                  f"TP order filled at ${exit_price:.4f}")
                    else:
                        cancelled = self.executor.cancel_order(position.tp_order_id)
                        if cancelled:
                            _emit(f"  Cancelled orphaned TP order {position.tp_order_id}")
                        else:
                            _emit(f"  TP order {position.tp_order_id} already gone")

                # Check trail order (only if TP didn't match)
                if reconcile_result is None and position.trail_order_id:
                    trail_status = self.executor.get_order_status(position.trail_order_id)
                    if trail_status and trail_status.get("status", "").lower() == "matched":
                        exit_price = trail_status.get("price", None)
                        if exit_price and exit_price > 0:
                            reconcile_result = TradeResult.TRAILING_STOP
                            _emit(f"\n[{timestamp}] RECONCILE: {position.direction.value} position "
                                  f"trail order filled at ${exit_price:.4f}")

                if reconcile_result is None:
                    _emit(f"\n[{timestamp}] RECONCILE: {position.direction.value} position "
                          "no longer on Polymarket — closing as MANUAL_CLOSE")

                # Fall back to best bid, then entry price
                if not exit_price or exit_price <= 0:
                    best_bid = self.executor.get_best_bid(position.token_id)
                    exit_price = best_bid if best_bid and best_bid > 0 else position.entry_price

                self.state.reconcile_position(
                    position, 0.0, exit_price, trade_result=reconcile_result,
                )

                _emit(f"  Entry: ${position.entry_price:.4f} | Est. exit: ${exit_price:.4f}")
                _emit(f"  Est. P&L: ${(position.shares * exit_price - position.cost_basis):+.2f}")

                # Sync capital from chain for ground truth
                real_balance = self.executor.get_usdc_balance()
                if real_balance is not None:
                    self.state.current_capital = real_balance
                    self.state.save()
                continue

            # Check for shares mismatch (> 0.01)
            share_diff = abs(actual_shares - position.shares)
            if share_diff > 0.01:
                _emit(f"\n[{timestamp}] RECONCILE: {position.direction.value} position "
                      f"shares mismatch — local: {position.shares:.4f}, "
                      f"actual: {actual_shares:.4f}")

                old_shares = position.shares
                best_bid = self.executor.get_best_bid(position.token_id)
                exit_price = best_bid if best_bid and best_bid > 0 else position.entry_price

                # Try to compute actual cost_basis from trade history
                actual_cost_basis = None
                if actual_shares > position.shares:
                    # More shares than expected — likely split fills or extra buys
                    actual_cost_basis = self._compute_cost_basis_from_trades(position)

                self.state.reconcile_position(
                    position, actual_shares, exit_price,
                    actual_cost_basis=actual_cost_basis,
                )

                if actual_cost_basis is not None:
                    _emit(f"  Adjusted: {old_shares:.4f} -> {actual_shares:.4f} shares "
                          f"(cost_basis from trade history: ${actual_cost_basis:.2f})")
                else:
                    _emit(f"  Adjusted: {old_shares:.4f} -> {actual_shares:.4f} shares")

                # Cancel and re-place TP order with corrected share count
                if position.tp_order_id:
                    cancelled = self.executor.cancel_order(position.tp_order_id)
                    if not cancelled:
                        _emit(f"  [WARN] Failed to cancel old TP {position.tp_order_id}, "
                              "skipping re-placement to avoid duplicate")
                    else:
                        tp_shares = math.floor(actual_shares * 100) / 100
                        tp_result = self.executor.sell_limit(
                            position.token_id, position.tp_price, tp_shares
                        )
                        if tp_result.success:
                            position.tp_order_id = tp_result.order_id
                            position.tp_placed_at = datetime.now(timezone.utc)
                            self.state.save()
                            _emit(f"  Re-placed TP order: {tp_result.order_id}")
                        else:
                            _emit(f"  Failed to re-place TP order: {tp_result.error_message}")

    def _compute_cost_basis_from_trades(self, position: Position) -> Optional[float]:
        """Query Polymarket trade history and compute true cost basis.

        Sums cost (size * price) of all BUY fills for the token since
        the position was opened. Retries with backoff if trades are empty
        (indexer lag).

        Args:
            position: The position to compute cost basis for.

        Returns:
            Total cost basis from actual fills, or None if trade history
            is unavailable.
        """
        lookup = getattr(self.executor, "get_execution_cashflow", None)
        if callable(lookup) and position.order_id:
            bought_shares = float((position.entry_context or {}).get("entry_fill_shares") or position.shares)
            receipt = lookup(position.order_id, position.token_id, "BUY", bought_shares, position.opened_at.timestamp() - 300)
            if receipt is not None:
                return -float(receipt.net_cashflow) * position.shares / bought_shares
            if getattr(position, "payoff_kind", "legacy") != "legacy":
                return None
        try:
            since_ts = position.opened_at.timestamp()

            # Retry with backoff — indexer may lag behind chain
            trades = []
            delays = [0, 2, 4]
            for attempt, delay in enumerate(delays):
                if delay > 0:
                    time.sleep(delay)
                trades = self.executor.get_trades(position.token_id, since_timestamp=since_ts)
                if trades:
                    break

            if not trades:
                _emit(f"  [WARN] No trade history returned after {len(delays)} attempts, "
                      "falling back to proportional scaling")
                return None

            total_cost = 0.0
            total_shares = 0.0
            for t in trades:
                side = t.get("side", "").upper()
                if side == "BUY":
                    size = t.get("size", 0)
                    price = t.get("price", 0)
                    total_cost += size * price
                    total_shares += size

            if total_shares <= 0:
                _emit("  [WARN] No BUY trades found in history, falling back to proportional scaling")
                return None

            _emit(f"  [INFO] Trade history: {len(trades)} trades, "
                  f"{total_shares:.4f} shares bought for ${total_cost:.2f} "
                  f"(avg ${total_cost/total_shares:.4f})")
            return total_cost

        except Exception as e:
            _emit(f"  [WARN] Failed to get trade history: {e}")
            return None

    def _register_market_tokens(self, market: dict) -> None:
        """Register market token IDs for historical orphan detection."""
        clob_ids = market.get("clob_token_ids", {})
        changed = False
        for tid in clob_ids.values():
            if tid and tid not in self.state.known_token_ids:
                self.state.known_token_ids.add(tid)
                changed = True
        if changed:
            self.state.save()

    def _adopt_orphan_position(
        self,
        token_id: str,
        shares: float,
        direction: Direction,
        market: dict,
    ) -> None:
        """Adopt current-market wallet shares into local live state."""
        trades = []
        try:
            trades = self.executor.get_trades(token_id)
        except Exception as exc:
            _emit(f"  [WARN] Could not fetch orphan trade history: {exc}")

        buy_cost = 0.0
        buy_shares = 0.0
        for trade in trades:
            if str(trade.get("side", "")).upper() != "BUY":
                continue
            size = float(trade.get("size", 0) or 0)
            price = float(trade.get("price", 0) or 0)
            if size > 0 and price > 0:
                buy_shares += size
                buy_cost += size * price

        if buy_shares > 0 and buy_cost > 0:
            entry_price = buy_cost / buy_shares
            cost_basis = entry_price * shares
        else:
            best_ask = self.executor.get_best_ask(token_id)
            best_bid = self.executor.get_best_bid(token_id)
            entry_price = best_ask or best_bid or 0.5
            cost_basis = entry_price * shares

        tp_price = min(entry_price * (1 + self.config.tp_percentage), 0.99)
        stop_loss_price = (
            entry_price * (1 - self.config.stop_loss_pct)
            if self.config.stop_loss_pct > 0 else 0.0
        )
        position = Position(
            direction=direction,
            token_id=token_id,
            entry_price=entry_price,
            shares=shares,
            cost_basis=cost_basis,
            tp_price=tp_price,
            stop_loss_price=stop_loss_price,
            opened_at=datetime.now(timezone.utc),
            market_id=market.get("market_id", ""),
            peak_price=entry_price,
            market_title=market.get("title", ""),
            reference_price=float(market.get("reference_price", 0.0) or 0.0),
            entry_context={
                "adopted_orphan": True,
                "direction": direction.value,
                "reference_price": market.get("reference_price", 0.0),
            },
        )
        _emit(f"  Adopting current-market orphan as {direction.value} position")
        self.state.add_position(position)
        if position.market_id:
            self.state.last_entered_market_id = position.market_id
        self.state.save()
        if not self._adopt_existing_tp_order(position):
            self._place_backup_tp_order(position, reason="adopted_orphan_position")

    def _detect_orphan_positions(self) -> None:
        """Detect shares on Polymarket that the bot doesn't track."""
        if not self.executor.is_live:
            return

        market = self.state.current_market
        if not market:
            return

        # Clean up expired settlement markers (> 2 hours old)
        now = datetime.now(timezone.utc)
        self._recently_settled = {
            tid: ts for tid, ts in self._recently_settled.items()
            if (now - ts).total_seconds() < 7200
        }

        tracked_token_ids = {p.token_id for p in self.state.open_positions}
        if self.state.pending_entry and self.state.pending_entry.get("token_id"):
            tracked_token_ids.add(self.state.pending_entry["token_id"])

        # Always check current market tokens, then a bounded slice of historical
        # tokens. The persisted known set grows across 15m markets; scanning all
        # of it every loop can stall live ticks.
        token_ids_to_check: set[str] = set()
        for direction in ("UP", "DOWN"):
            tid = market.get("clob_token_ids", {}).get(direction)
            if tid:
                token_ids_to_check.add(tid)
        historical_limit = max(
            0,
            int(getattr(self.config, "orphan_historical_token_scan_limit", 20) or 0),
        )
        historical_tokens = sorted(
            self.state.known_token_ids - token_ids_to_check - tracked_token_ids
        )
        if historical_tokens and historical_limit > 0:
            start = int(getattr(self.state, "orphan_scan_cursor", 0) or 0) % len(historical_tokens)
            count = min(historical_limit, len(historical_tokens))
            rotated = historical_tokens[start:] + historical_tokens[:start]
            token_ids_to_check |= set(rotated[:count])
            self.state.orphan_scan_cursor = (start + count) % len(historical_tokens)
            self.state.save()

        current_token_direction = {
            tid: Direction(direction)
            for direction, tid in market.get("clob_token_ids", {}).items()
            if tid
        }

        for token_id in token_ids_to_check:
            if token_id in tracked_token_ids:
                continue

            # Skip tokens recently closed by expiry (settlement pending on Polymarket)
            if token_id in self._recently_settled:
                continue

            try:
                shares = self.executor.get_position(token_id, strict=True)
            except Exception as exc:
                log.debug("orphan position balance lookup failed for %s: %s", token_id, exc)
                continue

            if shares > 0.01:  # More than dust
                timestamp = datetime.now().strftime("%H:%M:%S")
                _emit(f"\n[{timestamp}] [WARN] ORPHAN POSITION DETECTED: "
                      f"{shares:.4f} shares on token {token_id[:8]}... "
                      f"(not tracked by bot)")

                if token_id in current_token_direction:
                    self._adopt_orphan_position(
                        token_id, shares, current_token_direction[token_id], market,
                    )
                    continue

                # Check for open sell orders — shares may be locked by a pending order
                try:
                    open_orders = self.executor.get_open_orders()
                    has_sell_order = any(
                        o.get("token_id") == token_id
                        and o.get("side", "").upper() == "SELL"
                        for o in open_orders
                    )
                    if has_sell_order:
                        _emit(f"  Skipping — open sell order exists for this token")
                        continue
                except Exception as exc:
                    log.warning(
                        "orphan open-order lookup failed for %s; proceeding with residual sell: %s",
                        token_id,
                        exc,
                    )

                self._sell_residual_shares(token_id, shares)

    def _adopt_pending_entry(
        self,
        pending: dict,
        order_status: Optional[dict],
        actual_shares: float,
    ) -> None:
        """Convert a previously ambiguous live buy into a tracked position."""
        token_id = pending.get("token_id", "")
        order_id = pending.get("order_id")
        direction = Direction(pending.get("direction", "UP"))
        shares = actual_shares if actual_shares > 0.01 else 0.0
        entry_price = None
        cost_basis = None
        cost_source = "order_status"
        if pending.get("verified_fill"):
            entry_price = self._safe_pending_float(pending, "fill_price", "avg_fill_price") or None
            cost_basis = self._safe_pending_float(pending, "total_cost") or None
            cost_source = (pending.get("order_metadata") or {}).get("cost_basis_source", "order_status")

        since_timestamp = None
        created_at = pending.get("created_at")
        if created_at:
            try:
                since_timestamp = max(0.0, datetime.fromisoformat(created_at).timestamp() - 60)
            except ValueError:
                since_timestamp = None

        try:
            trades = self.executor.get_trades(token_id, since_timestamp=since_timestamp)
        except Exception as exc:
            log.debug("pending entry trade lookup failed for %s: %s", token_id, exc)
            trades = []

        matching_trades = []
        for trade in trades:
            if str(trade.get("taker_order_id", "")) != str(order_id):
                continue
            side = str(trade.get("side", "")).upper()
            if side and side != "BUY":
                continue
            matching_trades.append(trade)

        if matching_trades and entry_price is None:
            filled_shares = sum(float(trade.get("size", 0) or 0) for trade in matching_trades)
            total_cost = sum(
                float(trade.get("size", 0) or 0) * float(trade.get("price", 0) or 0)
                for trade in matching_trades
            )
            if shares > 0.01 and filled_shares >= shares * 0.95 and total_cost > 0:
                entry_price = total_cost / filled_shares
                cost_basis = entry_price * shares

        status_name = (order_status or {}).get("status", "").lower()
        if (entry_price is None or cost_basis is None) and status_name == "matched":
            matched_shares = float((order_status or {}).get("size_matched", 0) or 0)
            matched_price = float((order_status or {}).get("price", 0) or 0)
            if matched_shares > 0 and matched_price > 0:
                entry_price = matched_price
                cost_basis = matched_price * shares

        if shares <= 0.01:
            timestamp = datetime.now().strftime("%H:%M:%S")
            _emit(f"[{timestamp}] [WARN] Pending entry resolved without detectable shares; keeping it pending")
            return

        if entry_price is None or cost_basis is None:
            intended_cost = float(pending.get("usd_amount", 0.0) or 0.0)
            if intended_cost <= 0:
                intended_cost = shares * float(pending.get("expected_price", 0.0) or 0.5)
            cost_basis = intended_cost
            entry_price = intended_cost / shares if shares > 0 else 0.0

        lookup = getattr(self.executor, "get_execution_cashflow", None)
        if callable(lookup) and order_id and pending.get("submitted_order_hash"):
            receipt = lookup(order_id, token_id, "BUY", shares, since_timestamp or 0)
            if receipt is not None:
                cost_basis = -float(receipt.net_cashflow)
                entry_price = float(receipt.gross_collateral / receipt.shares)
                cost_source = "v2_receipt"

        if entry_price > self.config.max_entry_price + 1e-6:
            self.state.log_risk_event(
                kind="pending_entry_fill_above_cap",
                details={
                    "order_id": order_id or "",
                    "avg_fill_price": f"{entry_price:.4f}",
                    "max_entry_price": f"{self.config.max_entry_price:.4f}",
                    "shares": f"{shares:.4f}",
                    "token_id": token_id,
                    "direction": direction.value,
                    "market_id": pending.get("market_id", ""),
                },
            )
            pending.update(
                {
                    "manual_intervention_required": True,
                    "reason": "entry_fill_above_cap",
                    "fill_price": entry_price,
                    "shares": shares,
                    "total_cost": cost_basis,
                    "max_entry_price": self.config.max_entry_price,
                }
            )
            self.state.pending_entry = pending
            self.state.save()
            return

        modern_partial = pending.get("payoff_kind", "legacy") != "legacy" and shares > 0.01 and cost_basis > 0
        if not self._live_entry_fill_is_viable(shares, cost_basis) and not modern_partial:
            self._mark_entry_partial_below_minimum(
                order_id=order_id,
                token_id=token_id,
                direction=direction.value,
                market_id=pending.get("market_id", ""),
                market_title=pending.get("market_title", ""),
                reference_price=float(pending.get("reference_price", 0.0) or 0.0),
                shares=shares,
                fill_price=entry_price,
                total_cost=cost_basis,
                order_metadata={"source": "pending_entry_adoption"},
            )
            return

        tp_price = min(entry_price * (1 + self.config.tp_percentage), 0.99)
        sl_price = 0.0
        if self.config.stop_loss_pct > 0:
            sl_price = entry_price * (1 - self.config.stop_loss_pct)

        position = Position(
            direction=direction,
            token_id=token_id,
            entry_price=entry_price,
            shares=shares,
            cost_basis=cost_basis,
            tp_price=tp_price,
            stop_loss_price=sl_price,
            opened_at=datetime.now(timezone.utc),
            market_id=pending.get("market_id", ""),
            peak_price=entry_price,
            order_id=order_id,
            market_title=pending.get("market_title", ""),
            reference_price=float(pending.get("reference_price", 0.0) or 0.0),
            condition_id=str(pending.get("condition_id") or ""),
            payoff_kind=str(pending.get("payoff_kind") or "legacy"),
            averaging_window_seconds=int(pending.get("averaging_window_seconds") or 0),
            contract_metadata=dict(pending.get("contract_metadata") or {}),
            entry_context={"entry_fill_shares": shares, "cost_basis_source": cost_source} if pending.get("submitted_order_hash") else None,
            fee_schedule=(pending.get("contract_metadata") or {}).get("fee_schedule"),
        )

        timestamp = datetime.now().strftime("%H:%M:%S")
        _emit(f"\n[{timestamp}] ADOPTING PENDING ENTRY: {direction.value} {shares:.4f} shares")
        self.state.add_position(position)
        self.state.pending_entry = None
        self.state.last_entered_market_id = position.market_id
        get_cash = getattr(self.executor, "get_usdc_balance", None)
        current_cash = get_cash() if callable(get_cash) else None
        if current_cash is not None:
            self.state.current_capital = current_cash
        self.state.save()
        self._place_backup_tp_order(position, reason="adopted_pending_entry")

    def _maybe_run_history_reconciler(self) -> None:
        """Throttled call to the polymarket_history_reconciler.

        Runs at most once per ``history_reconcile_interval_seconds``
        (default 60 s). Iterates the most-recent closed trades,
        re-queries /data/trades for those tagged ``order_status`` /
        ``planned``, and rewrites the CSV + state in place when the
        exact value differs by > $0.01.
        """
        interval = float(getattr(
            self.config, "history_reconcile_interval_seconds", 60.0,
        ))
        if interval <= 0:
            return
        now = datetime.now(timezone.utc)
        last = getattr(self, "_last_history_reconcile", None)
        if last is not None:
            if (now - last).total_seconds() < interval:
                return
        self._last_history_reconcile = now
        try:
            from .polymarket_history_reconciler import reconcile_pending_proceeds, reconcile_pending_entry_costs

            n = reconcile_pending_entry_costs(self) + reconcile_pending_proceeds(self)
            if n > 0:
                _emit(f"[INFO] history_reconciler: upgraded {n} tentative row(s)")
        except Exception as exc:
            _emit(f"[WARN] history_reconciler raised: {exc}")

    def _cancel_and_recheck_pending_entry(self, pending: dict, *, cancellation_confirmed: bool = False) -> Optional[tuple[dict, float]]:
        """Confirm cancellation and then observe balances/fills, never in reverse."""
        order_id = pending.get("order_id")
        if not order_id:
            return None
        token_id = pending.get("token_id", "")
        baseline = self._safe_pending_float(pending, "pre_balance")
        try:
            cancel_detailed = getattr(self.executor, "cancel_order_detailed", None)
            if cancellation_confirmed:
                pass
            elif callable(cancel_detailed):
                cancelled = cancel_detailed(order_id)
                if not (cancelled.success or cancelled.terminal_absent):
                    return None
            elif not self.executor.cancel_order(order_id):
                return None
            reconcile = getattr(self.executor, "_post_cancel_buy_reconcile", None)
            if callable(reconcile):
                created = pending.get("created_at")
                result = reconcile(
                    order_id=order_id, token_id=token_id,
                    fallback_price=self._safe_pending_float(pending, "expected_price", "avg_fill_price", "fill_price"),
                    requested_shares=self._safe_pending_float(pending, "requested_shares"),
                    pre_balance=baseline,
                    started_at=datetime.fromisoformat(created).timestamp() if created else None,
                    metadata=dict(pending.get("order_metadata") or {}),
                )
                if result is not None:
                    if not result.success or result.ambiguous:
                        return None
                    pending.update({"total_cost": result.total_cost, "fill_price": result.avg_fill_price,
                                    "order_metadata": result.metadata, "verified_fill": True})
            status = self.executor.get_order_status(order_id) or {}
            if str(status.get("status") or "").lower() in {"live", "open", "active", "delayed", "pending", "unmatched"}:
                return None
            balance = self.executor.get_position(token_id, strict=True)
            return status, max(0.0, balance - baseline)
        except (RuntimeError, ValueError, TypeError):
            return None

    def _reconcile_pending_entry(self) -> None:
        """Resolve a previously ambiguous live buy before new trading decisions."""
        if not self.executor.is_live or not self.state.pending_entry:
            return

        pending = self.state.pending_entry
        if pending.get("manual_intervention_required"):
            self._reconcile_manual_pending_entry(pending)
            return
        token_id = pending.get("token_id", "")
        if not token_id:
            self.state.pending_entry = None
            self.state.save()
            return

        order_id = pending.get("order_id")
        order_status = None
        if order_id:
            try:
                order_status = self.executor.get_order_status(order_id)
            except Exception as exc:
                log.debug("pending entry order status lookup failed for %s: %s", order_id, exc)
                order_status = None

        status_name = (order_status or {}).get("status", "").lower()

        actual_shares = None
        try:
            actual_shares = max(0.0, self.executor.get_position(token_id, strict=True) - self._safe_pending_float(pending, "pre_balance"))
        except RuntimeError:
            actual_shares = None

        if status_name == "matched" or (actual_shares is not None and actual_shares > 0.01):
            if status_name not in {"matched", "cancelled", "canceled", "expired", "rejected", "failed"}:
                reconciled = self._cancel_and_recheck_pending_entry(pending)
                if reconciled is None:
                    return
                order_status, actual_shares = reconciled
            self._adopt_pending_entry(pending, order_status, actual_shares or 0.0)
            return

        if (
            status_name in ("cancelled", "canceled", "expired", "rejected", "failed")
            and actual_shares is not None
            and actual_shares <= 0.01
        ):
            timestamp = datetime.now().strftime("%H:%M:%S")
            _emit(f"\n[{timestamp}] Clearing pending entry {order_id} ({status_name})")
            self.state.pending_entry = None
            self.state.save()
            return

        # Stuck pending entry: order hasn't matched, no shares on chain, and
        # status is either missing or 'live'/'delayed'. For 15m markets this
        # must not ghost across a market transition — force-cancel once the
        # pending entry exceeds pending_entry_max_age_seconds.
        max_age = getattr(self.config, "pending_entry_max_age_seconds", 0)
        if max_age and status_name not in ("matched",) and (actual_shares is None or actual_shares <= 0.01):
            created_at_raw = pending.get("created_at")
            # Legacy pending entries (pre-atomicity refactor) may lack
            # created_at. Backfill with "now" so the stale guard starts the
            # clock fresh rather than silently no-op'ing forever.
            if not created_at_raw:
                created_at_raw = datetime.now(timezone.utc).isoformat()
                pending["created_at"] = created_at_raw
                self.state.save()
            try:
                created_at = datetime.fromisoformat(created_at_raw)
                age = (datetime.now(timezone.utc) - created_at).total_seconds()
            except (TypeError, ValueError):
                age = 0.0
            if age > max_age:
                timestamp = datetime.now().strftime("%H:%M:%S")
                _emit(f"\n[{timestamp}] STALE pending entry {order_id} "
                      f"(age={age:.0f}s > {max_age}s, status={status_name or 'unknown'}), force-cancelling")
                cancelled = False
                cancel_error = ""
                if order_id:
                    try:
                        cancelled = self.executor.cancel_order(order_id)
                    except Exception as exc:
                        _emit(f"  [WARN] Cancel raised: {exc}")
                        cancel_error = str(exc)[:160]
                        cancelled = False
                pending["last_cancel_attempt_at"] = datetime.now(timezone.utc).isoformat()
                pending["cancel_attempts"] = int(pending.get("cancel_attempts", 0) or 0) + 1
                pending["last_seen_order_status"] = status_name or ""
                rechecked = self._cancel_and_recheck_pending_entry(pending, cancellation_confirmed=True) if cancelled else None
                if rechecked is not None and rechecked[1] > 0.01:
                    self._adopt_pending_entry(pending, rechecked[0], rechecked[1])
                    return
                if rechecked is not None and rechecked[1] <= 0.01:
                    self.state.log_risk_event(
                        kind="pending_entry_stale_cancelled",
                        details={
                            "order_id": order_id or "",
                            "token_id": token_id,
                            "age_seconds": f"{age:.0f}",
                            "cancel_succeeded": str(cancelled),
                            "status_seen": status_name or "",
                        },
                    )
                    self.state.pending_entry = None
                else:
                    self.state.log_risk_event(
                        kind="pending_entry_cancel_unconfirmed",
                        details={
                            "order_id": order_id or "",
                            "token_id": token_id,
                            "age_seconds": f"{age:.0f}",
                            "cancel_succeeded": str(cancelled),
                            "status_seen": status_name or "",
                            "actual_shares": "" if actual_shares is None else f"{actual_shares:.4f}",
                            "error": cancel_error,
                        },
                    )
                    self.state.pending_entry = pending
                self.state.save()

    def _print_heartbeat(self) -> None:
        """Print heartbeat status every 60 seconds."""
        now = datetime.now(timezone.utc)

        # Check if 60 seconds have passed since last heartbeat
        if self._last_heartbeat is not None:
            elapsed = (now - self._last_heartbeat).total_seconds()
            if elapsed < 60:
                return

        self._last_heartbeat = now

        # Log orderbook snapshot
        self._log_orderbook_snapshot()

        # Verify TP orders for open positions
        self._verify_tp_orders()

        timestamp = datetime.now().strftime("%H:%M:%S")

        uptime = self._format_uptime()

        # Use real USDC balance for live mode
        if self.executor.is_live:
            real_balance = self.executor.get_usdc_balance()
            if real_balance is not None:
                cash = real_balance
            else:
                cash = self.state.current_capital
        else:
            cash = self.state.current_capital

        realized_pnl = self.state.total_pnl
        open_pos = len(self.state.open_positions)
        closed_trades = len(self.state.closed_positions)

        # Calculate unrealized P&L from open positions
        total_open_value = 0.0
        total_unrealized_pnl = 0.0
        for position in self.state.open_positions:
            market_value, unrealized = self.get_open_position_value(position)
            total_open_value += market_value
            total_unrealized_pnl += unrealized

        # Trading P&L excludes deposits, withdrawals and historical balance
        # adjustments. Cash/equity are reported independently below.
        total_pnl = realized_pnl + total_unrealized_pnl

        # Format P&L strings with sign
        total_pnl_str = f"+${total_pnl:.2f}" if total_pnl >= 0 else f"-${abs(total_pnl):.2f}"

        _emit(f"\n[{timestamp}] Heartbeat | Uptime: {uptime}")

        # Show current market from last tick
        if self._last_tick and self._last_tick.market_title:
            tick = self._last_tick
            remaining_secs = tick.time_remaining_seconds
            mins = int(remaining_secs // 60)
            secs = int(remaining_secs % 60)
            _emit(f"           Market: {tick.market_title} ({mins}m {secs}s to expiry)")
            spot_src = f" [{tick.spot_source}]" if tick.spot_source else ""
            _emit(f"           Barrier: ${tick.barrier:,.2f} | Spot: ${tick.spot_price:,.2f}{spot_src}")
            if tick.model_used:
                _emit(f"           Model({tick.model_used.upper()[:4]}): "
                      f"UP={tick.prob_above:.1%} DOWN={tick.prob_below:.1%} | "
                      f"Poly: UP={tick.poly_prob_up:.1%} DOWN={tick.poly_prob_down:.1%}")
            max_ask = self.config.max_entry_price
            up_ask = tick.up_order_book.best_ask if tick.up_order_book else None
            down_ask = tick.down_order_book.best_ask if tick.down_order_book else None
            blocked_sides = []
            if up_ask is not None and up_ask > max_ask:
                blocked_sides.append(f"UP ask={up_ask:.2f}")
            if down_ask is not None and down_ask > max_ask:
                blocked_sides.append(f"DOWN ask={down_ask:.2f}")
            if blocked_sides:
                _emit(f"           ENTRY BLOCKED (>{max_ask:.2f}): {', '.join(blocked_sides)}")
        elif self.state.current_market:
            market_title = self.state.current_market.get("title", "Unknown")
            reference_price = self.state.current_market.get("reference_price") or 0
            _emit(f"           Market: {market_title}")
            _emit(f"           Barrier: ${reference_price:,.2f}")
        else:
            _emit("           Market: None (waiting for tick...)")

        # Format capital line based on whether there are open positions
        if open_pos > 0:
            equity = cash + total_open_value
            unrealized_str = f"+${total_unrealized_pnl:.2f}" if total_unrealized_pnl >= 0 else f"-${abs(total_unrealized_pnl):.2f}"
            _emit(f"           Equity: ${equity:.2f} (Cash: ${cash:.2f} + Position: ${total_open_value:.2f}) | P&L: {total_pnl_str} ({unrealized_str} unrealized)")
            _emit(f"           Open: {open_pos} | Closed: {closed_trades}")
        else:
            _emit(f"           Equity: ${cash:.2f} | P&L: {total_pnl_str}")
            _emit(f"           Open: {open_pos} | Closed: {closed_trades}")

        for pos in self.state.open_positions:
            if self.config.trail_activation > 0:
                if pos.trail_order_id:
                    _emit(f"           Trail: ON BOOK @ ${pos.trail_order_price:.4f} | Peak: ${pos.peak_price:.4f}")
                else:
                    act_price = pos.entry_price * (1 + self.config.trail_activation)
                    if pos.peak_price >= act_price:
                        trail_lvl = pos.peak_price - pos.entry_price * self.config.trail_distance
                        _emit(f"           Trail: ARMED | Peak: ${pos.peak_price:.4f} | Stop: ${trail_lvl:.4f}")
                    else:
                        pct_to_act = ((act_price - pos.peak_price) / pos.entry_price) * 100
                        _emit(f"           Trail: {pct_to_act:.1f}pp to activation")
            if pos.stop_loss_price > 0:
                _emit(f"           SL: ${pos.stop_loss_price:.4f} ({self.config.stop_loss_pct*100:.0f}% below entry)")

        # Unified single-line summary — same shape in paper and live so a
        # side-by-side log diff is trivially parseable.
        mode_tag = "LIVE" if self.executor.is_live else "PAPER"
        tp_confirmed = sum(1 for p in self.state.open_positions if p.tp_order_id)
        _emit(
            f"           [{mode_tag}] cash={cash:.2f} "
            f"open={open_pos} closed={closed_trades} "
            f"rlz={realized_pnl:+.2f} urlz={total_unrealized_pnl:+.2f} "
            f"tp_confirmed={tp_confirmed}/{open_pos}"
        )

    def _direction_eval_snapshot(self, tick: TickData, direction_str: str) -> dict:
        if direction_str == "UP":
            model_prob = tick.prob_above
            poly_prob = tick.poly_prob_up
            alpha = self.config.edge_alpha_up
            floor = self.config.edge_floor_up
            order_book = tick.up_order_book
        else:
            model_prob = tick.prob_below
            poly_prob = tick.poly_prob_down
            alpha = self.config.edge_alpha_down
            floor = self.config.edge_floor_down
            order_book = tick.down_order_book

        if order_book and order_book.best_ask > 0:
            executable_price = order_book.best_ask
        else:
            executable_price = poly_prob

        required_prob = (
            required_model_prob(executable_price, alpha, floor)
            if executable_price > 0
            else 0.0
        )
        has_edge_now = (
            tick.barrier_confirmed
            and executable_price > 0
            and executable_price <= self.config.max_entry_price
            and has_edge(model_prob, executable_price, alpha, floor)
        )
        return {
            "model_prob": model_prob,
            "poly_prob": poly_prob,
            "executable_price": executable_price,
            "required_prob": required_prob,
            "has_edge": has_edge_now,
        }

    @staticmethod
    def _signal_value(signal, key: str, default=None):
        if isinstance(signal, dict):
            return signal.get(key, default)
        return getattr(signal, key, default)

    def _live_entry_signal(self, tick: TickData):
        for signal in tick.entry_signals or ():
            if str(self._signal_value(signal, "instance_id")) == "live":
                return signal
        return None

    def _log_live_tick_diagnostics(
        self,
        tick: TickData,
        *,
        skipped_before_eval: bool,
        pre_eval_skip_reason: str = "",
        chosen_direction: str = "",
        entry_attempt: Optional[EntryAttemptResult] = None,
    ) -> None:
        if not self.executor.is_live:
            return
        up = self._direction_eval_snapshot(tick, "UP")
        down = self._direction_eval_snapshot(tick, "DOWN")

        live_signal = self._live_entry_signal(tick)
        details = (
            json.dumps(entry_attempt.rejection_details, sort_keys=True)
            if entry_attempt and entry_attempt.rejection_details
            else ""
        )
        order_metadata = (
            json.dumps(getattr(entry_attempt, "order_metadata", {}), sort_keys=True)
            if entry_attempt and getattr(entry_attempt, "order_metadata", {})
            else ""
        )
        edge_details = getattr(self, "_last_edge_skip_details", {}) or {}
        edge_details_json = json.dumps(edge_details, sort_keys=True) if edge_details else ""

        self.state.log_live_tick_eval(
            market_id=tick.market_id,
            model_used=tick.model_used,
            spot_source=tick.spot_source,
            elapsed_seconds=tick.elapsed_seconds,
            remaining_seconds=tick.time_remaining_seconds,
            spot_price=tick.spot_price,
            barrier=tick.barrier,
            barrier_source=getattr(tick, "barrier_source", ""),
            barrier_confirmed=tick.barrier_confirmed,
            skipped_before_eval=skipped_before_eval,
            pre_eval_skip_reason=pre_eval_skip_reason,
            chosen_direction=chosen_direction,
            up_model_prob=up["model_prob"],
            up_poly_prob=up["poly_prob"],
            up_executable_price=up["executable_price"],
            up_required_prob=up["required_prob"],
            up_has_edge=up["has_edge"],
            down_model_prob=down["model_prob"],
            down_poly_prob=down["poly_prob"],
            down_executable_price=down["executable_price"],
            down_required_prob=down["required_prob"],
            down_has_edge=down["has_edge"],
            entry_signal_present=live_signal is not None,
            entry_signal_direction=str(self._signal_value(live_signal, "direction", "") or ""),
            entry_signal_price=self._signal_value(live_signal, "executable_price"),
            entry_signal_age_s=tick.entry_signal_age_s,
            entry_signal_market_id=str(
                self._signal_value(live_signal, "market_id", "")
                or getattr(tick, "entry_signal_market_id", "")
                or ""
            ),
            latest_signal_stale_age_s=getattr(tick, "entry_signal_stale_age_s", -1.0),
            edge_skip_reason=getattr(self, "_last_edge_skip_reason", "") or "",
            edge_skip_details=edge_details_json,
            post_eval_attempted=entry_attempt.attempted if entry_attempt else False,
            post_eval_entered=entry_attempt.entered if entry_attempt else False,
            post_eval_rejection_reason=entry_attempt.rejection_reason if entry_attempt else "",
            post_eval_rejection_details=details,
            post_eval_capital_source=getattr(entry_attempt, "capital_source", "") if entry_attempt else "",
            post_eval_raw_order_size_usd=getattr(entry_attempt, "raw_order_size_usd", None) if entry_attempt else None,
            post_eval_order_size_usd=entry_attempt.order_size_usd if entry_attempt else None,
            post_eval_capital_before_entry=(
                entry_attempt.capital_before_entry if entry_attempt else None
            ),
            post_eval_order_id=entry_attempt.order_id if entry_attempt else "",
            post_eval_order_ambiguous=entry_attempt.ambiguous if entry_attempt else False,
            post_eval_token_id=entry_attempt.token_id if entry_attempt else "",
            post_eval_rechecked_price=entry_attempt.rechecked_price if entry_attempt else None,
            post_eval_order_metadata=order_metadata,
        )

    # ---------------------------------------------------------------
    # Edge evaluation (replaces _run_edge_check subprocess pipeline)
    # ---------------------------------------------------------------

    def _evaluate_edge(self, tick: TickData) -> Optional[dict]:
        """Evaluate edge from pre-computed TickData.

        Replaces BTC_pricer's _run_edge_check which ran subprocesses.
        Returns opportunity dict if edge found, None otherwise.
        """
        self._last_edge_skip_reason = ""
        self._last_edge_skip_details = {}

        def _skip(reason: str, **details) -> None:
            self._last_edge_skip_reason = reason
            self._last_edge_skip_details = {k: v for k, v in details.items() if v is not None}
            return None

        if not tick.barrier_confirmed:
            return _skip(
                "barrier_unconfirmed",
                barrier_source=getattr(tick, "barrier_source", ""),
                barrier=f"{float(tick.barrier):.2f}" if tick.barrier else "",
            )
        # One-position-per-window rule: refuse re-entry in a market we've
        # already opened a position in, even after a TP/SL exit. Cleared when
        # a new market starts (state advances naturally as tick.market_id
        # changes; gate only blocks while we're still in the same window).
        if (
            tick.market_id
            and tick.market_id == self.state.last_entered_market_id
        ):
            return _skip("already_entered_market", market_id=tick.market_id)
        # Skip edge evaluation while Polymarket is undergoing maintenance —
        # quotes go stale/empty, so any "edge" against tick.poly_prob_* is
        # spurious. Resumes automatically once status flips back to UP.
        if not is_polymarket_operational():
            label = polymarket_status_label() or "down"
            self.log(f"Skipping edge eval: Polymarket {label}")
            return _skip("polymarket_down", status=label)
        # CLOB taker-health gate (shared with the dry-run grid via the flag file).
        clob_block = self._clob_taker_block_reason()
        if clob_block:
            self.log(f"Skipping edge eval: CLOB taker-health unhealthy ({clob_block})")
            return _skip("clob_taker_outage", clob_reason=clob_block)

        # Use centralized entry signal from calc if available
        if tick.entry_signals:
            saw_live_signal = False
            saw_wrong_market = False
            saw_invalid_direction = False
            for sig in tick.entry_signals:
                if str(self._signal_value(sig, "instance_id")) != "live":
                    continue
                saw_live_signal = True
                signal_market_id = str(self._signal_value(sig, "market_id", "") or "")
                if signal_market_id and signal_market_id != tick.market_id:
                    saw_wrong_market = True
                    self.log(
                        "Skipping live signal for wrong market: "
                        f"signal={signal_market_id} tick={tick.market_id}"
                    )
                    continue
                direction_str = self._signal_value(sig, "direction", "")
                if direction_str not in ("UP", "DOWN"):
                    saw_invalid_direction = True
                    continue
                order_book = tick.up_order_book if direction_str == "UP" else tick.down_order_book
                token_id = tick.up_token_id if direction_str == "UP" else tick.down_token_id
                edge_ratio = self._signal_value(sig, "edge_ratio", 0)
                model_prob = self._signal_value(sig, "model_prob", 0)
                exec_price = self._signal_value(sig, "executable_price", 0)
                req_prob = self._signal_value(sig, "required_prob", 0)
                self.log(f"EDGE FOUND (calc signal): {direction_str} {edge_ratio:.2f}x "
                         f"(model={model_prob:.4f} vs market={exec_price:.4f}, "
                         f"required={req_prob:.4f})")
                return {
                    "direction": direction_str,
                    "edge": edge_ratio,
                    "polymarket_prob": exec_price,
                    "model_prob": model_prob,
                    "required_prob": req_prob,
                    "token_id": token_id,
                    "tick": tick,
                }
            if not saw_live_signal:
                return _skip(
                    "no_live_signal",
                    signal_count=len(tick.entry_signals),
                    latest_signal_age_s=f"{float(tick.entry_signal_age_s):.1f}",
                    stale_signal_age_s=f"{float(getattr(tick, 'entry_signal_stale_age_s', -1.0)):.1f}",
                )
            if saw_wrong_market:
                return _skip(
                    "wrong_market_signal",
                    tick_market_id=tick.market_id,
                    signal_market_id=getattr(tick, "entry_signal_market_id", ""),
                )
            if saw_invalid_direction:
                return _skip("invalid_live_signal_direction")
            return _skip("no_valid_live_signal")

        return _skip(
            "no_entry_signals",
            stale_signal_age_s=f"{float(getattr(tick, 'entry_signal_stale_age_s', -1.0)):.1f}",
        )

    def _build_entry_context_from_tick(self, opportunity: dict) -> Optional[dict]:
        """Build entry_context from tick-based opportunity."""
        tick = opportunity.get("tick")
        if not tick:
            return None

        direction = opportunity.get("direction", "UP")
        is_up = direction == "UP"

        return {
            "model_used": tick.model_used.upper() if tick.model_used else "UNKNOWN",
            "model_prob": tick.prob_above if is_up else tick.prob_below,
            "poly_price": tick.poly_prob_up if is_up else tick.poly_prob_down,
            "edge_ratio": opportunity.get("edge"),
            "required_prob": opportunity.get("required_prob"),
            "ttm_hours": tick.time_remaining_seconds / 3600,
            "reference_price": tick.barrier,
            "spot_price": tick.spot_price,
            "direction": direction,
        }

    # ---------------------------------------------------------------
    # Settlement (replaces _settle_positions_for_market)
    # ---------------------------------------------------------------

    def _settle_position_at_final(
        self,
        position: Position,
        final_price: float,
        final_source: str = "",
    ) -> None:
        self._get_settlement_service().settle_position_at_final(
            position, final_price, final_source
        )

    def _attempt_live_position_settlement(
        self,
        position: Position,
        final_price: Optional[float] = None,
        final_source: str = "",
    ) -> bool:
        return self._get_settlement_service().attempt_settlement(
            position, final_price, final_source
        )

    def _retry_pending_settlements(self) -> None:
        self._get_settlement_service().retry_pending()

    def _settle_positions(self, tick: TickData) -> None:
        self._get_settlement_service().settle_positions(tick)

    def _closing_window_suppresses_stop_loss(self) -> bool:
        """Return True when the active tick is inside the final no-entry window."""
        tick = getattr(self, "_last_tick", None)
        remaining = getattr(tick, "time_remaining_seconds", None)
        if remaining is None:
            return False
        try:
            return float(remaining) <= float(self.config.no_trade_last_seconds)
        except (TypeError, ValueError):
            return False

    def _handle_pending_sl_for_closing_window(
        self,
        position: Position,
        *,
        timestamp: Optional[str] = None,
    ) -> bool:
        """Cancel still-live fallback SL orders once closing mode starts.

        Returns True when the pending SL order was handled and the caller
        should skip further work for this position on this pass.
        """
        order_id = position.pending_sl_order_id
        if not order_id or not self._closing_window_suppresses_stop_loss():
            return False

        timestamp = timestamp or datetime.now().strftime("%H:%M:%S")
        sl_status = self.executor.get_order_status(order_id)
        status_name = (sl_status or {}).get("status", "").lower() if sl_status else ""

        if status_name in ("matched", "filled"):
            fill_price = float((sl_status or {}).get("price") or position.stop_loss_price or 0.0)
            size_matched = float((sl_status or {}).get("size_matched") or 0.0)
            sl_proceeds = size_matched * fill_price if size_matched > 0 and fill_price > 0 else None
            _emit(
                f"\n[{timestamp}] CLOSING: Pending SL order already filled for "
                f"{position.direction.value} position at ${fill_price:.4f}"
            )
            position.pending_sl_order_id = None
            try:
                self._record_exit(
                    position,
                    fill_price,
                    TradeResult.STOP_LOSS,
                    proceeds_override=sl_proceeds,
                )
            except ProceedsUnverifiedError as exc:
                position.pending_settlement = True
                position.pending_settlement_reason = "proceeds_unverified"
                self.state.save()
                _emit(f"[{timestamp}] CLOSING: SL deferred - proceeds unverified ({exc})")
            return True

        if status_name in self._TERMINAL_ORDER_STATUSES:
            _emit(
                f"\n[{timestamp}] CLOSING: Pending SL order {status_name} - "
                "clearing without re-trigger"
            )
            position.pending_sl_order_id = None
            self.state.save()
            return True

        try:
            cancelled = self.executor.cancel_order(order_id)
        except Exception as exc:
            _emit(f"\n[{timestamp}] CLOSING: Pending SL cancel raised for {order_id}: {exc}")
            cancelled = False

        if cancelled:
            _emit(f"\n[{timestamp}] CLOSING: Cancelled pending SL order {order_id}")
            position.pending_sl_order_id = None
        else:
            _emit(
                f"\n[{timestamp}] CLOSING: Pending SL order {order_id} could not be "
                "cancelled; keeping it for reconciliation"
            )
        self.state.save()
        return True

    # ---------------------------------------------------------------
    # Mirror signal control
    # ---------------------------------------------------------------

    def _ack_mirror_signal(
        self,
        signal: MirrorTradeSignal,
        status: str,
        reason: str = "",
        details: Optional[dict] = None,
    ) -> None:
        subscriber = getattr(self, "_mirror_subscriber", None)
        if subscriber is None:
            return
        self._ensure_mirror_async_runtime()
        now_monotonic = time.monotonic()
        self._prune_mirror_ack_throttle(now_monotonic)
        if status == "duplicate":
            throttle = float(
                getattr(self.config, "mirror_signal_ack_duplicate_throttle_seconds", 30.0)
                or 0.0
            )
            key = (signal.signal_id, status, reason)
            last_sent = self._mirror_ack_last_sent_at.get(key)
            if throttle > 0 and last_sent is not None and now_monotonic - last_sent < throttle:
                return
            self._mirror_ack_last_sent_at[key] = now_monotonic
        ack_details = self._mirror_signal_latency_details(signal)
        ack_details.update(details or {})
        if signal.expires_at is not None:
            ack_details["expires_at"] = signal.expires_at.isoformat()
        try:
            subscriber.ack_signal(signal, status=status, reason=reason, details=ack_details)
        except Exception as exc:  # noqa: BLE001
            _emit(f"[WARN] mirror ack failed for {signal.signal_id}: {exc}")

    def _prune_mirror_ack_throttle(self, now_monotonic: Optional[float] = None) -> None:
        self._ensure_mirror_async_runtime()
        max_entries = 5000
        throttle = float(
            getattr(self.config, "mirror_signal_ack_duplicate_throttle_seconds", 30.0)
            or 0.0
        )
        if not self._mirror_ack_last_sent_at:
            return
        now_value = time.monotonic() if now_monotonic is None else now_monotonic
        if throttle > 0:
            stale_before = now_value - max(throttle * 2.0, throttle + 1.0)
            self._mirror_ack_last_sent_at = {
                key: sent_at
                for key, sent_at in self._mirror_ack_last_sent_at.items()
                if sent_at >= stale_before
            }
        if len(self._mirror_ack_last_sent_at) > max_entries:
            newest = sorted(
                self._mirror_ack_last_sent_at.items(),
                key=lambda item: item[1],
            )[-max_entries:]
            self._mirror_ack_last_sent_at = dict(newest)

    def _ack_mirror_delivery(self, signal: MirrorTradeSignal, *, stream_id: str = "") -> None:
        subscriber = getattr(self, "_mirror_subscriber", None)
        message_id = stream_id or self._mirror_signal_stream_ids.get(signal.signal_id, "")
        if subscriber is None or not message_id:
            return
        if not hasattr(subscriber, "ack_message"):
            return
        try:
            subscriber.ack_message(message_id)
        except Exception as exc:  # noqa: BLE001
            self.state.log_risk_event(
                kind="mirror_signal_delivery_ack_failed",
                details={
                    "signal_id": signal.signal_id,
                    "stream_id": message_id,
                    "error": str(exc)[:160],
                },
            )

    def _poll_mirror_signals(self) -> list[tuple[str, Optional[MirrorTradeSignal]]]:
        subscriber = getattr(self, "_mirror_subscriber", None)
        if subscriber is None:
            return []
        try:
            return subscriber.read_new(block_ms=0, count=50)
        except Exception as exc:  # noqa: BLE001
            self.state.log_risk_event(
                kind="mirror_signal_poll_failed",
                details={"error": str(exc)[:160]},
            )
            _emit(f"[WARN] mirror signal poll failed: {exc}")
            return []

    def _recover_pending_mirror_signals(self, tick: Optional[TickData]) -> None:
        if not self._mirror_control_enabled():
            return
        subscriber = getattr(self, "_mirror_subscriber", None)
        if subscriber is None:
            return
        self._ensure_mirror_async_runtime()
        now = datetime.now(timezone.utc)
        interval = float(
            getattr(self.config, "mirror_signal_pending_recover_interval_seconds", 1.0)
            or 0.0
        )
        if (
            interval > 0
            and self._last_mirror_pending_recover_at is not None
            and (now - self._last_mirror_pending_recover_at).total_seconds() < interval
        ):
            return
        self._last_mirror_pending_recover_at = now
        min_idle = int(getattr(self.config, "mirror_signal_pending_reclaim_min_idle_ms", 1000) or 0)
        try:
            messages = subscriber.claim_pending(min_idle_ms=min_idle, count=50)
        except Exception as exc:  # noqa: BLE001
            self.state.log_risk_event(
                kind="mirror_signal_pending_recover_failed",
                details={"error": str(exc)[:160]},
            )
            return
        pending_entry_ids = {
            item.signal_id for item in getattr(self, "_pending_async_mirror_entries", [])
        }
        for stream_id, signal in messages:
            if signal is not None and signal.event == "ENTRY" and signal.signal_id in pending_entry_ids:
                continue
            self._process_mirror_delivery(stream_id, signal, tick, source="pending_recover")

    def _check_mirror_publisher_heartbeat(self) -> None:
        if not self._mirror_control_enabled():
            return
        subscriber = getattr(self, "_mirror_subscriber", None)
        if subscriber is None:
            return
        if not hasattr(subscriber, "get_json_key"):
            return
        self._ensure_mirror_async_runtime()
        now = datetime.now(timezone.utc)
        interval = float(
            getattr(self.config, "mirror_heartbeat_check_interval_seconds", 10.0)
            or 0.0
        )
        if (
            interval > 0
            and self._last_mirror_heartbeat_check_at is not None
            and (now - self._last_mirror_heartbeat_check_at).total_seconds() < interval
        ):
            return
        self._last_mirror_heartbeat_check_at = now
        key = str(getattr(self.config, "mirror_heartbeat_key", "trade.mirror.heartbeat") or "")
        if not key:
            return
        try:
            heartbeat = subscriber.get_json_key(key)
        except Exception as exc:  # noqa: BLE001
            self.state.log_risk_event(
                kind="mirror_publisher_heartbeat_read_failed",
                details={"key": key, "error": str(exc)[:160]},
            )
            return
        stale_after = float(getattr(self.config, "mirror_heartbeat_stale_seconds", 60.0) or 0.0)
        reason = ""
        age_s: Optional[float] = None
        if not heartbeat:
            reason = "missing"
        else:
            try:
                ts = datetime.fromisoformat(str(heartbeat.get("timestamp") or ""))
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                ts = ts.astimezone(timezone.utc)
                age_s = (now - ts).total_seconds()
            except ValueError:
                reason = "bad_timestamp"
            if not reason and stale_after > 0 and age_s is not None and age_s > stale_after:
                reason = "stale"
        if not reason:
            return
        warn_interval = max(stale_after, interval, 1.0)
        if (
            self._last_mirror_heartbeat_warn_at is not None
            and (now - self._last_mirror_heartbeat_warn_at).total_seconds() < warn_interval
        ):
            return
        self._last_mirror_heartbeat_warn_at = now
        self.state.log_risk_event(
            kind="mirror_publisher_heartbeat_stale",
            details={
                "reason": reason,
                "key": key,
                "age_s": self._fmt_optional_float(age_s, 3),
                "stale_after_s": self._fmt_optional_float(stale_after, 3),
                "market_id": str((heartbeat or {}).get("market_id") or ""),
                "source_instance_id": str((heartbeat or {}).get("source_instance_id") or ""),
            },
        )

    def _process_mirror_delivery(
        self,
        stream_id: str,
        signal: Optional[MirrorTradeSignal],
        tick: Optional[TickData],
        *,
        source: str,
    ) -> None:
        if signal is None:
            subscriber = getattr(self, "_mirror_subscriber", None)
            if subscriber is not None and stream_id and hasattr(subscriber, "ack_message"):
                try:
                    subscriber.ack_message(stream_id)
                except Exception:
                    pass
            return
        self._note_mirror_signal_received(signal, stream_id=stream_id, source=source)
        self._process_mirror_signal(signal, tick, stream_id=stream_id)

    def _validate_mirror_signal(self, signal: MirrorTradeSignal) -> tuple[bool, str]:
        if not signal.signal_id:
            return False, "missing_signal_id"
        if self.state.has_processed_mirror_signal(signal.signal_id):
            return False, "duplicate"
        if signal.event not in MIRROR_ACTION_EVENTS:
            return False, "diagnostic_only"
        if self._mirror_instance_id is None:
            return False, "mirror_instance_unresolved"
        if int(signal.source_instance_id) != int(self._mirror_instance_id):
            return False, "wrong_instance"
        if not signal.source_config_hash:
            return False, "missing_config_hash"
        if signal.source_config_hash != self._mirror_config_hash:
            return False, "wrong_config_hash"
        now = datetime.now(timezone.utc)
        publish_ts = signal.publish_timestamp or signal.dry_tick_timestamp
        if signal.event == "ENTRY":
            max_entry_age = float(
                getattr(self.config, "mirror_signal_entry_max_consumption_age_seconds", 15.0)
                or 0.0
            )
            if max_entry_age > 0:
                if publish_ts is None:
                    return False, "entry_signal_missing_timestamp"
                if (now - publish_ts).total_seconds() > max_entry_age:
                    return False, "entry_signal_too_old"
        max_age = float(getattr(self.config, "mirror_signal_max_age_seconds", 30.0) or 0.0)
        if publish_ts is not None and max_age > 0:
            if (now - publish_ts).total_seconds() > max_age:
                return False, "stale"
        if signal.expires_at is not None and now > signal.expires_at:
            return False, "expired"
        return True, ""

    def _position_for_mirror_signal(self, signal: MirrorTradeSignal) -> Optional[Position]:
        for position in self.state.open_positions:
            if signal.market_id and position.market_id != signal.market_id:
                continue
            if signal.direction and position.direction.value != signal.direction:
                continue
            return position
        return None

    def _build_mirror_entry_opportunity(
        self,
        signal: MirrorTradeSignal,
        tick: TickData,
    ) -> Optional[dict]:
        if signal.market_id and signal.market_id != tick.market_id:
            return None
        direction = signal.direction
        if direction not in ("UP", "DOWN"):
            return None
        is_up = direction == "UP"
        token_id = tick.up_token_id if is_up else tick.down_token_id
        if not token_id:
            return None
        book = tick.up_order_book if is_up else tick.down_order_book
        executable_price = (
            signal.trigger_ask
            or signal.dry_entry_price
            or (book.best_ask if book and book.best_ask else None)
            or (tick.poly_prob_up if is_up else tick.poly_prob_down)
        )
        model_prob = tick.prob_above if is_up else tick.prob_below
        edge = model_prob / executable_price if executable_price and executable_price > 0 else 0.0
        return {
            "direction": direction,
            "edge": edge,
            "polymarket_prob": executable_price,
            "model_prob": model_prob,
            "required_prob": 0.0,
            "token_id": token_id,
            "tick": tick,
            "mirror_signal_id": signal.signal_id,
            "mirror_controlled_entry": True,
        }

    # ------------------------------------------------------------------
    # CLOB taker-health — keep live entries matched with the dry-run grid.
    # The live trader is the sole writer of the shared flag file (results/
    # 15m_clob_health.json): a periodic read-only probe writes it when the
    # market is affirmatively not accepting orders, and the order path writes
    # it when a real taker order is refused. The grid and all live entry gates
    # only READ the flag, so dry-run and live skip the same windows.
    # ------------------------------------------------------------------

    def _clob_health_path(self) -> str:
        return (
            getattr(self.config, "clob_health_shared_path", None)
            or "results/15m_clob_health.json"
        )

    def _clob_taker_block_reason(self) -> Optional[str]:
        """Cheap pre-entry check (file flag only) shared by every live entry
        gate. Returns a block reason, or None when clear."""
        if not getattr(self.config, "clob_health_gate_enabled", True):
            return None
        active, reason, _until = clob_health.taker_outage_active(self._clob_health_path())
        if active:
            return reason or "taker_outage"
        # Optional second gate: the normalized availability ledger (confirmed
        # intervals only). Config-controlled; fail-open inside untradable_at.
        if getattr(self.config, "availability_gate_enabled", False):
            try:
                from scripts.trader import availability as av

                led_active, led_reason, _iv = av.untradable_at(
                    datetime.now(timezone.utc),
                    market_id=getattr(self, "_current_market_id", None),
                    ledger_path=getattr(self.config, "availability_ledger_path", None)
                    or "results/availability_ledger.csv",
                    min_confidence=getattr(self.config, "availability_gate_min_confidence", "confirmed")
                    or "confirmed",
                )
                if led_active:
                    return led_reason or "availability_ledger_outage"
            except Exception:  # noqa: BLE001 - gate must never crash entry
                return None
        return None

    def _maybe_refresh_clob_taker_health(self) -> None:
        """Per-tick (when flat) read-only probe. Persists an affirmative
        "not accepting orders" verdict to the shared flag so the grid sees it
        too. Network call is cached inside clob_reads_healthy(); never clears a
        flag (that is left to a successful order / self-expiry)."""
        if not getattr(self.config, "clob_health_gate_enabled", True):
            return
        if not (self.executor.is_live or getattr(self.config, "live_shadow_mode", False)):
            return
        ttl = float(getattr(self.config, "clob_reads_probe_ttl_seconds", 8.0) or 8.0)
        try:
            healthy, label = clob_reads_healthy(ttl=ttl)
        except Exception as exc:  # pragma: no cover - probe must never crash a tick
            log.debug("clob_reads probe error (ignored): %s", exc)
            return
        if healthy:
            # Reads recovered: close a read-probe-opened outage so the audit log
            # records a real recovery edge. This fixes the 454-start / 0-clear
            # interval problem — without it the read probe
            # only ever wrote outage_start rows. Only clear outages WE opened via
            # the read probe: an order-POST post_only/425 taker outage can persist
            # while read-only checks look fine, so leave those to a successful
            # order / self-expiry to avoid re-opening the dry-run/live parity gap.
            active, reason, _until = clob_health.taker_outage_active(self._clob_health_path())
            if active and str(reason or "").startswith("reads:"):
                clob_health.mark_healthy(
                    path=self._clob_health_path(),
                    source="live_reads_probe",
                    reason="reads_recovered",
                )
                self.log("CLOB taker-health: reads probe recovered; cleared read-probe outage")
                self._refresh_availability_ledger()
            return
        # Only re-write while not already flagged, to avoid extending the window
        # every tick (which would never let it expire while reads stay stale).
        active, _reason, _until = clob_health.taker_outage_active(self._clob_health_path())
        if active:
            return
        until = clob_health.mark_unhealthy(
            f"reads:{label}",
            path=self._clob_health_path(),
            source="live_reads_probe",
            default_cooldown=float(
                getattr(self.config, "clob_health_default_cooldown_seconds", 90.0) or 90.0
            ),
            max_cooldown=float(
                getattr(self.config, "clob_health_max_cooldown_seconds", 120.0) or 120.0
            ),
        )
        self.log(f"CLOB taker-health: reads probe UNHEALTHY ({label}); skipping entries until {until.isoformat()}")
        self.state.log_risk_event(
            kind="clob_taker_outage",
            details={"source": "live_reads_probe", "label": label or "", "unhealthy_until": until.isoformat()},
        )
        self._refresh_availability_ledger()

    def _record_polymarket_availability(self) -> None:
        """Log Polymarket maintenance (down/up) edges to the shared audit CSV.

        Makes the unavailability record cover trading-in-general outages, not
        just CLOB taker outages. Edge-triggered: one ``polymarket_down`` row when
        status flips to down, one ``polymarket_up`` (with duration) when it
        recovers. Best-effort; never raises in the tick path.
        """
        if not (self.executor.is_live or getattr(self.config, "live_shadow_mode", False)):
            return
        try:
            operational = is_polymarket_operational()
        except Exception as exc:  # pragma: no cover - status probe must never crash a tick
            log.debug("polymarket status probe error (ignored): %s", exc)
            return
        prev = getattr(self, "_polymarket_available_last", None)
        if prev is None or operational == prev:
            self._polymarket_available_last = operational
            return
        self._polymarket_available_last = operational
        if not operational:
            self._polymarket_down_since = datetime.now(timezone.utc)
            label = polymarket_status_label() or "down"
            clob_health.log_availability_event(
                "polymarket_down",
                path=self._clob_health_path(),
                reason=label,
                source="polymarket_status",
            )
            self.log(f"Polymarket unavailable ({label}); recorded in availability audit log")
            self._refresh_availability_ledger()
        else:
            since = getattr(self, "_polymarket_down_since", None)
            duration = (
                (datetime.now(timezone.utc) - since).total_seconds() if since else None
            )
            self._polymarket_down_since = None
            clob_health.log_availability_event(
                "polymarket_up",
                path=self._clob_health_path(),
                reason="operational",
                source="polymarket_status",
                duration_seconds=duration,
            )
            self.log("Polymarket operational again; recorded in availability audit log")
            self._refresh_availability_ledger()

    def _seed_polymarket_status_state(self) -> None:
        """Resume an in-progress Polymarket outage across a restart (startup).

        ``_record_polymarket_availability`` is edge-triggered on in-memory
        state, which a restart wipes: restarting *during* an outage re-seeds
        the baseline silently, so the eventual recovery never writes its
        ``polymarket_up`` close edge and the ``polymarket_down`` row stays
        unclosed (downstream it then only seals at the conservative
        ``STATUS_DOWN_SELF_EXPIRY_SECONDS`` estimate). If the audit log's last
        status edge is ``down`` and Polymarket is *still* down right now, seed
        the recorder so the true recovery transition fires with the original
        outage start. Deliberately does nothing when Polymarket already
        recovered while we were off: writing a close edge at startup time
        would falsely cover the whole offline span (the estimate bounds it
        far more tightly). Best-effort; never raises.
        """
        if not (self.executor.is_live or getattr(self.config, "live_shadow_mode", False)):
            return
        try:
            log_path = clob_health.health_log_path(self._clob_health_path())
            if not log_path.exists():
                return
            last_edge: Optional[tuple[str, str]] = None
            with log_path.open("r", encoding="utf-8", newline="") as fh:
                for row in csv.DictReader(fh):
                    ev = (row.get("event") or "").strip()
                    if ev in ("polymarket_down", "polymarket_up"):
                        last_edge = (ev, (row.get("timestamp") or "").strip())
            if last_edge is None or last_edge[0] != "polymarket_down":
                return
            if is_polymarket_operational():
                return  # recovered while we were off — leave the estimate to bound it
            from scripts.trader import availability as av

            self._polymarket_available_last = False
            self._polymarket_down_since = av.parse_dt(last_edge[1])
            self.log(
                "Polymarket status: resuming inside the outage recorded at "
                f"{last_edge[1]}; its recovery edge will close that period"
            )
        except Exception as exc:  # noqa: BLE001 - seeding must never block startup
            log.debug("polymarket status state seed failed (ignored): %s", exc)

    def _record_clob_order_outcome(self, result) -> None:
        """Update the shared flag from a real order result: clear on success,
        flag a taker outage on a taker-blocking failure (so the grid skips)."""
        if not self.executor.is_live:
            return
        if not getattr(self.config, "clob_health_gate_enabled", True):
            return
        path = self._clob_health_path()
        if getattr(result, "success", False):
            clob_health.mark_healthy(path=path)
            self._refresh_availability_ledger()
            return
        err = getattr(result, "error_message", "") or ""
        if not clob_health.is_taker_blocking_error(err):
            return
        retry_after = clob_health.parse_retry_after(err)
        # Carry the parsed status code + the original error text into the audit
        # log so the availability ledger classifies the outage scope correctly
        # (425/503 → platform, 429 → account, "request exception" → bot_host).
        # A bare "order_post_refused" reason with no status code classified as
        # unknown, which the platform/market gate and the shared 5m export both
        # drop — so real POST-discovered outages never gated or shared.
        status_code = clob_health.parse_status_code(err)
        until = clob_health.mark_unhealthy(
            f"order_post_refused: {err[:160]}",
            path=path,
            status_code=status_code,
            retry_after=retry_after,
            default_cooldown=float(
                getattr(self.config, "clob_health_default_cooldown_seconds", 90.0) or 90.0
            ),
            max_cooldown=float(
                getattr(self.config, "clob_health_max_cooldown_seconds", 120.0) or 120.0
            ),
        )
        self.log(
            f"CLOB taker-health: marked UNHEALTHY until {until.isoformat()} "
            f"after order failure: {err[:120]}"
        )
        self.state.log_risk_event(
            kind="clob_taker_outage",
            details={
                "source": "live_order_post",
                "error": err[:200],
                "retry_after_seconds": str(retry_after or ""),
                "unhealthy_until": until.isoformat(),
            },
        )
        self._refresh_availability_ledger()

    def _refresh_availability_ledger(self) -> None:
        """Rebuild the normalized availability interval ledger from this bot's
        CLOB-health audit log so the optional forward gate, the dashboard, and
        the dry-run-only 5m bot have current interval truth.

        Sourced from the (now close-edged) health log only — cheap and bounded by
        the number of outages, never the 335 MB trades log. The offline cleaner
        additionally folds in the mirror audit for the discovery-lag Tier-A case.
        Best-effort and **never raises** in the order/tick path.
        """
        if not getattr(self.config, "availability_ledger_enabled", True):
            return
        try:
            from scripts.trader import availability as av

            health_log = clob_health.health_log_path(self._clob_health_path())
            events = av.events_from_health_log(health_log, source_bot="15m")
            gap = float(getattr(self.config, "availability_coalesce_gap_seconds", 120.0) or 120.0)
            intervals = av.build_intervals(events, coalesce_gap_seconds=gap)
            ledger = (
                getattr(self.config, "availability_ledger_path", None)
                or "results/availability_ledger.csv"
            )
            av.write_ledger(intervals, ledger_file=ledger)
            shared = getattr(self.config, "shared_availability_ledger_path", "") or ""
            if shared:
                platform = [iv for iv in intervals if iv.scope == av.SCOPE_PLATFORM]
                av.write_ledger(platform, ledger_file=shared)
        except Exception as exc:  # noqa: BLE001 - never break the order/tick path
            log.debug("availability ledger refresh failed (ignored): %s", exc)

    def _handle_mirror_entry(
        self,
        signal: MirrorTradeSignal,
        tick: Optional[TickData],
    ) -> str:
        if not getattr(self.config, "mirror_controls_entry", True):
            return "entry_control_disabled"
        if self.state.has_open_position():
            return "already_open"
        if signal.market_id and signal.market_id == self.state.last_entered_market_id:
            self.state.log_risk_event(
                kind="mirror_entry_rejected_already_entered_market",
                details={"signal_id": signal.signal_id, "market_id": signal.market_id},
            )
            return "already_entered_market"
        if tick is None:
            self._queue_async_mirror_entry(signal)
            return "entry_retryable:no_tick"
        opportunity = self._build_mirror_entry_opportunity(signal, tick)
        if opportunity is None:
            return "entry_signal_market_or_token_mismatch"
        # CLOB taker-health gate: if the exchange is refusing taker orders, skip
        # without firing a doomed order. The dry-run grid gates on the same flag,
        # so it won't have published/recorded this entry either — they stay
        # matched. Non-retryable: requeuing would only expire against the 30s TTL.
        clob_block = self._clob_taker_block_reason()
        if clob_block:
            self._log_mirror_signal_audit(
                signal,
                stage="attempt_result",
                status="rejected",
                reason="clob_taker_outage",
                details={"clob_reason": clob_block},
            )
            self.state.log_risk_event(
                kind="mirror_entry_clob_taker_outage",
                details={
                    "signal_id": signal.signal_id,
                    "market_id": signal.market_id,
                    "clob_reason": clob_block,
                },
            )
            self.log(f"Mirror ENTRY skipped: CLOB taker-health unhealthy ({clob_block})")
            return "clob_taker_outage"
        attempt_number = int(self._mirror_entry_retry_attempts.get(signal.signal_id, 0) or 0) + 1
        latency = self._mirror_signal_latency_details(signal)
        self._log_mirror_signal_audit(
            signal,
            stage="attempt",
            status="attempting",
            reason="live_entry",
            details=latency,
            attempt=attempt_number,
        )
        warn_after = float(getattr(self.config, "mirror_signal_latency_warn_seconds", 1.0) or 0.0)
        publish_to_attempt = latency.get("publish_to_attempt_s")
        if warn_after > 0 and publish_to_attempt:
            try:
                latency_value = float(publish_to_attempt)
            except ValueError:
                latency_value = 0.0
            if latency_value > warn_after:
                self.state.log_risk_event(
                    kind="mirror_entry_latency_warning",
                    details={
                        "signal_id": signal.signal_id,
                        "market_id": signal.market_id,
                        "publish_to_attempt_s": f"{latency_value:.3f}",
                        "warn_after_s": f"{warn_after:.3f}",
                    },
                )
                _emit(
                    "[WARN] mirror ENTRY latency "
                    f"{latency_value:.3f}s > {warn_after:.3f}s "
                    f"for {signal.signal_id}"
                )
        attempt = self.execute_entry(opportunity)
        if not attempt.entered:
            details = attempt.rejection_details or {}
            audit_details = {
                "rejection_details": details,
                "order_metadata": attempt.order_metadata or {},
                "ambiguous": str(bool(attempt.ambiguous)),
            }
            if self._is_retryable_mirror_entry_failure(attempt):
                reason = attempt.rejection_reason or "unknown"
                self.state.log_risk_event(
                    kind="mirror_entry_retryable_failed",
                    details={
                        "signal_id": signal.signal_id,
                        "market_id": signal.market_id,
                        "reason": reason,
                        "details": json.dumps(details, sort_keys=True)[:200],
                    },
                )
                self._log_mirror_signal_audit(
                    signal,
                    stage="attempt_result",
                    status="retryable",
                    reason=reason,
                    details=audit_details,
                    attempt=attempt_number,
                    rechecked_price=attempt.rechecked_price,
                    order_id=attempt.order_id or "",
                )
                self._queue_retryable_mirror_entry(
                    signal,
                    reason=reason,
                    details=details,
                )
                return f"entry_retryable:{reason}"
            self._log_mirror_signal_audit(
                signal,
                stage="attempt_result",
                status="rejected",
                reason=attempt.rejection_reason or "unknown",
                details=audit_details,
                attempt=attempt_number,
                rechecked_price=attempt.rechecked_price,
                order_id=attempt.order_id or "",
            )
            self.state.log_risk_event(
                kind="mirror_entry_failed",
                details={
                    "signal_id": signal.signal_id,
                    "reason": attempt.rejection_reason,
                    "details": json.dumps(details, sort_keys=True)[:200],
                },
            )
            return f"entry_failed:{attempt.rejection_reason or 'unknown'}"
        if attempt.position is not None:
            attempt.position.opened_from_mirror_signal_id = signal.signal_id
            self.state.save()
            self._log_mirror_signal_audit(
                signal,
                stage="attempt_result",
                status="accepted",
                reason="entered",
                details={
                    "token_id": getattr(attempt, "token_id", "") or getattr(attempt.position, "token_id", ""),
                    "shares": getattr(attempt.position, "shares", ""),
                    "entry_price": getattr(attempt.position, "entry_price", ""),
                    "cost_basis": getattr(attempt.position, "cost_basis", ""),
                    "order_metadata": getattr(attempt, "order_metadata", {}) or {},
                },
                attempt=attempt_number,
                rechecked_price=getattr(attempt, "rechecked_price", None),
                order_id=getattr(attempt, "order_id", "") or getattr(attempt.position, "order_id", "") or "",
                live_position_id=str(getattr(attempt.position, "id", "") or ""),
            )
        return "entered"

    @staticmethod
    def _mirror_entry_failure_text(attempt: EntryAttemptResult) -> str:
        parts = [
            attempt.rejection_reason or "",
            str(attempt.rejection_details.get("error", "") if attempt.rejection_details else ""),
            json.dumps(attempt.order_metadata or {}, sort_keys=True),
        ]
        return " ".join(parts).lower()

    def _is_retryable_mirror_entry_failure(self, attempt: EntryAttemptResult) -> bool:
        reason = attempt.rejection_reason or ""
        if reason in {
            "no_market",
            "no_token_id",
            "concurrency_gate",
            "price_above_max",
            "insufficient_capital",
            "slippage_rejected",
            "spot_drift_rejected",
            "fill_above_cap",
            "token_balance_not_visible",
            "cancel_ambiguous",
            "order_ambiguous",
            "phantom_entry_blocked",
            "entry_partial_below_minimum",
            "tp_filled_immediately",
        }:
            return False
        if reason in _RETRYABLE_MIRROR_ENTRY_REASONS:
            return True
        if reason == "delayed_book_recheck_failed":
            detail_reason = str((attempt.rejection_details or {}).get("reason") or "")
            if detail_reason == "price_above_max_entry":
                return False
            if detail_reason == "edge_lost_after_recheck":
                return True
            return detail_reason in {
                "book_unavailable",
                "current_book_no_fill",
                "delayed_book_no_fill",
            }
        text = self._mirror_entry_failure_text(attempt)
        return any(token in text for token in _RETRYABLE_MIRROR_ENTRY_ERROR_TOKENS)

    def _handle_mirror_stop_loss(self, signal: MirrorTradeSignal) -> str:
        if not getattr(self.config, "mirror_controls_stop_loss", True):
            return "stop_loss_control_disabled"
        position = self._position_for_mirror_signal(signal)
        if position is None:
            return "already_flat"
        signal_bid = signal.trigger_bid
        try:
            current_bid = self.executor.get_best_bid(position.token_id)
        except Exception:
            current_bid = None
        if current_bid is None:
            current_bid = signal_bid
        if current_bid is None:
            self.state.log_risk_event(
                kind="mirror_stop_loss_no_bid",
                details={"signal_id": signal.signal_id, "market_id": signal.market_id},
            )
            return "no_bid"
        self.state.log_risk_event(
            kind="mirror_stop_loss_bid_selected",
            details={
                "signal_id": signal.signal_id,
                "market_id": signal.market_id,
                "fresh_bid": self._fmt_optional_float(current_bid, 4),
                "signal_bid": self._fmt_optional_float(signal_bid, 4),
                "signal_age_s": self._fmt_optional_float(
                    self._mirror_signal_age_seconds(signal), 3
                ),
            },
        )
        self._execute_stop_loss_live(position, float(current_bid))
        if position not in self.state.open_positions:
            return "stop_loss_closed"
        if getattr(position, "pending_sl_order_id", None):
            return "stop_loss_pending_limit"
        self.state.log_risk_event(
            kind="mirror_stop_loss_not_closed",
            details={
                "signal_id": signal.signal_id,
                "market_id": signal.market_id,
                "position_id": position.id,
            },
        )
        return "stop_loss_not_closed"

    def _handle_mirror_tp_signal(self, signal: MirrorTradeSignal) -> str:
        action = getattr(self.config, "dry_tp_signal_action", "ignore")
        if action == "ignore":
            return "tp_signal_ignored"
        position = self._position_for_mirror_signal(signal)
        if position is None:
            return "already_flat"
        if action == "immediate_market_exit":
            return self._execute_tp_signal_market_exit(
                position,
                getattr(self, "_last_tick", None),
            )
        now = datetime.now(timezone.utc)
        grace = max(0.0, float(getattr(self.config, "dry_tp_live_fill_grace_seconds", 120.0) or 0.0))
        position.pending_dry_tp_signal_id = signal.signal_id
        position.dry_tp_seen_at = now
        position.dry_tp_market_exit_deadline = now + timedelta(seconds=grace)
        self.state.save()
        return "tp_fallback_timer_started"

    def _process_mirror_signal(
        self,
        signal: MirrorTradeSignal,
        tick: Optional[TickData],
        *,
        stream_id: str = "",
    ) -> None:
        if stream_id:
            self._mirror_signal_stream_ids[signal.signal_id] = stream_id
        if (
            signal.event == "ENTRY"
            and signal.signal_id
            and signal.signal_id not in self._mirror_signal_received_at
            and not self.state.has_processed_mirror_signal(signal.signal_id)
        ):
            self._note_mirror_signal_received(signal, stream_id=stream_id, source="poll")
        valid, reason = self._validate_mirror_signal(signal)
        if not valid:
            if reason == "duplicate":
                self._clear_mirror_entry_retry_state(signal.signal_id)
                self._ack_mirror_signal(signal, "duplicate")
                self._ack_mirror_delivery(signal, stream_id=stream_id)
                return
            self._clear_mirror_entry_retry_state(signal.signal_id)
            terminal_status = "diagnostic_only" if reason == "diagnostic_only" else "rejected"
            self.state.mark_mirror_signal_processed(
                signal.signal_id,
                status=terminal_status,
                reason=reason,
                stream_id=stream_id or self._mirror_signal_stream_ids.get(signal.signal_id, ""),
                event=signal.event,
                market_id=signal.market_id,
                direction=signal.direction,
                source_instance_id=str(signal.source_instance_id),
            )
            self._ack_mirror_signal(signal, "rejected", reason)
            self._ack_mirror_delivery(signal, stream_id=stream_id)
            if signal.event == "ENTRY":
                self._log_mirror_signal_audit(
                    signal,
                    stage="rejected",
                    status="rejected",
                    reason=reason,
                )
            if reason not in {"diagnostic_only", "wrong_instance"}:
                self.state.log_risk_event(
                    kind="mirror_signal_rejected",
                    details={
                        "signal_id": signal.signal_id,
                        "event": signal.event,
                        "reason": reason,
                        "source_instance_id": signal.source_instance_id,
                        "market_id": signal.market_id,
                    },
                )
            self.state.save()
            return

        if signal.event in {"TP_FILLED", "STOP_LOSS"}:
            self._cancel_pending_mirror_entry_retries_for_exit(signal)

        if signal.event == "ENTRY":
            status = self._handle_mirror_entry(signal, tick)
        elif signal.event == "STOP_LOSS":
            status = self._handle_mirror_stop_loss(signal)
        elif signal.event == "TP_FILLED":
            status = self._handle_mirror_tp_signal(signal)
        else:
            status = "diagnostic_only"

        ack_status = "accepted" if status in {
            "entered",
            "stop_loss_closed",
            "stop_loss_pending_limit",
            "tp_fallback_timer_started",
            "market_exited",
            "already_flat_on_chain",
            "pending_settlement",
            "expired_or_settlement_pending",
            "proceeds_unverified",
            "already_open",
            "already_flat",
        } else "rejected"
        if status.startswith("entry_retryable:"):
            ack_status = "retryable"
        retryable_statuses = {
            "no_bid",
            "stop_loss_not_closed",
            "tp_cancel_failed",
            "balance_unknown",
            "market_sell_failed",
        }
        if status in retryable_statuses:
            ack_status = "retryable"
        ack_details = {"handler_status": status}
        if signal.event == "ENTRY":
            retry_attempts = int(self._mirror_entry_retry_attempts.get(signal.signal_id, 0) or 0)
            ack_attempt = retry_attempts if ack_status == "retryable" else retry_attempts + 1
            ack_details["attempt"] = str(
                max(1, ack_attempt)
            )
            position = self._position_for_mirror_signal(signal)
            if position is not None:
                ack_details.update({
                    "live_order_id": getattr(position, "order_id", "") or "",
                    "live_position_id": str(getattr(position, "id", "") or ""),
                    "live_token_id": getattr(position, "token_id", "") or "",
                    "live_shares": self._fmt_optional_float(getattr(position, "shares", None), 4),
                    "live_entry_price": self._fmt_optional_float(getattr(position, "entry_price", None), 4),
                })
        if status not in retryable_statuses:
            if ack_status == "retryable":
                self.state.record_mirror_signal_status(
                    signal.signal_id,
                    status="retryable",
                    reason=status,
                    stream_id=stream_id or self._mirror_signal_stream_ids.get(signal.signal_id, ""),
                    event=signal.event,
                    market_id=signal.market_id,
                    direction=signal.direction,
                    source_instance_id=str(signal.source_instance_id),
                    details=ack_details,
                )
                self._ack_mirror_signal(signal, ack_status, status, details=ack_details)
                self.state.save()
                return
            self._clear_mirror_entry_retry_state(signal.signal_id)
            live_order_id = str(ack_details.get("live_order_id") or "")
            live_position_id = str(ack_details.get("live_position_id") or "")
            terminal_status = ack_status if ack_status in {"accepted", "rejected"} else "processed"
            self.state.mark_mirror_signal_processed(
                signal.signal_id,
                status=terminal_status,
                reason=status,
                stream_id=stream_id or self._mirror_signal_stream_ids.get(signal.signal_id, ""),
                event=signal.event,
                market_id=signal.market_id,
                direction=signal.direction,
                source_instance_id=str(signal.source_instance_id),
                attempts=int(ack_details["attempt"]) if str(ack_details.get("attempt") or "").isdigit() else None,
                live_order_id=live_order_id,
                live_position_id=live_position_id,
                details=ack_details,
            )
            self._ack_mirror_delivery(signal, stream_id=stream_id)
        else:
            self.state.record_mirror_signal_status(
                signal.signal_id,
                status="retryable",
                reason=status,
                stream_id=stream_id or self._mirror_signal_stream_ids.get(signal.signal_id, ""),
                event=signal.event,
                market_id=signal.market_id,
                direction=signal.direction,
                source_instance_id=str(signal.source_instance_id),
                details=ack_details,
            )
        self._ack_mirror_signal(signal, ack_status, status, details=ack_details)
        self.state.save()

    def _process_mirror_signals(self, tick: Optional[TickData]) -> None:
        if not self._mirror_control_enabled():
            return
        if not getattr(self, "_mirror_signals_lock_active", False):
            with self._mirror_action_lock:
                self._mirror_signals_lock_active = True
                try:
                    return self._process_mirror_signals(tick)
                finally:
                    self._mirror_signals_lock_active = False
        self._recover_pending_mirror_signals(tick)
        if self._mirror_async_enabled():
            thread = getattr(self, "_mirror_listener_thread", None)
            if thread is not None and thread.is_alive():
                return
        pending_entry_ids = {
            item.signal_id for item in getattr(self, "_pending_async_mirror_entries", [])
        }
        for stream_id, signal in self._poll_mirror_signals():
            if signal is None:
                subscriber = getattr(self, "_mirror_subscriber", None)
                if subscriber is not None and stream_id and hasattr(subscriber, "ack_message"):
                    subscriber.ack_message(stream_id)
                continue
            if signal.event == "ENTRY" and signal.signal_id in pending_entry_ids:
                continue
            self._process_mirror_delivery(stream_id, signal, tick, source="sync_poll")

    def _execute_tp_signal_market_exit(self, position: Position, tick: Optional[TickData]) -> str:
        if not getattr(self.config, "dry_tp_market_exit_enabled", True):
            return "market_exit_disabled"
        if getattr(self.config, "dry_tp_market_exit_skip_if_expired", True):
            if getattr(position, "pending_settlement", False):
                return "pending_settlement"
            if tick is not None and (tick.is_expired or tick.settlement_pending):
                return "expired_or_settlement_pending"

        timestamp = datetime.now().strftime("%H:%M:%S")
        if position.tp_order_id:
            if not self.executor.cancel_order(position.tp_order_id):
                self.state.log_risk_event(
                    kind="tp_signal_market_exit_cancel_failed",
                    details={
                        "position_id": position.id,
                        "tp_order_id": position.tp_order_id,
                        "market_id": position.market_id,
                    },
                )
                _emit(f"[{timestamp}] TP-SIGNAL EXIT: TP cancel failed; market sell skipped")
                return "tp_cancel_failed"
            _emit(f"[{timestamp}] TP-SIGNAL EXIT: Cancelled TP order {position.tp_order_id}")
            position.tp_order_id = None
            self.state.save()

        try:
            actual_shares = self.executor.get_position(position.token_id, strict=True)
        except RuntimeError as exc:
            self.state.log_risk_event(
                kind="tp_signal_market_exit_balance_unknown",
                details={"position_id": position.id, "error": str(exc)[:160]},
            )
            return "balance_unknown"
        if actual_shares <= 0.01:
            return "already_flat_on_chain"

        sell_shares = math.floor(actual_shares * 100) / 100
        result = self.executor.sell_market(position.token_id, sell_shares)
        if not result.success:
            self.state.log_risk_event(
                kind="tp_signal_market_exit_sell_failed",
                details={
                    "position_id": position.id,
                    "error": (result.error_message or "")[:160],
                    "order_id": result.order_id or "",
                },
            )
            return "market_sell_failed"

        fallback_price = float(getattr(position, "tp_price", 0.0) or 0.0)
        position.last_exit_order_id = result.order_id
        self.state.save()
        exit_price = result.avg_fill_price if result.avg_fill_price > 0 else fallback_price
        proceeds = (
            float(result.shares_filled) * float(result.avg_fill_price)
            if result.shares_filled and result.avg_fill_price
            else None
        )
        try:
            trade = self._record_exit(
                position,
                exit_price,
                TradeResult.TP_SIGNAL_MARKET_EXIT,
                proceeds_override=proceeds,
            )
        except ProceedsUnverifiedError as exc:
            position.pending_settlement = True
            position.pending_settlement_reason = "tp_signal_market_exit_proceeds_unverified"
            self.state.save()
            _emit(f"[{timestamp}] TP-SIGNAL EXIT deferred: {exc}")
            return "proceeds_unverified"
        _emit(f"\n[{timestamp}] >>> TP SIGNAL MARKET EXIT: {position.direction.value} <<<")
        _emit(f"  Exit: ${exit_price:.4f} | P&L: ${trade.pnl:+.2f} ({trade.pnl_pct:+.1f}%)")
        return "market_exited"

    def _check_dry_tp_fallbacks(self, tick: Optional[TickData]) -> None:
        if not self._mirror_control_enabled():
            return
        now = datetime.now(timezone.utc)
        changed = False
        for position in self.state.open_positions[:]:
            deadline = getattr(position, "dry_tp_market_exit_deadline", None)
            if deadline is None or now < deadline:
                continue
            status = self._execute_tp_signal_market_exit(position, tick)
            terminal_statuses = {
                "market_exited",
                "market_exit_disabled",
                "expired_or_settlement_pending",
                "pending_settlement",
                "already_flat_on_chain",
                "proceeds_unverified",
            }
            if status in terminal_statuses:
                position.pending_dry_tp_signal_id = ""
                position.dry_tp_seen_at = None
                position.dry_tp_market_exit_deadline = None
                changed = True
            if status != "market_exited":
                self.state.log_risk_event(
                    kind="tp_signal_market_exit_skipped",
                    details={
                        "position_id": position.id,
                        "market_id": position.market_id,
                        "status": status,
                    },
                )
        if changed:
            self.state.save()

    # ---------------------------------------------------------------
    # on_tick (replaces run_once)
    # ---------------------------------------------------------------

    def on_tick(self, tick: TickData) -> None:
        """Process one tick from SharedDataFeed.

        Replaces BTC_pricer's run_once(). All market data arrives in tick.
        """
        self._ensure_mirror_async_runtime()
        if self.executor.is_live:
            with self._mirror_action_lock:
                return self._on_tick_unlocked(tick)
        return self._on_tick_unlocked(tick)

    def _on_tick_unlocked(self, tick: TickData) -> None:
        """Process one tick with live state/executor mutations serialized."""
        self._last_tick = tick
        self._last_tick_received_at = datetime.now(timezone.utc)
        reconciliation = self._get_reconciliation_service()
        positions = self._get_position_tracker()
        orders = self._get_order_execution_service()
        if self.executor.is_live:
            reconciliation.reconcile_pending_entry()
            # Slow background pass: upgrade tentative trade rows
            # (proceeds_source == "order_status" / "planned") to the
            # exact /data/trades value once Polymarket has indexed
            # the matched sub-trades. Cadence configurable; default
            # 60 s. Cheap when nothing's tentative.
            reconciliation.maybe_run_history_reconciler()

        # 0. Startup reconciliation (first live tick — always seed timestamp
        #    so periodic reconciliation can start even if bot starts flat)
        if (self.executor.is_live and self.config.reconciliation_enabled
                and self._last_reconciliation is None):
            if self.state.open_positions:
                _emit(f"Reconciling {len(self.state.open_positions)} open position(s)...")
                reconciliation.reconcile_positions()
                positions.verify_tp_orders()
            reconciliation.mark_reconciled_now()

        # 1. Market transition detection
        #    On restart, _current_market_id is None — fall back to persisted
        #    state.current_market to detect transitions against old market.
        if tick.market_id != self._current_market_id:
            old_market_id = self._current_market_id
            if old_market_id is None and self.state.current_market:
                old_market_id = self.state.current_market.get("market_id")

            if old_market_id is not None and old_market_id != tick.market_id and self.state.open_positions:
                if self.executor.is_live:
                    stale_positions = [
                        p for p in self.state.open_positions
                        if p.market_id and p.market_id != tick.market_id
                    ]
                    if stale_positions:
                        _emit(f"\n>>> Market transition: resolving {len(stale_positions)} old live position(s) <<<")
                    for position in stale_positions:
                        self._attempt_live_position_settlement(position)
                else:
                    _emit(f"\n>>> Market transition: settling {len(self.state.open_positions)} position(s) <<<")
                    self._settle_positions(tick)

            # Force a full reconcile pass at the transition boundary — this is
            # the highest-risk moment for 15m markets. Don't wait up to 300s for
            # the periodic sweep; catch partial settlements, orphan positions,
            # and dangling TP/trail orders before the next entry window opens.
            if (old_market_id is not None
                    and old_market_id != tick.market_id
                    and self.executor.is_live
                    and self.config.reconciliation_enabled):
                try:
                    reconciliation.reconcile_positions()
                    reconciliation.detect_orphans()
                    positions.verify_tp_orders()
                except Exception as exc:
                    _emit(f"[WARN] Transition reconcile failed: {exc}")
                reconciliation.mark_reconciled_now()

            if tick.market_id:
                self._current_market_id = tick.market_id
                self._sync_current_market_from_tick(tick)
                if tick.is_new_market or old_market_id != tick.market_id:
                    _emit(f"\nNew market: {tick.market_title}")
                    _emit(f"Barrier: ${tick.barrier:,.2f}")
                if self.executor.is_live:
                    reconciliation.detect_orphans()
        elif tick.market_id:
            self._sync_current_market_from_tick(tick)

        if self.executor.is_live and self.state.pending_entry:
            reconciliation.reconcile_pending_entry()

        if self.executor.is_live:
            self._retry_pending_settlements()
            if self._has_pending_settlement():
                pending_pos = next(
                    (p for p in self.state.open_positions if getattr(p, "pending_settlement", False)),
                    None,
                )
                ps_detail = (
                    f"pending_settlement:market={getattr(pending_pos, 'market_id', '?')}"
                    f":reason={getattr(pending_pos, 'pending_settlement_reason', '') or '?'}"
                    if pending_pos else "pending_settlement"
                )
                orders.log_live_tick_diagnostics(
                    tick,
                    skipped_before_eval=True,
                    pre_eval_skip_reason=ps_detail,
                )
                return

        # 2. Handle expiry for current market
        if tick.is_expired and self.state.open_positions:
            self._settle_positions(tick)
            orders.log_live_tick_diagnostics(
                tick,
                skipped_before_eval=True,
                pre_eval_skip_reason="expired_with_open_position",
            )
            return

        # Skip further processing if market expired or no barrier
        if tick.is_expired or tick.spot_price <= 0:
            skip_reason = "expired" if tick.is_expired else "invalid_spot"
            orders.log_live_tick_diagnostics(
                tick,
                skipped_before_eval=True,
                pre_eval_skip_reason=skip_reason,
            )
            return

        # 3. Check TP fills
        positions.check_fills()

        # Record Polymarket maintenance (down/up) edges to the availability audit
        # log every tick, so position-holding outages are captured too.
        self._record_polymarket_availability()

        if self._mirror_control_enabled():
            self._check_mirror_publisher_heartbeat()
            self._drain_pending_async_mirror_entries(tick)
            self._process_mirror_signals(tick)
            self._check_dry_tp_fallbacks(tick)

        # 4. Evaluate edge and execute entry
        skipped_before_eval = False
        pre_eval_skip_reason = ""
        edge_result = None
        entry_attempt: Optional[EntryAttemptResult] = None
        if not self.state.has_open_position():
            # Refresh the shared CLOB taker-health flag from the read-only probe
            # (cached) so the dry-run grid — which only reads the flag — sees an
            # affirmative outage even between mirror signals.
            self._maybe_refresh_clob_taker_health()
            # Entry guards (15-min timing)
            can_enter = True

            if (
                self._mirror_control_enabled()
                and not getattr(self.config, "local_entry_enabled", True)
            ):
                can_enter = False
                if not pre_eval_skip_reason:
                    pre_eval_skip_reason = "mirror_control_local_entry_disabled"

            if self.state.in_grace_period():
                can_enter = False
                if not pre_eval_skip_reason:
                    pre_eval_skip_reason = "grace_period"

            if tick.elapsed_seconds < self.config.no_trade_first_seconds:
                can_enter = False
                if not pre_eval_skip_reason:
                    pre_eval_skip_reason = "blocked_first_window"

            if tick.time_remaining_seconds < self.config.no_trade_last_seconds:
                can_enter = False
                if not pre_eval_skip_reason:
                    pre_eval_skip_reason = "blocked_last_window"

            if can_enter:
                edge_result = orders.evaluate_edge(tick)
                if edge_result:
                    entry_attempt = orders.execute_entry(edge_result)
            else:
                skipped_before_eval = True
        else:
            skipped_before_eval = True
            if self.state.pending_entry is not None:
                pe = self.state.pending_entry_summary()
                pre_eval_skip_reason = (
                    f"pending_entry:age={pe.get('age_s', '?')}s"
                    f":order={pe.get('order_id', '?')}"
                    f":token={pe.get('token_id', '?')}"
                    f":market={pe.get('market_id', '?')}"
                    f":reason={pe.get('reason', '') or '?'}"
                )
            else:
                pre_eval_skip_reason = "open_position"

        if self.executor.is_live:
            orders.log_live_tick_diagnostics(
                tick,
                skipped_before_eval=skipped_before_eval,
                pre_eval_skip_reason=pre_eval_skip_reason,
                chosen_direction=edge_result["direction"] if edge_result else "",
                entry_attempt=entry_attempt,
            )

        # 5. Heartbeat
        self._print_heartbeat()

        # 6. Periodic reconciliation (live only)
        if self.executor.is_live and self.config.reconciliation_enabled:
            if reconciliation.should_run_periodic():
                reconciliation.reconcile_positions(mark_now=True)
                reconciliation.detect_orphans()

        # 7. Save state
        self.state.save()

    # ---------------------------------------------------------------
    # Pre-trade risk gates
    # ---------------------------------------------------------------

    def _check_concurrency_gate(self) -> tuple[bool, str]:
        return self._get_risk_engine().check_concurrency(
            self.state.open_positions,
            is_live=self.executor.is_live,
        )

    def _estimate_slippage_from_book(
        self,
        token_id: str,
        usd_amount: float,
    ) -> Optional[tuple[float, float, float]]:
        return self._get_risk_engine().check_slippage(token_id, usd_amount)

    def _simulate_live_entry_recheck(
        self,
        token_id: str,
        usd_amount: float,
        opportunity: dict,
    ) -> Optional[float]:
        return self._get_risk_engine().simulate_entry_recheck(
            token_id, usd_amount, opportunity
        )

    @staticmethod
    def _spot_side(spot_price: float, reference_price: float, buffer_usd: float) -> str:
        return RiskEngine.spot_side(spot_price, reference_price, buffer_usd)

    @staticmethod
    def _price_tick_down(price: float, tick: str = "0.01") -> float:
        """Quantize a buy cap down to the venue penny tick explicitly."""
        return float(Decimal(str(price)).quantize(Decimal(tick), rounding=ROUND_DOWN))

    def _live_entry_spot_recheck(
        self,
        opportunity: dict,
        reference_price: float,
        *,
        apply_live_gates: Optional[bool] = None,
    ) -> bool:
        is_live_gate = self.executor.is_live if apply_live_gates is None else apply_live_gates
        return self._get_risk_engine().check_spot_drift(
            opportunity,
            reference_price,
            is_live=is_live_gate,
            get_btc_price=self._get_fresh_spot,
        )

    def _get_fresh_spot(self) -> float | None:
        """Fetch the freshest BTC spot from the spot feed."""
        return get_current_btc_price()

    # ---------------------------------------------------------------
    # Entry execution
    # ---------------------------------------------------------------

    def _entry_pause_reason(self) -> str:
        """Read entry-only deployment control and persistent live stop intent."""
        if not getattr(self.config, "entries_enabled", True):
            return "entries_disabled"
        results_dir = Path(self.config.get_state_file()).parent
        if (results_dir / ".entries_paused").exists():
            return "entries_paused"
        if self.executor.is_live:
            from scripts.live_runtime_state import desired_live_running, safety_stop_active
            if safety_stop_active(results_dir):
                return "live_safety_stop"
            if any((results_dir / name).exists() for name in (".live_desired.json", ".live_location")) and not desired_live_running(results_dir):
                return "live_intentionally_stopped"
        return ""

    def execute_entry(self, opportunity: dict) -> EntryAttemptResult:
        """Execute a trade entry (paper or live)."""
        if getattr(self.state, "pending_entry", None):
            return EntryAttemptResult(rejection_reason="pending_entry_unresolved")
        pause_reason = self._entry_pause_reason()
        if pause_reason:
            return EntryAttemptResult(rejection_reason=pause_reason)
        market = self.state.current_market
        if not market:
            return EntryAttemptResult(rejection_reason="no_market")

        direction = opportunity["direction"]
        expected_price = opportunity.get("polymarket_prob", 1.0)
        rechecked_price: Optional[float] = None
        order_size: Optional[float] = None
        raw_order_size: Optional[float] = None
        capital_before_entry: Optional[float] = None
        capital_source = "state"
        tick = opportunity.get("tick")
        if tick is not None:
            inputs_ok, inputs_reason = entry_inputs_valid(tick)
            if not inputs_ok:
                return EntryAttemptResult(rejection_reason=inputs_reason)
            if getattr(tick, "payoff_kind", "legacy") != "legacy":
                opportunity["model_prob"] = tick.prob_above if direction == "UP" else tick.prob_below
        tick_market_id = getattr(tick, "market_id", None) if tick else None
        tick_market_title = getattr(tick, "market_title", None) if tick else None
        tick_barrier = getattr(tick, "barrier", None) if tick else None
        market_id = tick_market_id or market.get("market_id", "")
        market_title = tick_market_title or market.get("title", "")
        if tick_barrier is not None:
            reference_price = float(tick_barrier)
        else:
            reference_price = float(market.get("reference_price", 0.0) or 0.0)

        # Token ID from _evaluate_edge (preferred), fallback to state
        token_id = opportunity.get("token_id")
        if not token_id:
            token_id = market.get("clob_token_ids", {}).get(direction)

        if not token_id:
            self.log(f"No token ID found for {direction}")
            return EntryAttemptResult(
                rejection_reason="no_token_id",
                rejection_details={"direction": direction},
                direction=direction,
                signal_price=expected_price,
            )

        if tick is not None and getattr(tick, "payoff_kind", "legacy") != "legacy":
            expected_token = tick.up_token_id if direction == "UP" else tick.down_token_id
            if not expected_token or str(token_id) != str(expected_token):
                return EntryAttemptResult(rejection_reason="token_contract_mismatch")

        set_fee_schedule = getattr(self.executor, "set_fee_schedule", None)
        if callable(set_fee_schedule):
            set_fee_schedule(getattr(tick, "fee_schedule", None), token_id=token_id)

        def _no_entry(
            *,
            attempted: bool = False,
            rejection_reason: str = "",
            rejection_details: Optional[dict] = None,
            order_id: str = "",
            ambiguous: bool = False,
            order_metadata: Optional[dict] = None,
        ) -> EntryAttemptResult:
            return EntryAttemptResult(
                attempted=attempted,
                rejection_reason=rejection_reason,
                rejection_details=rejection_details or {},
                direction=direction,
                token_id=token_id,
                signal_price=expected_price,
                rechecked_price=rechecked_price,
                order_size_usd=order_size,
                raw_order_size_usd=raw_order_size,
                capital_before_entry=capital_before_entry,
                capital_source=capital_source,
                order_id=order_id,
                ambiguous=ambiguous,
                order_metadata=order_metadata or {},
            )

        # Pre-trade gate: concurrency + TP-confirmed on existing positions.
        ok, reason = self._check_concurrency_gate()
        if not ok:
            self.log(f"Entry blocked by concurrency gate: {reason}")
            self.state.log_risk_event(
                kind="entry_rejected_concurrency",
                details={"reason": reason, "direction": direction, "token_id": token_id},
            )
            return _no_entry(attempted=True, rejection_reason="concurrency_gate", rejection_details={"reason": reason})

        # Refresh balance from real USDC for live mode
        if self.executor.is_live:
            real_balance = self.executor.get_usdc_balance()
            if real_balance is not None:
                self.state.current_capital = real_balance
                capital_source = "live_usdc"
        capital_before_entry = self.state.current_capital

        # Calculate order size with floor for Polymarket minimum
        raw_order_size = self.state.current_capital * self.config.order_size_pct
        order_size = max(raw_order_size, self.config.min_order_size_usd)

        # Block entry if price too high (not enough room for TP profit)
        if expected_price > self.config.max_entry_price:
            self.log(f"Entry blocked: price ${expected_price:.4f} > max ${self.config.max_entry_price:.2f}")
            return _no_entry(
                attempted=True, rejection_reason="price_above_max",
                rejection_details={"price": f"{expected_price:.4f}", "max": f"{self.config.max_entry_price:.4f}"},
            )

        # Binary contracts cost at most $1/share, so spending min_position_shares
        # dollars guarantees at least that many shares AND ≥$5 notional (Polymarket min).
        order_size = max(order_size, self.config.min_position_shares)

        if order_size > self.state.current_capital:
            self.log(f"Insufficient capital: need ${order_size:.2f}, have ${self.state.current_capital:.2f}")
            return _no_entry(
                attempted=True, rejection_reason="insufficient_capital",
                rejection_details={"need": f"{order_size:.2f}", "have": f"{self.state.current_capital:.2f}"},
            )

        # Pre-trade gate: slippage / liquidity estimate from live order book.
        # Applied in live mode and live-shadow mode (paper with live gates).
        apply_live_gates = self.executor.is_live or self.config.live_shadow_mode
        if apply_live_gates:
            max_slip = getattr(self.config, "max_slippage_estimate_pct", 0.0)
            min_depth = getattr(self.config, "min_liquidity_depth_usd", 0.0)
            if getattr(self.config, "live_recheck_delay_seconds", 0.0) > 0:
                min_depth = 0.0
            if max_slip > 0 or min_depth > 0:
                est = self._estimate_slippage_from_book(token_id, order_size)
                if est is None:
                    self.log("Entry blocked: could not estimate slippage (no order book)")
                    self.state.log_risk_event(
                        kind="entry_rejected_book_unavailable",
                        details={"direction": direction, "token_id": token_id},
                    )
                    return _no_entry(attempted=True, rejection_reason="book_unavailable")
                slip_pct, depth_usd, best_ask = est
                if max_slip > 0 and slip_pct > max_slip:
                    self.log(f"Entry blocked: slippage {slip_pct*100:.2f}% > max {max_slip*100:.2f}% "
                             f"(best_ask=${best_ask:.4f})")
                    self.state.log_risk_event(
                        kind="entry_rejected_slippage",
                        details={
                            "direction": direction,
                            "token_id": token_id,
                            "slippage_pct": f"{slip_pct:.4f}",
                            "max_slippage_pct": f"{max_slip:.4f}",
                            "best_ask": f"{best_ask:.4f}",
                            "order_size_usd": f"{order_size:.2f}",
                        },
                    )
                    return _no_entry(
                        attempted=True, rejection_reason="slippage_rejected",
                        rejection_details={"slippage_pct": f"{slip_pct:.4f}", "max": f"{max_slip:.4f}"},
                    )
                # Top-of-book depth check used to fire here on a flat
                # min_liquidity_depth_usd floor (e.g. $50). With small
                # orders ($5.75) it rejected entries the dry-run would
                # accept whenever top-of-book was thin — even when the
                # remaining notional was available at lower levels.
                # _simulate_live_entry_recheck below runs the actual
                # current+delayed book-walk simulation and rejects if it
                # cannot fill ≥99% of order_size; that's the parity-correct
                # liquidity gate. The flat min_depth knob is left in the
                # config for backward compatibility but is no longer used.

            if hasattr(self, "order_book_client"):
                rechecked_price = self._simulate_live_entry_recheck(
                    token_id, order_size, opportunity,
                )
                if rechecked_price is None:
                    return _no_entry(
                        attempted=True,
                        rejection_reason="delayed_book_recheck_failed",
                        rejection_details=getattr(
                            self._get_risk_engine(), "last_entry_recheck_details", {}
                        ),
                    )
                direction_for_edge = opportunity.get("direction", "UP")
                if direction_for_edge == "UP":
                    alpha = self.config.edge_alpha_up
                    floor = self.config.edge_floor_up
                else:
                    alpha = self.config.edge_alpha_down
                    floor = self.config.edge_floor_down
                opportunity["polymarket_prob"] = rechecked_price
                opportunity["required_prob"] = required_model_prob(rechecked_price, alpha, floor)
                model_prob = opportunity.get("model_prob")
                opportunity["edge"] = model_prob / rechecked_price if model_prob and rechecked_price > 0 else 0
                if not self._live_entry_spot_recheck(
                    opportunity,
                    reference_price,
                    apply_live_gates=apply_live_gates,
                ):
                    return _no_entry(
                        attempted=True,
                        rejection_reason="spot_drift_rejected",
                        rejection_details=getattr(
                            self._get_risk_engine(), "last_spot_drift_details", {}
                        ),
                    )

        # Execute buy through executor
        mode = "LIVE" if self.executor.is_live else "PAPER"
        self.log(f"Executing {mode} buy for ${order_size:.2f}")

        buy_limit = getattr(self.executor, "buy_limit", None)
        first_attempt_price = max(
            0.01, min(expected_price or self.config.max_entry_price, self.config.max_entry_price)
        )
        if callable(buy_limit):
            # Limit GTC at max_entry_price delegates the price cap to Polymarket:
            # if the full size can't be filled at ≤ cap, nothing fills. Avoids
            # market orders walking the book to e.g. $0.99 when best_ask moves
            # between edge-check and placement.
            # Paper/live-shadow apply the same cap for dry-run parity.
            cap_price = self._price_tick_down(self.config.max_entry_price)
            # fill_price = expected execution avg (rechecked book-walk avg
            # or original ask). target_shares is sized off it so live spends
            # the full configured notional, not shares × actual_ask.
            executable_price = opportunity.get("polymarket_prob") or cap_price
            fill_price = max(0.01, min(executable_price, cap_price))
            first_attempt_price = fill_price
            # limit_price atomically caps notional. FOK rejects if the book
            # has moved beyond slip_budget between recheck and venue receipt
            # — this is the ONLY atomic cap, since the preflight slippage
            # gate above runs once per entry. Round UP to the next penny
            # tick so a sub-tick fill_price doesn't get rejected by
            # round-half-to-even. Bounded above by the max_entry_price cap.
            slip_budget = max(
                0.0, getattr(self.config, "max_slippage_estimate_pct", 0.0) or 0.0
            )
            # Switched from FOK to GTC with a tight wait-and-cancel window:
            # FOK rejected the entire order if the book couldn't fully cover
            # at limit_price (observed live), losing trades on partial liquidity.
            # GTC + wait_for_fill_seconds takes whatever fills in the window
            # and cancels the residual — IOC-like semantics. See plan
            # tingly-foraging-pebble.md.
            entry_cancel_timeout = float(
                getattr(self.config, "entry_cancel_timeout_s", 2.0) or 2.0
            )

            def place_entry_order(fill_price_override: Optional[float] = None):
                order_fill_price = fill_price
                if fill_price_override is not None:
                    order_fill_price = max(0.01, min(fill_price_override, cap_price))
                limit_price_raw = order_fill_price * (1.0 + slip_budget)
                limit_price = min(cap_price, math.ceil(limit_price_raw * 100) / 100)
                target_shares = math.ceil((order_size / order_fill_price) * 100) / 100
                if limit_price > 0:
                    affordable_shares = math.floor(
                        (capital_before_entry / limit_price) * 100
                    ) / 100
                    if affordable_shares > 0:
                        target_shares = min(target_shares, affordable_shares)
                schedule = getattr(tick, "fee_schedule", None)
                if schedule is not None:
                    # V2 charges BUY fees in collateral. Bound the complete
                    # cash debit at the signed price cap, not just gross size.
                    budget = min(order_size, capital_before_entry)
                    unit_cost = limit_price + fee_for_fill(limit_price, 1.0, schedule)
                    target_shares = math.floor((budget / unit_cost) * 100) / 100
                    while target_shares > 0 and target_shares * limit_price + fee_for_fill(limit_price, target_shares, schedule) > budget + 1e-9:
                        target_shares = round(target_shares - 0.01, 2)
                return buy_limit(
                    token_id, limit_price, target_shares,
                    order_type="GTC",
                    wait_for_fill_seconds=entry_cancel_timeout,
                )
        else:
            def place_entry_order(fill_price_override: Optional[float] = None):
                return self.executor.buy_market(token_id, order_size)

        def validate_before_submit(metadata: dict) -> None:
            # This runs after signing and before POST, so slow signing/network
            # metadata lookups cannot quietly bypass the freshness deadline.
            pause_reason = self._entry_pause_reason()
            if pause_reason:
                raise ValueError(f"Entry paused before submission: {pause_reason}")
            if str((self.state.current_market or {}).get("market_id") or "") != market_id:
                raise ValueError("Market changed before submission")
            if tick is not None:
                valid, reason = entry_inputs_valid(tick)
                if not valid:
                    raise ValueError(f"Entry no longer eligible before submission: {reason}")
                if getattr(tick, "payoff_kind", "legacy") != "legacy":
                    end = getattr(tick, "market_end_time", None)
                    if end is None:
                        raise ValueError("Missing market end time before submission")
                    remaining = (end - datetime.now(timezone.utc)).total_seconds()
                    elapsed = float(tick.window_duration_seconds) - remaining
                    if remaining <= self.config.no_trade_last_seconds or elapsed < self.config.no_trade_first_seconds:
                        raise ValueError("Entry window closed before submission")
                    p = tick.prob_above if direction == "UP" else tick.prob_below
                    alpha = self.config.edge_alpha_up if direction == "UP" else self.config.edge_alpha_down
                    floor = self.config.edge_floor_up if direction == "UP" else self.config.edge_floor_down
                    worst_price = float(metadata.get("limit_price") or expected_price)
                    quantity = float(metadata.get("shares_requested") or 0)
                    if quantity > 0:
                        worst_price += fee_for_fill(worst_price, quantity, tick.fee_schedule) / quantity
                    if not has_edge(p, worst_price, alpha, floor):
                        raise ValueError("Entry edge disappeared at the submitted price cap")
        def persist_before_submit(metadata: dict) -> None:
            validate_before_submit(metadata)
            self.state.pending_entry = {
                "reason": "order_submission",
                "manual_intervention_required": False,
                "submission_state": metadata.get("submission_state", "submitting"),
                "order_id": metadata["submitted_order_hash"],
                "submitted_order_hash": metadata["submitted_order_hash"],
                "token_id": token_id,
                "direction": direction,
                "market_id": market_id,
                "market_title": market_title,
                "reference_price": reference_price,
                "usd_amount": order_size,
                "expected_price": expected_price,
                "pre_balance": metadata.get("pre_balance"),
                "requested_shares": metadata.get("shares_requested"),
                "order_metadata": metadata,
                "condition_id": getattr(tick, "condition_id", ""),
                "payoff_kind": getattr(tick, "payoff_kind", "legacy"),
                "averaging_window_seconds": getattr(tick, "averaging_window_seconds", 0),
                "contract_metadata": dict(getattr(tick, "contract_metadata", None) or {}),
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
            self.state.known_token_ids.add(token_id)
            self.state.save()
            # Durability itself can be slow. The final decision must still be
            # fresh after the fsync/SQLite commit and immediately before POST.
            validate_before_submit(metadata)

        raw_place_entry_order = place_entry_order

        def place_entry_order(fill_price_override: Optional[float] = None):
            set_handler = getattr(self.executor, "set_entry_intent_handler", None)
            if callable(set_handler):
                set_handler(persist_before_submit)
            try:
                response = raw_place_entry_order(fill_price_override)
            finally:
                if callable(set_handler):
                    set_handler(None)
            pending = getattr(self.state, "pending_entry", None)
            phase = getattr(response, "submission_state", "") or (getattr(response, "metadata", {}) or {}).get("submission_state", "")
            if pending and pending.get("reason") == "order_submission":
                if not response.success and phase in {"not_submitted", "rejected", "cancelled"}:
                    self.state.pending_entry = None
                    self.state.save()
                elif not response.success:
                    response.ambiguous = True
                    response.order_id = response.order_id or pending.get("order_id")
                    response.submission_state = "uncertain"
            return response

        # Final cheap CLOB taker-health guard before sending a real order. The
        # flag is shared with the grid; if a taker outage is active, skip rather
        # than fire a doomed order (and never enter the retry loop below).
        if apply_live_gates and getattr(self.config, "clob_health_gate_enabled", True):
            clob_block = self._clob_taker_block_reason()
            if clob_block:
                self.log(f"Skipping LIVE entry: CLOB taker-health unhealthy ({clob_block})")
                return _no_entry(
                    attempted=False,
                    rejection_reason="clob_taker_outage",
                    rejection_details={"clob_reason": clob_block},
                )

        result = place_entry_order()
        if apply_live_gates and not result.success:
            max_retries = max(0, int(getattr(self.config, "live_entry_order_retries", 0) or 0))
            retry_window = max(
                0.0, float(getattr(self.config, "live_entry_retry_window_seconds", 0.0) or 0.0)
            )
            retry_delay = max(
                0.0, float(getattr(self.config, "live_entry_retry_delay_seconds", 0.0) or 0.0)
            )
            price_guard = max(
                0.0, float(getattr(self.config, "live_entry_retry_price_guard", 0.0) or 0.0)
            )
            retry_started = time.monotonic()

            def retry_still_safe() -> bool:
                phase = getattr(result, "submission_state", "") or (getattr(result, "metadata", {}) or {}).get("submission_state", "")
                if phase not in {"not_submitted", "rejected"}:
                    self.log("Live retry blocked: non-acceptance has not been established")
                    return False
                if clob_health.is_hard_taker_outage(getattr(result, "error_message", "")):
                    self.log(
                        "Live entry retry stopped: CLOB refusing taker orders "
                        "(post-only/425/503) — retrying within the window is futile"
                    )
                    return False
                if result.order_id or result.ambiguous:
                    self.log(
                        "Live entry retry skipped: failed order returned an order id "
                        "or ambiguous status; tracking pending state instead"
                    )
                    return False
                current_market = self.state.current_market or {}
                current_market_id = current_market.get("market_id")
                if current_market_id and market_id and current_market_id != market_id:
                    self.log("Live entry retry stopped: market changed")
                    return False
                if tick and getattr(tick, "time_remaining_seconds", None) is not None:
                    elapsed = time.monotonic() - retry_started
                    remaining = float(tick.time_remaining_seconds) - elapsed
                    if remaining <= self.config.no_trade_last_seconds:
                        self.log("Live entry retry stopped: market is inside last no-trade window")
                        return False
                return True

            def retry_entry_price() -> Optional[float]:
                if not callable(buy_limit) or not hasattr(self, "order_book_client"):
                    return first_attempt_price
                current_price = self._simulate_live_entry_recheck(
                    token_id, order_size, opportunity,
                )
                if current_price is None:
                    self.log("Live entry retry stopped: retry recheck could not fill order")
                    return None
                price_move = abs(current_price - first_attempt_price)
                if price_guard > 0 and price_move > price_guard + 1e-9:
                    self.log(
                        "Live entry retry stopped: entry price moved "
                        f"{price_move:.4f} from first attempt "
                        f"({first_attempt_price:.4f} -> {current_price:.4f}), "
                        f"guard={price_guard:.4f}"
                    )
                    self.state.log_risk_event(
                        kind="entry_retry_rejected_price_move",
                        details={
                            "direction": direction,
                            "token_id": token_id,
                            "market_id": market_id,
                            "first_attempt_price": f"{first_attempt_price:.4f}",
                            "retry_price": f"{current_price:.4f}",
                            "price_move": f"{price_move:.4f}",
                            "price_guard": f"{price_guard:.4f}",
                        },
                    )
                    return None
                return current_price

            for retry_index in range(1, max_retries + 1):
                if not retry_still_safe():
                    break
                elapsed = time.monotonic() - retry_started
                remaining_window = retry_window - elapsed
                if remaining_window <= 0:
                    self.log("Live entry retry stopped: retry window expired")
                    break
                sleep_for = min(retry_delay, remaining_window)
                if sleep_for > 0:
                    time.sleep(sleep_for)
                if not retry_still_safe():
                    break
                self.log(
                    f"Retrying LIVE buy after failure ({retry_index}/{max_retries}): "
                    f"{result.error_message}"
                )
                retry_price = retry_entry_price()
                if retry_price is None:
                    break
                result = place_entry_order(retry_price)
                if result.success:
                    self.log(f"LIVE buy retry succeeded ({retry_index}/{max_retries})")
                    break

        # Update the shared CLOB taker-health flag from the (final) order result:
        # clear it on a successful fill, raise it on a taker-blocking failure so
        # the dry-run grid skips the same windows and stays matched with live.
        if apply_live_gates:
            self._record_clob_order_outcome(result)

        if result.success and result.avg_fill_price > self.config.max_entry_price + 1e-6:
            # Belt-and-braces: the live limit cap should make this impossible.
            self.log(
                f"CRITICAL: fill ${result.avg_fill_price:.4f} > max_entry_price "
                f"${self.config.max_entry_price:.2f} - live limit cap should have prevented this"
            )
            self.state.log_risk_event(
                kind="entry_fill_above_cap",
                details={
                    "order_id": result.order_id or "",
                    "avg_fill_price": f"{result.avg_fill_price:.4f}",
                    "max_entry_price": f"{self.config.max_entry_price:.4f}",
                    "shares_filled": f"{result.shares_filled:.4f}",
                    "market_id": market_id,
                    "token_id": token_id,
                    "direction": direction,
                },
            )
            self.state.pending_entry = {
                "manual_intervention_required": True,
                "reason": "entry_fill_above_cap",
                "order_id": result.order_id,
                "token_id": token_id,
                "direction": direction,
                "market_id": market_id,
                "market_title": market_title,
                "reference_price": reference_price,
                "fill_price": result.avg_fill_price,
                "avg_fill_price": result.avg_fill_price,
                "shares": result.shares_filled,
                "shares_filled": result.shares_filled,
                "total_cost": result.total_cost,
                "max_entry_price": self.config.max_entry_price,
                "last_seen_order_status": "",
                "cancel_attempts": 0,
                "last_cancel_attempt_at": "",
                "order_metadata": getattr(result, "metadata", {}) or {},
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
            self.state.known_token_ids.add(token_id)
            self.state.save()
            return _no_entry(
                attempted=True, rejection_reason="fill_above_cap",
                rejection_details={
                    "fill": f"{result.avg_fill_price:.4f}",
                    "cap": f"{self.config.max_entry_price:.4f}",
                    "order_metadata": getattr(result, "metadata", {}) or {},
                },
                order_id=result.order_id or "",
                ambiguous=result.ambiguous,
                order_metadata=getattr(result, "metadata", {}) or {},
            )

        if not result.success:
            self.log(f"Order failed: {result.error_message}")
            error_text = (result.error_message or "").lower()
            if "balance is not visible" in error_text:
                failure_reason = "token_balance_not_visible"
            elif "unfilled within" in error_text:
                failure_reason = "order_unfilled_timeout"
            elif result.ambiguous and "cancel" in error_text:
                failure_reason = "cancel_ambiguous"
            elif result.order_id and result.ambiguous:
                failure_reason = "order_ambiguous"
            else:
                failure_reason = "order_failed"
            if self.executor.is_live and result.ambiguous:
                self.state.pending_entry = {
                    **(getattr(self.state, "pending_entry", None) or {}),
                    "manual_intervention_required": False,
                    "reason": failure_reason,
                    "order_id": result.order_id,
                    "token_id": token_id,
                    "direction": direction,
                    "market_id": market_id,
                    "market_title": market_title,
                    "reference_price": reference_price,
                    "usd_amount": order_size,
                    "expected_price": expected_price,
                    "shares_filled": float(result.shares_filled or 0.0),
                    "avg_fill_price": float(result.avg_fill_price or 0.0),
                    "last_seen_order_status": "",
                    "cancel_attempts": 0,
                    "last_cancel_attempt_at": "",
                    "order_metadata": getattr(result, "metadata", {}) or {},
                    "created_at": datetime.now(timezone.utc).isoformat(),
                }
                self.state.known_token_ids.add(token_id)
                self.state.save()
            return _no_entry(
                attempted=True, rejection_reason=failure_reason,
                rejection_details={
                    "error": (result.error_message or "")[:200],
                    "ambiguous": str(result.ambiguous),
                    "order_metadata": getattr(result, "metadata", {}) or {},
                },
                order_id=result.order_id or "",
                ambiguous=result.ambiguous,
                order_metadata=getattr(result, "metadata", {}) or {},
            )

        # Phantom-entry guard. The executor MUST return a real on-chain
        # order_id when running live; an empty/None order_id with success=True
        # means the bot would record a position that doesn't exist on
        # Polymarket. Refuse to create the position rather than silently
        # ghost-trade. Observed live on 2026-05-01: the
        # btc-updown-15m-1777642500 entry recorded $6.90 cost / 9.72 shares
        # with empty entry_order_id, then settled internally as
        # LOSS_EXPIRY — but no actual position was ever on Polymarket.
        if self.executor.is_live and not result.order_id:
            self.log(
                f"CRITICAL: executor returned success but order_id is empty — "
                f"refusing to create phantom position. shares_filled="
                f"{result.shares_filled} avg_fill={result.avg_fill_price} "
                f"token_id={token_id} direction={direction}"
            )
            self.state.log_risk_event(
                kind="phantom_entry_blocked",
                details={
                    "token_id": token_id,
                    "direction": direction,
                    "shares_filled": f"{result.shares_filled:.4f}",
                    "avg_fill_price": f"{result.avg_fill_price:.4f}",
                    "total_cost": f"{result.total_cost:.4f}",
                },
            )
            return _no_entry(
                attempted=True,
                rejection_reason="phantom_entry_blocked",
                order_id=result.order_id or "",
                ambiguous=result.ambiguous,
                order_metadata=getattr(result, "metadata", {}) or {},
            )

        modern_partial = getattr(tick, "payoff_kind", "legacy") != "legacy" and result.shares_filled > 0.01 and result.total_cost > 0
        if self.executor.is_live and not modern_partial and not self._live_entry_fill_is_viable(
            float(result.shares_filled or 0.0),
            float(result.total_cost or 0.0),
        ):
            self.log(
                "CRITICAL: live entry partial fill below viable minimum; "
                f"shares={result.shares_filled:.4f} cost=${result.total_cost:.4f}"
            )
            self._mark_entry_partial_below_minimum(
                order_id=result.order_id,
                token_id=token_id,
                direction=direction,
                market_id=market_id,
                market_title=market_title,
                reference_price=reference_price,
                shares=float(result.shares_filled or 0.0),
                fill_price=float(result.avg_fill_price or 0.0),
                total_cost=float(result.total_cost or 0.0),
                order_metadata=getattr(result, "metadata", {}) or {},
            )
            return _no_entry(
                attempted=True,
                rejection_reason="entry_partial_below_minimum",
                rejection_details={
                    "shares": f"{float(result.shares_filled or 0.0):.4f}",
                    "total_cost": f"{float(result.total_cost or 0.0):.4f}",
                    "min_position_shares": f"{float(self.config.min_position_shares):.4f}",
                    "min_order_size_usd": f"{float(self.config.min_order_size_usd):.4f}",
                    "order_metadata": getattr(result, "metadata", {}) or {},
                },
                order_id=result.order_id or "",
                ambiguous=True,
                order_metadata=getattr(result, "metadata", {}) or {},
            )

        # Calculate TP price (capped at $0.99 for binary options)
        tp_price = min(result.avg_fill_price * (1 + self.config.tp_percentage), 0.99)

        # Build entry context from tick data
        entry_context = dict(self._build_entry_context_from_tick(opportunity) or {})
        entry_context["entry_fill_shares"] = result.shares_filled
        if (getattr(result, "metadata", {}) or {}).get("cost_basis_source"):
            entry_context["cost_basis_source"] = result.metadata["cost_basis_source"]

        # Create position (MicroPosition with keyword arguments)
        position = Position(
            direction=Direction(direction),
            token_id=token_id,
            entry_price=result.avg_fill_price,
            shares=result.shares_filled,
            cost_basis=result.total_cost if self.executor.is_live else result.total_cost * (1 + self.config.friction_pct),
            tp_price=tp_price,
            stop_loss_price=0.0,
            opened_at=datetime.now(timezone.utc),
            market_id=market_id,
            peak_price=result.avg_fill_price,
            order_id=result.order_id,
            market_title=market_title,
            reference_price=reference_price,
            entry_context=entry_context,
            condition_id=getattr(tick, "condition_id", ""),
            payoff_kind=getattr(tick, "payoff_kind", "legacy"),
            averaging_window_seconds=getattr(tick, "averaging_window_seconds", 0),
            contract_metadata=dict(getattr(tick, "contract_metadata", None) or {}),
            fee_schedule=getattr(tick, "fee_schedule", None),
            decision_at=getattr(tick, "timestamp", None),
        )

        # Persist position immediately so a crash doesn't lose it
        self.state.add_position(position)
        if modern_partial and position.shares < 5.0:
            self._mark_pending_settlement(position, "partial_fill_below_exit_minimum")
        # One-position-per-window: record the market we just entered so a
        # subsequent exit (TP/SL) can't trigger a re-entry in the same window.
        if position.market_id:
            self.state.last_entered_market_id = position.market_id
        self.state.pending_entry = None
        if self.executor.is_live:
            observed_cash = self.executor.get_usdc_balance()
            if observed_cash is not None:
                self.state.current_capital = observed_cash
        self.state.save()

        # Place GTC limit sell at TP price (live mode only)
        if self.executor.is_live and not position.pending_settlement and getattr(self.config, "tp_limit_order_enabled", True):
            # Poll for position to settle on chain (Polygon ~2s blocks + API lag)
            try:
                actual_shares = self.executor.wait_for_position(
                    token_id, target=result.shares_filled * 0.95,
                    comparison="gte", timeout_seconds=15.0,
                )
            except TimeoutError:
                _emit("  [WARN] Position not confirmed on chain within 15s, using fill estimate")
                actual_shares = result.shares_filled

            # Sync position shares to on-chain reality
            if abs(actual_shares - result.shares_filled) > 0.01:
                _emit(f"  [INFO] On-chain shares ({actual_shares:.4f}) differ from fill "
                      f"({result.shares_filled:.4f}), adjusting position")
                position.shares = actual_shares

                # Try to get true cost from trade history (may lag behind chain)
                time.sleep(3)  # Wait for indexer to catch up
                trade_cost = self._compute_cost_basis_from_trades(position)
                if trade_cost is not None and trade_cost >= result.total_cost:
                    # Trade history shows higher cost — use it (split fills)
                    position.cost_basis = trade_cost
                    position.entry_price = trade_cost / actual_shares
                else:
                    # Trade history incomplete or unavailable — scale proportionally
                    ratio = actual_shares / result.shares_filled
                    position.cost_basis = result.total_cost * ratio

                # Correct current_capital for cost_basis delta
                original_cost = result.total_cost
                if abs(position.cost_basis - original_cost) > 0.001:
                    delta = position.cost_basis - original_cost
                    self.state.current_capital -= delta
                    _emit(f"  [INFO] Capital adjusted by ${-delta:+.2f} for cost_basis correction "
                          f"(${original_cost:.2f} -> ${position.cost_basis:.2f})")

            # Round DOWN to 2 decimals to avoid exceeding actual balance
            tp_shares = math.floor(actual_shares * 100) / 100

            tp_result = self.executor.sell_limit(token_id, tp_price, tp_shares)
            if tp_result.success:
                # Atomic TP confirmation: (a) if LIVE on book, we're good;
                # (b) if wait times out or order vanished, resolve via on-chain
                # shares — either TP filled immediately (close now) or the TP
                # order was rejected silently (fall through to backup placement).
                tp_live = False
                try:
                    _ = self.executor.wait_for_order_on_book(
                        tp_result.order_id,
                        timeout_seconds=15.0,
                        verify_live=True,
                    )
                    position.tp_order_id = tp_result.order_id
                    position.tp_placed_at = datetime.now(timezone.utc)
                    tp_live = True
                    _emit(f"  TP order confirmed LIVE at ${tp_price:.4f}")
                except TimeoutError:
                    _emit(f"  [WARN] TP order {tp_result.order_id} not LIVE after 15s — probing on-chain shares")
                except RuntimeError as exc:
                    _emit(f"  [WARN] TP order verification errored: {exc} — probing on-chain shares")

                if not tp_live:
                    # Determine outcome from authoritative sources: order status
                    # and actual shares on chain.
                    order_status = None
                    try:
                        order_status = self.executor.get_order_status(tp_result.order_id)
                    except Exception as exc:
                        log.debug("TP order status lookup failed for %s: %s", tp_result.order_id, exc)
                        order_status = None
                    status_name = (order_status or {}).get("status", "").lower() if order_status else ""

                    probe_shares = None
                    try:
                        probe_shares = self.executor.get_position(token_id, strict=False)
                    except Exception as exc:
                        _emit(f"  [WARN] get_position probe raised: {exc}")
                        probe_shares = None

                    if status_name == "matched":
                        # TP filled immediately. Close at tp_price — same
                        # semantics as a normal TP fill detection on the next
                        # heartbeat.
                        # (We explicitly DON'T treat probe_shares<=0.01 without
                        # status="matched" as "TP filled": it could equally be
                        # indexer lag or a failed buy fill, and booking a fake
                        # TP win in those cases would be worse than waiting for
                        # the next heartbeat to resolve.)
                        _emit(f"  TP filled immediately (status={status_name or 'n/a'}, "
                              f"shares={probe_shares if probe_shares is not None else 'n/a'}) — closing position")
                        self.state.log_risk_event(
                            kind="tp_filled_on_entry",
                            details={
                                "order_id": tp_result.order_id,
                                "token_id": token_id,
                                "status": status_name,
                                "probe_shares": f"{probe_shares:.4f}" if probe_shares is not None else "",
                                "tp_price": f"{tp_price:.4f}",
                            },
                        )
                        position.tp_order_id = tp_result.order_id
                        position.tp_placed_at = datetime.now(timezone.utc)
                        # Log the ENTRY event so the equity CSV has a matching
                        # pair before the TP_FILLED row that close_position_tp
                        # will write.
                        self.state.log_equity_event(
                            event="ENTRY",
                            direction=direction,
                            entry_price=result.avg_fill_price,
                            shares=position.shares,
                            position_value=position.cost_basis,
                        )
                        self.close_position_tp(position, tp_price)
                        return EntryAttemptResult(
                            attempted=True,
                            rejection_reason="tp_filled_immediately",
                            direction=direction,
                            token_id=token_id,
                            signal_price=expected_price,
                            rechecked_price=rechecked_price,
                            order_size_usd=order_size,
                            raw_order_size_usd=raw_order_size,
                            capital_before_entry=capital_before_entry,
                            capital_source=capital_source,
                            order_id=result.order_id or "",
                            ambiguous=result.ambiguous,
                            order_metadata=getattr(result, "metadata", {}) or {},
                        )
                    elif status_name in ("cancelled", "expired"):
                        # Original TP is dead on the venue. No cancellation
                        # needed; place backup fresh.
                        _emit(f"  [WARN] TP order was {status_name} — scheduling backup TP")
                        self.state.log_risk_event(
                            kind="tp_initial_rejected",
                            details={
                                "order_id": tp_result.order_id,
                                "token_id": token_id,
                                "status": status_name,
                            },
                        )
                        position.tp_order_id = None
                        self._place_backup_tp_order(position, reason=f"initial TP {status_name}")
                    elif status_name in ("live", "open", "delayed"):
                        # Venue confirms the order is alive; wait_for_order_on_book
                        # just raced the book refresh. Trust the order_id and let
                        # the 60s _verify_tp_orders heartbeat re-check. Do NOT
                        # place a backup (that would require cancelling a live
                        # order and churn an otherwise good one).
                        _emit(f"  TP order {tp_result.order_id} confirmed {status_name} "
                              f"via status probe (shares={probe_shares}); trusting order")
                        position.tp_order_id = tp_result.order_id
                        position.tp_placed_at = position.tp_placed_at or datetime.now(timezone.utc)
                        self.state.log_risk_event(
                            kind="tp_initial_status_trusted",
                            details={
                                "order_id": tp_result.order_id,
                                "token_id": token_id,
                                "status": status_name,
                                "probe_shares": f"{probe_shares:.4f}" if probe_shares is not None else "",
                            },
                        )
                    else:
                        # Truly ambiguous: status unknown/empty, shares present.
                        # Keep tp_order_id so _place_backup_tp_order cancels it
                        # first (it might silently be live on the venue), then
                        # places a fresh TP.
                        _emit(f"  [WARN] TP order {tp_result.order_id} ambiguous "
                              f"(status={status_name or 'n/a'}, shares={probe_shares}); "
                              f"cancelling and replacing")
                        self.state.log_risk_event(
                            kind="tp_initial_ambiguous",
                            details={
                                "order_id": tp_result.order_id,
                                "token_id": token_id,
                                "status": status_name,
                                "probe_shares": f"{probe_shares:.4f}" if probe_shares is not None else "",
                            },
                        )
                        position.tp_order_id = tp_result.order_id
                        position.tp_placed_at = position.tp_placed_at or datetime.now(timezone.utc)
                        self._place_backup_tp_order(position, reason="initial TP verify timeout")
            else:
                _emit(f"  WARNING: Failed to place TP limit order: {tp_result.error_message}")
                self.state.log_risk_event(
                    kind="tp_placement_failed",
                    details={
                        "token_id": token_id,
                        "tp_price": f"{tp_price:.4f}",
                        "error": (tp_result.error_message or "")[:200],
                    },
                )
                # Queue a backup TP placement so the position isn't uncovered
                # until the next 60s heartbeat.
                self._place_backup_tp_order(position, reason="initial TP placement failed")

        # Set stop-loss price if configured
        if self.config.stop_loss_pct > 0:
            position.stop_loss_price = result.avg_fill_price * (1 - self.config.stop_loss_pct)

        # Save again with TP order_id and any share adjustments
        self.state.save()

        # Log equity event for PnL curve tracking
        self.state.log_equity_event(
            event="ENTRY",
            direction=direction,
            entry_price=result.avg_fill_price,
            shares=position.shares,
            position_value=position.cost_basis,
        )

        _emit(f"\n>>> ENTRY ({mode}): {direction} <<<")
        _emit(f"  Shares: {result.shares_filled:.4f}")
        _emit(f"  Entry:  ${result.avg_fill_price:.4f}")
        _emit(f"  TP:     ${tp_price:.4f}")
        _emit(f"  Cost:   ${result.total_cost:.2f}")
        _emit(f"  Edge:   {opportunity['edge']:.2f}x")
        if self.config.trail_activation > 0:
            act_price = result.avg_fill_price * (1 + self.config.trail_activation)
            _emit(f"  Trail:  arm at ${act_price:.4f}, distance {self.config.trail_distance*100:.0f}pp")
        if self.config.stop_loss_pct > 0:
            _emit(f"  SL:     ${position.stop_loss_price:.4f} ({self.config.stop_loss_pct*100:.0f}% below entry)")
        if result.order_id:
            _emit(f"  Order:  {result.order_id}")
        if position.tp_order_id:
            _emit(f"  TP Order: {position.tp_order_id}")

        return EntryAttemptResult(
            position=position,
            attempted=True,
            direction=direction,
            token_id=token_id,
            signal_price=expected_price,
            rechecked_price=rechecked_price,
            order_size_usd=order_size,
            raw_order_size_usd=raw_order_size,
            capital_before_entry=capital_before_entry,
            capital_source=capital_source,
            order_id=result.order_id or "",
            ambiguous=result.ambiguous,
            order_metadata=getattr(result, "metadata", {}) or {},
        )

    # ---------------------------------------------------------------
    # TP / trailing stop / stop-loss fills
    # ---------------------------------------------------------------

    def check_tp_fills(self) -> None:
        """Check if any TP orders have filled, then check trailing stops."""
        for position in self.state.open_positions[:]:
            if self.executor.is_live:
                # Live mode: check TP order status if we have one
                if position.tp_order_id:
                    status = self.executor.get_order_status(position.tp_order_id)
                    if status:
                        # Reset failure counter on successful query
                        self._tp_status_failures.pop(position.id, None)

                        size_matched = status.get("size_matched", 0)
                        # Consider filled if >= 99% matched (handles rounding)
                        if size_matched >= position.shares * 0.99:
                            exit_price = status.get("price", position.tp_price)
                            # Belt-and-braces: order status can lag/lie. The
                            # phantom-TP-fill bug on 2026-04-30 (commit
                            # 4ca5f67b) was Polymarket reporting matched while
                            # shares stayed in the wallet. Require the on-chain
                            # balance to actually have dropped before treating
                            # the TP as filled.
                            try:
                                remaining_onchain = self.executor.get_position(
                                    position.token_id, strict=True,
                                )
                            except Exception as exc:
                                remaining_onchain = None
                                self.log(
                                    f"TP fill verify: get_position raised ({exc}); "
                                    "deferring TP confirmation to next tick"
                                )
                            remaining_threshold = position.shares * 0.05
                            if remaining_onchain is not None and remaining_onchain > remaining_threshold:
                                self.log(
                                    f"TP order_status reports filled "
                                    f"(matched={size_matched:.4f}/{position.shares:.4f}) "
                                    f"but on-chain balance still has "
                                    f"{remaining_onchain:.4f} shares — IGNORING fake fill"
                                )
                                continue  # let next tick re-check
                            if remaining_onchain is None:
                                # Couldn't verify; safer to defer than fabricate.
                                continue
                            confirm_delay = max(
                                0.0,
                                float(
                                    getattr(
                                        self.config,
                                        "tp_fill_balance_recheck_seconds",
                                        2.0,
                                    )
                                    or 0.0
                                ),
                            )
                            if confirm_delay > 0:
                                time.sleep(confirm_delay)
                            try:
                                confirm_remaining = self.executor.get_position(
                                    position.token_id, strict=True,
                                )
                            except Exception as exc:
                                self.log(
                                    f"TP fill verify: confirmation get_position raised ({exc}); "
                                    "deferring TP confirmation to next tick"
                                )
                                continue
                            if confirm_remaining > remaining_threshold:
                                self.log(
                                    f"TP order_status reports filled after first zero-balance read, "
                                    f"but confirmation still has {confirm_remaining:.4f} shares "
                                    f"- IGNORING fake fill"
                                )
                                continue
                            self.log(f"TP limit order filled: {size_matched:.4f} shares at ${exit_price:.4f}")
                            # Compute verified proceeds from the order_status
                            # payload directly. Polymarket already told us the
                            # match price and size; the on-chain balance check
                            # above confirms the fill happened. This is used
                            # as a fallback in _record_exit when /data/trades
                            # hasn't indexed the sub-trades yet (common on
                            # split CLOB matches like e312391d on 2026-05-01).
                            tp_proceeds = (
                                float(size_matched) * float(exit_price)
                                if size_matched and exit_price
                                else None
                            )
                            self.close_position_tp(
                                position,
                                exit_price,
                                proceeds_override=tp_proceeds,
                            )
                            continue
                    else:
                        # Track consecutive failures
                        failures = self._tp_status_failures.get(position.id, 0) + 1
                        self._tp_status_failures[position.id] = failures

                        if failures >= 3:
                            timestamp = datetime.now().strftime("%H:%M:%S")
                            _emit(f"\n[{timestamp}] [WARN] TP status query failed {failures}x for "
                                  f"{position.direction.value} position — falling back to position check")
                            # Fall back to position-based detection
                            try:
                                actual_shares = self.executor.get_position(position.token_id, strict=True)
                                if actual_shares <= 0.01:
                                    # Position is gone — TP likely filled
                                    _emit(f"  Position has 0 shares — treating as TP fill at ${position.tp_price:.4f}")
                                    self.close_position_tp(position, position.tp_price)
                                    self._tp_status_failures.pop(position.id, None)
                                    continue
                            except RuntimeError:
                                _emit("  Position query also failed - will retry next heartbeat")

                if self._handle_pending_sl_for_closing_window(position):
                    continue

                local_sl_enabled = (
                    not self._mirror_control_enabled()
                    or getattr(self.config, "local_stop_loss_enabled", True)
                )
                sl_allowed = local_sl_enabled and not self._closing_window_suppresses_stop_loss()

                # Fetch best bid for trail/SL checks (live mode)
                _need_bid = (
                    self.config.trail_activation > 0
                    or (sl_allowed and position.stop_loss_price > 0)
                )
                best_bid = None
                if _need_bid:
                    best_bid = self.executor.get_best_bid(position.token_id)
                    if best_bid is not None:
                        if best_bid > position.peak_price:
                            position.peak_price = best_bid
                            self.state.save()

                # Trailing stop check (live mode — GTC sell on book)
                if self.config.trail_activation > 0 and best_bid is not None:
                    if position.trail_order_id:
                        # Trail order already placed — check if filled
                        trail_status = self.executor.get_order_status(position.trail_order_id)
                        if trail_status:
                            size_matched = trail_status.get("size_matched", 0)
                            if size_matched >= position.shares * 0.99:
                                exit_price = trail_status.get("price", position.trail_order_price)
                                trail_proceeds = (
                                    float(size_matched) * float(exit_price)
                                    if size_matched and exit_price
                                    else None
                                )
                                self._close_trail_filled(
                                    position,
                                    exit_price,
                                    proceeds_override=trail_proceeds,
                                )
                                continue
                            order_st = trail_status.get("status", "").lower()
                            if order_st in ("cancelled", "expired"):
                                # Re-place trail order
                                self._place_trail_order(position, position.trail_order_price,
                                                        reason=f"trail order {order_st}")
                    else:
                        # No trail order yet — check if trail should fire
                        trail_level = self._get_trail_trigger(position, best_bid)
                        if trail_level is not None:
                            self._place_trail_order(position, trail_level, reason="trail triggered")

                # Stop-loss check (live mode - trigger on mid, sell at bid)
                if sl_allowed and position.stop_loss_price > 0 and best_bid is not None:
                    if position.pending_sl_order_id:
                        continue  # Fallback SL limit order already pending
                    best_ask = self.executor.get_best_ask(position.token_id)
                    mid_price = (best_bid + best_ask) / 2 if best_ask is not None else best_bid
                    if mid_price <= position.stop_loss_price:
                        self._execute_stop_loss_live(position, best_bid)
                        continue
            else:
                sl_allowed = not self._closing_window_suppresses_stop_loss()

                # Dry-run mode: poll best_bid
                best_bid = self.executor.get_best_bid(position.token_id)
                if best_bid is None:
                    continue

                # Update peak price
                if best_bid > position.peak_price:
                    position.peak_price = best_bid
                    self.state.save()

                # 1. TP check (priority) — fill at tp_price (matches live GTC limit sell)
                if best_bid >= position.tp_price:
                    self.log(f"TP triggered: bid ${best_bid:.4f} >= TP ${position.tp_price:.4f}")
                    self.close_position_tp(position, position.tp_price)
                    continue

                # 2. Trailing stop check — fill at trail_level (matches live GTC limit sell)
                trail_level = self._get_trail_trigger(position, best_bid)
                if trail_level is not None:
                    # Round trail_level like live mode does in _place_trail_order
                    rounded_trail = round(max(0.01, min(trail_level, 0.99)), 2)
                    self.close_position_trailing_stop(position, rounded_trail, trail_level)
                    continue

                # 3. Stop-loss check (trigger on mid, fill at bid)
                best_ask = self.executor.get_best_ask(position.token_id)
                mid_price = (best_bid + best_ask) / 2 if best_ask is not None else best_bid
                if sl_allowed and position.stop_loss_price > 0 and mid_price <= position.stop_loss_price:
                    self.close_position_stop_loss(position, best_bid)
                    continue

    def _record_exit(
        self,
        position: Position,
        exit_price: float,
        result: TradeResult,
        *,
        proceeds_override: Optional[float] = None,
    ) -> Trade:
        """Create trade, close position. Returns Trade.

        In live mode with a non-expiry result, the proceeds value is sourced
        in priority order:

        1. ``executor.get_proceeds_from_trades`` (Polymarket /data/trades) —
           the exact USDC delta after fees. Best when available.
        2. ``proceeds_override`` — verified value from order_status at fill
           time (size_matched × matched_price). Used when /data/trades
           hasn't indexed the fill yet (common on split CLOB matches). The
           background ``polymarket_history_reconciler`` upgrades these
           rows to source="polymarket_history" once indexing catches up.
        3. ``ProceedsUnverifiedError`` — only when neither (1) nor (2) is
           available, i.e. the trade truly cannot be priced. Caller marks
           the position pending_settlement.

        The override is gated on the caller having already verified an
        actual fill (typically via on-chain balance drop), so this is
        not a fabrication — the trade definitely happened.
        """
        # Pick the exit_order_id from the position based on how it closed —
        # each path writes its sell order_id to a different field on Position.
        if getattr(position, "last_exit_order_id", None):
            exit_oid = position.last_exit_order_id
        elif result == TradeResult.TP_FILLED:
            exit_oid = position.tp_order_id
        elif result == TradeResult.TP_SIGNAL_MARKET_EXIT:
            exit_oid = None
        elif result == TradeResult.TRAILING_STOP:
            exit_oid = position.trail_order_id or position.tp_order_id
        elif result == TradeResult.STOP_LOSS:
            exit_oid = position.pending_sl_order_id or position.tp_order_id
        else:
            exit_oid = None

        proceeds_source = "planned"
        if result in (TradeResult.WIN_EXPIRY, TradeResult.LOSS_EXPIRY):
            # Market resolved on-chain; no SELL fill to query. Apply the
            # payout (core CLOB has no resolution fee at the time of writing).
            payout_per_share = 1.0 if result == TradeResult.WIN_EXPIRY else 0.0
            proceeds = position.shares * payout_per_share
            proceeds_source = "resolution"
        elif self.executor.is_live:
            actual = None
            lookup_exc: Optional[Exception] = None
            since_ts = (position.tp_placed_at or position.opened_at).timestamp()
            try:
                actual = self.executor.get_proceeds_from_trades(
                    order_id=exit_oid,
                    token_id=position.token_id,
                    since_timestamp=since_ts,
                    target_shares=position.shares,
                )
            except Exception as exc:
                lookup_exc = exc
                _emit(f"[WARN] _record_exit: get_proceeds_from_trades raised: {exc}")
            if actual is not None and actual > 0:
                try:
                    current_balance = self.executor.get_position(position.token_id, strict=True)
                    if current_balance > position.shares * 0.05:
                        _emit(
                            f"[WARN] _record_exit: proceeds found (${actual:.4f}) but "
                            f"{current_balance:.4f}/{position.shares:.4f} shares still on chain — "
                            "rejecting as phantom"
                        )
                        actual = None
                except Exception as exc:
                    _emit(f"[WARN] _record_exit: pre-record balance check failed: {exc}")
            if actual is not None and actual > 0:
                proceeds = actual
                proceeds_source = getattr(self.executor, "last_proceeds_source", "") or "polymarket_history"
            elif proceeds_override is not None and proceeds_override > 0:
                # Tentative — caller already verified the fill happened
                # (on-chain balance dropped). Use the order_status-derived
                # proceeds; the background history reconciler will upgrade
                # this to "polymarket_history" once /data/trades indexes
                # the sub-trades. This branch closes the gap on split CLOB
                # matches that today's safety code was deferring on.
                proceeds = float(proceeds_override)
                proceeds_source = "order_status"
                _emit(
                    f"  [INFO] _record_exit: using order_status proceeds "
                    f"${proceeds:.4f} for {position.id} "
                    f"(/data/trades empty; will reconcile to history value)"
                )
            else:
                # Refuse to fabricate proceeds at the limit price. The
                # phantom-TP-fill bug on 2026-04-30 wrote a +$1.99 profit
                # this way for a trade that never matched (real -$6.32).
                # Defer instead — caller marks the position pending and
                # expiry resolution writes the correct WIN/LOSS row.
                raise ProceedsUnverifiedError(
                    f"_record_exit: proceeds unverifiable for {position.id} "
                    f"(exit_oid={exit_oid}, exit_price=${exit_price:.4f}, "
                    f"shares={position.shares:.4f}, lookup_error={lookup_exc})"
                )
        else:
            gross = position.shares * exit_price
            schedule = getattr(position, "fee_schedule", None)
            if schedule is not None:
                if proceeds_override is not None:
                    proceeds = float(proceeds_override)
                else:
                    proceeds = gross - fee_for_fill(exit_price, position.shares, schedule, is_maker=result == TradeResult.TP_FILLED)
                proceeds_source = "paper_market_fees"
            else:
                proceeds = gross
            proceeds *= 1 - self.config.friction_pct
        pnl = proceeds - position.cost_basis
        pnl_pct = (pnl / position.cost_basis) * 100 if position.cost_basis > 0 else 0
        if self.executor.is_live:
            _emit(f"  [INFO] _record_exit: proceeds=${proceeds:.4f} (source={proceeds_source}) "
                  f"cost_basis=${position.cost_basis:.4f} pnl=${pnl:+.4f}")

        trade = Trade(
            id=position.id,
            direction=position.direction,
            shares=position.shares,
            entry_price=position.entry_price,
            exit_price=exit_price,
            cost_basis=position.cost_basis,
            proceeds=proceeds,
            pnl=pnl,
            pnl_pct=pnl_pct,
            result=result,
            opened_at=position.opened_at,
            market_title=position.market_title,
            reference_price=position.reference_price,
            entry_context=position.entry_context,
            entry_order_id=position.order_id,
            exit_order_id=exit_oid,
            token_id=position.token_id,
            proceeds_source=proceeds_source,
        )

        self.state.close_position(position, trade)
        self.state.save()

        # Log equity event for PnL curve tracking
        self.state.log_equity_event(
            event=result.value,
            direction=position.direction.value,
            entry_price=position.entry_price,
            exit_price=exit_price,
            shares=position.shares,
            pnl=pnl,
        )

        # Log per-trade friction breakdown so the configured paper buffer can
        # be validated against live reality.
        try:
            self.state.log_friction_realized(position, trade)
        except Exception as exc:
            _emit(f"[WARN] log_friction_realized failed: {exc}")

        return trade

    def close_position_tp(
        self,
        position: Position,
        exit_price: float,
        *,
        proceeds_override: Optional[float] = None,
    ) -> None:
        """Close position at take profit.

        ``proceeds_override``: optional verified proceeds value (typically
        ``size_matched * matched_price`` from the order_status payload at
        the moment of TP fill). Used as a tentative fallback when
        Polymarket's /data/trades endpoint hasn't indexed the matched
        sub-trades yet (common on split CLOB matches). The background
        ``polymarket_history_reconciler`` upgrades these tentative rows
        to the exact /data/trades value once indexing catches up.
        """
        token_id = position.token_id  # Save before _record_exit removes position
        position_shares = position.shares  # Cache before close mutates state

        try:
            trade = self._record_exit(
                position,
                exit_price,
                TradeResult.TP_FILLED,
                proceeds_override=proceeds_override,
            )
        except ProceedsUnverifiedError as exc:
            # Live mode + on-chain proceeds query failed. Refuse to fabricate
            # a fill at the limit price; defer to a future tick or expiry
            # resolution. The phantom-TP-fill bug on 2026-04-30 wrote a
            # +$1.99 profit this way for a trade that never matched.
            position.pending_settlement = True
            position.pending_settlement_reason = "proceeds_unverified"
            self.state.save()
            timestamp = datetime.now().strftime("%H:%M:%S")
            _emit(f"\n[{timestamp}] >>> TP DEFERRED: proceeds unverified <<<")
            _emit(f"  {exc}")
            _emit("  Position kept open + marked pending_settlement; will retry "
                  "on a future tick or settle at market expiry.")
            return

        self.state.last_tp_fill_time = datetime.now(timezone.utc)
        self.state.save()

        _emit(f"\n>>> TP FILLED: {position.direction.value} <<<")
        _emit(f"  Entry:    ${position.entry_price:.4f}")
        _emit(f"  Exit:     ${exit_price:.4f}")
        _emit(f"  P&L:      ${trade.pnl:+.2f} ({trade.pnl_pct:+.1f}%)")

        # Verify position is fully closed on chain. If shares are still
        # in the wallet above the dust threshold (5% of original), the
        # CLOB order_status was lying — VOID the just-recorded record,
        # rewrite the CSV row in place, and re-mark the position as
        # pending so expiry resolution writes the correct PnL.
        if self.executor.is_live:
            timestamp = datetime.now().strftime("%H:%M:%S")
            time.sleep(2)
            try:
                remaining = self.executor.get_position(token_id, strict=True)
            except Exception as exc:
                source = (getattr(trade, "proceeds_source", "") or "").lower()
                self.state.log_risk_event(
                    kind="tp_post_close_balance_unreadable",
                    details={
                        "position_id": position.id,
                        "token_id": token_id,
                        "proceeds_source": source,
                        "error": str(exc),
                    },
                )
                if source in ("order_status", "planned"):
                    _emit(f"[{timestamp}] [CRITICAL] TP VERIFY UNKNOWN: "
                          f"could not read on-chain balance for {position.id}")
                    _emit(f"[{timestamp}]            VOIDING tentative TP record + "
                          f"rewriting {self.state.trades_file.name}")
                    phantom = self.state.void_last_close_for_position(
                        position, reason="tp_post_close_balance_unreadable",
                    )
                    self._rewrite_live_trade_row_as_voided(position, phantom)
                    self.state.save()
                    return
                _emit(f"[{timestamp}] [WARN] TP FILLED: could not verify post-close "
                      f"balance for {position.id}: {exc}")
                return
            if remaining > position_shares * 0.05 and remaining > 0.01:
                _emit(f"[{timestamp}] [CRITICAL] TP REJECT: {remaining:.4f}/"
                      f"{position_shares:.4f} shares still on chain after "
                      f"TP_FILLED record was written for {position.id}")
                _emit(f"[{timestamp}]            VOIDING phantom record + "
                      f"rewriting {self.state.trades_file.name}")
                phantom = self.state.void_last_close_for_position(
                    position, reason="tp_unverified_residual_on_chain",
                )
                self._rewrite_live_trade_row_as_voided(position, phantom)
                self.state.save()
                self._sell_residual_shares(token_id, remaining)
            elif remaining > 0.01:
                # Tiny dust remainder (< 5%): legitimate settlement-lag
                # leftover; clean up but keep the TP_FILLED record valid.
                _emit(f"[{timestamp}] [WARN] TP FILLED: {remaining:.4f} shares "
                      "still on chain (dust)")
                self._sell_residual_shares(token_id, remaining)
            else:
                _emit(f"[{timestamp}] TP FILLED: Verified 0 shares remaining on chain")

    def _rewrite_live_trade_row(
        self,
        position_id: str,
        updates: dict,
        *,
        audit_suffix: str = "voided_audit",
        audit_label: str = "VOID",
    ) -> Optional[list[str]]:
        """Atomically rewrite the most-recent CSV row for ``position_id``
        with the column updates in ``updates`` (mapping column name →
        new string value). Returns the *original* row before the
        rewrite (so callers can include the pre-state in an audit log)
        or None if no row matched.

        ``audit_suffix`` controls the audit-CSV file name
        (``<stem>_<suffix>.csv``). ``audit_label`` is just the log tag.

        Bumps the trades CSV mtime so the dashboard's trades_tail
        re-seeds via the in-place modification detection.
        """
        trades_path = self.state.trades_file
        if not trades_path.exists():
            return None

        audit_path = trades_path.parent / f"{trades_path.stem}_{audit_suffix}.csv"

        try:
            with open(trades_path, "r", newline="", encoding="utf-8") as f:
                reader = csv.reader(f)
                rows = list(reader)
        except OSError as exc:
            _emit(f"[WARN] _rewrite_live_trade_row: could not read "
                  f"{trades_path}: {exc}")
            return None

        if len(rows) < 2:
            return None

        header = rows[0]
        try:
            id_idx = header.index("id")
        except ValueError as exc:
            _emit(f"[WARN] _rewrite_live_trade_row: missing 'id' column: {exc}")
            return None

        # Resolve update column indices up-front so an unknown column
        # fails fast rather than silently dropping the update.
        try:
            update_indices = {col: header.index(col) for col in updates}
        except ValueError as exc:
            _emit(f"[WARN] _rewrite_live_trade_row: unknown column in "
                  f"updates ({exc}); aborting rewrite")
            return None

        original_row: Optional[list[str]] = None
        for i in range(len(rows) - 1, 0, -1):
            if rows[i] and len(rows[i]) > id_idx and rows[i][id_idx] == position_id:
                original_row = list(rows[i])
                for col, val in updates.items():
                    rows[i][update_indices[col]] = val
                break

        if original_row is None:
            _emit(f"[WARN] _rewrite_live_trade_row: no row for "
                  f"{position_id} in {trades_path.name}; nothing to rewrite")
            return None

        parent = trades_path.parent
        parent.mkdir(parents=True, exist_ok=True)
        tmp_fd, tmp_path = tempfile.mkstemp(dir=str(parent), suffix=".tmp")
        try:
            with open(tmp_fd, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerows(rows)
            for attempt in range(5):
                try:
                    os.replace(tmp_path, trades_path)
                    break
                except PermissionError:
                    if attempt < 4:
                        time.sleep(0.1 * (attempt + 1))
                    else:
                        raise
        except Exception as exc:
            log.warning("_rewrite_live_trade_row failed for %s: %s", position_id, exc)
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise

        try:
            first = not audit_path.exists()
            with open(audit_path, "a", newline="", encoding="utf-8") as af:
                w = csv.writer(af)
                if first:
                    w.writerow(["recorded_at", *header])
                w.writerow([datetime.now(timezone.utc).isoformat(), *original_row])
        except OSError as exc:
            _emit(f"[WARN] _rewrite_live_trade_row: audit write failed: {exc}")

        try:
            now_ts = datetime.now().timestamp()
            os.utime(trades_path, (now_ts, now_ts))
        except OSError:
            pass

        _emit(f"[{audit_label}] Rewrote {trades_path.name} row for "
              f"{position_id}: updated {list(updates.keys())} "
              f"(audit -> {audit_path.name})")
        return original_row

    def _rewrite_live_trade_row_as_voided(
        self,
        position: Position,
        phantom: Optional[Trade],
    ) -> None:
        """Compatibility wrapper around ``_rewrite_live_trade_row`` —
        keeps the original phantom-TP void semantics (zero proceeds/pnl,
        result=VOIDED_PHANTOM_TP) and reuses the same audit file name
        the dashboard auto-correction flow expects.
        """
        updates = {
            "result": TradeResult.VOIDED_PHANTOM_TP.value,
            "exit_price": "0.0000",
            "proceeds": "0.00",
            "pnl": "0.00",
            "pnl_pct": "0.00%",
            "closed_at": datetime.now(timezone.utc).isoformat(),
        }
        original = self._rewrite_live_trade_row(
            position.id,
            updates,
            audit_suffix="voided_audit",
            audit_label="VOID",
        )
        phantom_pnl = f"{phantom.pnl:+.2f}" if phantom is not None else "?"
        if original is not None:
            _emit(f"[VOID]   prev pnl={phantom_pnl} -> VOIDED_PHANTOM_TP")

    def _get_trail_trigger(self, position: Position, current_bid: float) -> Optional[float]:
        """Return trail_level if trailing stop should fire, else None."""
        if self.config.trail_activation <= 0 or self.config.trail_distance <= 0:
            return None
        activation_price = position.entry_price * (1 + self.config.trail_activation)
        if position.peak_price < activation_price:
            return None
        trail_level = position.peak_price - position.entry_price * self.config.trail_distance
        return trail_level if current_bid <= trail_level else None

    def _place_trail_order(self, position: Position, trail_level: float, reason: str = "") -> None:
        """Cancel TP order and place GTC sell at trail_level."""
        timestamp = datetime.now().strftime("%H:%M:%S")

        # Cancel TP order first (can't have two sells for same shares)
        if position.tp_order_id:
            if not self.executor.cancel_order(position.tp_order_id):
                _emit(f"[{timestamp}] TRAIL: Failed to cancel TP order {position.tp_order_id}, "
                      f"skipping trail placement to avoid duplicate sells")
                return
            position.tp_order_id = None

        # Cancel existing trail order if re-placing
        if position.trail_order_id:
            if not self.executor.cancel_order(position.trail_order_id):
                _emit(f"[{timestamp}] TRAIL: Failed to cancel old trail order {position.trail_order_id}")
                return
            position.trail_order_id = None

        try:
            actual_shares = self.executor.get_position(position.token_id, strict=True)
        except RuntimeError as exc:
            self.state.log_risk_event(
                kind="trail_order_balance_unknown",
                details={
                    "position_id": position.id,
                    "token_id": position.token_id,
                    "error": str(exc)[:160],
                },
            )
            _emit(f"[{timestamp}] TRAIL: Could not verify live balance, skipping trail placement: {exc}")
            return
        if actual_shares <= 0.01:
            _emit(f"[{timestamp}] TRAIL: No live shares available, skipping trail placement")
            return
        sell_shares = math.floor(actual_shares * 100) / 100

        trail_level = round(max(0.01, min(trail_level, 0.99)), 2)

        result = self.executor.sell_limit(position.token_id, trail_level, sell_shares)
        if result.success:
            position.trail_order_id = result.order_id
            position.trail_order_price = trail_level
            self.state.save()
            _emit(f"[{timestamp}] TRAIL: Sell placed @ ${trail_level:.4f} ({reason})")
        else:
            _emit(f"[{timestamp}] TRAIL: Failed to place sell: {result.error_message}")
            # Fall back to TP order
            self._place_backup_tp_order(position, reason="trail_order_failed")

    def _close_trail_filled(
        self,
        position: Position,
        exit_price: float,
        *,
        proceeds_override: Optional[float] = None,
    ) -> None:
        """Record exit when trail GTC sell fills.

        ``proceeds_override`` mirrors close_position_tp: the verified
        size_matched × matched_price from order_status at fill time,
        used as a tentative fallback when /data/trades hasn't indexed.
        """
        token_id = position.token_id
        trail_price = position.trail_order_price
        try:
            trade = self._record_exit(
                position,
                exit_price,
                TradeResult.TRAILING_STOP,
                proceeds_override=proceeds_override,
            )
        except ProceedsUnverifiedError as exc:
            position.pending_settlement = True
            position.pending_settlement_reason = "proceeds_unverified"
            self.state.save()
            timestamp = datetime.now().strftime("%H:%M:%S")
            _emit(f"\n[{timestamp}] >>> TRAIL DEFERRED: proceeds unverified <<<")
            _emit(f"  {exc}")
            return

        _emit(f"\n>>> TRAILING STOP FILLED: {position.direction.value} <<<")
        _emit(f"  Entry:    ${position.entry_price:.4f}")
        _emit(f"  Peak:     ${position.peak_price:.4f}")
        _emit(f"  Trail:    ${trail_price:.4f}")
        _emit(f"  Exit:     ${exit_price:.4f}")
        _emit(f"  P&L:      ${trade.pnl:+.2f} ({trade.pnl_pct:+.1f}%)")

        # Post-exit verification (reuses existing pattern)
        if self.executor.is_live:
            time.sleep(2)
            try:
                remaining = self.executor.get_position(token_id, strict=True)
            except RuntimeError as exc:
                position.pending_settlement = True
                position.pending_settlement_reason = "trail_balance_unknown"
                self.state.save()
                _emit(f"  [WARN] Could not verify trail exit balance: {exc}")
                return
            if remaining > 0.01:
                _emit(f"  [WARN] {remaining:.4f} shares still on chain")
                self._sell_residual_shares(token_id, remaining)
            else:
                _emit("  Verified 0 shares remaining on chain")

    def _sell_residual_shares(self, token_id: str, shares: float) -> None:
        """Sell residual shares left after an exit (e.g., from partial fill).

        Verifies the on-chain balance actually dropped before logging the
        sale as successful. The previous version logged success solely
        on ``OrderResult.success`` (which only confirms order placement,
        not match), so a phantom residual "sale" could be logged with
        confidence even when the shares stayed in the wallet — same
        category of bug as the phantom-TP-fill on 2026-04-30.
        """
        timestamp = datetime.now().strftime("%H:%M:%S")
        if shares < 0.01:
            return  # Dust, not worth the API call

        mid = self.executor.get_mid_price(token_id)
        if mid is None:
            _emit(f"[{timestamp}] RESIDUAL: No market data, "
                  f"cannot sell {shares:.4f} residual shares")
            return

        sell_price = round(mid - 0.01, 2)
        sell_price = max(sell_price, 0.01)
        sell_shares = math.floor(shares * 100) / 100

        result = self.executor.sell_limit(token_id, sell_price, sell_shares)
        if not result.success:
            err = (result.error_message or "").lower()
            if any(kw in err for kw in ("insufficient", "balance", "locked")):
                _emit(f"[{timestamp}] RESIDUAL: Shares likely locked by pending "
                      f"order ({result.error_message})")
            else:
                _emit(f"[{timestamp}] RESIDUAL: Failed to sell residual: "
                      f"{result.error_message}")
            return

        # Order placed — verify it actually filled by re-checking the wallet.
        # CLOB matches happen async; give it a short window before declaring
        # the residual "sold" or "still pending".
        time.sleep(2.0)
        try:
            balance_after = self.executor.get_position(token_id, strict=True)
        except Exception as exc:
            balance_after = None
            _emit(f"[{timestamp}] RESIDUAL: get_position raised after "
                  f"sell ({exc}); cannot verify fill")

        if balance_after is None:
            _emit(f"[{timestamp}] RESIDUAL: Sell placed @ ${sell_price:.4f} "
                  f"(order {result.order_id}) — fill UNVERIFIED (balance "
                  f"unreadable); will be reconciled next cycle")
        elif balance_after <= shares * 0.05 or balance_after <= 0.01:
            _emit(f"[{timestamp}] RESIDUAL: Sold {sell_shares:.2f} residual "
                  f"shares @ ${sell_price:.4f} (balance verified "
                  f"{shares:.4f} -> {balance_after:.4f})")
        else:
            _emit(f"[{timestamp}] RESIDUAL: Sell placed @ ${sell_price:.4f} "
                  f"(order {result.order_id}) but balance unchanged "
                  f"({shares:.4f} -> {balance_after:.4f}); order is "
                  f"resting, will be picked up by reconciliation")

    def _execute_limit_exit(self, position: Position) -> tuple[bool, float]:
        """Execute a limit-order exit for a position.

        Cancels TP, places limit sell above mid, polls for fill,
        refreshes price downward if mid drops.

        Returns:
            (success, fill_price) — (False, 0.0) on timeout.
        """

        timestamp = datetime.now().strftime("%H:%M:%S")

        # 1. Cancel TP order if exists
        if position.tp_order_id:
            if not self.executor.cancel_order(position.tp_order_id):
                _emit(
                    f"[{timestamp}] LIMIT EXIT: Failed to cancel TP order "
                    f"{position.tp_order_id}; aborting to avoid duplicate sells"
                )
                return (False, 0.0)
            _emit(f"[{timestamp}] LIMIT EXIT: Cancelled TP order {position.tp_order_id}")

        # 2. Check position still exists (TP may have filled during cancel)
        try:
            actual_shares = self.executor.get_position(position.token_id, strict=True)
        except RuntimeError as exc:
            _emit(f"[{timestamp}] LIMIT EXIT: Balance read failed; aborting: {exc}")
            return (False, 0.0)
        if actual_shares <= 0:
            _emit(f"[{timestamp}] LIMIT EXIT: No shares remaining — TP likely filled")
            return (False, 0.0)

        sell_shares = math.floor(actual_shares * 100) / 100

        # 3. Get mid price
        mid = self.executor.get_mid_price(position.token_id)
        if mid is None:
            _emit(f"[{timestamp}] LIMIT EXIT: No market data — aborting")
            return (False, 0.0)

        sell_price = round(mid + 0.01, 2)
        sell_price = min(sell_price, 0.99)

        # 4. Place limit sell
        result = self.executor.sell_limit(position.token_id, sell_price, sell_shares)
        if not result.success:
            _emit(f"[{timestamp}] LIMIT EXIT: Failed to place sell: {result.error_message}")
            return (False, 0.0)

        order_id = result.order_id
        _emit(f"[{timestamp}] LIMIT EXIT: Placed sell {sell_shares:.2f} @ ${sell_price:.4f} "
              f"(mid=${mid:.4f})")

        # 5. Poll loop
        timeout = self.config.exit_order_timeout_seconds
        refresh_interval = self.config.exit_order_refresh_seconds
        poll_interval = 1.5
        start = time.time()
        last_refresh = start

        while time.time() - start < timeout:
            time.sleep(poll_interval)

            # Check order status
            status = self.executor.get_order_status(order_id)
            if status:
                order_status = status.get("status", "").lower()
                if order_status == "matched":
                    fill_price = status.get("price", sell_price)
                    size_matched = status.get("size_matched", 0)
                    original_size = status.get("original_size", sell_shares)

                    # Check for partial fill
                    if size_matched > 0 and size_matched < original_size * 0.95:
                        _emit(f"[{timestamp}] LIMIT EXIT: Partial fill "
                              f"{size_matched:.2f}/{original_size:.2f}")
                        continue  # Keep polling for full fill

                    _emit(f"[{timestamp}] LIMIT EXIT: Filled {size_matched:.2f}/"
                          f"{original_size:.2f} shares at ${fill_price:.4f}")
                    return (True, fill_price)
                elif order_status in ("cancelled", "expired"):
                    # Re-check shares — maybe filled externally
                    try:
                        remaining = self.executor.get_position(position.token_id, strict=True)
                    except RuntimeError as exc:
                        _emit(f"[{timestamp}] LIMIT EXIT: Balance read failed after order terminal: {exc}")
                        return (False, 0.0)
                    if remaining <= 0:
                        _emit(f"[{timestamp}] LIMIT EXIT: Order gone, position empty — filled externally")
                        return (True, sell_price)
                    # Re-place
                    new_mid = self.executor.get_mid_price(position.token_id)
                    if new_mid is not None:
                        sell_price = round(new_mid + 0.01, 2)
                        sell_price = min(sell_price, 0.99)
                    sell_shares = math.floor(remaining * 100) / 100
                    result = self.executor.sell_limit(position.token_id, sell_price, sell_shares)
                    if result.success:
                        order_id = result.order_id
                        _emit(f"[{timestamp}] LIMIT EXIT: Re-placed sell @ ${sell_price:.4f}")
                    else:
                        _emit(f"[{timestamp}] LIMIT EXIT: Re-place failed: {result.error_message}")
                        break

            # Refresh price if mid dropped
            now = time.time()
            if now - last_refresh >= refresh_interval:
                last_refresh = now
                new_mid = self.executor.get_mid_price(position.token_id)
                if new_mid is not None and new_mid < sell_price - 0.005:
                    # Mid dropped — cancel and re-place at lower price
                    if not self.executor.cancel_order(order_id):
                        _emit(f"[{timestamp}] LIMIT EXIT: Failed to cancel sell {order_id}; aborting refresh")
                        return (False, 0.0)
                    sell_price = round(new_mid + 0.01, 2)
                    sell_price = min(sell_price, 0.99)
                    result = self.executor.sell_limit(position.token_id, sell_price, sell_shares)
                    if result.success:
                        order_id = result.order_id
                        _emit(f"[{timestamp}] LIMIT EXIT: Refreshed sell @ ${sell_price:.4f} "
                              f"(mid=${new_mid:.4f})")

        # 6. Timeout — cancel outstanding order
        if not self.executor.cancel_order(order_id):
            _emit(f"[{timestamp}] LIMIT EXIT: Timed out and cancel was not confirmed for {order_id}")
            return (False, 0.0)
        _emit(f"[{timestamp}] LIMIT EXIT: Timed out after {timeout}s, cancelled order")
        return (False, 0.0)

    def close_position_trailing_stop(self, position: Position, current_bid: float, trail_level: float) -> None:
        """Close position via trailing stop (paper: fill at trail limit price)."""
        exit_price = current_bid
        trade = self._record_exit(position, exit_price, TradeResult.TRAILING_STOP)

        _emit(f"\n>>> TRAILING STOP: {position.direction.value} <<<")
        _emit(f"  Entry:    ${position.entry_price:.4f}")
        _emit(f"  Peak:     ${position.peak_price:.4f}")
        _emit(f"  Trail:    ${trail_level:.4f}")
        _emit(f"  Exit:     ${exit_price:.4f}")
        _emit(f"  P&L:      ${trade.pnl:+.2f} ({trade.pnl_pct:+.1f}%)")

    def close_position_stop_loss(self, position: Position, current_bid: float) -> None:
        """Close position via stop-loss (paper: market sell with VWAP slippage)."""
        set_fees = getattr(self.executor, "set_fee_schedule", None)
        if callable(set_fees):
            set_fees(position.fee_schedule, token_id=position.token_id)
        # Simulate market sell to capture slippage (matches live FOK sell)
        result = self.executor.sell_market(position.token_id, position.shares)
        if not result.success:
            timestamp = datetime.now().strftime("%H:%M:%S")
            _emit(f"[{timestamp}] [WARN] SL sell failed: {result.error_message} — will retry next heartbeat")
            return

        exit_price = result.avg_fill_price
        # FOK market sell already returns verified shares_filled + avg_price.
        # Use as proceeds_override so we don't depend on /data/trades indexing.
        sl_proceeds = (
            float(result.proceeds)
            if result.shares_filled and result.avg_fill_price
            else None
        )
        try:
            trade = self._record_exit(
                position,
                exit_price,
                TradeResult.STOP_LOSS,
                proceeds_override=sl_proceeds,
            )
        except ProceedsUnverifiedError as exc:
            position.pending_settlement = True
            position.pending_settlement_reason = "proceeds_unverified"
            self.state.save()
            timestamp = datetime.now().strftime("%H:%M:%S")
            _emit(f"\n[{timestamp}] >>> STOP LOSS DEFERRED: proceeds unverified <<<")
            _emit(f"  {exc}")
            return

        _emit(f"\n>>> STOP LOSS: {position.direction.value} <<<")
        _emit(f"  Entry:    ${position.entry_price:.4f}")
        _emit(f"  SL:       ${position.stop_loss_price:.4f}")
        _emit(f"  Exit:     ${exit_price:.4f}")
        _emit(f"  P&L:      ${trade.pnl:+.2f} ({trade.pnl_pct:+.1f}%)")

    def _execute_stop_loss_live(self, position: Position, current_bid: float) -> None:
        """Execute stop-loss exit in live mode (cancel orders + market sell)."""
        timestamp = datetime.now().strftime("%H:%M:%S")
        token_id = position.token_id

        _emit(f"\n[{timestamp}] STOP LOSS TRIGGERED: {position.direction.value}")
        _emit(f"  Entry: ${position.entry_price:.4f} | SL: ${position.stop_loss_price:.4f} | Bid: ${current_bid:.4f}")

        # 1. Cancel TP order (shares locked by limit order prevent market sell)
        if position.tp_order_id:
            if not self.executor.cancel_order(position.tp_order_id):
                _emit(f"  [ERROR] Failed to cancel TP order {position.tp_order_id}, "
                      f"skipping SL — will retry next cycle")
                return
            _emit(f"  Cancelled TP order {position.tp_order_id}")
            position.tp_order_id = None

        # 2. Cancel trail order
        if position.trail_order_id:
            if not self.executor.cancel_order(position.trail_order_id):
                _emit(f"  [ERROR] Failed to cancel trail order {position.trail_order_id}, "
                      f"skipping SL — will retry next cycle")
                return
            _emit(f"  Cancelled trail order {position.trail_order_id}")
            position.trail_order_id = None

        # 3. Get actual shares on chain (strict=True to fail loudly on API error)
        try:
            actual_shares = self.executor.get_position(token_id, strict=True)
        except RuntimeError as e:
            _emit(f"  [ERROR] Cannot verify position on chain: {e}")
            _emit("  Skipping SL execution — will retry next cycle")
            return
        if actual_shares <= 0:
            _emit("  No shares on chain — position already closed")
            try:
                self._record_exit(position, current_bid, TradeResult.STOP_LOSS)
            except ProceedsUnverifiedError as exc:
                position.pending_settlement = True
                position.pending_settlement_reason = "proceeds_unverified"
                self.state.save()
                _emit(f"  [DEFERRED] proceeds unverified: {exc}")
            return

        # 4. Market sell (FOK for immediate execution)
        sell_shares = math.floor(actual_shares * 100) / 100
        result = self.executor.sell_market(token_id, sell_shares)

        if result.success:
            exit_price = result.avg_fill_price if result.avg_fill_price > 0 else current_bid
            position.last_exit_order_id = result.order_id
            self.state.save()
            sl_proceeds = (
                float(result.shares_filled) * float(result.avg_fill_price)
                if result.shares_filled and result.avg_fill_price
                else None
            )
            try:
                trade = self._record_exit(
                    position,
                    exit_price,
                    TradeResult.STOP_LOSS,
                    proceeds_override=sl_proceeds,
                )
            except ProceedsUnverifiedError as exc:
                position.pending_settlement = True
                position.pending_settlement_reason = "proceeds_unverified"
                self.state.save()
                _emit(f"  [DEFERRED] proceeds unverified: {exc}")
                return
            _emit(f"  SOLD {sell_shares:.2f} shares at ${exit_price:.4f}")
            _emit(f"  P&L: ${trade.pnl:+.2f} ({trade.pnl_pct:+.1f}%)")
        else:
            _emit(f"  Market sell failed: {result.error_message}")
            if result.order_id:
                _emit(f"  [WARN] Market sell order {result.order_id} may still be live")
                _emit("  Keeping position open and retrying on the next cycle/reconciliation")
                self.state.save()
                return

            # Fall back to limit sell at best_bid - 0.01 only after the
            # failed market sell is confirmed gone.
            sell_price = round(max(current_bid - 0.01, 0.01), 2)
            limit_result = self.executor.sell_limit(token_id, sell_price, sell_shares)
            if limit_result.success:
                # Store pending SL order — don't close state until fill confirmed
                position.pending_sl_order_id = limit_result.order_id
                position.tp_order_id = None
                position.trail_order_id = None
                self.state.save()
                _emit(f"  Fallback limit sell placed at ${sell_price:.4f} "
                      f"(order {limit_result.order_id})")

                # Poll for fill (5 iterations x 2s = 10s)
                filled = False
                for _ in range(5):
                    time.sleep(2)
                    status = self.executor.get_order_status(limit_result.order_id)
                    if status and status.get("status", "").lower() == "matched":
                        fill_price = status.get("price", sell_price)
                        size_matched = float(status.get("size_matched") or 0.0)
                        sl_proceeds = (
                            size_matched * float(fill_price)
                            if size_matched > 0 and fill_price
                            else None
                        )
                        try:
                            trade = self._record_exit(
                                position,
                                fill_price,
                                TradeResult.STOP_LOSS,
                                proceeds_override=sl_proceeds,
                            )
                        except ProceedsUnverifiedError as exc:
                            position.pending_settlement = True
                            position.pending_settlement_reason = "proceeds_unverified"
                            position.pending_sl_order_id = None
                            self.state.save()
                            _emit(f"  [DEFERRED] SL fill confirmed at ${fill_price:.4f} "
                                  f"but proceeds unverified: {exc}")
                            filled = True
                            break
                        position.pending_sl_order_id = None
                        _emit(f"  SL limit fill confirmed at ${fill_price:.4f}")
                        _emit(f"  P&L: ${trade.pnl:+.2f} ({trade.pnl_pct:+.1f}%)")
                        filled = True
                        break

                if not filled:
                    _emit(f"  SL limit order not yet filled — deferring to reconciliation")
                return  # Skip post-exit verification; reconciliation will handle
            else:
                _emit(f"  CRITICAL: Both market and limit sell failed: {limit_result.error_message}")
                _emit(f"  Position kept in state — shares may still be on chain.")
                _emit(f"  Manual intervention required.")
                return

        # 5. Post-exit verification
        time.sleep(2)
        try:
            remaining = self.executor.get_position(token_id, strict=True)
        except RuntimeError as exc:
            _emit(f"  [WARN] Could not strictly verify post-SL balance: {exc}")
            _emit("  Reconciliation/manual review will verify residual shares before new exits")
            risk_event_logger = getattr(self.state, "log_risk_event", None)
            if callable(risk_event_logger):
                risk_event_logger(
                    kind="stop_loss_balance_unknown",
                    details={
                        "token_id": token_id,
                        "market_id": position.market_id,
                        "position_id": getattr(position, "id", ""),
                        "reason": str(exc),
                    },
                )
            self.state.save()
            return
        if remaining > 0.01:
            _emit(f"  [WARN] {remaining:.4f} shares still on chain")
            self._sell_residual_shares(token_id, remaining)
        else:
            _emit("  Verified 0 shares remaining on chain")

    # ---------------------------------------------------------------
    # Startup / shutdown
    # ---------------------------------------------------------------

    def startup(self, confirm_live: bool = True) -> bool:
        """Perform startup tasks: balance sync, reconciliation, TP verification.

        Returns False if live trading confirmation was declined.
        Called by main.py before the main loop starts.
        """
        mode = "LIVE" if self.executor.is_live else "PAPER"
        _emit(f"\nStarting {mode} trading...")

        # Require confirmation for live trading
        if confirm_live and self.executor.is_live and not self._confirm_live_trading():
            return False

        self._sync_mirror_state_metadata()

        # If the previous process died inside a Polymarket outage, resume the
        # status-edge recorder so the recovery writes a real close edge.
        self._seed_polymarket_status_state()

        # Sync capital from real USDC balance for live trading
        if self.executor.is_live:
            real_balance = self.executor.get_usdc_balance()
            if real_balance is not None:
                _emit(f"Live USDC balance: ${real_balance:.2f}")
                self.state.current_capital = real_balance
                # Set starting capital from Polymarket on first run
                if not self.state.closed_positions and not self.state.open_positions:
                    self.state.starting_capital = real_balance
                self.state.save()

        # Startup reconciliation — catch manual closes that happened while bot was off
        if self.executor.is_live and self.config.reconciliation_enabled and self.state.open_positions:
            _emit(f"Reconciling {len(self.state.open_positions)} open position(s) against Polymarket...")
            self._get_reconciliation_service().reconcile_positions(mark_now=True)

        # Startup TP verification — ensure all open positions have TP orders
        if self.executor.is_live and self.state.open_positions:
            _emit(f"Verifying TP orders for {len(self.state.open_positions)} open position(s)...")
            self._get_position_tracker().verify_tp_orders()

        if self._mirror_control_enabled() and self.state.open_positions:
            self._recover_pending_mirror_signals(None)
            for stream_id, signal in self._poll_mirror_signals():
                if signal is None:
                    subscriber = getattr(self, "_mirror_subscriber", None)
                    if subscriber is not None and stream_id and hasattr(subscriber, "ack_message"):
                        subscriber.ack_message(stream_id)
                    continue
                if signal.event in {"STOP_LOSS", "TP_FILLED"}:
                    self._process_mirror_delivery(stream_id, signal, None, source="startup")

        _emit(f"Mode: {mode}")
        _emit(f"TP check interval: {self.config.check_interval_seconds}s")
        if self.config.edge_alpha_up == self.config.edge_alpha_down and self.config.edge_floor_up == self.config.edge_floor_down:
            _emit(f"Edge curve: alpha={self.config.edge_alpha_up}, floor={self.config.edge_floor_up:.0%}")
        else:
            _emit(f"Edge curve: UP alpha={self.config.edge_alpha_up} floor={self.config.edge_floor_up:.0%} | DOWN alpha={self.config.edge_alpha_down} floor={self.config.edge_floor_down:.0%}")
        _emit(f"TP target: {self.config.tp_percentage * 100:.0f}%")
        if self.config.trail_activation > 0:
            _emit(f"Trail stop: activate at {self.config.trail_activation*100:.0f}%, "
                  f"trail {self.config.trail_distance*100:.0f}pp from peak")
        if self.config.stop_loss_pct > 0:
            _emit(f"Stop loss: {self.config.stop_loss_pct*100:.0f}% below entry")
        if self.executor.is_live:
            _emit(
                "Entry orders: GTC wait/cancel "
                f"{self.config.entry_cancel_timeout_s:.1f}s, "
                f"price cap ${self.config.max_entry_price:.2f}, "
                f"retry {self.config.live_entry_order_retries}x/"
                f"{self.config.live_entry_retry_window_seconds:.0f}s "
                f"(guard {self.config.live_entry_retry_price_guard:.2f})"
            )
        else:
            _emit(f"Max slippage: {self.config.max_slippage_pct * 100:.1f}% (paper trading only)")
        _emit(f"No-trade zones: first {self.config.no_trade_first_seconds}s, last {self.config.no_trade_last_seconds}s")

        # Resolved risk-gate + observability configuration for audit. Same
        # layout in paper and live so side-by-side logs line up.
        _emit(
            "Risk gates: "
            f"max_concurrent={self.config.max_concurrent_positions}, "
            f"require_tp_confirmed={self.config.require_tp_confirmed_before_new_entry}, "
            f"max_slip_est={self.config.max_slippage_estimate_pct*100:.2f}%, "
            f"min_depth=${self.config.min_liquidity_depth_usd:.0f}"
        )
        _emit(
            "Timing: "
            f"reconcile_interval={self.config.reconciliation_interval_seconds}s, "
            f"delayed_order_timeout={self.config.delayed_order_timeout_seconds}s, "
            f"pending_entry_max_age={self.config.pending_entry_max_age_seconds}s, "
            f"friction_pct={self.config.friction_pct*100:.2f}% (paper only)"
        )

        self.state.display_status(
            self._get_position_tracker().get_position_value,
            live_equity=self._get_live_equity(),
        )

        self._start_mirror_signal_listener()

        return True

    def shutdown(self) -> None:
        """Clean shutdown: save state, display status."""
        mode = "LIVE" if self.executor.is_live else "PAPER"
        _emit(f"\nStopping {mode} trader...")
        self._stop_mirror_signal_listener()
        subscriber = getattr(self, "_mirror_subscriber", None)
        if subscriber is not None:
            subscriber.close()
        self.state.save()
        self.state.display_status(
            self._get_position_tracker().get_position_value,
            live_equity=self._get_live_equity(),
        )
