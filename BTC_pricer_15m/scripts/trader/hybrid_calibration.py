from __future__ import annotations

import json
import logging
import math
import random
import threading
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from scripts.polymarket_edge import run_terminal_script
from scripts.trader._io import atomic_write_json

log = logging.getLogger(__name__)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_iso(dt: str) -> datetime:
    parsed = datetime.fromisoformat(dt)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _atomic_write_json(path: Path, payload: dict) -> None:
    atomic_write_json(path, payload)


@dataclass(frozen=True)
class MarketContext:
    market_id: str
    barrier: float
    market_end_time_iso: str
    # New in schema v2 — let consumers (live/grid) know what the barrier was
    # captured from without running their own ChainlinkPriceFeed. "unknown"
    # is the back-compat default for code paths that don't set it.
    barrier_source: str = "unknown"
    barrier_offset_s: Optional[float] = None
    clob_token_ids: Optional[dict] = None
    market_slug: Optional[str] = None
    market_title: Optional[str] = None
    market_start_time_iso: Optional[str] = None
    contract_metadata: Optional[dict] = None

    @property
    def market_end_time(self) -> datetime:
        return _parse_iso(self.market_end_time_iso)


@dataclass(frozen=True)
class CalibrationEvent:
    schema_version: int
    event_id: str
    source: str
    market_id: str
    barrier: float
    market_end_time_iso: str
    ready_at_iso: str
    ready_at_unix_ms: int
    prob_above: float
    prob_below: float
    model_used: str
    calibration_seconds: float
    terminal_payload: dict
    # New in schema v2 — defaults so legacy v1 events deserialize cleanly.
    barrier_source: str = "unknown"
    barrier_offset_s: Optional[float] = None
    spot_price: Optional[float] = None
    spot_source: str = ""
    spot_source_label: str = ""
    spot_sequence: Optional[int] = None
    spot_published_at_ms: Optional[int] = None
    spot_details: Optional[dict] = None
    up_best_ask: Optional[float] = None
    down_best_ask: Optional[float] = None
    # v3: pre-computed entry signals for all grid instances + live
    entry_signals: tuple = ()
    # v4: MC terminal-return distribution (sorted quantiles of ln(S_T/spot) on
    # an evenly spaced probability grid) + the TTM it was simulated for.
    # Consumers with local evaluation enabled re-threshold this CDF at the
    # live spot each tick instead of acting on the frozen scalar above.
    # Optional/additive — v1-v3 events and older consumers are unaffected.
    terminal_return_quantiles: Optional[tuple] = None
    terminal_return_ttm_days: Optional[float] = None
    # How prob_above/prob_below were produced ("mc", "cf", "bl", "avg").
    prob_model: str = ""
    # v5: explicit payoff and actual evaluation/source times, not receipt age.
    payoff_kind: str = "legacy"
    averaging_window_seconds: float = 0.0
    probability_evaluated_at_iso: Optional[str] = None  # pricing as-of / MC start; ready_at is completion
    probability_spot_observed_at_iso: Optional[str] = None
    contract_metadata: Optional[dict] = None

    # Both producers (from_parts) and consumers (from_dict) construct through
    # here, so a NaN/Inf/out-of-range probability can neither be published nor
    # consumed. Subscribers drop-and-ack events that fail construction, and the
    # grid then holds its non-tradable 0.5/0.5 prior.
    def __post_init__(self) -> None:
        pa, pb = self.prob_above, self.prob_below
        if not (math.isfinite(pa) and math.isfinite(pb)):
            raise ValueError(
                f"non-finite calibration probs: prob_above={pa!r} prob_below={pb!r}"
            )
        if not (0.0 <= pa <= 1.0 and 0.0 <= pb <= 1.0):
            raise ValueError(
                f"calibration probs out of [0,1]: prob_above={pa!r} prob_below={pb!r}"
            )
        if abs(pa + pb - 1.0) > 0.05:
            raise ValueError(
                f"calibration probs do not sum to 1: prob_above={pa!r} prob_below={pb!r}"
            )
        if self.payoff_kind == "chainlink_twap_60s" and self.terminal_return_quantiles is not None:
            raise ValueError("terminal-price quantiles cannot represent a TWAP payoff")

    @property
    def sort_key(self) -> tuple[int, str]:
        return self.ready_at_unix_ms, self.event_id

    @property
    def ready_at(self) -> datetime:
        return _parse_iso(self.ready_at_iso)

    @property
    def market_end_time(self) -> datetime:
        return _parse_iso(self.market_end_time_iso)

    def is_ready_before_market_end(self) -> bool:
        return self.ready_at < self.market_end_time

    @classmethod
    def from_parts(
        cls,
        *,
        source: str,
        context: MarketContext,
        ready_at: datetime,
        model_data: dict,
        terminal_payload: dict,
        entry_signals: tuple = (),
    ) -> "CalibrationEvent":
        quantiles_raw = model_data.get("terminal_return_quantiles")
        quantiles = (
            tuple(float(q) for q in quantiles_raw)
            if isinstance(quantiles_raw, (list, tuple)) and quantiles_raw
            else None
        )
        ttm_days_raw = model_data.get("terminal_return_ttm_days")
        return cls(
            schema_version=5,
            event_id=f"{int(ready_at.timestamp() * 1000)}-{uuid.uuid4().hex}",
            source=source,
            market_id=context.market_id,
            barrier=float(context.barrier),
            market_end_time_iso=context.market_end_time_iso,
            ready_at_iso=ready_at.isoformat(),
            ready_at_unix_ms=int(ready_at.timestamp() * 1000),
            prob_above=float(model_data["prob_above"]),
            prob_below=float(model_data["prob_below"]),
            model_used=str(model_data.get("model_used") or ""),
            calibration_seconds=float(model_data.get("calibration_seconds") or 0.0),
            terminal_payload=terminal_payload,
            barrier_source=str(context.barrier_source),
            barrier_offset_s=context.barrier_offset_s,
            spot_price=float(model_data["spot_price"]) if model_data.get("spot_price") else None,
            spot_source=str(model_data.get("spot_source") or ""),
            spot_source_label=str(model_data.get("spot_source_label") or ""),
            spot_sequence=(
                int(model_data["spot_sequence"])
                if model_data.get("spot_sequence") is not None
                else None
            ),
            spot_published_at_ms=(
                int(model_data["spot_published_at_ms"])
                if model_data.get("spot_published_at_ms") is not None
                else None
            ),
            spot_details=(
                dict(model_data["spot_details"])
                if isinstance(model_data.get("spot_details"), dict)
                else None
            ),
            up_best_ask=float(model_data["up_best_ask"]) if model_data.get("up_best_ask") else None,
            down_best_ask=float(model_data["down_best_ask"]) if model_data.get("down_best_ask") else None,
            entry_signals=entry_signals,
            terminal_return_quantiles=quantiles,
            terminal_return_ttm_days=(
                float(ttm_days_raw) if ttm_days_raw is not None else None
            ),
            prob_model=str(model_data.get("probability_mode") or ""),
            payoff_kind=str(model_data.get("payoff_kind") or "legacy"),
            averaging_window_seconds=float(model_data.get("averaging_window_seconds") or 0.0),
            probability_evaluated_at_iso=model_data.get("probability_evaluated_at"),
            probability_spot_observed_at_iso=model_data.get("probability_spot_observed_at"),
            contract_metadata=context.contract_metadata,
        )

    @classmethod
    def from_dict(cls, raw: dict) -> "CalibrationEvent":
        offset_raw = raw.get("barrier_offset_s")
        spot_raw = raw.get("spot_price")
        up_ask_raw = raw.get("up_best_ask")
        down_ask_raw = raw.get("down_best_ask")
        spot_sequence_raw = raw.get("spot_sequence")
        spot_published_raw = raw.get("spot_published_at_ms")
        return cls(
            schema_version=int(raw.get("schema_version", 1)),
            event_id=str(raw["event_id"]),
            source=str(raw["source"]),
            market_id=str(raw["market_id"]),
            barrier=float(raw["barrier"]),
            market_end_time_iso=str(raw["market_end_time_iso"]),
            ready_at_iso=str(raw["ready_at_iso"]),
            ready_at_unix_ms=int(raw["ready_at_unix_ms"]),
            prob_above=float(raw["prob_above"]),
            prob_below=float(raw["prob_below"]),
            model_used=str(raw.get("model_used") or ""),
            calibration_seconds=float(raw.get("calibration_seconds") or 0.0),
            terminal_payload=dict(raw["terminal_payload"]),
            # v2 fields — default to "unknown" / None for legacy v1 events.
            barrier_source=str(raw.get("barrier_source") or "unknown"),
            barrier_offset_s=float(offset_raw) if offset_raw is not None else None,
            spot_price=float(spot_raw) if spot_raw is not None else None,
            spot_source=str(raw.get("spot_source") or ""),
            spot_source_label=str(raw.get("spot_source_label") or ""),
            spot_sequence=int(spot_sequence_raw) if spot_sequence_raw is not None else None,
            spot_published_at_ms=int(spot_published_raw) if spot_published_raw is not None else None,
            spot_details=dict(raw.get("spot_details")) if isinstance(raw.get("spot_details"), dict) else None,
            up_best_ask=float(up_ask_raw) if up_ask_raw is not None else None,
            down_best_ask=float(down_ask_raw) if down_ask_raw is not None else None,
            entry_signals=tuple(raw.get("entry_signals") or ()) if isinstance(raw.get("entry_signals", []), (list, tuple)) else (),
            # v4 fields — None/"" for v1-v3 events.
            terminal_return_quantiles=(
                tuple(float(q) for q in raw["terminal_return_quantiles"])
                if isinstance(raw.get("terminal_return_quantiles"), (list, tuple))
                and raw.get("terminal_return_quantiles")
                else None
            ),
            terminal_return_ttm_days=(
                float(raw["terminal_return_ttm_days"])
                if raw.get("terminal_return_ttm_days") is not None
                else None
            ),
            prob_model=str(raw.get("prob_model") or ""),
            payoff_kind=str(raw.get("payoff_kind") or "legacy"),
            averaging_window_seconds=float(raw.get("averaging_window_seconds") or 0.0),
            probability_evaluated_at_iso=raw.get("probability_evaluated_at_iso"),
            probability_spot_observed_at_iso=raw.get("probability_spot_observed_at_iso"),
            contract_metadata=raw.get("contract_metadata") if isinstance(raw.get("contract_metadata"), dict) else None,
        )

    def to_dict(self) -> dict:
        return asdict(self)


class HybridCalibrationBroker:
    """Accept calibration results from multiple producers without races.

    Producers:
    - local VPS worker thread (same code path as the old synchronous calibration)
    - remote inbox files pushed from the local machine

    The broker keeps the newest result per market and only applies results for
    the active market. Output paths (terminal_path / meta_path) are derived
    per-consumer from ``inbox_dir.name`` — e.g. ``calibration_inbox_live`` →
    ``terminal_data.live.json`` — so a grid broker and a live broker writing
    concurrently never race on the same destination. Legacy single-consumer
    deployments (inbox_dir = ``calibration_inbox``) keep the bare filenames
    ``terminal_data.json`` / ``.hybrid_calibration_meta.json`` for backward
    compatibility with operator tooling.
    """

    def __init__(
        self,
        *,
        results_dir: Path,
        enable_local_worker: bool = True,
        enable_remote_inbox: bool = True,
        worker_delay_min_seconds: float = 0.0,
        worker_delay_max_seconds: float = 0.0,
        producer_skew_tolerance_ms: Optional[int] = None,
        inbox_dir: Optional[Path] = None,
        ipc_backend: str = "file",
        redis_url: Optional[str] = None,
        redis_stream: str = "market.calibration",
        redis_consumer_group: str = "trader",
        redis_consumer_name: str = "consumer",
        ipc_file_fallback_enabled: bool = True,
        push_consumer_enabled: bool = True,
    ) -> None:
        self.results_dir = results_dir
        self.enable_local_worker = enable_local_worker
        self.enable_remote_inbox = enable_remote_inbox
        self.worker_delay_min_seconds = worker_delay_min_seconds
        self.worker_delay_max_seconds = worker_delay_max_seconds
        self.ipc_backend = ipc_backend
        self.redis_url = redis_url
        self.redis_stream = redis_stream
        self.redis_consumer_group = redis_consumer_group
        self.redis_consumer_name = redis_consumer_name
        self.ipc_file_fallback_enabled = ipc_file_fallback_enabled
        # Push consumer: blocking XREADGROUP on a daemon thread so events are
        # applied the moment they land instead of waiting for the next tick's
        # poll. Fail-open — if the listener can't start, the poll path runs
        # exactly as before.
        self.push_consumer_enabled = push_consumer_enabled
        self._push_listener = None
        # None → use the class-level _MAX_PRODUCER_SKEW_MS default.
        # Tests that use synthetic epoch timestamps pass a huge value to disable.
        self._producer_skew_tolerance_ms = (
            producer_skew_tolerance_ms
            if producer_skew_tolerance_ms is not None
            else self._MAX_PRODUCER_SKEW_MS
        )

        # Per-consumer inbox dir (shared-calc topology) — each broker reads
        # from its own inbox so destructive poll_inbox semantics don't race
        # between grid and live.
        self.inbox_dir = inbox_dir if inbox_dir is not None else (
            self.results_dir / "calibration_inbox"
        )
        # Output paths follow the same per-consumer rule. When grid and live
        # both write the SAME terminal_data.json on a host-mounted Windows
        # volume, Windows rename semantics surface as EACCES on the loser's
        # atomic-replace, and the trader main loop treats it as an uncaught
        # Exception → exit 0 → restart loop (caught in the wild via the
        # shutdown_log breadcrumb on 2026-04-30).
        suffix = self.inbox_dir.name.removeprefix("calibration_inbox").lstrip("_")
        if suffix:
            self.terminal_path = self.results_dir / f"terminal_data.{suffix}.json"
            self.meta_path = self.results_dir / f".hybrid_calibration_meta.{suffix}.json"
        else:
            # Legacy single-consumer deployment — preserve bare filenames so
            # operator tooling that cats results/terminal_data.json still works.
            self.terminal_path = self.results_dir / "terminal_data.json"
            self.meta_path = self.results_dir / ".hybrid_calibration_meta.json"
        # The 15m streaming dashboard (POLYMARKET_STREAMING_DASHBOARD)
        # reads results/terminal_data.json mtime as its `bot_live` freshness
        # signal. After the per-consumer split, the bare path stopped
        # updating and the dashboard reported the live bot as offline. Have
        # the LIVE consumer (and only the live consumer) mirror its writes
        # to the bare path. Grid stays per-consumer-only, so live remains
        # the sole writer of terminal_data.json — no rename race.
        if suffix == "live":
            self.legacy_terminal_path: Optional[Path] = self.results_dir / "terminal_data.json"
            self.legacy_meta_path: Optional[Path] = (
                self.results_dir / ".hybrid_calibration_meta.json"
            )
        else:
            self.legacy_terminal_path = None
            self.legacy_meta_path = None
        self.worker_terminal_path = self.results_dir / "terminal_data.vps_local.json"

        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._worker_thread: Optional[threading.Thread] = None
        self._current_context: Optional[MarketContext] = None
        self._latest_by_market: dict[str, CalibrationEvent] = {}
        self._last_gap_s_by_market: dict[str, float] = {}
        # Broker-side timestamps for each accepted event (ms). Used for TTL on
        # latest_for_market() so the trader never acts on a stale calibration
        # even if both producers stop delivering.
        self._received_ms_by_market: dict[str, int] = {}
        self._event_subscriber = None
        self._last_snapshot_write_warning_s_by_path: dict[Path, float] = {}

    # Max tolerated skew between a producer's self-reported ready_at and the
    # broker's wall clock. Events further off than this are refused — catches
    # laptop-clock drift, NTP steps after resume, and pathological SSH delays.
    _MAX_PRODUCER_SKEW_MS: int = 30_000
    # Max tolerated relative difference between the producer's event.barrier
    # and the consumer's MarketContext.barrier when both are set. Mirrors the
    # trader's >1% Chainlink/Binance cross-validation policy.
    _BARRIER_REJECT_THRESHOLD: float = 0.01
    _SNAPSHOT_WRITE_WARN_INTERVAL_S: float = 60.0

    def start(self) -> None:
        if self.enable_remote_inbox:
            self.inbox_dir.mkdir(parents=True, exist_ok=True)
            from scripts.trader.ipc import build_calibration_subscriber

            use_push = (
                self.push_consumer_enabled
                and (self.ipc_backend or "").lower() == "redis"
                and bool(self.redis_url)
                and self._push_listener is None
            )
            if use_push:
                try:
                    from scripts.trader.ipc import RedisCalibrationListener

                    self._push_listener = RedisCalibrationListener(
                        redis_url=self.redis_url,
                        stream=self.redis_stream,
                        group=self.redis_consumer_group,
                        consumer=self.redis_consumer_name,
                        on_event=self._on_pushed_event,
                    )
                    self._push_listener.start()
                    log.info("calibration push consumer active (blocking Redis reads)")
                except Exception as exc:  # noqa: BLE001
                    log.warning(
                        "calibration push consumer unavailable (%s); using per-tick polling",
                        exc,
                    )
                    self._push_listener = None

            if self._push_listener is not None:
                # Redis events arrive via the listener thread; the per-tick
                # poll only needs to cover the file-inbox fallback.
                if self.ipc_file_fallback_enabled:
                    self._event_subscriber = build_calibration_subscriber(
                        backend="file",
                        inbox_dir=self.inbox_dir,
                    )
                else:
                    self._event_subscriber = None
            else:
                self._event_subscriber = build_calibration_subscriber(
                    backend=self.ipc_backend,
                    inbox_dir=self.inbox_dir,
                    redis_url=self.redis_url,
                    redis_stream=self.redis_stream,
                    redis_group=self.redis_consumer_group,
                    redis_consumer=self.redis_consumer_name,
                    file_fallback=self.ipc_file_fallback_enabled,
                )
        if self.enable_local_worker and self._worker_thread is None:
            self._worker_thread = threading.Thread(
                target=self._worker_loop,
                name="hybrid-calibration-worker",
                daemon=True,
            )
            self._worker_thread.start()

    def _on_pushed_event(self, event: CalibrationEvent) -> None:
        accepted = self.publish(event)
        if accepted:
            log.info(
                "accepted %s calibration for %s (fit %.2fs, mc %.2fs)",
                event.source,
                event.market_id,
                float((event.terminal_payload.get("timing") or {}).get("surface_fit_s") or 0.0),
                float((event.terminal_payload.get("timing") or {}).get("mc_s") or 0.0),
            )

    def stop(self) -> None:
        self._stop.set()
        if self._worker_thread is not None:
            self._worker_thread.join(timeout=2.0)
        if self._push_listener is not None:
            try:
                self._push_listener.stop()
            except Exception:  # noqa: BLE001
                pass
            self._push_listener = None
        if self._event_subscriber is not None and hasattr(self._event_subscriber, "close"):
            self._event_subscriber.close()

    def update_market_context(self, context: Optional[MarketContext]) -> None:
        with self._lock:
            self._current_context = context
            if context is None:
                return
            latest = self._latest_by_market.get(context.market_id)
            if latest is None:
                return
            # Re-validate stored event barrier against the refreshed
            # consumer context. Mirrors the publish-time guard. If the
            # consumer's barrier has refined (e.g. binance fallback →
            # chainlink) past the threshold, the stored event was
            # calibrated against a now-stale price and must be evicted —
            # otherwise latest_for_market() keeps serving probabilities
            # from a divergent calibration until a fresh event lands.
            if context.barrier > 0:
                rel_diff = abs(latest.barrier - context.barrier) / context.barrier
                if rel_diff > self._BARRIER_REJECT_THRESHOLD:
                    log.warning(
                        "evicting stale calibration for %s on context refresh: "
                        "stored event barrier $%.2f vs new context barrier "
                        "$%.2f (diff %.2f%% > %.2f%%)",
                        context.market_id,
                        latest.barrier,
                        context.barrier,
                        rel_diff * 100,
                        self._BARRIER_REJECT_THRESHOLD * 100,
                    )
                    self._latest_by_market.pop(context.market_id, None)
                    self._received_ms_by_market.pop(context.market_id, None)
                    self._last_gap_s_by_market.pop(context.market_id, None)
                    return
            self._apply_event_locked(latest)

    def latest_for_market(
        self,
        market_id: str,
        max_age_s: Optional[float] = None,
    ) -> Optional[CalibrationEvent]:
        """Return the newest event for `market_id`, or None if too old.

        When `max_age_s` is set, compares against the BROKER-side receive
        timestamp (not the producer clock). Returns None if the event is
        older than `max_age_s` or if we have no receive-timestamp on record.
        """
        with self._lock:
            event = self._latest_by_market.get(market_id)
            if event is None:
                return None
            if max_age_s is None:
                return event
            received_ms = self._received_ms_by_market.get(market_id)
            if received_ms is None:
                return None
            age_s = (int(time.time() * 1000) - received_ms) / 1000.0
            if age_s > max_age_s:
                return None
            return event

    def age_for_market(self, market_id: str) -> Optional[float]:
        """Return broker-side age in seconds for the latest event, if known."""
        with self._lock:
            if market_id not in self._latest_by_market:
                return None
            received_ms = self._received_ms_by_market.get(market_id)
            if received_ms is None:
                return None
            return (int(time.time() * 1000) - received_ms) / 1000.0

    def latest_barrier_for_market(
        self,
        market_id: str,
        *,
        max_age_s: float = 60.0,
    ) -> Optional[tuple[float, str, Optional[float]]]:
        """Return ``(barrier, source, offset_s)`` from the freshest event for
        ``market_id``, or ``None`` if no fresh event is available.

        Used by `SharedDataFeed` in non-calc containers to consume calc's
        captured barrier instead of running their own ChainlinkPriceFeed.
        Same freshness semantics as ``latest_for_market``.
        """
        event = self.latest_for_market(market_id, max_age_s=max_age_s)
        if event is None:
            return None
        return (event.barrier, event.barrier_source, event.barrier_offset_s)

    def publish(self, event: CalibrationEvent) -> bool:
        now_ms = int(time.time() * 1000)
        if not event.is_ready_before_market_end():
            log.warning(
                "rejecting %s event for %s: ready_at %s is not before market end %s",
                event.source,
                event.market_id,
                event.ready_at_iso,
                event.market_end_time_iso,
            )
            return False
        # Clock-skew / transport-delay sanity: reject events whose producer
        # ready_at drifts more than the configured tolerance from broker time.
        skew_ms = event.ready_at_unix_ms - now_ms
        if abs(skew_ms) > self._producer_skew_tolerance_ms:
            log.warning(
                "rejecting %s event for %s: producer ready_at skewed %+dms from broker clock (> %dms)",
                event.source,
                event.market_id,
                skew_ms,
                self._producer_skew_tolerance_ms,
            )
            return False
        with self._lock:
            # Barrier-divergence guard: when the consumer has its own
            # MarketContext for this market, refuse calibrations whose
            # producer barrier disagrees with the consumer's by >1%. This
            # mirrors the trader's existing >1% Chainlink/Binance cross-
            # validation and prevents the broker from publishing a model
            # that was calibrated against a meaningfully different price.
            ctx = self._current_context
            if (
                ctx is not None
                and ctx.market_id == event.market_id
                and ctx.barrier > 0
            ):
                rel_diff = abs(event.barrier - ctx.barrier) / ctx.barrier
                if rel_diff > self._BARRIER_REJECT_THRESHOLD:
                    log.warning(
                        "rejecting %s event for %s: producer barrier $%.2f vs "
                        "consumer barrier $%.2f (diff %.2f%% > %.2f%%)",
                        event.source,
                        event.market_id,
                        event.barrier,
                        ctx.barrier,
                        rel_diff * 100,
                        self._BARRIER_REJECT_THRESHOLD * 100,
                    )
                    return False
            current = self._latest_by_market.get(event.market_id)
            if current is not None and event.sort_key <= current.sort_key:
                return False
            if current is not None:
                self._last_gap_s_by_market[event.market_id] = max(
                    0.0,
                    (event.ready_at_unix_ms - current.ready_at_unix_ms) / 1000.0,
                )
            self._latest_by_market[event.market_id] = event
            self._received_ms_by_market[event.market_id] = now_ms
            if ctx is not None and ctx.market_id == event.market_id:
                self._apply_event_locked(event)
            return True

    def poll_inbox(self) -> None:
        if not self.enable_remote_inbox:
            return
        if self._event_subscriber is None and self._push_listener is None:
            self.start()
        if self._event_subscriber is None:
            # Push listener active with no file fallback — nothing to poll.
            return
        for event in self._event_subscriber.poll():
            accepted = self.publish(event)
            if accepted:
                log.info(
                    "accepted %s calibration for %s (fit %.2fs, mc %.2fs)",
                    event.source,
                    event.market_id,
                    float((event.terminal_payload.get("timing") or {}).get("surface_fit_s") or 0.0),
                    float((event.terminal_payload.get("timing") or {}).get("mc_s") or 0.0),
                )

    def _apply_event_locked(self, event: CalibrationEvent) -> None:
        payload = dict(event.terminal_payload)
        timing = dict(payload.get("timing") or {})
        timing["used_source"] = event.source
        timing["used_gap_s"] = self._last_gap_s_by_market.get(event.market_id)
        payload["timing"] = timing
        # Surface market identity + barrier source for downstream consumers
        # (notably the streaming dashboard, which derives the active-market
        # slug + window times from this field). Without an explicit slug
        # the frontend can't detect market transitions and falls back to a
        # sticky-slug merge — UI lags until manual refresh.
        payload["market_id"] = event.market_id
        payload["market_slug"] = event.market_id
        payload["barrier_source"] = event.barrier_source
        if event.barrier_offset_s is not None:
            payload["barrier_offset_s"] = event.barrier_offset_s
        self._best_effort_snapshot_write_json(self.terminal_path, payload)
        received_ms = self._received_ms_by_market.get(event.market_id)
        meta_payload = {
            "event_id": event.event_id,
            "source": event.source,
            "market_id": event.market_id,
            "ready_at_iso": event.ready_at_iso,
            "used_gap_s": self._last_gap_s_by_market.get(event.market_id),
            "received_at_ms": received_ms,
        }
        self._best_effort_snapshot_write_json(self.meta_path, meta_payload)
        # Live consumer also mirrors to the bare paths so the streaming
        # dashboard's mtime-based liveness check keeps working. Only live
        # touches the bare paths in shared-calc mode → no rename race.
        if self.legacy_terminal_path is not None:
            self._best_effort_snapshot_write_json(
                self.legacy_terminal_path,
                payload,
            )
        if self.legacy_meta_path is not None:
            self._best_effort_snapshot_write_json(
                self.legacy_meta_path,
                meta_payload,
            )

    def _best_effort_snapshot_write_json(self, path: Path, payload: dict) -> bool:
        try:
            _atomic_write_json(path, payload)
            self._last_snapshot_write_warning_s_by_path.pop(path, None)
            return True
        except OSError as exc:
            now_s = time.monotonic()
            last_warn_s = self._last_snapshot_write_warning_s_by_path.get(path)
            if (
                last_warn_s is None
                or now_s - last_warn_s >= self._SNAPSHOT_WRITE_WARN_INTERVAL_S
            ):
                log.warning(
                    "calibration snapshot write failed for %s; continuing with "
                    "in-memory calibration event (%s: %s)",
                    path,
                    exc.__class__.__name__,
                    exc,
                )
                self._last_snapshot_write_warning_s_by_path[path] = now_s
            return False

    def _worker_loop(self) -> None:
        while not self._stop.is_set():
            try:
                with self._lock:
                    context = self._current_context
                if context is None:
                    self._stop.wait(0.5)
                    continue

                now = _utc_now()
                hours_remaining = max(0.0, (context.market_end_time - now).total_seconds() / 3600.0)
                if context.barrier <= 0 or hours_remaining <= 0:
                    self._stop.wait(0.5)
                    continue

                model_data = run_terminal_script(
                    barrier=context.barrier,
                    hours=hours_remaining,
                    skip_heston=True,
                    json_path=self.worker_terminal_path,
                    market_end_time=context.market_end_time_iso,
                )
                ready_at = _utc_now()
                with self._lock:
                    if self._current_context != context:
                        log.info("discarding stale local worker result for %s (context changed)", context.market_id)
                        continue
                if model_data.get("prob_above") is not None and self.worker_terminal_path.exists():
                    terminal_payload = json.loads(self.worker_terminal_path.read_text(encoding="utf-8"))
                    event = CalibrationEvent.from_parts(
                        source="vps_local",
                        context=context,
                        ready_at=ready_at,
                        model_data=model_data,
                        terminal_payload=terminal_payload,
                    )
                    self.publish(event)

                delay = random.uniform(
                    self.worker_delay_min_seconds,
                    self.worker_delay_max_seconds,
                )
                self._stop.wait(delay)
            except Exception:
                # Any raise here would otherwise kill the daemon thread silently
                # (Deribit 5xx, worker/subprocess fallback, JSON decode error, etc.).
                # Log + brief backoff so the broker survives transient failures.
                log.exception("hybrid vps-local worker iteration failed; backing off 5s")
                self._stop.wait(5.0)
