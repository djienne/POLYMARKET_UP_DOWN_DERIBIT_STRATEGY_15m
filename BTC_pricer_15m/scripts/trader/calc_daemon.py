"""Fit the option surface and publish a shared, timestamped payoff forecast.

Current TWAP contracts use the trailing-average MC approximation and settle
from resolved token payouts. Legacy terminal-price events remain readable.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import random
import signal
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import yaml

from scripts.polymarket_btc_15m import (
    extract_market_data,
    find_active_15m_market,
    get_binance_price_at,
    get_current_btc_price,
    _is_sane_btc_price,
)
from scripts.polymarket_edge import run_terminal_script
from scripts.trader.chainlink_archive import ChainlinkArchive
from scripts.trader.chainlink_feed import ChainlinkPriceFeed
from scripts.trader.contracts import MarketContract, TWAP_PAYOFF, fetch_market_contract, fetch_market_resolution
from scripts.trader.hybrid_calibration import (
    CalibrationEvent,
    MarketContext,
    _parse_iso,
)
from scripts.trader._io import atomic_write_json, atomic_write_text
from scripts.trader.ipc import build_calibration_publisher, serialize_calibration_event
from scripts.trader.local_eval import compute_entry_signals, load_signal_params
from scripts.trader.market_context_ipc import (
    DEFAULT_CHANNEL as MARKET_CONTEXT_DEFAULT_CHANNEL,
    DEFAULT_LATEST_KEY as MARKET_CONTEXT_DEFAULT_LATEST_KEY,
    DEFAULT_STREAM as MARKET_CONTEXT_DEFAULT_STREAM,
    MarketContextTick,
    RedisMarketContextPublisher,
)
from scripts.trader.market_orchestrator import MarketOrchestrator
from scripts.trader.spot_ipc import (
    DEFAULT_CHANNEL as SPOT_DEFAULT_CHANNEL,
    DEFAULT_LATEST_KEY as SPOT_DEFAULT_LATEST_KEY,
    DEFAULT_MAX_AGE_SECONDS as SPOT_DEFAULT_MAX_AGE_SECONDS,
    DEFAULT_STREAM as SPOT_DEFAULT_STREAM,
    RedisSpotConsumer,
    SpotTick,
)

from scripts.trader.persistence import SqliteStore
from scripts.trader.polymarket_ptb import PolymarketPTB

log = logging.getLogger(__name__)

# Wait this many seconds past window-start before reading Polymarket's
# /api/crypto/crypto-price. Mirrors fetch_ptb.PolymarketPTB.freshness_minimum.
# Below this threshold the API serves a still-refining ``openPrice`` that
# can drift by tens of dollars before settling. See
# scripts/trader/polymarket_ptb.py docstring for the 2026-05-01 incident
# that motivated this gate.
POLYMARKET_FRESHNESS_MIN_S: float = 30.0
POLYMARKET_CAPTURE_MAX_S: float = 300.0


DEFAULT_CONFIG_PATH = Path("config_calc.json")
DEFAULT_SOURCE_LABEL = "shared_calc"
WORKER_TERMINAL_PATH = Path("results/terminal_data.shared_calc.json")
HEALTH_PATH = Path("results/calc_daemon.health")

# How often (seconds) to emit the throttled "calc is idle / producing no
# calibrations" WARNING. The per-iteration scan/capture failures are logged at
# DEBUG to avoid the multi-thousand-line floods seen during the 2026-06-17
# Polymarket market-creation gap; this single throttled line is the operator-
# facing signal instead.
IDLE_LOG_INTERVAL_SECONDS: float = 60.0


@dataclass(frozen=True)
class CalcConfig:
    inbox_targets: list[Path]
    snapshot_path: Optional[Path]
    delay_min_seconds: float
    delay_max_seconds: float
    inbox_ttl_seconds: float
    source_label: str
    market_scan_interval_seconds: float
    # Publish pre-computed per-instance entry signals on each event (~100KB
    # payload). Kept for consumers without local evaluation; can be disabled
    # once all consumers compute signals locally (Phase E rollout).
    publish_entry_signals: bool = True
    # Polygon JSON-RPC endpoints used by ChainlinkArchive as a fallback
    # when the WebSocket buffer doesn't have a needed point. Empty list
    # disables the archive (calc degrades to WS-only).
    polygon_rpc_urls: tuple[str, ...] = ()
    ipc_backend: str = "file"
    redis_url: str = "redis://redis:6379/0"
    redis_stream: str = "market.calibration"
    redis_maxlen: int = 10_000
    ipc_file_fallback_enabled: bool = True
    market_db_path: Optional[Path] = None
    spot_aggregator_enabled: bool = True
    spot_aggregator_feeds: tuple[str, ...] = (
        "coinbase",
        "kraken",
        "bitstamp",
        "gemini",
        "bitfinex",
        "binance",
    )
    spot_aggregator_outlier_tolerance_pct: float = 0.2
    spot_rtds_stale_after_seconds: float = 10.0
    spot_rtds_refresh_seconds: float = 5.0
    spot_ws_fallback_enabled: bool = True
    spot_ws_stale_after_seconds: float = 10.0
    spot_ws_outlier_bps: float = 20.0
    spot_ws_min_sources: int = 3
    spot_rest_last_resort_enabled: bool = True
    spot_redis_url: str = "redis://redis:6379/0"
    spot_redis_latest_key: str = SPOT_DEFAULT_LATEST_KEY
    spot_redis_stream: str = SPOT_DEFAULT_STREAM
    spot_redis_channel: str = SPOT_DEFAULT_CHANNEL
    spot_max_age_seconds: float = SPOT_DEFAULT_MAX_AGE_SECONDS
    market_context_enabled: bool = True
    market_context_redis_url: str = "redis://redis:6379/0"
    market_context_latest_key: str = MARKET_CONTEXT_DEFAULT_LATEST_KEY
    market_context_stream: str = MARKET_CONTEXT_DEFAULT_STREAM
    market_context_channel: str = MARKET_CONTEXT_DEFAULT_CHANNEL
    market_context_redis_maxlen: int = 10_000

    @classmethod
    def from_json(cls, path: Path) -> "CalcConfig":
        if path.suffix.lower() in {".yaml", ".yml"}:
            loaded = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
            raw = dict(loaded.get("calc") or loaded)
            spot_raw = dict(loaded.get("spot") or {})
            market_context_raw = dict(loaded.get("market_context") or {})
        else:
            loaded = json.loads(path.read_text(encoding="utf-8"))
            raw = dict(loaded)
            spot_raw = dict(loaded.get("spot") or {})
            market_context_raw = dict(loaded.get("market_context") or {})
        targets = [Path(p) for p in raw.get("inbox_targets") or []]
        if not targets:
            raise ValueError(
                f"{path}: 'inbox_targets' must be a non-empty list of inbox dirs"
            )
        snapshot = raw.get("snapshot_path")
        # Env-var override has priority over the JSON config.
        env_urls = os.environ.get("POLYGON_RPC_URLS", "").strip()
        if env_urls:
            polygon_rpc_urls = tuple(u.strip() for u in env_urls.split(",") if u.strip())
        else:
            polygon_rpc_urls = tuple(raw.get("polygon_rpc_urls") or ())
        return cls(
            inbox_targets=targets,
            snapshot_path=Path(snapshot) if snapshot else None,
            delay_min_seconds=float(raw.get("delay_min_seconds", 0.0)),
            delay_max_seconds=float(raw.get("delay_max_seconds", 0.0)),
            inbox_ttl_seconds=float(raw.get("inbox_ttl_seconds", 300.0)),
            source_label=str(raw.get("source_label", DEFAULT_SOURCE_LABEL)),
            market_scan_interval_seconds=float(
                raw.get("market_scan_interval_seconds", 30.0)
            ),
            publish_entry_signals=bool(raw.get("publish_entry_signals", True)),
            polygon_rpc_urls=polygon_rpc_urls,
            ipc_backend=str(raw.get("ipc_backend") or raw.get("backend") or "file"),
            redis_url=str(raw.get("redis_url") or "redis://redis:6379/0"),
            redis_stream=str(raw.get("redis_stream") or "market.calibration"),
            redis_maxlen=int(raw.get("redis_maxlen") or 10_000),
            ipc_file_fallback_enabled=bool(
                raw.get("ipc_file_fallback_enabled", True)
            ),
            market_db_path=(
                Path(raw["market_db_path"])
                if raw.get("market_db_path")
                else None
            ),
            spot_aggregator_enabled=bool(
                raw.get("spot_price_provider_enabled", raw.get("spot_aggregator_enabled", True))
            ),
            spot_aggregator_feeds=tuple(
                raw.get("spot_ws_sources")
                or raw.get("spot_aggregator_feeds")
                or ("coinbase", "kraken", "bitstamp", "gemini", "bitfinex", "binance")
            ),
            spot_aggregator_outlier_tolerance_pct=float(
                raw.get("spot_aggregator_outlier_tolerance_pct", 0.2)
            ),
            spot_rtds_stale_after_seconds=float(
                raw.get("spot_rtds_stale_after_seconds", 10.0)
            ),
            spot_rtds_refresh_seconds=float(
                raw.get("spot_rtds_refresh_seconds", 5.0)
            ),
            spot_ws_fallback_enabled=bool(raw.get("spot_ws_fallback_enabled", True)),
            spot_ws_stale_after_seconds=float(
                raw.get("spot_ws_stale_after_seconds", 10.0)
            ),
            spot_ws_outlier_bps=float(raw.get("spot_ws_outlier_bps", 20.0)),
            spot_ws_min_sources=int(raw.get("spot_ws_min_sources", 3)),
            spot_rest_last_resort_enabled=bool(
                raw.get("spot_rest_last_resort_enabled", True)
            ),
            spot_redis_url=str(
                spot_raw.get("redis_url")
                or raw.get("spot_redis_url")
                or raw.get("redis_url")
                or "redis://redis:6379/0"
            ),
            spot_redis_latest_key=str(
                spot_raw.get("latest_key")
                or raw.get("spot_redis_latest_key")
                or SPOT_DEFAULT_LATEST_KEY
            ),
            spot_redis_stream=str(
                spot_raw.get("stream")
                or raw.get("spot_redis_stream")
                or SPOT_DEFAULT_STREAM
            ),
            spot_redis_channel=str(
                spot_raw.get("channel")
                or raw.get("spot_redis_channel")
                or SPOT_DEFAULT_CHANNEL
            ),
            spot_max_age_seconds=float(
                spot_raw.get("max_age_seconds")
                or raw.get("spot_max_age_seconds")
                or SPOT_DEFAULT_MAX_AGE_SECONDS
            ),
            market_context_enabled=bool(
                market_context_raw.get(
                    "enabled",
                    raw.get("market_context_enabled", True),
                )
            ),
            market_context_redis_url=str(
                market_context_raw.get("redis_url")
                or raw.get("market_context_redis_url")
                or raw.get("redis_url")
                or "redis://redis:6379/0"
            ),
            market_context_latest_key=str(
                market_context_raw.get("latest_key")
                or raw.get("market_context_latest_key")
                or MARKET_CONTEXT_DEFAULT_LATEST_KEY
            ),
            market_context_stream=str(
                market_context_raw.get("stream")
                or raw.get("market_context_stream")
                or MARKET_CONTEXT_DEFAULT_STREAM
            ),
            market_context_channel=str(
                market_context_raw.get("channel")
                or raw.get("market_context_channel")
                or MARKET_CONTEXT_DEFAULT_CHANNEL
            ),
            market_context_redis_maxlen=int(
                market_context_raw.get("redis_maxlen")
                or raw.get("market_context_redis_maxlen")
                or 10_000
            ),
        )


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _validate_captured_barrier(
    start_utc: datetime,
    winning_price: float,
    winning_source: str,
    winning_offset: float,
    origin_tag: str,
) -> Optional[tuple[float, str, Optional[float]]]:
    """Cross-check and format a captured barrier value."""
    try:
        kline = get_binance_price_at(start_utc)
        if kline and _is_sane_btc_price(kline):
            diff_pct = abs(winning_price - kline) / kline * 100
            if diff_pct > 1.0:
                log.error(
                    "%s/Binance divergence %.2f%% ($%.2f vs $%.2f) - "
                    "REJECTING barrier; will retry next loop",
                    winning_source, diff_pct, winning_price, kline,
                )
                return None
            if diff_pct > 0.5:
                log.warning(
                    "%s/Binance divergence %.2f%%: $%.2f vs $%.2f",
                    winning_source, diff_pct, winning_price, kline,
                )
            log.info(
                "Barrier [%s] at %s: $%.2f (offset %.1fs)",
                origin_tag, start_utc.isoformat(), winning_price, winning_offset,
            )
        else:
            log.info(
                "Barrier [%s, unvalidated] at %s: $%.2f (offset %.1fs)",
                origin_tag, start_utc.isoformat(), winning_price, winning_offset,
            )
    except Exception as exc:  # noqa: BLE001
        log.warning("%s/Binance cross-validation failed: %s", winning_source, exc)
        log.info(
            "Barrier [%s, unvalidated] at %s: $%.2f (offset %.1fs)",
            origin_tag, start_utc.isoformat(), winning_price, winning_offset,
        )

    return (float(winning_price), winning_source, float(winning_offset))


def _capture_barrier(
    start_utc: datetime,
    chainlink: Optional[ChainlinkPriceFeed] = None,
    archive: Optional[ChainlinkArchive] = None,
    polymarket: Optional[PolymarketPTB] = None,
) -> Optional[tuple[float, str, Optional[float]]]:
    """Resolve the BTC barrier (Price-To-Beat) at market window start.

    Returns ``(price, source, offset_s)`` or ``None`` if no source
    yielded a sane value.

    **Polymarket primary.** The barrier value Polymarket displays as
    "Price to Beat" on its market page comes from
    ``GET /api/crypto/crypto-price?variant=fiveminute&eventStartTime=<start>``
    — that's the openPrice for the 15-minute window. Sourcing the
    barrier from that endpoint aligns our trader with what users see
    on the Polymarket UI, which is the value we have to beat. Fixes
    the chainlink-tick-alignment class of phantom we hit on
    ``4ca5f67b`` / ``147bfad2`` / ``7117df01`` where chainlink at our
    instant disagreed with Polymarket's settle-instant.

    The 15-minute market still uses Polymarket's ``variant=fiveminute``
    crypto-price payload at the 15m market start; do not shift the query
    to start+5m. A new Polymarket capture is only valid when the market
    is older than 30 seconds and younger than 5 minutes.

    Fallback chain when Polymarket is unavailable:
    1. ``polymarket`` (PolymarketPTB) — primary.
    2. WebSocket buffer (``chainlink``) — fast on-chain reading.
    3. On-chain archive (``archive``) — Polygon JSON-RPC lookup.

    The Binance kline cross-check still applies to whichever source
    won: if the captured price diverges from the Binance kline open by
    more than 1 %, we REJECT it. If Binance is unavailable, accept
    unvalidated.

    When *both* Polymarket and chainlink return a price, log the delta
    as a divergence WARN — telemetry to detect the rare case where
    they disagree.
    """
    pm_price: Optional[float] = None
    if polymarket is not None:
        # Freshness gate: Polymarket's ``openPrice`` for a 15-minute window
        # can drift during the first ~30s as more ticks arrive. On
        # 2026-05-01 we read at window+1s and got $77,767.70; Polymarket
        # later settled to $77,744.57 ($23 drift). Defer the *entire*
        # capture if we're within the gate — returning None so the calc
        # loop retries on the next iteration. We do NOT fall through to
        # chainlink here: that would re-introduce the chainlink-tick
        # alignment bug we're trying to escape (commits 4ca5f67b /
        # 147bfad2 / 7117df01). Chainlink stays as a fallback for the
        # case where Polymarket *attempts* and *fails* — see below.
        age_s = (_utc_now() - start_utc).total_seconds()
        if age_s <= POLYMARKET_FRESHNESS_MIN_S:
            log.debug(
                "deferring barrier capture: window only %.1fs old "
                "(Polymarket gate > %.1fs)",
                age_s, POLYMARKET_FRESHNESS_MIN_S,
            )
            return None
        if age_s >= POLYMARKET_CAPTURE_MAX_S:
            # DEBUG, not WARNING: this fires every loop (~0.5s) for the whole
            # remainder of a late-published market's window, which used to
            # flood the log with thousands of identical WARNINGs (2026-06-17).
            # The daemon-level throttled idle WARNING reports this condition
            # once per IDLE_LOG_INTERVAL_SECONDS as reason="barrier_uncapturable".
            log.debug(
                "refusing late Polymarket PTB capture: window %.1fs old "
                "(valid capture window is >%.1fs and <%.1fs)",
                age_s, POLYMARKET_FRESHNESS_MIN_S, POLYMARKET_CAPTURE_MAX_S,
            )
            return None
        try:
            window_end = start_utc + timedelta(minutes=15)
            pm = polymarket.ptb_for_window(start_utc, window_end)
            if pm is not None and _is_sane_btc_price(pm):
                pm_price = float(pm)
        except Exception as exc:  # noqa: BLE001
            log.warning("Polymarket PTB fetch raised: %s", exc)

    # A valid Polymarket PTB is authoritative and independent of the RTDS
    # current-spot stream. Do not query Chainlink/RTDS even for telemetry here.
    if pm_price is not None:
        return _validate_captured_barrier(
            start_utc,
            pm_price,
            "polymarket",
            0.0,
            "polymarket",
        )

    cl_price: Optional[float] = None
    cl_offset: Optional[float] = None
    cl_origin: str = ""

    if chainlink is not None and chainlink.is_healthy:
        ws_price, ws_offset = chainlink.get_first_at_or_after(
            start_utc, max_offset_s=60.0
        )
        if ws_price is not None and ws_offset is not None and _is_sane_btc_price(ws_price):
            cl_price, cl_offset, cl_origin = ws_price, ws_offset, "ws"

    if cl_price is None and archive is not None:
        target_ts = int(start_utc.timestamp())
        archive_result = archive.get_round_at_or_after(target_ts, max_lookahead_s=60)
        if archive_result is not None:
            ar_price, ar_round_ts, _round_id = archive_result
            ar_offset = float(ar_round_ts - target_ts)
            if _is_sane_btc_price(ar_price):
                cl_price, cl_offset, cl_origin = ar_price, ar_offset, "archive"

    if cl_price is not None and cl_offset is not None:
        winning_price = cl_price
        winning_source = "chainlink"
        winning_offset = float(cl_offset)
        origin_tag = "chainlink, archive" if cl_origin == "archive" else "chainlink"
    else:
        return None

    # Binance cross-check (applies to whichever source won).
    try:
        kline = get_binance_price_at(start_utc)
        if kline and _is_sane_btc_price(kline):
            diff_pct = abs(winning_price - kline) / kline * 100
            if diff_pct > 1.0:
                log.error(
                    "%s/Binance divergence %.2f%% ($%.2f vs $%.2f) — "
                    "REJECTING barrier; will retry next loop",
                    winning_source, diff_pct, winning_price, kline,
                )
                return None
            if diff_pct > 0.5:
                log.warning(
                    "%s/Binance divergence %.2f%%: $%.2f vs $%.2f",
                    winning_source, diff_pct, winning_price, kline,
                )
            log.info(
                "Barrier [%s] at %s: $%.2f (offset %.1fs)",
                origin_tag, start_utc.isoformat(), winning_price, winning_offset,
            )
        else:
            log.info(
                "Barrier [%s, unvalidated] at %s: $%.2f (offset %.1fs)",
                origin_tag, start_utc.isoformat(), winning_price, winning_offset,
            )
    except Exception as exc:  # noqa: BLE001
        log.warning("%s/Binance cross-validation failed: %s", winning_source, exc)
        log.info(
            "Barrier [%s, unvalidated] at %s: $%.2f (offset %.1fs)",
            origin_tag, start_utc.isoformat(), winning_price, winning_offset,
        )

    return (float(winning_price), winning_source, float(winning_offset))


def _sweep_inbox(target: Path, ttl_seconds: float) -> None:
    """Delete inbox files older than ``ttl_seconds``.

    Protects against unbounded growth when a consumer is stopped while the
    daemon keeps producing.
    """
    if ttl_seconds <= 0 or not target.exists():
        return
    now = time.time()
    for path in target.glob("*.json"):
        try:
            if now - path.stat().st_mtime > ttl_seconds:
                path.unlink(missing_ok=True)
        except OSError:
            continue




def _resolve_active_market_context(
    chainlink: Optional[ChainlinkPriceFeed] = None,
    locked_barriers: Optional[dict] = None,
    archive: Optional[ChainlinkArchive] = None,
    chain_broken_markets: Optional[set] = None,
    polymarket: Optional[PolymarketPTB] = None,
    status: Optional[dict] = None,
) -> Optional[MarketContext]:
    """Resolve the current active 15-minute market and its barrier.

    Preserve the captured reference per market. TWAP contracts require the
    published Polymarket PTB; an instantaneous oracle point is not a substitute.
    ``status`` distinguishes missing markets from unavailable reference inputs.
    """

    def _diag(reason: Optional[str], market_present: bool) -> None:
        if status is not None:
            status["reason"] = reason
            status["market_present"] = market_present

    try:
        market = find_active_15m_market()
    except Exception as exc:  # noqa: BLE001
        log.warning("market scan failed: %s", exc)
        _diag("market_scan_failed", False)
        return None
    if not market:
        _diag("no_active_market", False)
        return None
    try:
        data = extract_market_data(market)
    except Exception as exc:  # noqa: BLE001
        log.warning("market extract failed: %s", exc)
        _diag("market_extract_failed", True)
        return None

    market_id = data.get("market_id")
    end_utc = data.get("end_date_utc")
    start_utc = data.get("window_start_utc")
    if not market_id or not end_utc or not start_utc:
        _diag("market_data_incomplete", True)
        return None

    market_id = str(market_id)

    # Preserve the market's captured PTB across cycles. A TWAP contract
    # cannot substitute an instantaneous oracle point for its published PTB.
    cached = locked_barriers.get(market_id) if locked_barriers is not None else None
    contract = MarketContract.from_dict(data.get("contract_metadata"))
    if contract.payoff_kind == TWAP_PAYOFF and cached is not None and cached[1] != "polymarket":
        cached = None

    if cached is not None:
        barrier, source, offset_s = cached
    else:
        captured = _capture_barrier(
            start_utc,
            chainlink=None if contract.payoff_kind == TWAP_PAYOFF else chainlink,
            archive=None if contract.payoff_kind == TWAP_PAYOFF else archive,
            polymarket=polymarket,
        )
        if captured is None:
            # Barrier not capturable (chainlink not yet available, or the
            # market was published too late — past POLYMARKET_CAPTURE_MAX_S).
            # calc publishes nothing for this market; consumers see no event,
            # barrier_confirmed stays False, entries blocked. We retry next
            # loop. The market itself DOES exist, so flag market_present so
            # the health/log path can say "market present but uncapturable".
            _diag("barrier_uncapturable", True)
            return None
        barrier, source, offset_s = captured
        if locked_barriers is not None:
            locked_barriers[market_id] = (barrier, source, offset_s)

    if barrier is None or barrier <= 0:
        _diag("barrier_invalid", True)
        return None

    # Chain-break gate: if this market's strike was flagged as broken
    # (settlement of prev market disagrees with this strike at the
    # boundary), publish source="chain_broken" so consumers' barrier_
    # confirmed evaluates False → no entries on the suspect market.
    if contract.payoff_kind != TWAP_PAYOFF and chain_broken_markets is not None and market_id in chain_broken_markets:
        source = "chain_broken"

    _diag(None, True)
    return MarketContext(
        market_id=market_id,
        barrier=float(barrier),
        market_end_time_iso=end_utc.isoformat(),
        barrier_source=str(source),
        barrier_offset_s=offset_s,
        clob_token_ids=data.get("token_ids"),
        market_slug=str(data.get("slug") or market_id),
        market_title=data.get("title"),
        market_start_time_iso=start_utc.isoformat(),
        contract_metadata=data.get("contract_metadata"),
    )


class CalcDaemon:
    """Publish a shared forecast when market context and pricing inputs permit."""

    def __init__(
        self,
        config: CalcConfig,
        terminal_path: Path = WORKER_TERMINAL_PATH,
        chainlink: Optional[ChainlinkPriceFeed] = None,
        polymarket: Optional[PolymarketPTB] = None,
        spot_aggregator=None,
        spot_feed: Optional[RedisSpotConsumer] = None,
    ):
        self.config = config
        self.terminal_path = terminal_path
        self._stop = False
        self._last_market_id: Optional[str] = None
        self._last_resolution_poll: dict[str, float] = {}
        self._sweep_counter = 0
        # --- Calibration-freshness tracking (health file + throttled idle log).
        # `calc_daemon.health` used to be a bare touch() recording only loop
        # liveness, so the container HEALTHCHECK (mtime < 120s) and any monitor
        # reported "healthy" straight through a total calibration outage. On
        # 2026-06-17 Polymarket skipped one 15m market and published the next
        # ~11 min late (past POLYMARKET_CAPTURE_MAX_S), so calc produced no
        # calibration for ~32 min while looking healthy and logging nothing
        # but the gamma-poll flood. These fields let the health file report
        # *calibration* freshness and let the loop log a single throttled
        # WARNING explaining why it is idle.
        self._last_calibration_unix: Optional[float] = None
        self._last_calibration_market_id: Optional[str] = None
        self._last_active_market_unix: Optional[float] = None
        self._idle_reason: Optional[str] = None
        self._idle_since_unix: Optional[float] = None
        self._last_idle_log_monotonic: float = 0.0
        # `chainlink` is the live BTC/USD feed used in `_capture_barrier`.
        # Pass `None` (the default) to disable Chainlink entirely (tests do
        # this; production `main()` always passes a started feed).
        self._chainlink = chainlink
        # `polymarket` is the new primary barrier source: queries
        # /api/crypto/crypto-price variant=fiveminute at the 15m market
        # start for the openPrice (== Polymarket's PTB displayed on the
        # market page). Sourcing the barrier from
        # the same place users see it eliminates the chainlink-tick
        # alignment class of phantom we saw on 4ca5f67b / 147bfad2 /
        # 7117df01. Production `main()` constructs a real instance;
        # tests pass ``None`` (the default) to keep the chainlink-only
        # fallback path exercised. No auto-instantiation here so unit
        # tests don't accidentally hit the network.
        self._polymarket = polymarket
        self._spot_aggregator = spot_aggregator
        self._spot_feed = spot_feed
        from scripts.trader.order_book import OrderBookClient
        # Concurrent early book fetches: books are submitted before the
        # calibration runs and harvested after, so their latency overlaps the
        # fit+pricing instead of delaying the publish. One client per side —
        # requests.Session is not thread-safe.
        from concurrent.futures import ThreadPoolExecutor
        self._book_pool = ThreadPoolExecutor(max_workers=2, thread_name_prefix="calc-books")
        self._book_jobs: dict = {}
        self._last_priced_observation = None
        self._settlement_pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="calc-settlement")
        self._settlement_job = None
        self._book_clients = {
            "UP": OrderBookClient(timeout=3),
            "DOWN": OrderBookClient(timeout=3),
        }
        # Market-context cache: the active market is immutable until its
        # window_end, so the gamma scan only needs to re-run near/after the
        # end, while the barrier is still unlocked, or on the periodic
        # revalidation interval (catches Polymarket-side voids).
        self._cached_context: Optional[MarketContext] = None
        self._cached_context_at: float = 0.0
        self._entry_signal_params = self._load_signal_params()
        # Per-market PTB lock, restored from the durable market registry.
        self._market_barriers: dict[str, tuple[float, str, Optional[float]]] = {}
        # Current contracts await authoritative token payouts. Price-only
        # historical rows retain their legacy recovery path.
        self._settlements_dir = self.terminal_path.parent / "settlements"
        try:
            self._settlements_dir.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            log.warning("could not create settlements dir %s: %s",
                        self._settlements_dir, exc)
        # market_id -> (start_utc, end_utc, strike, strike_offset_s)
        self._pending_settlements: dict[str, tuple[datetime, datetime, float, Optional[float]]] = {}
        self._market_sqlite_store = (
            SqliteStore(self.config.market_db_path)
            if self.config.market_db_path is not None
            else None
        )
        self._market_orchestrator = MarketOrchestrator(
            settlements_dir=self._settlements_dir,
            sqlite_store=self._market_sqlite_store,
        )
        self._calibration_publisher = build_calibration_publisher(
            backend=self.config.ipc_backend,
            inbox_targets=self.config.inbox_targets,
            redis_url=self.config.redis_url,
            redis_stream=self.config.redis_stream,
            redis_maxlen=self.config.redis_maxlen,
            file_fallback=self.config.ipc_file_fallback_enabled,
        )
        self._market_context_publisher: Optional[RedisMarketContextPublisher] = None
        self._market_context_sequence = 0
        self._last_market_context_signature: Optional[tuple] = None
        if self.config.market_context_enabled:
            try:
                self._market_context_publisher = RedisMarketContextPublisher(
                    redis_url=self.config.market_context_redis_url,
                    latest_key=self.config.market_context_latest_key,
                    stream=self.config.market_context_stream,
                    channel=self.config.market_context_channel,
                    maxlen=self.config.market_context_redis_maxlen,
                )
            except Exception as exc:  # noqa: BLE001
                log.warning("market context publisher disabled: %s", exc)
        # Hard cap on how long past window-end we keep retrying.
        self._settlement_max_wait_s: float = 300.0
        # Markets whose chainlink strike disagrees with the previous market's
        # settlement at the boundary timestamp by more than the tolerance
        # below. By construction (both = "first chainlink at-or-after the
        # same boundary") they should match exactly. When they don't,
        # something has gone wrong on the calc side and we don't trust
        # this market's strike — entries blocked until the next clean
        # chain transition.
        self._chain_broken_markets: set[str] = set()
        # Half-cent tolerance covers floating-point roundtrip noise. For
        # chainlink (8 on-chain decimals) the values should agree to the
        # last decimal, so anything wider is a real anomaly.
        self._chain_tolerance_usd: float = 0.005
        self._restore_settlement_state_from_disk()
        self._archive: Optional[ChainlinkArchive] = None
        if self.config.polygon_rpc_urls:
            try:
                self._archive = ChainlinkArchive(
                    rpc_urls=list(self.config.polygon_rpc_urls)
                )
                log.info(
                    "chainlink archive enabled with %d RPC endpoint(s)",
                    len(self.config.polygon_rpc_urls),
                )
            except Exception as exc:  # noqa: BLE001
                log.warning("could not initialize ChainlinkArchive: %s", exc)
                self._archive = None

    def _load_signal_params(self) -> list[dict]:
        """Load grid + live param combos from config.yaml for entry signal computation.

        Delegates to scripts.trader.local_eval so the calc daemon and
        local-eval consumers resolve the identical parameter set.
        """
        return load_signal_params(self.terminal_path.parent.parent / "config.yaml")

    def _compute_entry_signals(
        self, prob_above: float, prob_below: float,
        up_best_ask: float, down_best_ask: float,
        market_id: str = "",
    ) -> tuple:
        """Evaluate edge for all param combos (shared local_eval implementation)."""
        return compute_entry_signals(
            prob_above,
            prob_below,
            up_best_ask,
            down_best_ask,
            self._entry_signal_params,
            market_id,
        )

    def request_stop(self) -> None:
        self._stop = True

    # ------------------------------------------------------------------
    # Settlement persistence
    # ------------------------------------------------------------------

    def _settlement_path(self, market_id: str) -> Path:
        return self._market_orchestrator.settlement_path(market_id)

    def _restore_settlement_state_from_disk(self) -> None:
        """On startup, scan settlement files and rehydrate state.

        For markets whose strike has been captured but settlement is not
        yet written, restore both the in-memory barrier lock and the
        pending-settlement queue. This guards against calc restarts
        mid-market: without it, the buffer would have aged out the
        original chainlink point and we'd recapture a later (wrong) one.
        """
        barriers, pending, chain_broken = self._market_orchestrator.restore_state()
        self._market_barriers.update(barriers)
        self._chain_broken_markets.update(chain_broken)
        for market_id, item in pending.items():
            self._pending_settlements[market_id] = (
                item.window_start,
                item.window_end,
                item.strike,
                item.strike_offset_s,
            )
        if self._market_barriers or self._pending_settlements:
            log.info(
                "restored %d barrier lock(s), %d pending settlement(s) from disk",
                len(self._market_barriers), len(self._pending_settlements),
            )

    def _write_settlement_strike(
        self,
        market_id: str,
        window_start: datetime,
        window_end: datetime,
        strike: float,
        strike_offset_s: Optional[float],
        strike_source: str = "chainlink",
        contract_metadata: Optional[dict] = None,
    ) -> None:
        try:
            wrote = self._market_orchestrator.write_strike(
                market_id=market_id,
                window_start=window_start,
                window_end=window_end,
                strike=strike,
                strike_offset_s=strike_offset_s,
                strike_source=strike_source,
                contract_metadata=contract_metadata,
            )
        except OSError as exc:
            log.warning("failed to persist settlement strike for %s: %s",
                        market_id, exc)
            return
        if not wrote:
            return
        log.info("settlement strike persisted for %s: $%.2f", market_id, strike)
        # Chain-consistency: previous market's settlement (if already
        # finalized) must equal this market's strike — both come from the
        # same chainlink emission at the boundary timestamp.
        if contract_metadata is not None:
            return  # an averaging payoff need not equal the next market's PTB
        self._verify_chain_consistency(
            market_id=market_id,
            window_start=window_start,
            window_end=window_end,
            strike=float(strike),
            settlement_price=None,
        )

    def _verify_chain_consistency(
        self,
        *,
        market_id: str,
        window_start: datetime,
        window_end: datetime,
        strike: Optional[float],
        settlement_price: Optional[float],
    ) -> None:
        """Structural invariant: settlement of market N == strike of market
        N+1 (both = "first chainlink at-or-after the same boundary
        timestamp"). Tolerance ``_chain_tolerance_usd`` covers float noise;
        anything wider marks the *suspect* market for blocking.

        On break, the suspect market_id is added to
        ``_chain_broken_markets``. Subsequent emissions for that market
        publish ``barrier_source="chain_broken"`` instead of "chainlink",
        which causes consumer ``barrier_confirmed`` to evaluate False —
        no entries on that market. The next market resumes normally if
        its own chain is consistent."""
        try:
            tol = self._chain_tolerance_usd
            # Case A: just wrote the STRIKE of market_id. Compare against
            # the previous market's already-finalized settlement.
            if strike is not None:
                prev_window_end_iso = window_start.isoformat()
                prev = self._find_settlement_at("window_end_iso", prev_window_end_iso)
                if prev is not None:
                    prev_settle = prev.get("settlement_price")
                    if prev_settle is not None:
                        delta = strike - float(prev_settle)
                        if abs(delta) > tol:
                            self._chain_broken_markets.add(market_id)
                            log.critical(
                                "CHAIN_BREAK: prev market %s settlement=$%.4f "
                                "vs new market %s strike=$%.4f (delta=$%+.4f, "
                                "tol=$%.4f) at boundary %s — BLOCKING %s "
                                "from new entries; will resume on next clean "
                                "chain transition",
                                prev.get("market_id"), float(prev_settle),
                                market_id, strike, delta, tol,
                                window_start.isoformat(), market_id,
                            )
                            self._mark_strike_chain_broken(market_id)

            # Case B: just wrote the SETTLEMENT of market_id. Compare
            # against the next market's already-locked strike. The
            # SUSPECT market is the next one (its strike is the one that
            # disagrees with the resolution-source value we just captured).
            if settlement_price is not None:
                next_window_start_iso = window_end.isoformat()
                nxt = self._find_settlement_at("window_start_iso", next_window_start_iso)
                if nxt is not None:
                    next_strike = nxt.get("strike")
                    next_market_id = nxt.get("market_id")
                    if next_strike is not None and next_market_id:
                        delta = float(next_strike) - settlement_price
                        if abs(delta) > tol:
                            self._chain_broken_markets.add(next_market_id)
                            log.critical(
                                "CHAIN_BREAK: market %s settlement=$%.4f "
                                "vs next market %s strike=$%.4f (delta=$%+.4f, "
                                "tol=$%.4f) at boundary %s — BLOCKING %s "
                                "from new entries; will resume on next clean "
                                "chain transition",
                                market_id, settlement_price,
                                next_market_id, float(next_strike), delta, tol,
                                window_end.isoformat(), next_market_id,
                            )
                            self._mark_strike_chain_broken(next_market_id)
        except Exception as exc:  # noqa: BLE001
            log.warning("chain-consistency check failed (non-fatal): %s", exc)

    def _find_settlement_at(self, key: str, iso: str) -> Optional[dict]:
        """Find a settlement file whose `key` field equals `iso`.

        Linear scan over the settlements dir; the dir is bounded (one
        file per market and we GC eventually) so this stays cheap.
        """
        return self._market_orchestrator.find_market_at(key=key, iso=iso)

    def _mark_strike_chain_broken(self, market_id: str) -> None:
        """Rewrite the affected market's settlement file with
        ``strike_source="chain_broken"`` so downstream readers (dashboard,
        manual audit) see the suspect status."""
        try:
            self._market_orchestrator.mark_chain_broken(market_id)
        except OSError as exc:
            log.warning("could not rewrite %s as chain_broken: %s", market_id, exc)

    def _enqueue_old_market_for_settlement(self, old_market_id: str) -> None:
        """When a new market is detected, enqueue the previous market for
        settlement capture. Reads window_start/end and strike from the
        on-disk strike file (written when the strike was first locked).
        If the file doesn't exist, the previous market never had a
        chainlink strike captured — nothing to settle."""
        payload = self._market_orchestrator.read_market(old_market_id)
        if payload is None:
            return
        try:
            if (payload.get("settlement_price") is not None or payload.get("settlement_payouts")
                    or payload.get("settlement_status") in ("finalized", "abandoned")):
                return  # already settled
            window_start = _parse_iso(payload["window_start_iso"])
            window_end = _parse_iso(payload["window_end_iso"])
            strike = float(payload["strike"])
            strike_offset_raw = payload.get("strike_offset_s")
            strike_offset_s = float(strike_offset_raw) if strike_offset_raw is not None else None
        except (KeyError, ValueError, TypeError) as exc:
            log.warning("could not enqueue %s for settlement: %s", old_market_id, exc)
            return
        self._pending_settlements[old_market_id] = (
            window_start, window_end, strike, strike_offset_s,
        )
        log.info("enqueued %s for settlement capture (window_end=%s)",
                 old_market_id, window_end.isoformat())

    def _resolve_pending_settlements(self) -> None:
        """Resolve pending rows from their actual contract and token payouts.

        Missing metadata is an unknown contract, not permission to infer an
        outcome from a spot price. Existing finalized history is left intact.
        """
        # Public resolution reads can lag or time out. One background reader
        # keeps that latency out of the pricing loop; this thread alone writes
        # market state once a completed response is available.
        if self._settlement_job is not None:
            market_id, future = self._settlement_job
            if not future.done():
                return
            self._settlement_job = None
            try:
                contract, resolution = future.result()
                saved = self._market_orchestrator.read_market(market_id) or {}
                if saved.get("settlement_status") not in ("finalized", "abandoned"):
                    if contract is not None:
                        self._market_orchestrator.attach_pending_contract(market_id, contract)
                    if resolution is not None:
                        self._market_orchestrator.finalize_payout(market_id, resolution)
                        self._pending_settlements.pop(market_id, None)
                        self._last_resolution_poll.pop(market_id, None)
            except Exception as exc:
                log.warning("settlement read failed for %s; retaining pending: %s", market_id, exc)
        if not self._pending_settlements:
            return
        now = _utc_now()
        resolved: list[str] = []
        for market_id, (window_start, window_end, strike, strike_offset_s) in \
                list(self._pending_settlements.items()):
            saved = self._market_orchestrator.read_market(market_id) or {}
            if (saved.get("settlement_status") in ("finalized", "abandoned")
                    or saved.get("settlement_price") is not None or saved.get("settlement_payouts")):
                resolved.append(market_id)
                continue
            if now < window_end:
                continue
            last_poll = self._last_resolution_poll.get(market_id, 0.0)
            if time.monotonic() - last_poll < 10.0:
                continue
            self._last_resolution_poll[market_id] = time.monotonic()
            def read_resolution(raw=saved.get("contract_metadata"), slug=str(saved.get("slug") or market_id),
                                contract_fetch=fetch_market_contract, payout_fetch=fetch_market_resolution,
                                rpc_urls=tuple(self.config.polygon_rpc_urls or ())):
                contract = MarketContract.from_dict(raw)
                if not contract.condition_id or not contract.has_binary_slots:
                    contract = contract_fetch(slug, timeout=3.0)
                if contract is None:
                    return None, None
                return contract, payout_fetch(contract, timeout=3.0, rpc_urls=rpc_urls)
            self._settlement_job = (market_id, self._settlement_pool.submit(read_resolution))
            break
        for market_id in resolved:
            self._pending_settlements.pop(market_id, None)
            self._last_resolution_poll.pop(market_id, None)

    def _mark_settlement_abandoned(self, market_id: str, reason: str) -> None:
        try:
            self._market_orchestrator.mark_abandoned(market_id, reason=reason)
        except OSError as exc:
            log.warning("failed to persist abandoned settlement for %s: %s", market_id, exc)

    def _finalize_settlement(
        self,
        market_id: str,
        window_start: datetime,
        window_end: datetime,
        strike: float,
        strike_offset_s: Optional[float],
        settlement_price: float,
        settlement_offset_s: float,
        settlement_source: str = "chainlink",
    ) -> None:
        try:
            self._market_orchestrator.finalize(
                market_id=market_id,
                window_start=window_start,
                window_end=window_end,
                strike=strike,
                strike_offset_s=strike_offset_s,
                settlement_price=settlement_price,
                settlement_offset_s=settlement_offset_s,
                settlement_source=settlement_source,
            )
            log.info(
                "settlement finalized for %s: strike=$%.2f final=$%.2f (offset %.1fs) "
                "→ %s wins",
                market_id, strike, settlement_price, settlement_offset_s,
                "DOWN" if settlement_price < strike else "UP",
            )
        except OSError as exc:
            log.warning("failed to write settlement for %s: %s", market_id, exc)
        # Chain consistency: now that this market's settlement is known,
        # check it against the next market's strike (if already locked).
        self._verify_chain_consistency(
            market_id=market_id,
            window_start=window_start,
            window_end=window_end,
            strike=None,
            settlement_price=float(settlement_price),
        )

    def run_forever(self) -> None:
        log.info(
            "calc daemon starting: targets=%s snapshot=%s ttl=%.0fs",
            [str(p) for p in self.config.inbox_targets],
            str(self.config.snapshot_path) if self.config.snapshot_path else "off",
            self.config.inbox_ttl_seconds,
        )
        while not self._stop:
            self._write_health()
            try:
                self.run_once()
            except Exception:  # noqa: BLE001
                # Mirror hybrid_calibration._worker_loop exception handling:
                # any raise here would otherwise crash the daemon. Log + back
                # off so transient failures (Deribit 5xx, JSON decode, etc.)
                # don't take the calculator down.
                self._note_idle("iteration_error")
                self._write_health()
                log.exception("calc iteration failed; backing off 5s")
                self._maybe_log_idle()
                self._sleep(5.0)
                continue

            self._maybe_log_idle()
            self._sweep_inbox_targets_throttled()

            delay = random.uniform(
                self.config.delay_min_seconds,
                max(self.config.delay_min_seconds, self.config.delay_max_seconds),
            )
            if delay > 0:
                self._sleep(delay)

    def _write_health(self) -> None:
        """Write the JSON health/status snapshot.

        The file *mtime* doubles as the process-liveness heartbeat consumed by
        the container HEALTHCHECK (``mtime < 120s``); writing every loop — even
        when idle — keeps that heartbeat fresh so a legitimate upstream market
        gap does NOT flap the container unhealthy or trigger a pointless
        restart (restarting calc cannot conjure a market Polymarket hasn't
        published). The JSON *body* additionally records calibration freshness
        so the dashboard / an external monitor can tell "process alive" apart
        from "model probabilities are fresh" — the blind spot behind the
        2026-06-17 silent 32-minute calibration outage.
        """
        now = time.time()
        since_calc = (
            now - self._last_calibration_unix
            if self._last_calibration_unix is not None
            else None
        )
        idle_seconds = (
            now - self._idle_since_unix
            if self._idle_reason is not None and self._idle_since_unix is not None
            else None
        )
        payload = {
            "heartbeat_unix": now,
            "last_calibration_unix": self._last_calibration_unix,
            "last_calibration_market_id": self._last_calibration_market_id,
            "seconds_since_calibration": (
                round(since_calc, 1) if since_calc is not None else None
            ),
            "last_active_market_unix": self._last_active_market_unix,
            "idle_reason": self._idle_reason,
            "idle_seconds": round(idle_seconds, 1) if idle_seconds is not None else None,
        }
        try:
            HEALTH_PATH.parent.mkdir(parents=True, exist_ok=True)
            atomic_write_json(HEALTH_PATH, payload)
        except OSError as exc:
            # Non-fatal: the daemon keeps calibrating. But this file is now the
            # primary outage-detection signal, so leave a DEBUG breadcrumb if
            # it ever starts failing persistently (read-only / full results/)
            # rather than swallowing it completely.
            log.debug("health write failed: %s", exc)

    def _mark_calibrated(self, market_id: str) -> None:
        """Record a successful calibration publish and clear idle state."""
        prev_reason = self._idle_reason
        prev_idle_since = self._idle_since_unix
        now = time.time()
        self._last_calibration_unix = now
        self._last_calibration_market_id = market_id
        self._last_active_market_unix = now
        self._idle_reason = None
        self._idle_since_unix = None
        # Reset the throttle so the NEXT idle period logs promptly rather than
        # waiting out a stale interval inherited from a previous outage.
        self._last_idle_log_monotonic = 0.0
        if prev_reason is not None:
            idle_for = now - prev_idle_since if prev_idle_since is not None else 0.0
            log.info(
                "calc recovered: calibrating %s again after %.0fs idle (was: %s)",
                market_id, idle_for, prev_reason,
            )

    def _note_idle(self, reason: str, *, market_present: bool = False) -> None:
        """Record that this iteration produced no calibration, and why."""
        if self._idle_reason is None:
            self._idle_since_unix = time.time()
        self._idle_reason = reason
        if market_present:
            self._last_active_market_unix = time.time()

    def _maybe_log_idle(self) -> None:
        """Emit one throttled WARNING while calc is producing no calibrations.

        Replaces the previous behaviour where the no-active-market path logged
        *nothing* (silent outage) while the late-capture path logged a WARNING
        every ~0.5s (thousands of lines). One line per
        ``IDLE_LOG_INTERVAL_SECONDS`` stating the reason and staleness.
        """
        if self._idle_reason is None:
            return
        nowm = time.monotonic()
        if nowm - self._last_idle_log_monotonic < IDLE_LOG_INTERVAL_SECONDS:
            return
        self._last_idle_log_monotonic = nowm
        since = (
            time.time() - self._last_calibration_unix
            if self._last_calibration_unix is not None
            else None
        )
        log.warning(
            "calc idle (%s): no calibration published for %s — model "
            "probabilities are stale. Last calibrated market=%s. Expected when "
            "Polymarket has no active 15m market or published it too late to "
            "capture a barrier (valid window %.0f-%.0fs after window-start); "
            "calc resumes automatically on the next clean market.",
            self._idle_reason,
            f"{since:.0f}s" if since is not None else "the entire session",
            self._last_calibration_market_id or "n/a",
            POLYMARKET_FRESHNESS_MIN_S,
            POLYMARKET_CAPTURE_MAX_S,
        )

    def _publish_market_context(self, context: MarketContext) -> None:
        publisher = self._market_context_publisher
        if publisher is None:
            return
        signature = (
            context.market_id,
            context.market_slug,
            context.market_title,
            context.market_start_time_iso,
            context.market_end_time_iso,
            round(float(context.barrier), 8),
            context.barrier_source,
            context.barrier_offset_s,
            json.dumps(context.clob_token_ids or {}, sort_keys=True),
            json.dumps(context.contract_metadata or {}, sort_keys=True),
        )
        if signature == self._last_market_context_signature:
            return
        self._market_context_sequence += 1
        tick = MarketContextTick.create(
            source="calc_daemon",
            market_id=context.market_id,
            slug=context.market_slug or context.market_id,
            title=context.market_title,
            window_start_iso=context.market_start_time_iso,
            window_end_iso=context.market_end_time_iso,
            barrier=context.barrier,
            barrier_source=context.barrier_source,
            barrier_offset_s=context.barrier_offset_s,
            clob_token_ids=context.clob_token_ids,
            sequence=self._market_context_sequence,
            contract_metadata=context.contract_metadata,
        )
        try:
            publisher.publish(tick)
            self._last_market_context_signature = signature
        except Exception as exc:  # noqa: BLE001
            log.warning("failed to publish market context: %s", exc)

    def _resolve_context_cached(self, status: dict) -> Optional[MarketContext]:
        """Return the active market context, re-scanning gamma only when needed.

        The active market is immutable until window_end, so re-resolving every
        loop iteration (2+ HTTP probes) was pure overhead. Re-scan when: no
        cache; the cached window is ending (find the next market); the barrier
        is not yet locked to an authoritative source (the lock can upgrade);
        or the periodic revalidation interval elapsed (catches market voids).
        """
        now = time.time()
        cached = self._cached_context
        if cached is not None:
            try:
                end_ts = cached.market_end_time.timestamp()
            except Exception:  # noqa: BLE001
                end_ts = 0.0
            rescan_due = (
                now >= end_ts - 5.0
                or cached.barrier_source not in ("polymarket", "chainlink")
                or now - self._cached_context_at
                >= self.config.market_scan_interval_seconds
            )
            if not rescan_due:
                status["reason"] = None
                status["market_present"] = True
                return cached
        context = _resolve_active_market_context(
            chainlink=self._chainlink,
            locked_barriers=self._market_barriers,
            archive=self._archive,
            chain_broken_markets=self._chain_broken_markets,
            polymarket=self._polymarket,
            status=status,
        )
        if context is not None:
            self._cached_context = context
            self._cached_context_at = now
        else:
            self._cached_context = None
        return context

    def _submit_book_fetches(self, context: MarketContext) -> dict:
        """Kick off UP/DOWN best-ask fetches so they overlap the calibration."""
        futures: dict = {}
        if not context.clob_token_ids:
            return futures
        for side in ("UP", "DOWN"):
            token_id = context.clob_token_ids.get(side)
            if token_id:
                previous = self._book_jobs.get(side)
                if previous is not None and not previous.done():
                    # One outstanding request per side; a slow CLOB must not
                    # accumulate work across skipped calibration cycles.
                    continue
                futures[side] = self._book_pool.submit(
                    self._book_clients[side].fetch_order_book, token_id
                )
                self._book_jobs[side] = futures[side]
        return futures

    def _harvest_book_fetches(self, futures: dict, model_data: dict) -> None:
        deadline = time.monotonic() + 0.5
        if model_data.get("payoff_kind") == TWAP_PAYOFF:
            timestamps = [model_data.get(k) for k in
                          ("probability_evaluated_at", "probability_spot_observed_at")]
            try:
                remaining = min(2.0 - (_utc_now() - datetime.fromisoformat(t)).total_seconds()
                                for t in timestamps)
            except (TypeError, ValueError):
                remaining = 0.0
            # Reserve transport/consumer time rather than using the entire TTL.
            deadline = min(deadline, time.monotonic() + max(0.0, remaining - 0.2))
        model_data["up_best_ask"] = 0.0
        model_data["down_best_ask"] = 0.0
        for side, future in futures.items():
            try:
                book = future.result(timeout=max(0.0, deadline - time.monotonic()))
            except Exception as exc:  # noqa: BLE001
                log.debug("Quote fetch failed (%s): %s", side, exc)
                continue
            if book and book.best_ask > 0:
                key = "up_best_ask" if side == "UP" else "down_best_ask"
                model_data[key] = book.best_ask

    def run_once(self) -> Optional[CalibrationEvent]:
        cycle_t0 = time.monotonic()
        # Always try to drain pending settlements first — even when we
        # have no active market (e.g. between window-end and next market
        # start), the previous market may need its end-price captured.
        try:
            self._resolve_pending_settlements()
        except Exception:  # noqa: BLE001
            log.exception("settlement resolution failed; continuing")

        status: dict = {}
        context = self._resolve_context_cached(status)
        if context is None:
            self._note_idle(
                status.get("reason") or "no_active_market",
                market_present=bool(status.get("market_present")),
            )
            self._sleep(0.5)
            return None
        self._last_active_market_unix = time.time()
        self._publish_market_context(context)
        contract = MarketContract.from_dict(context.contract_metadata)

        if context.market_id != self._last_market_id:
            log.info(
                "calc daemon focused on market %s (barrier=%.2f)",
                context.market_id,
                context.barrier,
            )
            # Enqueue the OLD market for settlement capture. We need its
            # window_end and strike — read both from the existing settlement
            # file, which was written when the strike was first locked.
            if self._last_market_id is not None:
                self._enqueue_old_market_for_settlement(self._last_market_id)
            self._last_market_id = context.market_id
            # Persist the new market's strike to disk (idempotent).
            # Polymarket 15m markets are exactly 15 minutes wide.
            window_start = context.market_end_time - timedelta(minutes=15)
            self._write_settlement_strike(
                market_id=context.market_id,
                window_start=window_start,
                window_end=context.market_end_time,
                strike=context.barrier,
                strike_offset_s=context.barrier_offset_s,
                strike_source=context.barrier_source or "chainlink",
                contract_metadata=context.contract_metadata,
            )
            # Garbage-collect locks for markets that are no longer active.
            # Single-market trader: keep ONLY the current market's lock.
            self._market_barriers = {
                k: v for k, v in self._market_barriers.items()
                if k == context.market_id
            }

        if context.contract_metadata is not None and not contract.supported:
            self._note_idle(contract.reason or "unsupported_contract", market_present=True)
            self._sleep(0.5)
            return None
        now = _utc_now()
        hours_remaining = max(
            0.0, (context.market_end_time - now).total_seconds() / 3600.0
        )
        if hours_remaining <= 0:
            self._note_idle("market_ended", market_present=True)
            self._sleep(0.5)
            return None
        if contract.payoff_kind == TWAP_PAYOFF and hours_remaining * 3600.0 <= contract.averaging_window_seconds:
            self._note_idle("twap_history_window_no_entries", market_present=True)
            self._sleep(0.5)
            return None

        agg_spot: Optional[float] = None
        agg_source = ""
        agg_details: dict = {}
        spot_tick: Optional[SpotTick] = None
        if self._spot_feed is not None:
            spot_tick = self._spot_feed.latest(max_age_s=self.config.spot_max_age_seconds)
            if spot_tick is not None:
                agg_spot = spot_tick.price
                agg_source = spot_tick.source
                agg_details = spot_tick.to_dict()
                log.info(
                    "shared spot: $%.2f [%s seq=%s age=%.2fs]",
                    agg_spot,
                    agg_source,
                    spot_tick.sequence,
                    spot_tick.age_seconds(),
                )
        elif self._spot_aggregator is not None:
            agg_result = self._spot_aggregator.get_aggregated_spot()
            if agg_result is not None:
                agg_spot, agg_source, agg_details = agg_result
                log.info("aggregated spot: $%.2f [%s]", agg_spot, agg_source)

        if agg_spot is None:
            log.warning("shared BTC spot unavailable or stale; skipping calibration")
            self._note_idle("spot_unavailable", market_present=True)
            self._sleep(0.5)
            return None

        # Start CLOB book fetches now so they run concurrently with the
        # calibration below instead of delaying the publish afterwards.
        book_futures = self._submit_book_fetches(context)
        books_t0 = time.monotonic()

        # Post-fit spot refresh: the surface fit doesn't depend on spot, so
        # the pricing step re-reads the freshest tick (~1-2s newer than the
        # cycle-start read above). The holder records which tick was actually
        # priced so the event's spot metadata stays truthful.
        fresh_spot_holder: dict = {}

        def _fresh_spot() -> Optional[float]:
            if self._spot_feed is None:
                return None
            tick = self._spot_feed.latest(max_age_s=self.config.spot_max_age_seconds)
            if contract.payoff_kind == TWAP_PAYOFF:
                # Leave 0.6s of the entry TTL for MC and delivery. If the
                # upstream feed cannot meet this, skip; measure latency
                # before changing the scheduling budget or pricing method.
                deadline = time.monotonic() + 1.0
                while True:
                    if (context.market_end_time - _utc_now()).total_seconds() <= contract.averaging_window_seconds:
                        raise ValueError("twap_history_window_no_entries after spot wait")
                    observation = (tick.source, tick.source_timestamp_ms) if tick else None
                    age = (_utc_now().timestamp() - tick.source_timestamp_ms / 1000.0) if tick else float("inf")
                    if (tick is not None and 0.0 <= age <= 1.4
                            and observation != self._last_priced_observation):
                        break
                    remaining = deadline - time.monotonic()
                    if remaining <= 0 or self._stop:
                        raise ValueError("fresh_spot_timeout: no new observation within 1.4s source age")
                    tick = self._spot_feed.wait_for_update(
                        tick.sequence if tick else None, timeout_s=remaining,
                    )
                self._last_priced_observation = observation
            if tick is None:
                return None
            fresh_spot_holder["tick"] = tick
            return tick.price

        model_data = run_terminal_script(
            barrier=context.barrier,
            hours=hours_remaining,
            skip_heston=True,
            json_path=self.terminal_path,
            market_end_time=context.market_end_time_iso,
            spot=agg_spot,
            spot_provider=_fresh_spot,
            payoff_kind=contract.payoff_kind if context.contract_metadata is not None else "terminal_spot",
            averaging_window_seconds=contract.averaging_window_seconds or 60,
        )
        ready_at = _utc_now()

        refreshed_tick = fresh_spot_holder.get("tick")
        if refreshed_tick is not None:
            spot_tick = refreshed_tick
            agg_spot = spot_tick.price
            agg_source = spot_tick.source
            agg_details = spot_tick.to_dict()

        if agg_spot is not None:
            model_data["spot_price"] = agg_spot
            model_data["spot_source"] = agg_source
            model_data["spot_details"] = agg_details
            if spot_tick is not None:
                model_data["spot_sequence"] = spot_tick.sequence
                model_data["spot_published_at_ms"] = spot_tick.published_at_ms
                model_data["spot_source_label"] = spot_tick.source_label
                model_data["probability_spot_observed_at"] = datetime.fromtimestamp(
                    spot_tick.source_timestamp_ms / 1000.0, tz=timezone.utc,
                ).isoformat()

        if model_data.get("error") or model_data.get("prob_above") is None:
            log.warning(
                "calibration produced no usable result for %s: %s",
                context.market_id,
                str(model_data.get("error") or "missing prob_above")[:200],
            )
            self._note_idle("calibration_error", market_present=True)
            return None

        terminal_payload = model_data.pop("terminal_payload", None)
        if terminal_payload is None:
            # Subprocess fallback path — the payload only exists on disk.
            if not self.terminal_path.exists():
                log.warning(
                    "calibration ran but terminal payload missing at %s",
                    self.terminal_path,
                )
                self._note_idle("calibration_error", market_present=True)
                return None
            terminal_payload = json.loads(self.terminal_path.read_text(encoding="utf-8"))

        self._harvest_book_fetches(book_futures, model_data)
        books_s = time.monotonic() - books_t0

        entry_signals: tuple = ()
        if self.config.publish_entry_signals:
            entry_signals = self._compute_entry_signals(
                float(model_data["prob_above"]),
                float(model_data["prob_below"]),
                float(model_data.get("up_best_ask") or 0),
                float(model_data.get("down_best_ask") or 0),
                context.market_id,
            )
        timing = terminal_payload.setdefault("timing", {})
        timing["books_s"] = round(books_s, 4)
        timing["cycle_s"] = round(time.monotonic() - cycle_t0, 4)
        if spot_tick is not None and model_data.get("probability_evaluated_at"):
            evaluated_at = datetime.fromisoformat(model_data["probability_evaluated_at"])
            source_at = spot_tick.source_timestamp_ms / 1000.0
            timing.update(
                source_to_receive_s=round(spot_tick.received_at_ms / 1000.0 - source_at, 4),
                pricing_spot_age_s=round(evaluated_at.timestamp() - source_at, 4),
                pricing_to_publish_s=round((_utc_now() - evaluated_at).total_seconds(), 4),
                publication_spot_age_s=round(_utc_now().timestamp() - source_at, 4),
            )
        # The distribution rides as a first-class v4 event field; drop the
        # duplicate copy inside terminal_payload to keep events lean.
        terminal_payload.pop("terminal_return_quantiles", None)
        event = CalibrationEvent.from_parts(
            source=self.config.source_label,
            context=context,
            ready_at=ready_at,
            model_data=model_data,
            terminal_payload=terminal_payload,
            entry_signals=entry_signals,
        )

        # Serialize once; the same string feeds the publisher (Redis + file
        # fallback) and the snapshot write.
        event_json = serialize_calibration_event(event)
        try:
            self._calibration_publisher.publish(event, payload_json=event_json)
        except Exception as exc:  # noqa: BLE001
            log.warning("failed to publish calibration event: %s", exc)

        if self.config.snapshot_path is not None:
            try:
                atomic_write_text(self.config.snapshot_path, event_json)
            except OSError as exc:
                log.warning(
                    "failed to write snapshot %s: %s",
                    self.config.snapshot_path,
                    exc,
                )

        log.info(
            "wrote calibration event for %s (fit %.2fs, mc %.2fs, cycle %.2fs, books %.2fs)",
            context.market_id,
            float(timing.get("surface_fit_s") or 0.0),
            float(timing.get("mc_s") or 0.0),
            float(timing.get("cycle_s") or 0.0),
            float(timing.get("books_s") or 0.0),
        )
        # Record calibration freshness (drives the health file) and emit a
        # one-line recovery INFO if we were idle. Placed after the publish +
        # snapshot so the timestamp reflects a fully fanned-out event.
        self._mark_calibrated(context.market_id)
        return event

    def _sweep_inbox_targets_throttled(self) -> None:
        # Sweep every ~10 iterations to amortize stat() cost.
        self._sweep_counter = (self._sweep_counter + 1) % 10
        if self._sweep_counter != 0:
            return
        for target in self.config.inbox_targets:
            _sweep_inbox(target, self.config.inbox_ttl_seconds)

    def _sleep(self, seconds: float) -> None:
        end = time.monotonic() + seconds
        while not self._stop and time.monotonic() < end:
            time.sleep(min(0.25, max(0.0, end - time.monotonic())))


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG_PATH,
        help=f"Path to calc daemon config (default: {DEFAULT_CONFIG_PATH})",
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    args = _build_arg_parser().parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-5s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    # Keep third-party HTTP client logs out of the daemon log even under -v.
    # The gamma market-discovery poll fires several requests/second; at DEBUG
    # urllib3 floods the log with thousands of connectionpool lines that bury
    # every diagnostic signal (this masked the 2026-06-17 calc outage — the
    # log was 100% gamma-request DEBUG noise with no "no active market" line).
    for _noisy in ("urllib3", "urllib3.connectionpool", "requests", "websocket", "websockets"):
        logging.getLogger(_noisy).setLevel(logging.WARNING)

    if not args.config.exists():
        log.error("config file not found: %s", args.config)
        return 2

    config = CalcConfig.from_json(args.config)
    shared_spot = RedisSpotConsumer(
        redis_url=config.spot_redis_url,
        latest_key=config.spot_redis_latest_key,
        stream=config.spot_redis_stream,
        channel=config.spot_redis_channel,
    )
    polymarket = PolymarketPTB()

    daemon = CalcDaemon(
        config,
        chainlink=shared_spot,
        polymarket=polymarket,
        spot_feed=shared_spot,
    )

    def _handle_signal(signum, _frame):  # noqa: ARG001
        log.info("received signal %s, stopping", signum)
        daemon.request_stop()

    signal.signal(signal.SIGINT, _handle_signal)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _handle_signal)

    try:
        shared_spot.start()
        daemon.run_forever()
    finally:
        daemon._settlement_pool.shutdown(wait=False, cancel_futures=True)
        daemon._book_pool.shutdown(wait=False, cancel_futures=True)
        try:
            shared_spot.close()
        except Exception:  # noqa: BLE001
            pass
        if hasattr(daemon._calibration_publisher, "close"):
            try:
                daemon._calibration_publisher.close()
            except Exception:  # noqa: BLE001
                pass
        if daemon._market_context_publisher is not None:
            try:
                daemon._market_context_publisher.close()
            except Exception:  # noqa: BLE001
                pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
