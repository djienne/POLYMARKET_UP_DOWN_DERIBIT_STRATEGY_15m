"""Market data and timestamped payoff forecasts for one trader/grid loop.

Current contracts consume the shared calc daemon's schema-v5 probabilities.
Each grid loop shares its book snapshots across all configured paper instances;
the live trader runs its own feed and execution checks. Legacy calibration and
terminal-distribution evaluation remain compatibility paths.
"""

import logging
import json
import math
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from scripts.polymarket_btc_15m import (
    find_active_15m_market,
    extract_market_data,
    get_current_btc_price,
    get_coinbase_exchange_ticker_price,
    get_coinbase_spot_price,
    get_binance_price_at,
    _is_sane_btc_price,
)
from scripts.polymarket_edge import run_terminal_script
from scripts.trader.config import GridConfig
from scripts.trader.hybrid_calibration import (
    HybridCalibrationBroker,
    MarketContext,
)
from scripts.trader.order_book import (
    OrderBookClient,
    OrderBookSnapshot,
    clone_order_book,
)
from scripts.trader.spot_ipc import RedisSpotConsumer
from scripts.trader.contracts import AUTHORITATIVE_PAYOUT_SOURCES, MarketContract, TWAP_PAYOFF, entry_inputs_valid, payout_for_position

log = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_RESULTS_DIR = PROJECT_ROOT / "results"
TERMINAL_JSON = DEFAULT_RESULTS_DIR / "terminal_data.json"
LAST_GOOD_SPOT_TTL_SECONDS = 30.0


_slippage_warned: set[str] = set()


def _results_dir_from_config(config: object) -> Path:
    """Resolve trader output paths from config when available.

    Grid mode has historically used the project ``results/`` directory; live
    configs may carry explicit state/output paths. Either way, avoid depending
    on the process working directory.
    """
    for attr in (
        "state_file",
        "trades_file",
        "orderbook_file",
        "live_diagnostics_file",
        "risk_events_file",
        "equity_curve_file",
    ):
        raw = getattr(config, attr, None)
        if raw:
            return Path(raw).expanduser().resolve().parent
    return DEFAULT_RESULTS_DIR


def _apply_book_slippage(
    snapshot: Optional[OrderBookSnapshot], slippage: float
) -> Optional[OrderBookSnapshot]:
    """Shift asks up and bids down by `slippage` to penalize the delayed book."""
    if snapshot is None or slippage <= 0:
        return snapshot
    if snapshot.best_bid > 0 and slippage >= snapshot.best_bid:
        if snapshot.token_id not in _slippage_warned:
            log.warning(
                "slippage %.4f exceeds best_bid %.4f; adjusted bids will clamp to 0",
                slippage,
                snapshot.best_bid,
            )
            _slippage_warned.add(snapshot.token_id)
    asks = [(price + slippage, size) for price, size in snapshot.asks]
    bids = [(max(0.0, price - slippage), size) for price, size in snapshot.bids]
    return OrderBookSnapshot(
        token_id=snapshot.token_id,
        best_bid=max(0.0, snapshot.best_bid - slippage),
        best_ask=snapshot.best_ask + slippage,
        bids=bids,
        asks=asks,
        timestamp=snapshot.timestamp,
    )


def _format_calibration_source(
    *,
    enable_local_worker: bool,
    enable_remote_inbox: bool,
    inbox_dir: Path,
) -> str:
    """Human-readable summary printed at startup so it's obvious if a flag
    flip was missed (e.g. local worker still on alongside the shared calc).
    """
    parts = []
    if enable_local_worker:
        parts.append("local_worker")
    if enable_remote_inbox:
        parts.append(f"inbox={inbox_dir}")
    return ", ".join(parts) or "none"


@dataclass(frozen=True)
class TickData:
    """Immutable snapshot broadcast to all instances each tick."""
    timestamp: datetime
    market_id: str
    market_title: str
    barrier: float              # BTC price at window start
    spot_price: float           # Current BTC price
    time_remaining_seconds: float
    elapsed_seconds: float      # Seconds since window start
    window_duration_seconds: float  # Total window length (300 for 15-min)
    prob_above: float           # Model probability (SSVI Surface MC + BL avg)
    prob_below: float
    poly_prob_up: float         # Polymarket mid-price for UP
    poly_prob_down: float       # Polymarket mid-price for DOWN
    up_order_book: Optional[OrderBookSnapshot]   # Full UP order book for VWAP
    down_order_book: Optional[OrderBookSnapshot]  # Full DOWN order book for VWAP
    canonical_up_ask: Optional[float] = None    # Calc's published UP best_ask
    canonical_down_ask: Optional[float] = None  # Calc's published DOWN best_ask
    up_order_book_delayed: Optional[OrderBookSnapshot] = None
    down_order_book_delayed: Optional[OrderBookSnapshot] = None
    up_token_id: str = ""            # CLOB token ID for UP
    down_token_id: str = ""          # CLOB token ID for DOWN
    model_used: str = ""             # Which model produced the probabilities
    calibration_seconds: float = 0.0  # How long the calibration took
    market_end_time: Optional[datetime] = None
    barrier_source: str = ""          # "chainlink", "binance", or "spot_fallback"
    barrier_confirmed: bool = False   # True only if barrier from Chainlink (resolution source)
    is_new_market: bool = False       # First tick of new market window
    is_expired: bool = False          # Market just resolved
    settlement_pending: bool = False
    final_spot: Optional[float] = None  # BTC price at resolution (only on expiry tick)
    final_spot_source: str = ""
    spot_source: str = ""
    entry_signals: tuple = ()
    entry_signal_age_s: float = -1.0
    entry_signal_market_id: str = ""
    entry_signal_stale_age_s: float = -1.0
    # Per-tick lookup index {str(instance_id): signal} built once by the feed
    # (first-wins, matching the historical first-match scan). Replaces the
    # per-instance O(instances x signals) linear scan in the entry path.
    entry_signal_index: Optional[dict] = None
    # Probability provenance (legacy local evaluation and current v5 scalar):
    #   "local_mc_cdf" — prob_above/below re-thresholded from the event's MC
    #                    terminal distribution at the live spot this tick;
    #   "event_scalar" — the event's frozen scalar probability (legacy path);
    #   ""             — 0.5/0.5 prior (no event).
    prob_source: str = ""
    prob_spot_age_s: float = -1.0
    prob_event_age_s: float = -1.0
    payoff_kind: str = "legacy"
    contract_supported: bool = True
    contract_reason: str = ""
    condition_id: str = ""
    averaging_window_seconds: float = 0.0
    fee_schedule: Optional[dict] = None
    contract_metadata: Optional[dict] = None
    probability_evaluated_at: Optional[datetime] = None
    probability_spot_observed_at: Optional[datetime] = None
    probability_max_age_s: float = 2.0
    settlement_payouts: Optional[dict] = None
    settlement_source: str = ""


class SharedDataFeed:
    """Fetches all market data and model probabilities for the grid.

    Each tick runs:
    1. Market scan (Gamma API, throttled to every 30s)
    2. Full SSVI Surface + MC calibration via the shared calibration worker (~5-60s)
    3. Polymarket CLOB order book fetch (shared across all instances)
    4. Polymarket/Chainlink spot price (Binance fallback)
    """

    def __init__(
        self,
        config: GridConfig,
        *,
        hybrid_calibration_enabled: bool = False,
        hybrid_remote_inbox_enabled: bool = True,
        hybrid_local_worker_enabled: bool = True,
        hybrid_local_worker_delay_min_seconds: float = 0.0,
        hybrid_local_worker_delay_max_seconds: float = 0.0,
        hybrid_inbox_dir: Optional[Path] = None,
        ipc_backend: str = "file",
        redis_url: Optional[str] = None,
        redis_stream: str = "market.calibration",
        redis_consumer_group: str = "trader",
        redis_consumer_name: str = "consumer",
        ipc_file_fallback_enabled: bool = True,
        hybrid_push_consumer_enabled: bool = True,
        local_prob_eval_enabled: bool = False,
        local_eval_event_max_age_s: float = 30.0,
        local_eval_spot_max_age_s: float = 2.0,
    ):
        self.config = config
        self._freshness_counts: dict[str, int] = {}
        self._freshness_logged_at = 0.0
        self.results_dir = _results_dir_from_config(config)
        self.terminal_json = self.results_dir / "terminal_data.json"
        self.ob_client = OrderBookClient(timeout=8)
        # Concurrent UP/DOWN book fetches: one client per side because
        # requests.Session is not thread-safe. Halves each book round's
        # wall-clock without changing what is fetched or when.
        self._book_pool = ThreadPoolExecutor(max_workers=2, thread_name_prefix="feed-books")
        self._book_clients = {
            "UP": OrderBookClient(timeout=8),
            "DOWN": OrderBookClient(timeout=8),
        }
        # Binance barrier-sanity memoization: the 1-minute kline at a past
        # window-start never changes, so the verdict is deterministic per
        # (market, barrier). Definitive verdicts are cached; unreachable-
        # Binance outcomes stay fail-open and uncached with a retry limiter.
        self._sanity_verdicts: dict = {}
        self._sanity_last_attempt: float = 0.0
        # Legacy terminal-price re-evaluation only. Current contract inputs
        # use v5 producer scalars and two-second source/pricing age checks.
        self._local_eval_enabled = bool(local_prob_eval_enabled)
        self._local_eval_event_max_age_s = float(local_eval_event_max_age_s)
        self._local_eval_spot_max_age_s = float(local_eval_spot_max_age_s)
        self._local_signal_params: list[dict] = []
        self._local_eval_log_at: float = 0.0
        if self._local_eval_enabled:
            from scripts.trader.local_eval import load_signal_params

            self._local_signal_params = load_signal_params(
                PROJECT_ROOT / "config.yaml"
            )
            log.info(
                "local probability re-evaluation ENABLED (%d signal param combos)",
                len(self._local_signal_params),
            )
        self.hybrid_calibration_enabled = hybrid_calibration_enabled

        # Note: live and grid containers no longer hold a Chainlink WebSocket.
        # Calc is the sole owner of the on-chain feed; consumers read calc's
        # captured barrier (and its source) out of the calibration event
        # payload via HybridCalibrationBroker.latest_barrier_for_market().
        # See plan: tingly-foraging-pebble.md.

        # Market state
        self._current_market = None   # dict from extract_market_data
        self._current_market_id = None
        self._barrier = None          # BTC price captured at window start
        self._barrier_captured = False
        self._barrier_finalized = False  # True once calc has supplied a barrier
        self._barrier_offset_s: Optional[float] = None  # Chainlink offset (calc-side)
        self._barrier_source = ""     # "chainlink"/"binance"/"binance_local"/"binance_sanity_failed"
        self._last_good_spot: Optional[float] = None
        self._last_good_spot_source: str = ""
        self._last_good_spot_at: Optional[datetime] = None
        self._last_reported_spot_source: str = ""
        self._spot_consumer: Optional[RedisSpotConsumer] = None
        self._shared_spot_required = (ipc_backend or "").lower() == "redis" and bool(redis_url)
        if (ipc_backend or "").lower() == "redis" and redis_url:
            try:
                self._spot_consumer = RedisSpotConsumer(redis_url=redis_url)
                self._spot_consumer.start()
                log.info("current spot source: shared Redis feed")
            except Exception as exc:  # noqa: BLE001
                log.warning("shared Redis spot feed unavailable at startup: %s", exc)
        self._canonical_up_ask: Optional[float] = None
        self._canonical_down_ask: Optional[float] = None
        self._latest_entry_signals: tuple = ()
        self._latest_entry_signals_ts: Optional[datetime] = None
        self._latest_entry_signals_market_id: str = ""
        self._latest_entry_signals_stale_age_s: float = -1.0

        # Scan throttle
        self._last_market_scan = 0.0
        self._last_tick_market_id = None  # For is_new_market detection

        # Expiry tracking
        self._expiry_reported = False
        self._expiry_wait_started_at: Optional[datetime] = None
        self._hybrid_broker: Optional[HybridCalibrationBroker] = None
        if self.hybrid_calibration_enabled:
            self._hybrid_broker = HybridCalibrationBroker(
                results_dir=self.results_dir,
                enable_local_worker=hybrid_local_worker_enabled,
                enable_remote_inbox=hybrid_remote_inbox_enabled,
                worker_delay_min_seconds=hybrid_local_worker_delay_min_seconds,
                worker_delay_max_seconds=hybrid_local_worker_delay_max_seconds,
                inbox_dir=hybrid_inbox_dir,
                ipc_backend=ipc_backend,
                redis_url=redis_url,
                redis_stream=redis_stream,
                redis_consumer_group=redis_consumer_group,
                redis_consumer_name=redis_consumer_name,
                ipc_file_fallback_enabled=ipc_file_fallback_enabled,
                push_consumer_enabled=hybrid_push_consumer_enabled,
            )
            self._hybrid_broker.start()
            log.info(
                "calibration source: %s",
                _format_calibration_source(
                    enable_local_worker=hybrid_local_worker_enabled,
                    enable_remote_inbox=hybrid_remote_inbox_enabled,
                    inbox_dir=self._hybrid_broker.inbox_dir,
                ),
            )

    def _scan_market(self):
        """Find the current/next active 15-min market."""
        now = time.time()
        if now - self._last_market_scan < self.config.market_scan_interval_seconds:
            return

        self._last_market_scan = now
        try:
            event = find_active_15m_market()
            if event:
                data = extract_market_data(event)
                new_id = data.get("market_id")

                if new_id != self._current_market_id:
                    old_id = self._current_market_id
                    self._current_market = data
                    self._current_market_id = new_id
                    self._barrier = None
                    self._barrier_captured = False
                    self._barrier_finalized = False
                    self._barrier_offset_s = None
                    self._barrier_source = ""
                    self._last_good_spot = None
                    self._last_good_spot_source = ""
                    self._last_good_spot_at = None
                    self._latest_entry_signals = ()
                    self._latest_entry_signals_ts = None
                    self._latest_entry_signals_market_id = ""
                    self._latest_entry_signals_stale_age_s = -1.0
                    self._sanity_verdicts.clear()
                    self._sanity_last_attempt = 0.0
                    self._expiry_reported = False
                    self._expiry_wait_started_at = None
                    _slippage_warned.clear()
                    log.info(f"New market: {data.get('title')} (prev={old_id})")
                else:
                    self._current_market["prob_up"] = data.get("prob_up")
                    self._current_market["prob_down"] = data.get("prob_down")
                    self._current_market["contract_metadata"] = data.get("contract_metadata")
        except Exception as e:
            log.warning(f"Market scan failed: {e}")

    _CALIBRATION_MAX_ATTEMPTS = 2
    _CALIBRATION_RETRY_DELAY = 2.0

    def _run_calibration(self, barrier, hours_remaining, market_end_time_iso):
        """Run full SSVI Surface + MC calibration via the shared worker."""
        for attempt in range(self._CALIBRATION_MAX_ATTEMPTS):
            t0 = time.time()
            try:
                model_data = run_terminal_script(
                    barrier=barrier,
                    hours=hours_remaining,
                    skip_heston=True,
                    json_path=self.terminal_json,
                    market_end_time=market_end_time_iso,
                )

                elapsed = time.time() - t0

                if model_data.get("error"):
                    log.warning(f"Calibration error ({elapsed:.0f}s): {model_data['error'][:200]}")
                    return None, None, "", elapsed

                prob_above = model_data.get("prob_above")
                prob_below = model_data.get("prob_below")
                model_used = model_data.get("model_used", "ssvi_surface")

                if prob_above is None:
                    log.warning(f"Calibration returned None probabilities ({elapsed:.0f}s)")
                    return None, None, "", elapsed

                log.info(f"Calibration OK ({elapsed:.1f}s): "
                         f"P(UP)={prob_above:.1%} P(DOWN)={prob_below:.1%} "
                         f"[{model_used}]")
                return prob_above, prob_below, model_used, elapsed

            except Exception as e:
                elapsed = time.time() - t0
                if attempt < self._CALIBRATION_MAX_ATTEMPTS - 1:
                    log.warning(f"Calibration attempt {attempt + 1} failed ({elapsed:.0f}s): {e}, retrying...")
                    time.sleep(self._CALIBRATION_RETRY_DELAY)
                    continue
                log.error(f"Calibration exception ({elapsed:.0f}s): {e}")
                return None, None, "", elapsed

    def _fetch_book_pair(self, up_id: str, down_id: str):
        """Fetch UP and DOWN books concurrently (each side has its own client).

        Same data as the old serial pair, roughly half the wall-clock.
        """
        up_future = (
            self._book_pool.submit(self._book_clients["UP"].fetch_order_book, up_id)
            if up_id else None
        )
        down_future = (
            self._book_pool.submit(self._book_clients["DOWN"].fetch_order_book, down_id)
            if down_id else None
        )
        up_snap = None
        down_snap = None
        if up_future is not None:
            try:
                up_snap = up_future.result()
            except Exception as exc:  # noqa: BLE001
                log.warning(f"UP order book fetch failed: {exc}")
        if down_future is not None:
            try:
                down_snap = down_future.result()
            except Exception as exc:  # noqa: BLE001
                log.warning(f"DOWN order book fetch failed: {exc}")
        return up_snap, down_snap

    def _fetch_order_books(self):
        """Fetch full order books for UP and DOWN tokens.

        Returns (
            up_snapshot, down_snapshot, up_snapshot_delayed, down_snapshot_delayed,
            poly_prob_up, poly_prob_down
        ).
        Order book snapshots are shared across all configured instances for VWAP.
        """
        if not self._current_market:
            return None, None, None, None, None, None

        token_ids = self._current_market.get("token_ids", {})
        up_id = token_ids.get("UP", "")
        down_id = token_ids.get("DOWN", "")

        up_snap = None
        down_snap = None
        up_snap_delayed = None
        down_snap_delayed = None
        prob_up = self._current_market.get("prob_up")
        prob_down = self._current_market.get("prob_down")

        up_snap, down_snap = self._fetch_book_pair(up_id, down_id)
        if up_id:
            if up_snap and up_snap.best_bid > 0 and up_snap.best_ask > 0:
                prob_up = (up_snap.best_bid + up_snap.best_ask) / 2
            elif up_snap:
                log.warning(f"UP order book empty: bid={up_snap.best_bid} ask={up_snap.best_ask}")

        if down_id:
            if down_snap and down_snap.best_bid > 0 and down_snap.best_ask > 0:
                prob_down = (down_snap.best_bid + down_snap.best_ask) / 2
            elif down_snap:
                log.warning(f"DOWN order book empty: bid={down_snap.best_bid} ask={down_snap.best_ask}")

        up_snap_delayed = clone_order_book(up_snap)
        down_snap_delayed = clone_order_book(down_snap)

        delay = max(0.0, self.config.paper_recheck_delay_seconds)
        if delay > 0 and (up_id or down_id):
            time.sleep(delay)
            delayed_up, delayed_down = self._fetch_book_pair(up_id, down_id)
            if delayed_up:
                up_snap_delayed = delayed_up
            if delayed_down:
                down_snap_delayed = delayed_down

        slippage = self.config.paper_delayed_book_slippage
        if slippage > 0:
            up_snap_delayed = _apply_book_slippage(up_snap_delayed, slippage)
            down_snap_delayed = _apply_book_slippage(down_snap_delayed, slippage)

        return up_snap, down_snap, up_snap_delayed, down_snap_delayed, prob_up, prob_down

    def _resolve_final_spot(
        self,
        end_utc: datetime,
        spot: float,
        allow_degraded: bool = False,
    ) -> tuple[Optional[float], str]:
        """Read a legacy recorded price proxy, with its historical provenance.

        Current contracts use token payouts instead. Missing or invalid prices
        remain pending; ``allow_degraded`` never substitutes the latest spot.
        """
        market_id = self._current_market_id
        if (self._current_market or {}).get("contract_metadata") is not None:
            return None, "settlement_pending"  # current contracts settle token payouts
        if not market_id:
            return None, "settlement_pending"
        path = self.results_dir / "settlements" / f"{market_id}.json"
        if not path.exists():
            return None, "settlement_pending"
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            log.warning(f"could not read settlement file {path}: {exc}")
            return None, "settlement_pending"
        settlement_price = payload.get("settlement_price")
        settlement_source = payload.get("settlement_source")
        settlement_status = str(payload.get("settlement_status") or "")
        if settlement_price is None and settlement_status == "abandoned":
            return None, "settlement_abandoned"
        if settlement_price is None or settlement_source not in (
            "chainlink", "polymarket",
        ):
            return None, "settlement_pending"
        try:
            value = float(settlement_price)
            if not math.isfinite(value) or value <= 0:
                return None, "settlement_pending"
            return value, f"{settlement_source}_settlement"
        except (TypeError, ValueError):
            return None, "settlement_pending"

    def _resolve_settlement_payouts(self, contract: MarketContract) -> tuple[Optional[dict], str]:
        path = self.results_dir / "settlements" / f"{self._current_market_id}.json"
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return None, ""
        payouts = payload.get("settlement_payouts")
        if (payload.get("settlement_status") != "finalized"
                or payload.get("settlement_source") not in AUTHORITATIVE_PAYOUT_SOURCES
                or payload.get("condition_id") != contract.condition_id):
            return None, ""
        if not contract.outcome_token_ids or any(
            payout_for_position(payouts, token, side) is None
            for side, token in contract.outcome_token_ids.items()
        ):
            return None, ""
        return payouts, payload["settlement_source"]

    def _fallback_live_reference_price(self) -> Optional[float]:
        live_state = self.results_dir / "15m_live_state.json"
        try:
            raw = json.loads(live_state.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        current_market = raw.get("current_market") or {}
        if current_market.get("market_id") != self._current_market_id:
            return None
        ref = current_market.get("reference_price")
        try:
            return float(ref) if ref is not None else None
        except (TypeError, ValueError):
            return None

    def _remember_good_spot(self, price: Optional[float], source: str) -> tuple[float, str]:
        if price is not None and _is_sane_btc_price(price):
            self._last_good_spot = float(price)
            self._last_good_spot_source = source
            self._last_good_spot_at = datetime.now(timezone.utc)
            return float(price), source
        return 0.0, ""

    def _resolve_current_spot(self) -> tuple[float, str]:
        """Resolve a sane current BTC spot as fallback when calc hasn't provided one.

        The primary spot source is calc's calibration event (spot_source="calc"),
        consumed in the hybrid calibration block above. This method is only called
        when no fresh event is available. Fallbacks: Binance → Coinbase Exchange →
        Coinbase Data API → last-good cached → barrier value → external state file.
        """
        if self._spot_consumer is not None:
            tick = self._spot_consumer.latest(max_age_s=10.0)
            if tick is not None:
                return self._remember_good_spot(tick.price, tick.source)
            return 0.0, ""
        if self._shared_spot_required:
            return 0.0, ""

        try:
            price = get_current_btc_price()
            if price is not None:
                return self._remember_good_spot(price, "binance")
        except Exception as exc:
            log.debug(f"Binance current spot failed: {exc}")

        try:
            price = get_coinbase_exchange_ticker_price()
            if price is not None:
                return self._remember_good_spot(price, "coinbase_exchange")
        except Exception as exc:
            log.debug(f"Coinbase Exchange spot failed: {exc}")

        try:
            price = get_coinbase_spot_price()
            if price is not None:
                return self._remember_good_spot(price, "coinbase_spot")
        except Exception as exc:
            log.debug(f"Coinbase Data API spot failed: {exc}")

        if (
            self._last_good_spot is not None
            and _is_sane_btc_price(self._last_good_spot)
        ):
            if self._last_good_spot_at is None:
                return float(self._last_good_spot), "last_good_spot"
            age = (datetime.now(timezone.utc) - self._last_good_spot_at).total_seconds()
            if age <= LAST_GOOD_SPOT_TTL_SECONDS:
                return float(self._last_good_spot), "last_good_spot"
            log.warning("last_good_spot is stale (age %.1fs); ignoring cached fallback", age)

        if self._last_good_spot is not None and _is_sane_btc_price(self._last_good_spot):
            self._last_good_spot = None
            self._last_good_spot_source = ""
            self._last_good_spot_at = None

        if self._barrier and _is_sane_btc_price(self._barrier):
            return float(self._barrier), "barrier_fallback"

        fallback_barrier = self._fallback_live_reference_price()
        if fallback_barrier and _is_sane_btc_price(fallback_barrier):
            return float(fallback_barrier), "reference_fallback"

        return 0.0, ""

    # Rate limit between Binance sanity HTTP attempts while no definitive
    # verdict is cached (fail-open in between). Keeps a stalled Binance from
    # inserting a blocking HTTP call (10s timeout) into EVERY tick.
    _SANITY_RETRY_INTERVAL_S = 30.0

    def _binance_sanity_passes(
        self, calc_barrier: float, start_utc: datetime
    ) -> bool:
        """Independent Binance-only check on calc's published Chainlink barrier.

        Live/grid don't run a Chainlink WebSocket, so the only cross-check
        available locally is the Binance kline at window-start. Returns True
        on agreement (or when Binance is unreachable — degrading gracefully
        rather than blocking entries on a transient Binance outage). Returns
        False on divergence above the configured tolerance, in which case
        the caller should NOT confirm the barrier for entries.

        The verdict is deterministic per (market, barrier) — the 1-minute
        kline at a past window-start never changes — so definitive verdicts
        are memoized and the HTTP call happens once per market instead of
        once per tick.
        """
        cache_key = (self._current_market_id or "", round(float(calc_barrier), 2))
        cached = self._sanity_verdicts.get(cache_key)
        if cached is not None:
            return cached

        now = time.time()
        if now - self._sanity_last_attempt < self._SANITY_RETRY_INTERVAL_S:
            # No definitive verdict yet and inside the retry window — keep
            # the historical fail-open behavior without re-blocking the tick.
            return True
        self._sanity_last_attempt = now

        tol_pct = float(
            getattr(self.config, "binance_sanity_tolerance_pct", 1.0)
        )
        try:
            kline = get_binance_price_at(start_utc)
        except Exception as exc:
            log.warning(
                f"Binance sanity check unavailable ({exc}) — trusting calc"
            )
            return True
        if not kline or kline <= 0 or not _is_sane_btc_price(kline):
            return True
        diff_pct = abs(calc_barrier - kline) / kline * 100
        if diff_pct > tol_pct:
            log.error(
                f"Calc/Binance sanity check failed: {diff_pct:.2f}% "
                f"> {tol_pct:.2f}% tolerance "
                f"(${calc_barrier:,.2f} vs ${kline:,.2f}) — "
                "barrier_confirmed=False for this market"
            )
            self._sanity_verdicts[cache_key] = False
            return False
        if diff_pct > tol_pct * 0.5:
            log.warning(
                f"Calc/Binance sanity check warning: {diff_pct:.2f}% "
                f"(${calc_barrier:,.2f} vs ${kline:,.2f})"
            )
        self._sanity_verdicts[cache_key] = True
        return True

    def tick(self) -> Optional[TickData]:
        """Execute one data cycle: scan market, calibrate, fetch prices.

        Returns TickData or None if no active market.
        """
        now = datetime.now(timezone.utc)

        # Emit the old market's expiry status once before moving discovery
        # forward. Unresolved exposure is managed by its own market registry;
        # it must not pin the shared active-market feed to an expired window.
        if self._current_market and not self._expiry_reported:
            old_end = self._current_market.get("end_date_utc")
            if old_end and now >= old_end:
                # Current market expired — don't scan yet, emit expiry tick first
                # so open positions get their settlement event. Reset the
                # scan rate-limit so the NEXT tick (after the expiry tick)
                # detects the new market immediately, not 30s into the new
                # window. Without this prime, _scan_market's rate-limit
                # pushes new-market detection past the 30s chainlink-
                # refinement window, forcing 0% chainlink hit on live/grid.
                self._last_market_scan = 0.0
            else:
                self._scan_market()
        else:
            self._scan_market()

        if not self._current_market:
            return None

        mkt = self._current_market
        contract_raw = mkt.get("contract_metadata")
        contract = MarketContract.from_dict(contract_raw)
        end_utc = mkt.get("end_date_utc")
        start_utc = mkt.get("window_start_utc")

        if not end_utc:
            return None
        if now >= end_utc and self._expiry_reported:
            return None  # discovery has not found the next market yet

        # Check expiry
        remaining_secs = (end_utc - now).total_seconds()
        is_expired = remaining_secs <= 0

        # Fetch spot price for display / trade context. Do not hard-depend on
        # Binance here — on some VPS regions Binance is unavailable.
        spot, spot_source = self._resolve_current_spot()

        # Capture barrier from calc via the calibration broker. Calc is the
        # sole owner of the on-chain Chainlink WebSocket; live/grid trust
        # calc's captured barrier (and the source string) and run an
        # independent Binance-only sanity check before treating the barrier
        # as confirmed for entries.
        #
        # Source semantics (drives `barrier_confirmed = source in
        # ("chainlink", "polymarket")`):
        #   - "polymarket" — calc captured Polymarket's openPrice (PTB).
        #                    Primary source post-091be1b; matches what
        #                    Polymarket displays as the "Price to Beat".
        #                    Sanity check applies → entries OK.
        #   - "chainlink"  — calc captured a Chainlink point at-or-after
        #                    window-start as fallback when Polymarket was
        #                    unavailable. Sanity check applies → entries
        #                    OK.
        #   - "binance"    — calc fell back to Binance kline → entries blocked
        #   - "spot_fallback" — calc's last-resort spot → entries blocked
        #   - "binance_local" — calc has not yet emitted; tentative Binance
        #                    kline so calibration/spot UI still populate.
        #                    Entries blocked until calc's first event lands.
        #   - "binance_sanity_failed" — calc said chainlink/polymarket but
        #                    the local Binance check disagrees by more than
        #                    tolerance. Entries blocked.
        if start_utc and now >= start_utc:
            captured = None
            capture_market_id = self._current_market_id
            if self._hybrid_broker is not None and capture_market_id:
                captured = self._hybrid_broker.latest_barrier_for_market(
                    capture_market_id
                )

            if captured is not None:
                cb_price, cb_source, cb_offset = captured
                sanity_ok = True
                if (
                    cb_source in ("chainlink", "polymarket")
                    and getattr(self.config, "binance_sanity_check_enabled", True)
                ):
                    sanity_ok = self._binance_sanity_passes(cb_price, start_utc)

                if sanity_ok:
                    if (
                        not self._barrier_captured
                        or self._barrier != cb_price
                        or self._barrier_source != cb_source
                    ):
                        offset_str = (
                            f" (offset {cb_offset:.1f}s)" if cb_offset is not None else ""
                        )
                        log.info(
                            f"Barrier from calc [{cb_source}] at "
                            f"{start_utc.isoformat()}: ${cb_price:,.2f}{offset_str}"
                        )
                    self._barrier = cb_price
                    self._barrier_source = cb_source
                    self._barrier_offset_s = cb_offset
                    self._barrier_captured = True
                    if self._current_market_id == capture_market_id:
                        self._barrier_finalized = True
                else:
                    # Sanity failed — keep the price for diagnostics but mark
                    # the source so `barrier_confirmed` returns False.
                    self._barrier = cb_price
                    self._barrier_source = "binance_sanity_failed"
                    self._barrier_offset_s = None
                    self._barrier_captured = True
                    if self._current_market_id == capture_market_id:
                        self._barrier_finalized = True
            elif not self._barrier_captured:
                # Calc has not published an event for this market yet (early
                # in the window, calc starting up, or calc is unhealthy).
                # Use a tentative Binance kline so the rest of the tick
                # (spot UI, calibration request, order book) still populates.
                # `barrier_confirmed` stays False — entries are blocked
                # until calc's first event arrives.
                try:
                    kline = get_binance_price_at(start_utc)
                    if kline and _is_sane_btc_price(kline):
                        self._barrier = float(kline)
                        self._barrier_source = "binance_local"
                        self._barrier_offset_s = None
                        self._barrier_captured = True
                        self._barrier_finalized = False
                        log.info(
                            f"Barrier [binance_local, awaiting calc] at "
                            f"{start_utc.isoformat()}: ${kline:,.2f}"
                        )
                except Exception as e:
                    log.warning(f"Binance local kline failed: {e}")

        barrier = self._barrier or spot
        if (not barrier or barrier <= 0) and self._current_market_id:
            fallback_barrier = self._fallback_live_reference_price()
            if fallback_barrier and fallback_barrier > 0:
                barrier = fallback_barrier

        # Handle expiry tick — use Chainlink for final price (same as resolution source)

        # Run full SSVI Surface + MC calibration (same as daily trader)
        hours_remaining = max(0.0, remaining_secs / 3600)
        market_end_iso = end_utc.isoformat() if end_utc else None

        prob_above = 0.5
        prob_below = 0.5
        model_used = ""
        calib_secs = 0.0
        prob_source = ""
        latest_event = None

        if self._hybrid_broker is not None:
            self._hybrid_broker.poll_inbox()
            if self._current_market_id and market_end_iso:
                self._hybrid_broker.update_market_context(
                    MarketContext(
                        market_id=self._current_market_id,
                        barrier=barrier,
                        market_end_time_iso=market_end_iso,
                    )
                )

        if not is_expired and barrier > 0 and hours_remaining > 0:
            if self._hybrid_broker is not None and self._current_market_id:
                # TTL=60s on the broker's received-at timestamp. When both
                # producers stop delivering, we fall back to the default 0.5/0.5
                # prior rather than acting on an increasingly stale event.
                latest = self._hybrid_broker.latest_for_market(
                    self._current_market_id, max_age_s=60.0
                )
                if latest is not None:
                    latest_event = latest
                    prob_above = latest.prob_above
                    prob_below = latest.prob_below
                    model_used = latest.model_used
                    calib_secs = latest.calibration_seconds
                    prob_source = "event_scalar"
                    if latest.up_best_ask is not None and latest.up_best_ask > 0:
                        self._canonical_up_ask = latest.up_best_ask
                    if latest.down_best_ask is not None and latest.down_best_ask > 0:
                        self._canonical_down_ask = latest.down_best_ask
                    self._latest_entry_signals = latest.entry_signals
                    self._latest_entry_signals_ts = latest.ready_at
                    self._latest_entry_signals_market_id = latest.market_id
                    self._latest_entry_signals_stale_age_s = -1.0
                else:
                    stale_age_s = -1.0
                    if hasattr(self._hybrid_broker, "age_for_market"):
                        try:
                            age = self._hybrid_broker.age_for_market(self._current_market_id)
                            stale_age_s = float(age) if age is not None else -1.0
                        except Exception:
                            stale_age_s = -1.0
                    self._latest_entry_signals = ()
                    self._latest_entry_signals_ts = None
                    self._latest_entry_signals_market_id = ""
                    self._latest_entry_signals_stale_age_s = stale_age_s
                    log.warning(
                        "no fresh calibration for %s within 60s; clearing stale signals, holding uninformative 0.5/0.5 prior",
                        self._current_market_id,
                    )
            else:
                pa, pb, mu, cs = self._run_calibration(barrier, hours_remaining, market_end_iso)
                if pa is not None:
                    if datetime.now(timezone.utc) >= end_utc:
                        log.warning(
                            "discarding calibration for %s: finished after market end %s",
                            self._current_market_id,
                            end_utc.isoformat(),
                        )
                    else:
                        prob_above = pa
                        prob_below = pb
                        model_used = mu
                        calib_secs = cs

        # Calibration can take several seconds; refresh from the shared spot
        # feed after consuming probabilities so TickData carries a current
        # propagated spot, not the older calibration event's sampled spot.
        refreshed_spot, refreshed_spot_source = self._resolve_current_spot()
        if refreshed_spot > 0:
            spot = refreshed_spot
            spot_source = refreshed_spot_source

        if spot_source != self._last_reported_spot_source:
            if spot_source:
                log.info(f"Current spot [{spot_source}]: ${spot:,.2f}")
            else:
                log.warning("Current spot unavailable from all configured sources")
            self._last_reported_spot_source = spot_source

        # Recompute timing AFTER calibration (may have taken 5-60s)
        now_post = datetime.now(timezone.utc)
        remaining_secs = max(0.0, (end_utc - now_post).total_seconds())
        elapsed_secs = 0.0
        window_duration = 900.0
        if start_utc:
            elapsed_secs = max(0.0, (now_post - start_utc).total_seconds())
            window_duration = (end_utc - start_utc).total_seconds()

        # Re-check expiry after calibration and wait briefly for a cutoff-aligned print.
        is_expired = remaining_secs <= 0
        final_spot = None
        final_spot_source = ""
        settlement_pending = False
        settlement_payouts = None
        settlement_source = ""

        if is_expired and not self._expiry_reported and contract_raw is not None:
            settlement_payouts, settlement_source = self._resolve_settlement_payouts(contract)
            if settlement_payouts is None:
                settlement_pending = True
                is_expired = False
            # Report pending once, then permit active-market discovery on
            # the next tick. Grid/live retain and settle old exposure by ID.
            self._expiry_reported = True
            self._expiry_wait_started_at = None
        elif is_expired and not self._expiry_reported:
            if self._expiry_wait_started_at is None:
                self._expiry_wait_started_at = now_post

            final_spot, final_spot_source = self._resolve_final_spot(
                end_utc, spot, allow_degraded=False
            )
            waited_secs = (now_post - self._expiry_wait_started_at).total_seconds()
            settlement_abandoned = final_spot_source == "settlement_abandoned"

            if (
                final_spot is None
                and waited_secs < self.config.settlement_wait_seconds
                and not settlement_abandoned
            ):
                settlement_pending = True
                is_expired = False
            else:
                if final_spot is None:
                    final_spot, final_spot_source = self._resolve_final_spot(
                        end_utc, spot, allow_degraded=True
                    )

                if final_spot is not None:
                    if final_spot_source == "polymarket_settlement":
                        log.info(f"Final spot from Polymarket settlement: ${final_spot:,.2f}")
                    elif final_spot_source == "chainlink_settlement":
                        log.info(f"Final spot from Chainlink settlement: ${final_spot:,.2f}")
                    elif final_spot_source == "chainlink_end":
                        log.info(f"Final spot from Chainlink@cutoff: ${final_spot:,.2f}")
                    elif final_spot_source == "binance_end":
                        log.warning(f"Final spot from Binance@cutoff fallback: ${final_spot:,.2f}")
                    else:
                        log.warning(f"Final spot from degraded fallback: ${final_spot:,.2f}")
                    self._expiry_reported = True
                    self._expiry_wait_started_at = None
                elif settlement_abandoned:
                    log.warning(
                        "Settlement abandoned for %s; advancing to next market",
                        self._current_market_id,
                    )
                    settlement_pending = True
                    is_expired = False
                    self._expiry_reported = True
                    self._expiry_wait_started_at = None
                else:
                    settlement_pending = True
                    is_expired = False
        elif not is_expired:
            self._expiry_wait_started_at = None

        # Fetch order books (shared across all instances for VWAP)
        up_snap = None
        down_snap = None
        up_snap_delayed = None
        down_snap_delayed = None
        poly_up = 0.50
        poly_down = 0.50
        if not settlement_pending and not is_expired:
            (
                up_snap,
                down_snap,
                up_snap_delayed,
                down_snap_delayed,
                poly_up,
                poly_down,
            ) = self._fetch_order_books()
            if poly_up is None:
                poly_up = 0.50
            if poly_down is None:
                poly_down = 0.50

        token_ids = mkt.get("token_ids", {})

        # Books may have taken a second or more. Consume the newest producer
        # result after them; stale terminal marginals must never price TWAP.
        probability_evaluated_at = None
        probability_spot_observed_at = None
        twap_signals = None
        if contract_raw is not None and not is_expired and not settlement_pending:
            if self._hybrid_broker is not None:
                self._hybrid_broker.poll_inbox()  # include arrivals during delayed-book work even without push IPC
            fresh_event = self._hybrid_broker.latest_for_market(self._current_market_id, max_age_s=2.0) if self._hybrid_broker is not None else None
            latest_event = fresh_event
            prob_source = ""
            self._latest_entry_signals = ()
            if (fresh_event is not None and getattr(fresh_event, "schema_version", 1) >= 5
                    and fresh_event.payoff_kind == contract.payoff_kind):
                try:
                    probability_evaluated_at = datetime.fromisoformat(fresh_event.probability_evaluated_at_iso)
                    probability_spot_observed_at = datetime.fromisoformat(fresh_event.probability_spot_observed_at_iso)
                except (TypeError, ValueError):
                    pass
                prob_above, prob_below = fresh_event.prob_above, fresh_event.prob_below
                if fresh_event.spot_price is not None and fresh_event.spot_price > 0:
                    spot = fresh_event.spot_price
                    spot_source = fresh_event.spot_source
                model_used = fresh_event.model_used
                prob_source = "twap_mc_scalar" if contract.payoff_kind == TWAP_PAYOFF else "event_scalar"
                from scripts.trader.local_eval import compute_entry_signals
                twap_signals = compute_entry_signals(
                    prob_above, prob_below,
                    up_snap.best_ask if up_snap is not None else 0.0,
                    down_snap.best_ask if down_snap is not None else 0.0,
                    self._local_signal_params, self._current_market_id or "",
                )

        # ------------------------------------------------------------------
        # Local probability re-evaluation (schema v4, flag-gated). Re-threshold
        # the event's MC terminal distribution at the freshest Redis spot so
        # the decision probability tracks spot in ~real time instead of being
        # frozen at the event's publish-time spot. Falls back to the scalar on
        # any missing input (legacy events, stale spot, stale event).
        # ------------------------------------------------------------------
        prob_event_age_s = -1.0
        if self._hybrid_broker is not None and self._current_market_id:
            try:
                _age = self._hybrid_broker.age_for_market(self._current_market_id)
                prob_event_age_s = float(_age) if _age is not None else -1.0
            except Exception:  # noqa: BLE001
                prob_event_age_s = -1.0

        prob_spot_age_s = -1.0
        local_signals: Optional[tuple] = None
        if (
            self._local_eval_enabled
            and contract_raw is None
            and latest_event is not None
            and not is_expired
            and not settlement_pending
            and 0.0 <= prob_event_age_s <= self._local_eval_event_max_age_s
        ):
            spot_tick_for_eval = (
                self._spot_consumer.latest(max_age_s=self._local_eval_spot_max_age_s)
                if self._spot_consumer is not None
                else None
            )
            if spot_tick_for_eval is not None:
                from scripts.trader.local_eval import (
                    compute_entry_signals as _compute_local_signals,
                    evaluate_prob_from_event,
                )

                try:
                    prob_spot_age_s = float(spot_tick_for_eval.age_seconds())
                except Exception:  # noqa: BLE001
                    prob_spot_age_s = -1.0
                local = evaluate_prob_from_event(
                    latest_event,
                    spot=spot_tick_for_eval.price,
                    now=now_post,
                )
                if local is not None:
                    scalar_above = prob_above
                    prob_above = local.prob_above
                    prob_below = local.prob_below
                    prob_source = "local_mc_cdf"
                    model_used = f"{model_used}+local" if model_used else "local"
                    spot = local.spot_used
                    # Local entry signals against THIS tick's executable asks
                    # (what an order would actually pay), falling back to the
                    # event's canonical asks when a book is missing.
                    up_ask = (
                        up_snap.best_ask
                        if (up_snap is not None and up_snap.best_ask > 0)
                        else (self._canonical_up_ask or 0.0)
                    )
                    down_ask = (
                        down_snap.best_ask
                        if (down_snap is not None and down_snap.best_ask > 0)
                        else (self._canonical_down_ask or 0.0)
                    )
                    if self._local_signal_params:
                        local_signals = _compute_local_signals(
                            prob_above,
                            prob_below,
                            float(up_ask or 0.0),
                            float(down_ask or 0.0),
                            self._local_signal_params,
                            self._current_market_id or "",
                        )
                    _now_mono = time.monotonic()
                    if _now_mono - self._local_eval_log_at >= 30.0:
                        log.info(
                            "local eval: p_up=%.4f (event %.4f, d=%+.4f) "
                            "spot_age=%.2fs event_age=%.1fs ttm_scale=%.4f signals=%s",
                            prob_above,
                            scalar_above,
                            prob_above - scalar_above,
                            prob_spot_age_s,
                            prob_event_age_s,
                            local.ttm_scale,
                            len(local_signals) if local_signals is not None else "event",
                        )
                        self._local_eval_log_at = _now_mono

        # New market detection
        is_new = self._current_market_id != self._last_tick_market_id
        self._last_tick_market_id = self._current_market_id

        # Build the per-tick signal index once (first-wins, matching the
        # historical first-match scan semantics) instead of every instance
        # scanning the full signal tuple.
        tick_signals = (
            twap_signals if twap_signals is not None else (
                local_signals if local_signals is not None else self._latest_entry_signals
            )
        )
        signal_index: dict = {}
        for sig in tick_signals or ():
            key = str(sig.get("instance_id"))
            if key not in signal_index:
                signal_index[key] = sig

        tick_time = datetime.now(timezone.utc) if contract_raw is not None else now_post
        if probability_spot_observed_at is not None and probability_spot_observed_at.tzinfo is not None:
            prob_spot_age_s = (tick_time - probability_spot_observed_at).total_seconds()
        if contract_raw is not None:
            remaining_secs = max(0.0, (end_utc - tick_time).total_seconds())
            if start_utc is not None:
                elapsed_secs = max(0.0, (tick_time - start_utc).total_seconds())
            if remaining_secs <= 0 and not self._expiry_reported:
                # Delayed books can cross the boundary after the earlier
                # expiry check. Never send that frame through ordinary exits.
                settlement_payouts, settlement_source = self._resolve_settlement_payouts(contract)
                settlement_pending = settlement_payouts is None
                is_expired = settlement_payouts is not None
                self._expiry_reported = True
                self._last_market_scan = 0.0
                tick_signals, signal_index = (), {}
        if self._expiry_reported:
            # Expiry may have been crossed inside calibration/spot I/O, after
            # the initial clock check. The next scan must not wait another30s.
            self._last_market_scan = 0.0
        tick_result = TickData(
            timestamp=tick_time,
            market_id=self._current_market_id or "",
            market_title=mkt.get("title", ""),
            barrier=barrier,
            spot_price=spot,
            time_remaining_seconds=remaining_secs,
            elapsed_seconds=elapsed_secs,
            window_duration_seconds=window_duration,
            prob_above=prob_above,
            prob_below=prob_below,
            poly_prob_up=poly_up,
            poly_prob_down=poly_down,
            up_order_book=up_snap,
            down_order_book=down_snap,
            up_order_book_delayed=up_snap_delayed,
            down_order_book_delayed=down_snap_delayed,
            up_token_id=token_ids.get("UP", ""),
            down_token_id=token_ids.get("DOWN", ""),
            model_used=model_used,
            calibration_seconds=calib_secs,
            market_end_time=end_utc,
            barrier_source=self._barrier_source,
            barrier_confirmed=self._barrier_source in ("chainlink", "polymarket"),
            is_new_market=is_new,
            is_expired=is_expired,
            settlement_pending=settlement_pending,
            final_spot=final_spot,
            final_spot_source=final_spot_source,
            spot_source=spot_source,
            canonical_up_ask=self._canonical_up_ask,
            canonical_down_ask=self._canonical_down_ask,
            entry_signals=tick_signals,
            entry_signal_age_s=(
                max(0.0, (tick_time - probability_evaluated_at).total_seconds())
                if twap_signals is not None and probability_evaluated_at is not None
                else 0.0
                if local_signals is not None
                else (
                    (datetime.now(timezone.utc) - self._latest_entry_signals_ts).total_seconds()
                    if self._latest_entry_signals_ts is not None
                    else -1.0
                )
            ),
            entry_signal_market_id=(
                (self._current_market_id or "")
                if local_signals is not None or twap_signals is not None
                else self._latest_entry_signals_market_id
            ),
            entry_signal_stale_age_s=self._latest_entry_signals_stale_age_s,
            entry_signal_index=signal_index,
            prob_source=prob_source,
            prob_spot_age_s=prob_spot_age_s,
            prob_event_age_s=prob_event_age_s,
            payoff_kind=contract.payoff_kind if contract_raw is not None else "legacy",
            contract_supported=contract.supported if contract_raw is not None else True,
            contract_reason=contract.reason if contract_raw is not None else "",
            condition_id=contract.condition_id,
            averaging_window_seconds=contract.averaging_window_seconds,
            fee_schedule=contract.fee_schedule,
            contract_metadata=contract_raw,
            probability_evaluated_at=probability_evaluated_at,
            probability_spot_observed_at=probability_spot_observed_at,
            settlement_payouts=settlement_payouts,
            settlement_source=settlement_source,
        )
        if contract_raw is not None:
            valid, _reason = entry_inputs_valid(tick_result, now=tick_result.timestamp)
            if (elapsed_secs >= self.config.no_trade_first_seconds
                    and remaining_secs > self.config.no_trade_last_seconds):
                reason = "valid" if valid else _reason
                self._freshness_counts[reason] = self._freshness_counts.get(reason, 0) + 1
                if time.monotonic() - self._freshness_logged_at >= 60.0:
                    log.info("entry input freshness: counts=%s consumer_spot_age_s=%.3f",
                             self._freshness_counts, prob_spot_age_s)
                    self._freshness_counts.clear()
                    self._freshness_logged_at = time.monotonic()
            if not valid:
                tick_result = replace(tick_result, entry_signals=(), entry_signal_index={})
        return tick_result

    def next_sleep(self) -> float:
        """Pace active work normally and avoid spinning on expired/discovery waits."""
        if self._current_market and not self._expiry_reported:
            end = self._current_market.get("end_date_utc")
            if end is not None and datetime.now(timezone.utc) >= end:
                return 2.0
            return self.config.tick_interval_seconds  # 0 = sequential, no delay
        return 2.0

    def shutdown(self) -> None:
        if self._hybrid_broker is not None:
            self._hybrid_broker.stop()
        if self._spot_consumer is not None:
            self._spot_consumer.close()
        self._book_pool.shutdown(wait=False)
