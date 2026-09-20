"""Polymarket maintenance/outage detector for the trader.

Polls the Instatus public summary feed (https://status.polymarket.com/summary.json)
and caches the result for `CACHE_TTL_SECONDS`. During a broad CLOB maintenance
flag, it can allow entries again only after repeated current-market CLOB health
checks confirm books and order acceptance are usable. The trader can call
`is_polymarket_operational()` from any entry-decision path with negligible cost
— a network call is only issued when the cache expires. Unknown cold-start
status fails closed; after at least one successful refresh, a transient fetch
failure preserves the last known value.

This is intentionally a tiny standalone module (no httpx, no asyncio, no
threads) so it can be imported anywhere — grid tick paths, single-trader,
backtests — without disturbing the surrounding code.
"""
from __future__ import annotations

import json
import logging
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

import requests

log = logging.getLogger(__name__)

STATUS_URL = "https://status.polymarket.com/summary.json"
COMPONENTS_URL = "https://status.polymarket.com/v3/components.json"
GAMMA_MARKETS_URL = "https://gamma-api.polymarket.com/markets"
CLOB_BOOK_URL = "https://clob.polymarket.com/book"
CLOB_OK_URL = "https://clob.polymarket.com/ok"
CACHE_TTL_SECONDS = 60.0
CLOB_READS_TTL_SECONDS = 8.0
REQUEST_TIMEOUT_SECONDS = 5.0
EMPIRICAL_CLOB_OK_POLLS = 2
COMPLETED_MAINTENANCE_STATUSES = {"COMPLETED", "RESOLVED"}
SCHEDULED_MAINTENANCE_STATUSES = {"NOTSTARTEDYET", "SCHEDULED", "UPCOMING"}
UNKNOWN_STATUS_LABEL = "status unknown"
FETCH_FAILED_STATUS_LABEL = "status fetch failed"

_cached_is_up: bool = False
_cached_at: float = 0.0
_cached_label: Optional[str] = UNKNOWN_STATUS_LABEL
_has_successful_refresh: bool = False
_consecutive_failures: int = 0
_consecutive_empirical_ok: int = 0
_last_empirical_slug: Optional[str] = None


@dataclass(frozen=True)
class ClobHealth:
    healthy: bool
    reason: str
    slug: Optional[str] = None
    accepting_orders: Optional[bool] = None
    enable_order_book: Optional[bool] = None


def _parse_start_at(value: object) -> Optional[datetime]:
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _maintenance_is_active(item: dict, now: datetime) -> bool:
    status = str(item.get("status") or "").upper()
    if status in COMPLETED_MAINTENANCE_STATUSES:
        return False
    if status in SCHEDULED_MAINTENANCE_STATUSES:
        return False
    start_at = _parse_start_at(item.get("start"))
    if start_at is not None and start_at > now:
        return False
    return True


def _active_maintenance_name(data: dict) -> Optional[str]:
    active_list = data.get("activeMaintenances")
    if not isinstance(active_list, list):
        return None
    now = datetime.now(timezone.utc)
    for item in active_list:
        if not isinstance(item, dict):
            continue
        if not _maintenance_is_active(item, now):
            continue
        name = item.get("name")
        if isinstance(name, str) and name:
            return name
        return "active maintenance"
    return None


def _bool_or_none(value: object) -> Optional[bool]:
    if isinstance(value, bool):
        return value
    return None


def _component_status(data: Optional[dict], component_name: str) -> Optional[str]:
    components = data.get("components") if isinstance(data, dict) else None
    if not isinstance(components, list):
        return None
    wanted = component_name.casefold()
    for item in components:
        if not isinstance(item, dict):
            continue
        name = item.get("name")
        if isinstance(name, str) and name.casefold() == wanted:
            status = item.get("status")
            return str(status) if isinstance(status, str) else None
    return None


def _current_15m_slug() -> str:
    epoch = int(time.time() // 900) * 900
    return f"btc-updown-15m-{epoch}"


def _json_list_or_value(value: object) -> object:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except ValueError:
            return value
    return value


def _best_bid_ask(book: dict) -> tuple[Optional[float], Optional[float]]:
    bids = []
    asks = []
    for bid in book.get("bids", []):
        try:
            price = float(bid.get("price", 0))
            size = float(bid.get("size", 0))
        except (AttributeError, TypeError, ValueError):
            continue
        if price > 0 and size > 0:
            bids.append(price)
    for ask in book.get("asks", []):
        try:
            price = float(ask.get("price", 0))
            size = float(ask.get("size", 0))
        except (AttributeError, TypeError, ValueError):
            continue
        if price > 0 and size > 0:
            asks.append(price)
    return (max(bids) if bids else None, min(asks) if asks else None)


def _has_usable_mid(book: dict) -> bool:
    bid, ask = _best_bid_ask(book)
    return bid is not None and ask is not None and bid > 0 and ask > 0


def _extract_tokens(market: dict) -> tuple[Optional[str], Optional[str]]:
    tokens = _json_list_or_value(market.get("clobTokenIds"))
    outcomes = _json_list_or_value(market.get("outcomes"))
    if not isinstance(tokens, list) or len(tokens) < 2:
        return None, None
    up_token: Optional[str] = None
    down_token: Optional[str] = None
    if isinstance(outcomes, list) and len(outcomes) == len(tokens):
        for outcome, token in zip(outcomes, tokens):
            if isinstance(outcome, str) and outcome.lower().startswith("up"):
                up_token = str(token)
            elif isinstance(outcome, str) and outcome.lower().startswith("down"):
                down_token = str(token)
    return up_token or str(tokens[0]), down_token or str(tokens[1])


def _empirical_clob_health() -> ClobHealth:
    slug = _current_15m_slug()
    try:
        r = requests.get(
            GAMMA_MARKETS_URL,
            params={"slug": slug},
            timeout=REQUEST_TIMEOUT_SECONDS,
            headers={"User-Agent": "btc-pricer-trader/0.1"},
        )
        r.raise_for_status()
        data = r.json()
    except (requests.RequestException, ValueError) as exc:
        return ClobHealth(False, f"gamma lookup failed: {exc}", slug=slug)

    market = None
    if isinstance(data, list) and data:
        market = data[0]
    elif isinstance(data, dict):
        market = data
    if not isinstance(market, dict):
        return ClobHealth(False, "gamma market not found", slug=slug)

    accepting_orders = _bool_or_none(market.get("acceptingOrders"))
    enable_order_book = _bool_or_none(market.get("enableOrderBook"))
    if accepting_orders is not True:
        return ClobHealth(False, "market is not accepting orders", slug, accepting_orders, enable_order_book)
    if enable_order_book is not True:
        return ClobHealth(False, "order book disabled for market", slug, accepting_orders, enable_order_book)

    up_token, down_token = _extract_tokens(market)
    if not up_token or not down_token:
        return ClobHealth(False, "CLOB token ids unavailable", slug, accepting_orders, enable_order_book)

    books: list[dict] = []
    for token in (up_token, down_token):
        try:
            br = requests.get(
                CLOB_BOOK_URL,
                params={"token_id": token},
                timeout=REQUEST_TIMEOUT_SECONDS,
                headers={"User-Agent": "btc-pricer-trader/0.1"},
            )
            br.raise_for_status()
            book = br.json()
        except (requests.RequestException, ValueError) as exc:
            return ClobHealth(False, f"CLOB book unavailable: {exc}", slug, accepting_orders, enable_order_book)
        if not isinstance(book, dict):
            return ClobHealth(False, "CLOB book payload invalid", slug, accepting_orders, enable_order_book)
        books.append(book)

    if not any(_has_usable_mid(book) for book in books):
        return ClobHealth(False, "CLOB books are empty", slug, accepting_orders, enable_order_book)
    return ClobHealth(True, "live CLOB books reachable", slug, accepting_orders, enable_order_book)


def _mark_refresh_failed(reason: object) -> None:
    global _cached_is_up, _cached_at, _cached_label, _consecutive_failures
    _consecutive_failures += 1
    if _consecutive_failures <= 2:
        log.warning("polymarket status fetch failed: %s", reason)
    # Unknown status must not authorize entries. Once a status has been
    # confirmed, preserve that last known state across transient misses.
    if not _has_successful_refresh:
        _cached_is_up = False
        _cached_label = FETCH_FAILED_STATUS_LABEL
    _cached_at = time.time()


def _refresh() -> None:
    global _cached_is_up, _cached_at, _cached_label, _consecutive_failures
    global _consecutive_empirical_ok, _has_successful_refresh, _last_empirical_slug
    try:
        r = requests.get(
            STATUS_URL,
            timeout=REQUEST_TIMEOUT_SECONDS,
            headers={"User-Agent": "btc-pricer-trader/0.1"},
        )
        r.raise_for_status()
        data = r.json()
    except (requests.RequestException, ValueError) as e:
        _mark_refresh_failed(e)
        return

    if not isinstance(data, dict):
        _mark_refresh_failed("status payload is not an object")
        return
    page = data.get("page")
    if not isinstance(page, dict):
        _mark_refresh_failed("status payload missing page")
        return
    raw_page_status = page.get("status")
    if not isinstance(raw_page_status, str) or not raw_page_status.strip():
        _mark_refresh_failed("status payload missing page.status")
        return
    page_status = raw_page_status.strip()
    _consecutive_failures = 0

    active_name = _active_maintenance_name(data)

    components_data: Optional[dict] = None
    try:
        cr = requests.get(
            COMPONENTS_URL,
            timeout=REQUEST_TIMEOUT_SECONDS,
            headers={"User-Agent": "btc-pricer-trader/0.1"},
        )
        cr.raise_for_status()
        raw_components = cr.json()
        if isinstance(raw_components, dict):
            components_data = raw_components
    except (requests.RequestException, ValueError) as exc:
        log.debug("polymarket components fetch failed: %s", exc)

    clob_status = _component_status(components_data, "CLOB API")
    page_upper = page_status.upper()
    clob_upper = clob_status.upper() if isinstance(clob_status, str) else None
    maintenance_flag = (
        active_name is not None
        or page_upper == "UNDERMAINTENANCE"
        or clob_upper == "UNDERMAINTENANCE"
    )

    clob_component_ok = clob_upper in (None, "OPERATIONAL")
    degraded_flag = maintenance_flag or page_upper != "UP" or not clob_component_ok

    health: Optional[ClobHealth] = None
    if degraded_flag:
        health = _empirical_clob_health()
        if health.slug != _last_empirical_slug:
            _consecutive_empirical_ok = 0
            _last_empirical_slug = health.slug
        if health.healthy:
            _consecutive_empirical_ok += 1
        else:
            _consecutive_empirical_ok = 0
    else:
        _consecutive_empirical_ok = 0
        _last_empirical_slug = None

    empirical_ready = _consecutive_empirical_ok >= EMPIRICAL_CLOB_OK_POLLS

    if maintenance_flag and empirical_ready:
        is_up = True
        label = "CLOB maintenance; empirical health OK"
    elif maintenance_flag:
        is_up = False
        label = active_name or clob_status or page_status
    elif page_upper != "UP" and clob_component_ok and empirical_ready:
        # Page-level incident (e.g. website chart glitch) with the CLOB
        # component fine and live books reachable: keep trading.
        is_up = True
        label = f"page {page_status}; CLOB books reachable"
    elif page_upper != "UP":
        is_up = False
        label = page_status
    elif not clob_component_ok:
        # CLOB component explicitly non-operational is never overridden.
        is_up = False
        label = clob_status
    else:
        is_up = True
        label = None

    if is_up != _cached_is_up:
        log.warning(
            "polymarket status changed: operational=%s page=%s clob=%s "
            "maintenance=%s empirical=%s/%s reason=%s",
            is_up,
            page_status,
            clob_status,
            active_name,
            health.healthy if health is not None else None,
            _consecutive_empirical_ok,
            health.reason if health is not None else None,
        )
    _cached_is_up = is_up
    _cached_label = label
    _cached_at = time.time()
    _has_successful_refresh = True


def is_polymarket_operational(force_refresh: bool = False) -> bool:
    """True if Polymarket trading is operational (no maintenance/outage).

    Cached for `CACHE_TTL_SECONDS` to keep tick-path overhead negligible.
    Defaults to non-operational until the first successful refresh confirms
    the status endpoint is reachable and reports UP.
    """
    if force_refresh or _cached_at == 0.0 or (time.time() - _cached_at) >= CACHE_TTL_SECONDS:
        _refresh()
    return _cached_is_up


def polymarket_status_label() -> Optional[str]:
    """Human-readable label for the current non-operational state, or None."""
    return _cached_label


# ---------------------------------------------------------------------------
# Always-on read-only CLOB taker-readiness probe.
#
# is_polymarket_operational() above only runs the empirical CLOB probe when the
# Instatus status page declares maintenance. That misses *undeclared* order-book
# states (a market flipped to not-accepting-orders, books vanished, or the CLOB
# /ok endpoint reporting not-ready). clob_reads_healthy() runs the empirical
# probe on every call (short TTL cache) so those are caught before an order is
# ever sent. It is read-only — GET /ok, Gamma market flags, and /book — so it
# risks no capital and needs no auth.
#
# IMPORTANT: it blocks entry ONLY on *affirmative* "exchange is refusing orders"
# signals. A probe transport failure (Gamma/CLOB unreachable) does NOT block —
# that would let a blip in our own connectivity halt all trading. Those cases
# are covered by the reactive clob_health flag (written when a real order POST
# fails) and by is_polymarket_operational()'s status-page check.
# ---------------------------------------------------------------------------

_reads_cached_until: float = 0.0
_reads_cached_healthy: bool = True
_reads_cached_label: Optional[str] = None


def _probe_clob_ok() -> Optional[bool]:
    """GET /ok. False = server affirmatively not ready (425/503/5xx);
    True = reachable; None = probe transport failure (do not block on this)."""
    try:
        r = requests.get(
            CLOB_OK_URL,
            timeout=REQUEST_TIMEOUT_SECONDS,
            headers={"User-Agent": "btc-pricer-trader/0.1"},
        )
    except requests.RequestException:
        return None
    if r.status_code in (425, 503) or r.status_code >= 500:
        return False
    return True


def clob_reads_healthy(
    ttl: float = CLOB_READS_TTL_SECONDS,
    force_refresh: bool = False,
) -> tuple[bool, Optional[str]]:
    """Read-only CLOB taker-readiness check (no orders, no auth, no capital risk).

    Returns ``(healthy, label)`` where ``label`` describes the block reason when
    unhealthy. Cached for ``ttl`` seconds so a hot entry path triggers at most one
    network refresh per TTL. Blocks only on affirmative not-accepting signals;
    fails open on probe transport errors (see module note above).

    ``force_refresh`` re-probes before the TTL lapses — used by the background
    refresher so tick-path callers always land inside a warm cache.
    """
    global _reads_cached_until, _reads_cached_healthy, _reads_cached_label
    now = time.time()
    if not force_refresh and now < _reads_cached_until:
        return _reads_cached_healthy, _reads_cached_label

    healthy = True
    label: Optional[str] = None

    if _probe_clob_ok() is False:
        healthy, label = False, "CLOB /ok not ready"
    else:
        health = _empirical_clob_health()
        if health.accepting_orders is False:
            healthy, label = False, "market not accepting orders"
        elif health.enable_order_book is False:
            healthy, label = False, "order book disabled for market"
        elif health.reason == "CLOB books are empty":
            healthy, label = False, "CLOB books are empty"

    _reads_cached_healthy = healthy
    _reads_cached_label = label
    _reads_cached_until = now + max(0.0, float(ttl or 0.0))
    return healthy, label


def reset_reads_cache() -> None:
    """Clear the clob_reads_healthy() cache (test helper)."""
    global _reads_cached_until, _reads_cached_healthy, _reads_cached_label
    _reads_cached_until = 0.0
    _reads_cached_healthy = True
    _reads_cached_label = None


# ---------------------------------------------------------------------------
# Background cache pre-warming. Both probes above are cached, but the FIRST
# caller after a TTL lapse pays the HTTP refresh inline — in the grid that
# was a 0.3-1s stall injected between instance evaluations every <=8s. The
# refresher thread keeps both caches warm from the outside so tick-path
# callers always hit the cache; if the thread ever dies, the getters simply
# resume their historical inline-refresh behavior (fail-open).
# ---------------------------------------------------------------------------

_background_thread: Optional[threading.Thread] = None
_background_lock = threading.Lock()


def start_background_refresh(interval_s: float = 3.0) -> None:
    """Start the daemon thread that pre-warms both probe caches. Idempotent."""
    global _background_thread
    with _background_lock:
        if _background_thread is not None and _background_thread.is_alive():
            return

        def _loop() -> None:
            while True:
                try:
                    # Refresh the reads probe just before its TTL lapses so no
                    # tick-path caller ever lands on an expired cache.
                    if time.time() >= _reads_cached_until - 1.5 * interval_s:
                        clob_reads_healthy(force_refresh=True)
                except Exception:  # noqa: BLE001
                    pass
                try:
                    if (
                        _cached_at == 0.0
                        or time.time() - _cached_at >= CACHE_TTL_SECONDS * 0.5
                    ):
                        is_polymarket_operational(force_refresh=True)
                except Exception:  # noqa: BLE001
                    pass
                time.sleep(interval_s)

        _background_thread = threading.Thread(
            target=_loop,
            name="polymarket-status-refresh",
            daemon=True,
        )
        _background_thread.start()
