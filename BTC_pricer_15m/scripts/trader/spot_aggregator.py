"""RTDS-first BTC spot provider with multi-exchange WebSocket fallback.

Primary source is Polymarket's public RTDS Chainlink BTC/USD stream. If that
stream is stale, this module falls back to a median of top-of-book bid/ask mids
from multiple exchange WebSockets. Binance REST is retained as a final last
resort for uptime.
"""

from __future__ import annotations

import asyncio
import json
import logging
import statistics
import threading
import time
from abc import abstractmethod
from collections import deque
from datetime import datetime
from typing import Callable, Optional

from btc_pricer.utils.price_sanity import is_sane_btc_price

log = logging.getLogger(__name__)

MAX_RECONNECT_DELAY = 30
BUFFER_SECONDS = 30
HEALTH_WINDOW_S = 10.0
RTDS_STALE_AFTER_S = 10.0
WS_OUTLIER_BPS = 20.0
WS_MIN_SOURCES = 3
DEFAULT_SOURCES = ("coinbase", "kraken", "bitstamp", "gemini", "bitfinex", "binance")


def _now_ms() -> int:
    return int(time.time() * 1000)


def _float_or_none(value) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _midpoint(bid, ask) -> Optional[float]:
    bid_f = _float_or_none(bid)
    ask_f = _float_or_none(ask)
    if bid_f is None or ask_f is None or bid_f <= 0 or ask_f <= 0:
        return None
    return (bid_f + ask_f) / 2.0


def _parse_iso_ms(value: str | None) -> int:
    if not value:
        return _now_ms()
    try:
        ts = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return int(ts.timestamp() * 1000)
    except ValueError:
        return _now_ms()


def _first_price(levels) -> Optional[float]:
    if not levels:
        return None
    level = levels[0]
    if isinstance(level, (list, tuple)) and level:
        return _float_or_none(level[0])
    return _float_or_none(level)


class ExchangeWebSocketFeed:
    """Base class for persistent top-of-book WebSocket price feeds."""

    def __init__(
        self,
        name: str,
        *,
        buffer_seconds: int = BUFFER_SECONDS,
        health_window_s: float = HEALTH_WINDOW_S,
    ):
        self.name = name
        self._buffer: deque[tuple[int, float]] = deque()
        self._lock = threading.Lock()
        self._last_price_time: float = 0.0
        self._connected: bool = False
        self._running: bool = False
        self._thread: Optional[threading.Thread] = None
        self._loop = None
        self._session_task = None
        self._stop_signal = threading.Event()
        self._reconnect_delay: float = 1.0
        self._buffer_seconds = buffer_seconds
        self._health_window_s = health_window_s

    @property
    def is_healthy(self) -> bool:
        return (time.time() - self._last_price_time) < self._health_window_s

    @property
    def buffer_size(self) -> int:
        with self._lock:
            return len(self._buffer)

    def get_latest_price(self) -> Optional[float]:
        with self._lock:
            if self._buffer:
                return self._buffer[-1][1]
        return None

    def get_latest_price_info(self) -> Optional[dict]:
        with self._lock:
            if not self._buffer:
                return None
            source_ms, price = self._buffer[-1]
            return {"price": price, "source_timestamp_ms": source_ms,
                    "received_at_ms": int(self._last_price_time * 1000)}

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._stop_signal.clear()
        self._thread = threading.Thread(
            target=self._run_loop,
            daemon=True,
            name=f"spot-{self.name}",
        )
        self._thread.start()
        log.info("%s feed started", self.name)

    def stop(self) -> None:
        self._running = False
        self._stop_signal.set()
        loop = self._loop
        if loop is not None and not loop.is_closed():
            def cancel_session():
                if self._session_task is not None:
                    self._session_task.cancel()
            try:
                loop.call_soon_threadsafe(cancel_session)
            except RuntimeError:
                pass
        if self._thread:
            self._thread.join(timeout=5)
            if self._thread.is_alive():
                log.warning("%s websocket thread did not stop within timeout", self.name)
        log.info("%s feed stopped", self.name)

    @property
    @abstractmethod
    def _ws_uri(self) -> str: ...

    def _subscribe_message(self) -> Optional[str]:
        return None

    @abstractmethod
    def _parse_price_from_message(self, raw_msg: str) -> Optional[tuple[int, float]]:
        """Return ``(timestamp_ms, top_of_book_mid)`` or ``None``."""
        ...

    def _ws_connect_kwargs(self) -> dict:
        return {}

    def _buffer_point(self, ts_ms: int, price: float) -> bool:
        if not is_sane_btc_price(price):
            log.warning("%s: insane price rejected: %s", self.name, price)
            return False
        with self._lock:
            self._buffer.append((int(ts_ms), float(price)))
            self._last_price_time = time.time()
        return True

    def _trim_buffer(self) -> None:
        cutoff_ms = _now_ms() - (self._buffer_seconds * 1000)
        with self._lock:
            while self._buffer and self._buffer[0][0] < cutoff_ms:
                self._buffer.popleft()

    def _run_loop(self) -> None:
        loop = asyncio.new_event_loop()
        self._loop = loop
        asyncio.set_event_loop(loop)
        try:
            while self._running:
                try:
                    if loop.is_closed():
                        break
                    self._session_task = loop.create_task(self._ws_session())
                    loop.run_until_complete(self._session_task)
                except asyncio.CancelledError:
                    if self._running:
                        log.warning("%s websocket session cancelled while running", self.name)
                except Exception as e:
                    if self._running:
                        log.warning("%s WebSocket session ended: %s", self.name, e)
                if self._running:
                    self._connected = False
                    delay = min(self._reconnect_delay, MAX_RECONNECT_DELAY)
                    log.info("%s reconnecting in %.0fs...", self.name, delay)
                    self._stop_signal.wait(delay)
                    self._reconnect_delay = min(
                        self._reconnect_delay * 2,
                        MAX_RECONNECT_DELAY,
                    )
        finally:
            self._session_task = None
            self._loop = None
            if not loop.is_closed():
                loop.close()

    async def _ws_session(self) -> None:
        import websockets

        extra = self._ws_connect_kwargs()
        async with websockets.connect(
            self._ws_uri,
            close_timeout=5,
            open_timeout=10,
            ping_interval=20,
            ping_timeout=20,
            **extra,
        ) as ws:
            self._connected = True
            self._reconnect_delay = 1.0
            log.info("%s WebSocket connected", self.name)

            sub = self._subscribe_message()
            if sub is not None:
                await ws.send(sub)

            await self._receive_loop(ws)

    async def _receive_loop(self, ws) -> None:
        while self._running:
            try:
                msg = await asyncio.wait_for(ws.recv(), timeout=30)
            except asyncio.TimeoutError:
                if self._running:
                    log.warning("%s: no message for 30s; reconnecting", self.name)
                return
            except Exception as e:
                if self._running:
                    log.warning("%s: receive error: %s", self.name, e)
                return

            try:
                parsed = self._parse_price_from_message(msg)
                if parsed is not None:
                    ts_ms, price = parsed
                    self._buffer_point(ts_ms, price)
                    self._trim_buffer()
            except Exception as e:
                log.debug("%s: message parse error: %s", self.name, e)


class BinanceWebSocketFeed(ExchangeWebSocketFeed):
    """Binance BTC/USDT book-ticker mid."""

    def __init__(self) -> None:
        super().__init__("binance")

    @property
    def _ws_uri(self) -> str:
        return "wss://stream.binance.com:9443/ws/btcusdt@bookTicker"

    def _parse_price_from_message(self, raw_msg: str) -> Optional[tuple[int, float]]:
        data = json.loads(raw_msg)
        if isinstance(data.get("data"), dict):
            data = data["data"]
        if data.get("s") not in (None, "BTCUSDT"):
            return None
        mid = _midpoint(data.get("b"), data.get("a"))
        if mid is None:
            return None
        return (int(data.get("E") or _now_ms()), mid)


class CoinbaseWebSocketFeed(ExchangeWebSocketFeed):
    """Coinbase Exchange BTC-USD ticker best bid/ask mid."""

    def __init__(self) -> None:
        super().__init__("coinbase")

    @property
    def _ws_uri(self) -> str:
        return "wss://ws-feed.exchange.coinbase.com"

    def _subscribe_message(self) -> Optional[str]:
        return json.dumps({
            "type": "subscribe",
            "product_ids": ["BTC-USD"],
            "channels": ["ticker"],
        })

    def _parse_price_from_message(self, raw_msg: str) -> Optional[tuple[int, float]]:
        data = json.loads(raw_msg)
        if data.get("type") != "ticker":
            return None
        if data.get("product_id") not in (None, "BTC-USD"):
            return None
        mid = _midpoint(data.get("best_bid"), data.get("best_ask"))
        if mid is None:
            mid = _float_or_none(data.get("price"))
        if mid is None:
            return None
        return (_parse_iso_ms(data.get("time")), mid)


class KrakenWebSocketFeed(ExchangeWebSocketFeed):
    """Kraken BTC/USD ticker bid/ask mid (WebSocket API v2)."""

    def __init__(self) -> None:
        super().__init__("kraken")

    @property
    def _ws_uri(self) -> str:
        return "wss://ws.kraken.com/v2"

    def _subscribe_message(self) -> Optional[str]:
        return json.dumps({
            "method": "subscribe",
            "params": {"channel": "ticker", "symbol": ["BTC/USD"]},
        })

    def _parse_price_from_message(self, raw_msg: str) -> Optional[tuple[int, float]]:
        data = json.loads(raw_msg)
        if data.get("channel") != "ticker" or data.get("type") not in ("snapshot", "update"):
            return None
        rows = data.get("data")
        if not rows or not isinstance(rows, list):
            return None
        row = rows[-1]
        if row.get("symbol") not in (None, "BTC/USD"):
            return None
        mid = _midpoint(row.get("bid"), row.get("ask"))
        if mid is None:
            return None
        return (_parse_iso_ms(row.get("timestamp")), mid)


class BitstampWebSocketFeed(ExchangeWebSocketFeed):
    """Bitstamp BTC/USD order-book top-of-book mid."""

    def __init__(self) -> None:
        super().__init__("bitstamp")

    @property
    def _ws_uri(self) -> str:
        return "wss://ws.bitstamp.net"

    def _subscribe_message(self) -> Optional[str]:
        return json.dumps({
            "event": "bts:subscribe",
            "data": {"channel": "order_book_btcusd"},
        })

    def _parse_price_from_message(self, raw_msg: str) -> Optional[tuple[int, float]]:
        data = json.loads(raw_msg)
        if data.get("event") != "data":
            return None
        if data.get("channel") not in (None, "order_book_btcusd"):
            return None
        book = data.get("data")
        if not isinstance(book, dict):
            return None
        bid = _first_price(book.get("bids"))
        ask = _first_price(book.get("asks"))
        mid = _midpoint(bid, ask)
        if mid is None:
            return None
        ts = _float_or_none(book.get("timestamp"))
        return (int(ts * 1000) if ts else _now_ms(), mid)


class GeminiWebSocketFeed(ExchangeWebSocketFeed):
    """Gemini BTC/USD top-of-book mid."""

    def __init__(self) -> None:
        super().__init__("gemini")
        self._best_bid: Optional[float] = None
        self._best_ask: Optional[float] = None

    @property
    def _ws_uri(self) -> str:
        return "wss://api.gemini.com/v1/marketdata/BTCUSD?top_of_book=true"

    def _parse_price_from_message(self, raw_msg: str) -> Optional[tuple[int, float]]:
        data = json.loads(raw_msg)
        if data.get("type") not in ("update", None):
            return None
        for event in data.get("events") or []:
            if event.get("type") not in ("change", "trade"):
                continue
            side = str(event.get("side") or "").lower()
            price = _float_or_none(event.get("price"))
            if price is None:
                continue
            if side in ("bid", "buy"):
                self._best_bid = price
            elif side in ("ask", "offer", "sell"):
                self._best_ask = price
        mid = _midpoint(self._best_bid, self._best_ask)
        if mid is None:
            return None
        return (int(data.get("timestampms") or _now_ms()), mid)


class BitfinexWebSocketFeed(ExchangeWebSocketFeed):
    """Bitfinex BTC/USD ticker bid/ask mid."""

    def __init__(self) -> None:
        super().__init__("bitfinex")
        self._chan_id: Optional[int] = None

    @property
    def _ws_uri(self) -> str:
        return "wss://api-pub.bitfinex.com/ws/2"

    def _subscribe_message(self) -> Optional[str]:
        return json.dumps({"event": "subscribe", "channel": "ticker", "symbol": "tBTCUSD"})

    def _parse_price_from_message(self, raw_msg: str) -> Optional[tuple[int, float]]:
        data = json.loads(raw_msg)
        if isinstance(data, dict):
            if data.get("event") == "subscribed" and data.get("channel") == "ticker":
                self._chan_id = int(data.get("chanId"))
            return None
        if not isinstance(data, list) or len(data) < 2:
            return None
        if self._chan_id is not None and data[0] != self._chan_id:
            return None
        ticker = data[1]
        if ticker == "hb" or not isinstance(ticker, list) or len(ticker) < 3:
            return None
        mid = _midpoint(ticker[0], ticker[2])
        if mid is None:
            return None
        return (_now_ms(), mid)


_FEED_CLASSES: dict[str, type[ExchangeWebSocketFeed]] = {
    "binance": BinanceWebSocketFeed,
    "coinbase": CoinbaseWebSocketFeed,
    "kraken": KrakenWebSocketFeed,
    "bitstamp": BitstampWebSocketFeed,
    "gemini": GeminiWebSocketFeed,
    "bitfinex": BitfinexWebSocketFeed,
}


def _default_rest_fallback() -> Optional[float]:
    try:
        from scripts.polymarket_btc_5m import _get_binance_rest_current_btc_price
    except ImportError:
        from scripts.polymarket_btc_15m import _get_binance_rest_current_btc_price

    return _get_binance_rest_current_btc_price()


class SpotAggregator:
    """RTDS-first BTC spot provider.

    Source order:
      1. Fresh Polymarket RTDS Chainlink BTC/USD.
      2. WebSocket top-of-book median from at least three exchanges.
      3. Binance REST last resort.
    """

    def __init__(
        self,
        *,
        chainlink_feed=None,
        enabled_feeds: Optional[list[str]] = None,
        outlier_tolerance_pct: Optional[float] = None,
        rtds_stale_after_s: float = RTDS_STALE_AFTER_S,
        ws_stale_after_s: float = HEALTH_WINDOW_S,
        outlier_bps: Optional[float] = WS_OUTLIER_BPS,
        min_sources: int = WS_MIN_SOURCES,
        rest_last_resort_enabled: bool = True,
        rest_fallback_fn: Optional[Callable[[], Optional[float]]] = None,
    ):
        self._chainlink = chainlink_feed
        self._rtds_stale_after_s = float(rtds_stale_after_s)
        self._ws_stale_after_s = float(ws_stale_after_s)
        if outlier_bps is None:
            outlier_bps = (
                float(outlier_tolerance_pct) * 100.0
                if outlier_tolerance_pct is not None
                else WS_OUTLIER_BPS
            )
        self._outlier_bps = float(outlier_bps)
        self._min_sources = int(min_sources)
        self._rest_last_resort_enabled = bool(rest_last_resort_enabled)
        self._rest_fallback_fn = rest_fallback_fn or _default_rest_fallback
        self._last_good_spot: Optional[float] = None
        self._feeds: dict[str, ExchangeWebSocketFeed] = {}
        self._last_ws_observations: dict[str, dict] = {}

        if enabled_feeds is None:
            enabled_feeds = list(DEFAULT_SOURCES)
        for name in enabled_feeds:
            cls = _FEED_CLASSES.get(name)
            if cls is None:
                log.warning("unknown spot feed %r; skipping", name)
                continue
            feed = cls()
            feed._health_window_s = self._ws_stale_after_s
            self._feeds[name] = feed

        log.info(
            "SpotAggregator configured RTDS-first with feeds=%s min_sources=%d outlier=%.1fbps",
            list(self._feeds.keys()),
            self._min_sources,
            self._outlier_bps,
        )

    def start(self) -> None:
        for feed in self._feeds.values():
            feed.start()

    def stop(self) -> None:
        for feed in self._feeds.values():
            feed.stop()

    def health_summary(self) -> dict[str, bool]:
        return {name: feed.is_healthy for name, feed in self._feeds.items()}

    def get_aggregated_spot(self) -> Optional[tuple[float, str, dict]]:
        """Return ``(price, source_label, details)`` or ``None``."""
        details: dict = {
            "health": self.health_summary(),
            "source_order": ["polymarket_rtds", "ws_median", "binance_rest_last_resort"],
        }

        rtds_info = self._latest_rtds_info()
        if rtds_info is not None:
            details["rtds"] = dict(rtds_info)
            if self._is_rtds_fresh(rtds_info):
                price = float(rtds_info["price"])
                self._last_good_spot = price
                return (price, "polymarket_rtds", details)
            details["fallback_reason"] = "polymarket_rtds_stale"
        else:
            details["fallback_reason"] = "polymarket_rtds_unavailable"

        prices = self._fresh_ws_prices()
        details["raw_prices"] = dict(prices)
        if len(prices) >= self._min_sources:
            filtered = self._reject_outliers(prices)
            details["used_prices"] = dict(filtered)
            if len(filtered) >= self._min_sources:
                spot = float(statistics.median(filtered.values()))
                names = ",".join(sorted(filtered.keys()))
                dispersion = (max(filtered.values()) - min(filtered.values())) / spot * 10_000
                details["dispersion_bps"] = dispersion
                observations = [self._last_ws_observations[name] for name in filtered if name in self._last_ws_observations]
                if len(observations) == len(filtered):
                    # A median depends on every contributing observation;
                    # republishing it cannot reset the oldest sample's age.
                    details["source_timestamp_ms"] = min(item["source_timestamp_ms"] for item in observations)
                    details["received_at_ms"] = min(item["received_at_ms"] for item in observations)
                self._last_good_spot = spot
                return (spot, f"ws_median({names})", details)
            details["ws_reject_reason"] = "insufficient_sources_after_outlier_filter"
        else:
            details["ws_reject_reason"] = "insufficient_fresh_sources"

        if self._rest_last_resort_enabled:
            try:
                rest = self._rest_fallback_fn()
                if rest is not None and is_sane_btc_price(rest):
                    details["fallback"] = "binance_rest_last_resort"
                    return (float(rest), "binance_rest_last_resort", details)
            except Exception as exc:
                log.debug("Binance REST last-resort fallback failed: %s", exc)

        return None

    def _latest_rtds_info(self) -> Optional[dict]:
        if self._chainlink is None:
            return None

        info_fn = getattr(self._chainlink, "get_latest_price_info", None)
        if callable(info_fn):
            try:
                info = info_fn()
            except Exception:
                info = None
            if isinstance(info, dict):
                price = _float_or_none(info.get("price"))
                if price is not None and is_sane_btc_price(price):
                    return {
                        "price": price,
                        "source_timestamp_ms": info.get("source_timestamp_ms"),
                        "source_age_seconds": float(info.get("source_age_seconds") or 0.0),
                        "receive_age_seconds": float(info.get("receive_age_seconds") or 0.0),
                    }

        try:
            if not bool(getattr(self._chainlink, "is_healthy", False)):
                return None
            price = self._chainlink.get_latest_price()
        except Exception:
            return None
        if price is None or not is_sane_btc_price(price):
            return None
        return {"price": float(price), "source_age_seconds": 0.0, "receive_age_seconds": 0.0}

    def _is_rtds_fresh(self, info: dict) -> bool:
        return (
            float(info.get("source_age_seconds") or 0.0) <= self._rtds_stale_after_s
            and float(info.get("receive_age_seconds") or 0.0) <= self._rtds_stale_after_s
        )

    def _fresh_ws_prices(self) -> dict[str, float]:
        prices: dict[str, float] = {}
        self._last_ws_observations = {}
        for name, feed in self._feeds.items():
            if feed.is_healthy:
                info_fn = getattr(feed, "get_latest_price_info", None)
                info = info_fn() if callable(info_fn) else None
                if isinstance(info, dict):
                    if (_now_ms() - info["source_timestamp_ms"]) / 1000.0 > self._ws_stale_after_s:
                        continue
                    self._last_ws_observations[name] = info
                price = feed.get_latest_price()
                if price is not None and is_sane_btc_price(price):
                    prices[name] = float(price)
        return prices

    def _reject_outliers(self, prices: dict[str, float]) -> dict[str, float]:
        if len(prices) < self._min_sources:
            return prices
        median = statistics.median(prices.values())
        threshold = self._outlier_bps / 10_000.0
        kept: dict[str, float] = {}
        for name, price in prices.items():
            deviation = abs(price - median) / median
            if deviation <= threshold:
                kept[name] = price
            else:
                log.warning(
                    "outlier rejected: %s=$%.2f deviates %.2fbps from median $%.2f",
                    name,
                    price,
                    deviation * 10_000,
                    median,
                )
        return kept
