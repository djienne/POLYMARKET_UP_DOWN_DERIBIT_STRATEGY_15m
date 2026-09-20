"""Persistent Chainlink BTC/USD price feed via Polymarket WebSocket.

Maintains a background WebSocket connection to wss://ws-live-data.polymarket.com,
buffers recent Chainlink BTC/USD prices, and provides instant lookups.
Auto-reconnects on disconnect with exponential backoff.

Instantaneous spot and authoritative 60-second TWAP observations are buffered
separately. Resolved token payouts, rather than an inferred spot close, govern
the accounting of current TWAP markets.

Message formats from the server:
  - Initial snapshot: {"payload": {"data": [{"timestamp": ..., "value": ...}, ...]}}
  - Live updates:     {"payload": {"symbol": "btc/usd", "timestamp": ..., "value": ...}}

Connection keepalive includes the RTDS application-level PING every 5 seconds.
"""

import asyncio
import json
import logging
import threading
import time
from decimal import Decimal, InvalidOperation
from collections import deque
from datetime import datetime, timezone
from typing import Optional

from btc_pricer.utils.price_sanity import (
    BTC_PRICE_MAX,
    BTC_PRICE_MIN,
    is_sane_btc_price,
)

log = logging.getLogger(__name__)

WS_URI = "wss://ws-live-data.polymarket.com"
BTC_MIN = BTC_PRICE_MIN
BTC_MAX = BTC_PRICE_MAX
BUFFER_SECONDS = 180  # Keep 3 minutes of price history
MAX_RECONNECT_DELAY = 30
# Watchdog: force reconnect if no fresh price lands for this long on an
# otherwise-alive socket. Larger than RTDS spot freshness so consumers can
# fall back before the connection is recycled.
UNHEALTHY_RECONNECT_SECONDS = 20
UNHEALTHY_CHECK_INTERVAL = 5
RTDS_SPOT_FRESH_SECONDS = 10.0
CHAINLINK_SUBSCRIPTION = {
    "action": "subscribe",
    "subscriptions": [{
        "topic": "crypto_prices_chainlink",
        "type": "update",
    }, {
        "topic": "crypto_prices_twap_sixty",
        "type": "update",
        "filters": '{"symbol":"btc/usd"}',
    }],
}


def _is_sane_price(price):
    return is_sane_btc_price(price)


class ChainlinkPriceFeed:
    """Persistent WebSocket connection to Polymarket's Chainlink BTC/USD stream."""

    def __init__(self):
        self._buffer = deque()  # (timestamp_ms, price) tuples
        self._twap_buffer = deque(maxlen=1200)
        self._lock = threading.Lock()
        self._last_message_time = 0.0
        # Fix #7: track when we last *buffered a price*, separately from
        # when any socket message arrived (keepalive acks bump _last_message_time
        # without adding fresh data). is_healthy gates on this.
        self._last_price_time = 0.0
        self._latest_price: Optional[float] = None
        self._latest_source_ts_ms: Optional[int] = None
        self._latest_receive_time: Optional[float] = None
        self._connected = False
        self._running = False
        self._thread = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._session_task = None
        self._stop_signal = threading.Event()
        self._ws = None
        self._reconnect_delay = 1.0

    def start(self):
        """Start the background WebSocket thread."""
        if self._running:
            return
        self._running = True
        self._stop_signal.clear()
        self._thread = threading.Thread(target=self._run_loop, daemon=True, name="chainlink-feed")
        self._thread.start()
        log.info("ChainlinkPriceFeed started")

    def stop(self):
        """Stop the background thread."""
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
                log.warning("ChainlinkPriceFeed thread did not stop within timeout")
        log.info("ChainlinkPriceFeed stopped")

    @property
    def is_healthy(self) -> bool:
        """True only if a fresh price point was buffered within the last 30s.

        Tighter than `_last_message_time` because keepalive acks (or empty
        payloads) update message time without producing usable price data.
        `get_price_at` rejects anything more than 15s from target, so the
        30s health window gives a small margin for windows that just started.
        """
        return (time.time() - self._last_price_time) < 30

    @property
    def buffer_size(self) -> int:
        with self._lock:
            return len(self._buffer)

    def get_latest_price(self) -> Optional[float]:
        """Get the most recent Chainlink BTC/USD price from the buffer."""
        with self._lock:
            if self._buffer:
                return self._buffer[-1][1]
        return None

    def get_latest_twap(self) -> Optional[dict]:
        """Latest authoritative 60s observation, distinct from instantaneous spot."""
        with self._lock:
            return dict(self._twap_buffer[-1]) if self._twap_buffer else None

    def _buffer_twap(self, payload: dict) -> bool:
        if str(payload.get("symbol") or "").lower() != "btc/usd":
            return False
        if payload.get("window_s", 60) != 60:
            return False
        try:
            raw = payload.get("full_accuracy_value")
            value = Decimal(str(raw)) / Decimal(10**18) if raw is not None else Decimal(str(payload["value"]))
            ts_ms = int(payload["timestamp"])
            if not value.is_finite() or not _is_sane_price(float(value)) or ts_ms <= 0:
                return False
        except (KeyError, TypeError, ValueError, InvalidOperation):
            return False
        observation = {
            "schema_version": 1, "symbol": "BTC/USD", "window_seconds": 60,
            "value": str(value), "source_timestamp_ms": ts_ms,
            "received_at_ms": int(time.time() * 1000), "source": "chainlink_twap_60s",
        }
        with self._lock:
            if self._twap_buffer and ts_ms <= self._twap_buffer[-1]["source_timestamp_ms"]:
                return False
            self._twap_buffer.append(observation)
        return True

    def get_latest_price_info(self) -> Optional[dict]:
        """Return latest RTDS price plus source/receive ages for freshness gates."""
        with self._lock:
            if (
                self._latest_price is None
                or self._latest_source_ts_ms is None
                or self._latest_receive_time is None
            ):
                return None
            price = self._latest_price
            source_ts_ms = self._latest_source_ts_ms
            receive_time = self._latest_receive_time

        now = time.time()
        return {
            "price": price,
            "source_timestamp_ms": source_ts_ms,
            "source_age_seconds": max(0.0, now - (source_ts_ms / 1000.0)),
            "receive_age_seconds": max(0.0, now - receive_time),
        }

    def get_latest_price_if_fresh(
        self,
        max_age_seconds: float = RTDS_SPOT_FRESH_SECONDS,
    ) -> Optional[float]:
        """Return latest price only when both source and receive ages are fresh."""
        info = self.get_latest_price_info()
        if info is None:
            return None
        if (
            float(info["source_age_seconds"]) <= max_age_seconds
            and float(info["receive_age_seconds"]) <= max_age_seconds
        ):
            return float(info["price"])
        return None

    def get_price_at(self, timestamp_utc: datetime) -> Optional[float]:
        """Get the Chainlink BTC/USD price closest to a specific timestamp.

        Args:
            timestamp_utc: Target datetime (UTC).

        Returns:
            Price if a data point within 15s of target exists, else None.
        """
        target_ms = int(timestamp_utc.timestamp() * 1000)

        with self._lock:
            if not self._buffer:
                return None

            best_price = None
            best_diff = float("inf")
            for ts_ms, price in self._buffer:
                diff = abs(ts_ms - target_ms)
                if diff < best_diff:
                    best_diff = diff
                    best_price = price

        diff_s = best_diff / 1000
        if diff_s > 15:
            log.warning(f"Closest Chainlink point is {diff_s:.0f}s from target â€” too stale (>15s)")
            return None

        log.debug(f"Chainlink price at target: ${best_price:,.2f} ({diff_s:.0f}s offset)")
        return best_price

    def get_first_at_or_after(
        self,
        start_utc: datetime,
        *,
        max_offset_s: float = 10.0,
    ) -> tuple[Optional[float], Optional[float]]:
        """Return ``(price, offset_seconds)`` for the FIRST buffered Chainlink
        point at or after ``start_utc`` within ``max_offset_s``.

        Distinct from :meth:`get_price_at`, which returns the closest point in
        either direction. This method matches Polymarket's resolution
        semantics for window-start barriers â€” the resolution price is the
        Chainlink round at-or-after the boundary.

        Returns:
            ``(price, offset_seconds)`` if a qualifying point exists, else
            ``(None, None)``.
        """
        target_ms = int(start_utc.timestamp() * 1000)

        with self._lock:
            if not self._buffer:
                return (None, None)
            best_price: Optional[float] = None
            best_offset_ms = float("inf")
            for ts_ms, price in self._buffer:
                offset_ms = ts_ms - target_ms
                if offset_ms < 0:
                    continue  # before window start â€” skip
                if offset_ms < best_offset_ms:
                    best_offset_ms = offset_ms
                    best_price = price
                    if offset_ms == 0:
                        break

        if best_price is None:
            return (None, None)
        offset_s = best_offset_ms / 1000
        if offset_s > max_offset_s:
            log.debug(
                "Earliest at-or-after Chainlink point is %.0fs after target â€” too late (>%s s)",
                offset_s,
                max_offset_s,
            )
            return (None, None)
        log.debug(
            "Chainlink at-or-after target: $%.2f (%.0fs after start)",
            best_price,
            offset_s,
        )
        return (best_price, offset_s)

    def _buffer_point(self, ts_ms, value):
        """Add a single (timestamp_ms, price) point to the buffer if valid."""
        if ts_ms is None or value is None:
            return False
        try:
            ts_ms = int(ts_ms)
            value = float(value)
        except (TypeError, ValueError):
            return False
        if not _is_sane_price(value):
            log.warning(f"Insane Chainlink price rejected: {value}")
            return False
        with self._lock:
            self._buffer.append((ts_ms, value))
            self._latest_price = value
            self._latest_source_ts_ms = ts_ms
            self._latest_receive_time = time.time()
        # Mark the feed as healthy only once we've actually landed a price point.
        self._last_price_time = time.time()
        return True

    def _trim_buffer(self):
        """Remove points older than BUFFER_SECONDS."""
        cutoff_ms = int(time.time() * 1000) - (BUFFER_SECONDS * 1000)
        with self._lock:
            while self._buffer and self._buffer[0][0] < cutoff_ms:
                self._buffer.popleft()

    def _process_message(self, raw_msg: str) -> bool:
        """Parse a WebSocket message and buffer valid price points.

        Handles two formats:
          1. Initial snapshot: payload.data[] array of {timestamp, value}
          2. Live update: payload with {symbol, timestamp, value} directly

        Returns True if this was a valid server message, False if empty/junk.
        """
        if not raw_msg or not raw_msg.strip():
            return False

        text = raw_msg.strip()
        if text.upper() in {"PING", "PONG"}:
            self._last_message_time = time.time()
            return True

        try:
            data = json.loads(raw_msg)
        except json.JSONDecodeError:
            log.warning(f"Invalid JSON from WebSocket: {raw_msg[:100]}")
            return False

        topic = data.get("topic")
        if topic == "crypto_prices_twap_sixty":
            self._buffer_twap(data.get("payload") or {})
            self._last_message_time = time.time()
            return True
        if topic and topic not in ("crypto_prices_chainlink", "crypto_prices"):
            self._last_message_time = time.time()
            return True

        payload = data.get("payload")
        if not payload:
            # Valid JSON but no payload (e.g. empty ack) â€” still counts as alive
            self._last_message_time = time.time()
            return True

        symbol = str(payload.get("symbol") or "").lower()
        if symbol and symbol != "btc/usd":
            self._last_message_time = time.time()
            return True

        added = 0

        # Format 1: initial snapshot â€” payload.data[] array
        price_data = payload.get("data", [])
        if price_data and isinstance(price_data, list):
            for point in price_data:
                point_symbol = str(point.get("symbol") or symbol or "").lower()
                if point_symbol and point_symbol != "btc/usd":
                    continue
                if self._buffer_point(point.get("timestamp"), point.get("value")):
                    added += 1

        # Format 2: live update â€” payload has timestamp/value directly
        if not price_data and "value" in payload and "timestamp" in payload:
            if symbol != "btc/usd":
                self._last_message_time = time.time()
                return True
            if self._buffer_point(payload.get("timestamp"), payload.get("value")):
                added += 1

        self._trim_buffer()

        # Update last message time on ANY valid server response
        self._last_message_time = time.time()
        if added > 0:
            self._reconnect_delay = 1.0  # Reset backoff on data received

        return True

    def _run_loop(self):
        """Background thread: run asyncio event loop for WebSocket."""
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
                        log.warning("Chainlink session cancelled while running")
                except Exception as e:
                    if self._running:
                        log.warning(f"WebSocket session ended: {e}")
                # Reconnect with backoff
                if self._running:
                    self._connected = False
                    delay = min(self._reconnect_delay, MAX_RECONNECT_DELAY)
                    log.info(f"Reconnecting in {delay:.0f}s...")
                    self._stop_signal.wait(delay)
                    self._reconnect_delay = min(self._reconnect_delay * 2, MAX_RECONNECT_DELAY)
        finally:
            self._session_task = None
            self._loop = None
            if not loop.is_closed():
                loop.close()

    def _subscribe_msg(self):
        """Build the subscription JSON message."""
        return json.dumps(CHAINLINK_SUBSCRIPTION)

    async def _ws_session(self):
        """One WebSocket session: connect, subscribe, receive messages.

        The live Chainlink subscription pushes BTC/USD updates after a single
        subscribe message. Protocol and RTDS application heartbeats are sent.
        """
        import websockets

        async with websockets.connect(
            WS_URI, close_timeout=5, open_timeout=10,
            ping_interval=5,
            ping_timeout=20,
        ) as ws:
            self._ws = ws
            self._connected = True
            self._reconnect_delay = 1.0
            log.info("Chainlink WebSocket connected")

            # Initial subscribe
            await ws.send(self._subscribe_msg())

            # Run receive + health watchdog concurrently
            receive_task = asyncio.create_task(self._receive_loop(ws))
            watchdog_task = asyncio.create_task(self._unhealthy_watchdog(ws))
            heartbeat_task = asyncio.create_task(self._application_heartbeat(ws))
            tasks = [receive_task, watchdog_task, heartbeat_task]

            try:
                done, pending = await asyncio.wait(
                    tasks,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                for task in pending:
                    task.cancel()
                if pending:
                    await asyncio.gather(*pending, return_exceptions=True)
                for task in done:
                    task.result()
            except BaseException:
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                raise
            finally:
                self._ws = None
                self._connected = False

    async def _receive_loop(self, ws):
        """Continuously receive and process messages."""
        while self._running:
            try:
                msg = await asyncio.wait_for(ws.recv(), timeout=60)
                self._process_message(msg)
            except asyncio.TimeoutError:
                # 60s without any message (not even from re-subscribe)
                # Something is wrong â€” let session restart
                if self._running:
                    log.warning("No WebSocket message for 60s â€” reconnecting")
                return
            except Exception as e:
                if self._running:
                    log.warning(f"WebSocket receive error: {e}")
                return

    async def _application_heartbeat(self, ws):
        """RTDS requires text PING in addition to websocket protocol pings."""
        while self._running:
            await asyncio.sleep(5)
            if self._running:
                await ws.send("PING")

    async def _unhealthy_watchdog(self, ws):
        """Force reconnect when prices stop flowing despite a live socket.

        Track the last-buffered-price time and tear the socket down if it goes
        stale â€” the outer reconnect loop will then start a fresh session.
        """
        while self._running:
            await asyncio.sleep(UNHEALTHY_CHECK_INTERVAL)
            # Only act once we've seen at least one price on this session;
            # don't force reconnect during the connect grace window before
            # the first snapshot arrives.
            if self._last_price_time <= 0:
                continue
            idle = time.time() - self._last_price_time
            if idle > UNHEALTHY_RECONNECT_SECONDS:
                log.warning(
                    f"No Chainlink price for {idle:.0f}s on live socket â€” forcing reconnect"
                )
                try:
                    await ws.close()
                except Exception:
                    pass
                return
