#!/usr/bin/env python3
"""Fetch the Price-To-Beat (PTB) for Polymarket "BTC Up or Down 15m" markets.

The PTB is the `openPrice` returned by `polymarket.com/api/crypto/crypto-price`
at the market's 15-minute window start. Polymarket's displayed 15m PTB uses
the endpoint's `variant=fiveminute` payload at that exact start timestamp; do
not shift the query five minutes forward. It is the reference price: the
market resolves "Up" if the closing price is >= PTB, otherwise "Down".

Typical usage:

    from fetch_ptb import PolymarketPTB

    client = PolymarketPTB()
    slug, ptb = client.current_ptb()   # both in one call
    # or, step by step:
    slug = client.active_slug()
    ptb  = client.ptb_for_slug(slug)

Official resolution source: Chainlink BTC/USD data stream
(https://data.chain.link/streams/btc-usd).
"""

import time
from datetime import datetime, timedelta, timezone

import requests


class PolymarketPTB:
    """Client for Polymarket's BTC Up/Down 15-minute markets.

    A single instance can be reused across many calls — it holds a
    `requests.Session` for connection pooling. Configuration is set once
    at construction time.

    Attributes:
        timeout:            Per-request timeout (seconds).
        freshness_minimum:  Minimum age a market must have (now - start)
                            before its PTB is considered valid. The client
                            sleeps if a market is younger than this.
        transition_guard:   Buffer at the END of a window during which the
                            client treats the current market as closing and
                            switches to the next one (after waiting for it
                            to reach `freshness_minimum`).
        session:            Underlying `requests.Session`.
    """

    CRYPTO_PRICE_API = "https://polymarket.com/api/crypto/crypto-price"
    GAMMA_EVENTS_API = "https://gamma-api.polymarket.com/events"
    FIFTEEN_MIN_PREFIX = "btc-updown-15m-"
    CRYPTO_PRICE_VARIANT = "fiveminute"
    WINDOW_LENGTH = timedelta(minutes=15)

    def __init__(
        self,
        *,
        timeout: float = 10.0,
        freshness_minimum: timedelta = timedelta(seconds=30),
        transition_guard: timedelta = timedelta(seconds=5),
        session: requests.Session | None = None,
    ) -> None:
        self.timeout = timeout
        self.freshness_minimum = freshness_minimum
        self.transition_guard = transition_guard
        self.session = session or requests.Session()
        # Force a content-encoding the local `requests` install can decode
        # (some installs ship without brotli support; the Session default
        # advertises `br` which can then fail to decode).
        self.session.headers["Accept-Encoding"] = "gzip, deflate"

    def close(self) -> None:
        """Close the underlying HTTP session."""
        self.session.close()

    def __enter__(self) -> "PolymarketPTB":
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    # ------------------------------------------------------------------ #
    # Slug helpers                                                       #
    # ------------------------------------------------------------------ #

    @classmethod
    def window_from_slug(cls, slug: str) -> tuple[datetime, datetime]:
        """Derive the 15-minute UTC window from a market slug.

        Polymarket encodes the window's start time directly in the slug as
        a Unix timestamp, e.g. `btc-updown-15m-1777631400` -> 2026-05-01
        10:30:00Z. The end time is always `start + 15 minutes`.

        Args:
            slug: Market slug starting with `btc-updown-15m-`.

        Returns:
            `(start_dt, end_dt)` as timezone-aware UTC datetimes.

        Raises:
            ValueError: If `slug` does not have the expected prefix.
        """
        if not slug.startswith(cls.FIFTEEN_MIN_PREFIX):
            raise ValueError(f"Unexpected slug: {slug!r}")
        start_unix = int(slug[len(cls.FIFTEEN_MIN_PREFIX):])
        start_dt = datetime.fromtimestamp(start_unix, tz=timezone.utc)
        return start_dt, start_dt + cls.WINDOW_LENGTH

    @classmethod
    def slug_from_start(cls, start_dt: datetime) -> str:
        """Build a market slug from a window start time."""
        if start_dt.tzinfo is None:
            start_dt = start_dt.replace(tzinfo=timezone.utc)
        return f"{cls.FIFTEEN_MIN_PREFIX}{int(start_dt.timestamp())}"

    # ------------------------------------------------------------------ #
    # Low-level fetchers                                                 #
    # ------------------------------------------------------------------ #

    def ptb_for_window(self, start_dt: datetime, end_dt: datetime) -> float | None:
        """Return the PTB (`openPrice`) for an explicit 15-minute window.

        Calls the crypto-price endpoint with `variant=fiveminute`. Datetimes
        are formatted as `%Y-%m-%dT%H:%M:%SZ` and treated as UTC by the API.
        For 15m markets, the query still uses the 15m market start timestamp;
        only the API variant remains `fiveminute`.

        **Caller must enforce a freshness wait before calling.** During the
        first ~30s of a window the ``openPrice`` Polymarket returns can
        still drift — on 2026-05-01 12:25 we read at start+1s and got
        $77,767.70, which Polymarket later revised to $77,744.57 ($23
        drift). The recommended pattern is to call via ``active_slug()``
        which sleeps ``freshness_minimum`` past start before returning,
        OR to gate manually (e.g. ``calc_daemon._capture_barrier``
        checks ``(now - start_utc) >= POLYMARKET_FRESHNESS_MIN_S``).

        The ``incomplete`` / ``completed`` flags on the response track
        whether the *window* has closed, NOT whether the ``openPrice``
        is finalized — they stay set for the full 15-minute duration of
        the active window even when the openPrice is already stable.
        So we don't gate on those flags here; the time gate is the
        discriminator.

        Args:
            start_dt: Window start (must align with a 15-minute boundary).
            end_dt:   Window end (`start_dt + 15 minutes`).

        Returns:
            The open price as a float, or `None` on error / missing
            field.
        """
        try:
            r = self.session.get(
                self.CRYPTO_PRICE_API,
                params={
                    "symbol": "BTC",
                    "eventStartTime": _utc_z(start_dt),
                    "variant": self.CRYPTO_PRICE_VARIANT,
                    "endDate": _utc_z(end_dt),
                },
                timeout=self.timeout,
            )
            r.raise_for_status()
            data = r.json()
            open_price = data.get("openPrice")
            return float(open_price) if open_price is not None else None
        except (requests.RequestException, ValueError) as e:
            print(f"PTB fetch error: {e}")
            return None

    def ptb_for_slug(self, slug: str) -> float | None:
        """Fetch the PTB for the 15-minute market identified by `slug`."""
        start_dt, end_dt = self.window_from_slug(slug)
        return self.ptb_for_window(start_dt, end_dt)

    def current_price(self, lag_seconds: float = 2.0) -> float | None:
        """Return current BTC spot without touching PTB/openPrice retrieval.

        ``crypto-price`` is only safe for PTB/window reference prices here;
        its ``openPrice`` can equal the barrier and must not be used as the
        model calibration spot. This diagnostic helper also stays independent
        of the RTDS-first spot provider so PTB tooling is not coupled to RTDS.
        """
        try:
            from scripts.polymarket_btc_15m import _get_binance_rest_current_btc_price

            return _get_binance_rest_current_btc_price()
        except Exception:
            return None

    # ------------------------------------------------------------------ #
    # Active-market selection                                            #
    # ------------------------------------------------------------------ #

    def _list_15m_windows(self) -> list[tuple[datetime, datetime, str]]:
        """List all `btc-updown-15m-*` events as `(start, end, slug)` tuples."""
        try:
            r = self.session.get(
                self.GAMMA_EVENTS_API,
                params={
                    "closed": "false",
                    "active": "true",
                    "limit": 500,
                    "tag_slug": "crypto-prices",
                },
                timeout=self.timeout,
            )
            r.raise_for_status()
            events = r.json()
        except (requests.RequestException, ValueError) as e:
            print(f"Active markets fetch error: {e}")
            return []

        out: list[tuple[datetime, datetime, str]] = []
        for ev in events:
            slug = ev.get("slug") or ""
            if not slug.startswith(self.FIFTEEN_MIN_PREFIX):
                continue
            try:
                start_dt, end_dt = self.window_from_slug(slug)
            except ValueError:
                continue
            out.append((start_dt, end_dt, slug))
        out.sort()
        return out

    def active_slug(self) -> str | None:
        """Return the slug of the currently safe-to-query 15-minute market.

        Two thresholds are enforced to avoid reading a stale or in-flight
        PTB (defaults: `freshness_minimum=30s`, `transition_guard=5s`):

          - now in [start, start+freshness)  : market too fresh — sleep
                                               until start+freshness, then
                                               return its slug.
          - now in [end-guard, end)          : current market closing —
                                               sleep until the next window's
                                               freshness threshold and
                                               return that slug (verified
                                               against the listing).
          - now in a gap (no active)         : sleep until next_start +
                                               freshness, return upcoming.
          - otherwise                        : return the active slug now.

        Returns:
            The slug to use, or `None` if no suitable market can be
            identified (network error, empty listing, or a derived next
            market is not present in the listing).

        Note:
            May block for up to `freshness_minimum + transition_guard`
            seconds (worst case: caught at end-of-window).
        """
        windows = self._list_15m_windows()
        if not windows:
            return None
        slugs = {sl for _, _, sl in windows}

        now = datetime.now(timezone.utc)
        freshness = self.freshness_minimum
        guard = self.transition_guard

        current = next(((s, e, sl) for s, e, sl in windows if s <= now < e), None)
        if current is not None:
            start_dt, end_dt, slug = current
            if now - start_dt < freshness:
                self._sleep_until(
                    start_dt + freshness,
                    f"market <{freshness.total_seconds():.0f}s fresh",
                )
                return slug
            if end_dt - now < guard:
                self._sleep_until(
                    end_dt + freshness,
                    "transition imminent — waiting for next market to mature",
                )
                next_slug = self.slug_from_start(end_dt)
                return next_slug if next_slug in slugs else None
            return slug

        upcoming = next(((s, e, sl) for s, e, sl in windows if now < s), None)
        if upcoming is None:
            return None
        start_dt, _end_dt, slug = upcoming
        self._sleep_until(
            start_dt + freshness, f"no active market — waiting for {slug}"
        )
        return slug

    def current_ptb(self) -> tuple[str, float] | None:
        """Resolve the active market and return `(slug, ptb)` in one call.

        Returns `None` if no active slug can be selected or the PTB lookup
        fails.
        """
        slug = self.active_slug()
        if slug is None:
            return None
        ptb = self.ptb_for_slug(slug)
        return (slug, ptb) if ptb is not None else None

    # ------------------------------------------------------------------ #
    # Internals                                                          #
    # ------------------------------------------------------------------ #

    @staticmethod
    def _sleep_until(target: datetime, reason: str) -> None:
        """Sleep until `target` (UTC), printing a brief status line."""
        wait = (target - datetime.now(timezone.utc)).total_seconds()
        if wait <= 0:
            return
        print(f"{reason} — sleeping {wait:.2f}s.")
        time.sleep(wait)


def _utc_z(dt: datetime) -> str:
    """Format a datetime as `YYYY-MM-DDTHH:MM:SSZ` (UTC)."""
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


if __name__ == "__main__":
    with PolymarketPTB() as client:
        result = client.current_ptb()
        if result is None:
            print("No active 15-minute BTC market available.")
        else:
            slug, ptb = result
            start_dt, end_dt = PolymarketPTB.window_from_slug(slug)
            current = client.current_price()
            print(f"Active market : {slug}")
            print(f"Window        : {start_dt.isoformat()} -> {end_dt.isoformat()}")
            print(f"PTB           : {ptb}")
            print(f"Current price : {current}")
            if current is not None:
                delta = current - ptb
                pct = delta / ptb * 100
                print(f"Delta vs PTB  : {delta:+.2f} ({pct:+.4f}%)")
