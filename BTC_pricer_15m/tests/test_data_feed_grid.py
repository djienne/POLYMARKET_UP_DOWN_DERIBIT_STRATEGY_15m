"""Tests for SharedDataFeed (live/grid) realism features.

After the centralize-Chainlink refactor, live/grid no longer hold a
`ChainlinkPriceFeed` themselves. Calc owns the on-chain feed; live/grid
read calc's captured barrier (and source) from the calibration event
payload via ``HybridCalibrationBroker.latest_barrier_for_market``. These
tests exercise:

* Spot fallback chain (Binance → Coinbase Exchange → Coinbase Data API →
  last-good cache → barrier fallback).
* Expiry handling (Binance kline at end_utc, degraded spot fallback after
  ``settlement_wait_seconds``).
* Broker-driven barrier capture (the new path).
* Independent Binance-only sanity check on calc's published Chainlink
  barrier.
* Scan rate-limit + ``next_sleep`` behaviour around market expiry.

Tests for the old in-feed Chainlink refinement loop have been removed —
that loop now lives only in ``calc_daemon._capture_barrier`` and is
covered in ``test_calc_daemon.py``.
"""

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Optional

from scripts.trader.config import GridConfig
from scripts.trader.data_feed import DEFAULT_RESULTS_DIR, SharedDataFeed, TERMINAL_JSON


# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------


def test_default_runtime_paths_are_project_root_anchored(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)

    feed = SharedDataFeed(GridConfig())

    assert feed.results_dir == DEFAULT_RESULTS_DIR
    assert feed.terminal_json == TERMINAL_JSON
    assert feed.results_dir.is_absolute()


def test_order_book_latency_path_is_project_root_anchored(monkeypatch, tmp_path):
    from scripts.trader import order_book

    monkeypatch.chdir(tmp_path)

    assert order_book._LATENCY_PATH.is_absolute()
    assert order_book._LATENCY_PATH == DEFAULT_RESULTS_DIR / ".clob_latency_ms"


class _FakeBroker:
    """Minimal stand-in for HybridCalibrationBroker.

    Only implements the surface SharedDataFeed actually calls in the
    capture path: ``latest_barrier_for_market``, ``poll_inbox``,
    ``update_market_context``, ``latest_for_market``.
    """

    def __init__(self) -> None:
        self.barrier_result: Optional[tuple] = None
        self.event = None

    def latest_barrier_for_market(self, _market_id: str, *, max_age_s: float = 60.0):
        return self.barrier_result

    def latest_for_market(self, _market_id: str, max_age_s: Optional[float] = None):
        return self.event

    def age_for_market(self, _market_id: str):
        return 73.4 if self.event is not None else None

    def poll_inbox(self) -> None:
        return None

    def update_market_context(self, _context) -> None:
        return None


def _patch_externals(monkeypatch, *, spot=70100.0):
    """Stub out network-bound calls used by SharedDataFeed."""
    monkeypatch.setattr("scripts.trader.data_feed.find_active_15m_market", lambda: None)
    monkeypatch.setattr("scripts.trader.data_feed.get_current_btc_price", lambda: spot)
    monkeypatch.setattr("scripts.trader.data_feed.get_coinbase_exchange_ticker_price", lambda: None)
    monkeypatch.setattr("scripts.trader.data_feed.get_coinbase_spot_price", lambda: None)
    monkeypatch.setattr("scripts.trader.data_feed.run_terminal_script", lambda **kwargs: {})


def _make_feed(monkeypatch, config: GridConfig, now: datetime) -> SharedDataFeed:
    """Feed with a JUST-EXPIRED market — exercises the expiry-tick path."""
    _patch_externals(monkeypatch)
    feed = SharedDataFeed(config)
    start = now - timedelta(minutes=15)
    feed._current_market = {
        "market_id": "btc-updown-15m-test",
        "title": "Test Market",
        "end_date_utc": now - timedelta(seconds=1),
        "window_start_utc": start,
        "prob_up": 0.5,
        "prob_down": 0.5,
        "token_ids": {"UP": "up", "DOWN": "down"},
    }
    feed._current_market_id = "btc-updown-15m-test"
    feed._barrier = 70000.0
    feed._barrier_source = "chainlink"
    feed._barrier_captured = True
    return feed


def _make_active_feed(monkeypatch, config: GridConfig, now: datetime) -> SharedDataFeed:
    """Feed with an active market (not expired) — exercises the tick path."""
    _patch_externals(monkeypatch)
    feed = SharedDataFeed(config)
    start = now - timedelta(seconds=120)
    end = now + timedelta(seconds=180)
    feed._current_market = {
        "market_id": "btc-updown-15m-test",
        "title": "Test Market",
        "end_date_utc": end,
        "window_start_utc": start,
        "prob_up": 0.5,
        "prob_down": 0.5,
        "token_ids": {"UP": "up", "DOWN": "down"},
    }
    feed._current_market_id = "btc-updown-15m-test"
    feed._barrier = 70000.0
    feed._barrier_source = "chainlink"
    feed._barrier_captured = True
    return feed


def _make_capturing_feed(monkeypatch, now: datetime, *, window_age_s: int) -> SharedDataFeed:
    """Active feed with `_barrier_captured=False` so we exercise the
    capture path. Wires up a `_FakeBroker` so the broker-driven barrier
    branch is reachable; tests set ``feed._hybrid_broker.barrier_result``
    to control what calc has "published"."""
    _patch_externals(monkeypatch)
    feed = SharedDataFeed(GridConfig())
    feed._hybrid_broker = _FakeBroker()
    start = now - timedelta(seconds=window_age_s)
    end = start + timedelta(minutes=15)
    feed._current_market = {
        "market_id": "btc-updown-15m-test",
        "title": "Test Market",
        "end_date_utc": end,
        "window_start_utc": start,
        "prob_up": 0.5,
        "prob_down": 0.5,
        "token_ids": {"UP": "up", "DOWN": "down"},
    }
    feed._current_market_id = "btc-updown-15m-test"
    feed._barrier = None
    feed._barrier_captured = False
    feed._barrier_finalized = False
    feed._barrier_offset_s = None
    feed._barrier_source = ""
    return feed


def _freeze_time(monkeypatch, now: datetime) -> None:
    monkeypatch.setattr(
        "scripts.trader.data_feed.datetime",
        type("FakeDateTime", (), {"now": staticmethod(lambda tz=None: now)}),
    )


def test_shared_feed_clears_stale_entry_signals_without_fresh_calibration(monkeypatch):
    now = datetime.now(timezone.utc)
    feed = _make_active_feed(monkeypatch, GridConfig(), now)
    feed._hybrid_broker = _FakeBroker()
    feed._latest_entry_signals = ({"instance_id": "live", "direction": "UP"},)
    feed._latest_entry_signals_ts = now
    feed._latest_entry_signals_market_id = feed._current_market_id
    feed._fetch_order_books = lambda: (None, None, None, None, 0.5, 0.5)

    tick = feed.tick()

    assert tick.entry_signals == ()
    assert tick.entry_signal_age_s == -1.0
    assert tick.entry_signal_market_id == ""
    assert tick.entry_signal_stale_age_s == -1.0


def test_shared_feed_reports_stale_signal_age_when_calibration_ttl_expires(monkeypatch):
    class _StaleBroker(_FakeBroker):
        def latest_for_market(self, _market_id: str, max_age_s: Optional[float] = None):
            return None if max_age_s is not None else self.event

        def age_for_market(self, _market_id: str):
            return 73.4

    now = datetime.now(timezone.utc)
    feed = _make_active_feed(monkeypatch, GridConfig(), now)
    broker = _StaleBroker()
    broker.event = SimpleNamespace(market_id=feed._current_market_id)
    feed._hybrid_broker = broker
    feed._latest_entry_signals = ({"instance_id": "live", "direction": "UP"},)
    feed._latest_entry_signals_ts = now
    feed._latest_entry_signals_market_id = feed._current_market_id
    feed._fetch_order_books = lambda: (None, None, None, None, 0.5, 0.5)

    tick = feed.tick()

    assert tick.entry_signals == ()
    assert tick.entry_signal_age_s == -1.0
    assert tick.entry_signal_stale_age_s == 73.4


def test_shared_feed_reports_fresh_entry_signal_age_and_market(monkeypatch):
    now = datetime.now(timezone.utc)
    feed = _make_active_feed(monkeypatch, GridConfig(), now)
    ready_at = now - timedelta(seconds=5)
    signal = {"instance_id": "live", "market_id": feed._current_market_id, "direction": "UP"}
    broker = _FakeBroker()
    broker.event = SimpleNamespace(
        prob_above=0.7,
        prob_below=0.3,
        model_used="ssvi_surface",
        calibration_seconds=2.0,
        spot_price=70100.0,
        spot_source="calc",
        up_best_ask=0.51,
        down_best_ask=0.49,
        entry_signals=(signal,),
        ready_at=ready_at,
        market_id=feed._current_market_id,
    )
    feed._hybrid_broker = broker
    feed._fetch_order_books = lambda: (None, None, None, None, 0.5, 0.5)

    tick = feed.tick()

    assert tick.entry_signals == (signal,)
    assert tick.entry_signal_age_s >= 0.0
    assert tick.entry_signal_market_id == feed._current_market_id
    assert tick.entry_signal_stale_age_s == -1.0


# ---------------------------------------------------------------------------
# Expiry path
# ---------------------------------------------------------------------------


def test_expiry_defers_when_no_settlement_file(monkeypatch, tmp_path):
    """No settlement file → settlement is deferred. The position
    stays open (is_expired=False), settlement_pending=True. Crucially,
    even past the settlement_wait_seconds threshold there is NO Binance
    fallback — chainlink-only resolution. The trader retries on every
    tick until calc writes the file."""
    now = datetime(2026, 4, 15, 12, 0, tzinfo=timezone.utc)
    config = GridConfig(settlement_wait_seconds=20)
    feed = _make_feed(monkeypatch, config, now)
    feed.results_dir = tmp_path / "results"
    _freeze_time(monkeypatch, now)
    # Point the settlement-file lookup at an empty tmp_path (no files).
    monkeypatch.chdir(tmp_path)

    tick = feed.tick()
    assert tick.settlement_pending
    assert not tick.is_expired
    assert tick.final_spot is None

    # Even 25 s past expiry — no degraded fallback. Still pending.
    feed._expiry_wait_started_at = now - timedelta(seconds=25)
    tick = feed.tick()
    assert tick.settlement_pending
    assert not tick.is_expired
    assert tick.final_spot is None


def test_expiry_reads_chainlink_settlement_file(monkeypatch, tmp_path):
    """When calc has written a complete settlement file with chainlink
    final price, the consumer's _resolve_final_spot returns that price
    with source="chainlink_settlement"; settlement proceeds."""
    now = datetime(2026, 4, 15, 12, 0, tzinfo=timezone.utc)
    config = GridConfig(settlement_wait_seconds=0)
    feed = _make_feed(monkeypatch, config, now)
    feed.results_dir = tmp_path / "results"
    _freeze_time(monkeypatch, now)

    # Write a settlement file where the feed resolves runtime outputs.
    settlements_dir = feed.results_dir / "settlements"
    settlements_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "market_id": feed._current_market_id,
        "window_start_iso": (now - timedelta(minutes=15)).isoformat(),
        "window_end_iso": (now - timedelta(seconds=1)).isoformat(),
        "strike": 70000.0,
        "strike_offset_s": 0.0,
        "strike_source": "chainlink",
        "strike_captured_at_iso": now.isoformat(),
        "settlement_price": 69950.0,
        "settlement_offset_s": 12.0,
        "settlement_source": "chainlink",
        "settlement_captured_at_iso": now.isoformat(),
    }
    import json as _json
    (settlements_dir / f"{feed._current_market_id}.json").write_text(
        _json.dumps(payload), encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)

    tick = feed.tick()

    assert tick.is_expired
    assert tick.final_spot == 69950.0
    assert tick.final_spot_source == "chainlink_settlement"


def test_expiry_abandoned_settlement_advances_to_next_market(monkeypatch, tmp_path):
    """An abandoned settlement should not pin the feed to the old market.

    Once calc gives up on the old window, the grid should keep the current
    tick blocked but allow the next tick to scan the next active market
    immediately.
    """
    now = datetime(2026, 4, 15, 12, 0, tzinfo=timezone.utc)
    config = GridConfig(settlement_wait_seconds=0)
    feed = _make_feed(monkeypatch, config, now)
    feed.results_dir = tmp_path / "results"
    _freeze_time(monkeypatch, now)
    monkeypatch.chdir(tmp_path)

    settlements_dir = feed.results_dir / "settlements"
    settlements_dir.mkdir(parents=True, exist_ok=True)
    abandoned_payload = {
        "market_id": feed._current_market_id,
        "window_start_iso": (now - timedelta(minutes=15)).isoformat(),
        "window_end_iso": (now - timedelta(seconds=1)).isoformat(),
        "strike": 70000.0,
        "strike_offset_s": 0.0,
        "strike_source": "chainlink",
        "strike_captured_at_iso": now.isoformat(),
        "settlement_price": None,
        "settlement_offset_s": None,
        "settlement_source": None,
        "settlement_captured_at_iso": None,
        "settlement_status": "abandoned",
        "settlement_error": "stale (4558s)",
        "settlement_abandoned_at_iso": now.isoformat(),
    }
    import json as _json
    (settlements_dir / f"{feed._current_market_id}.json").write_text(
        _json.dumps(abandoned_payload),
        encoding="utf-8",
    )

    broker = _FakeBroker()
    feed._hybrid_broker = broker

    tick = feed.tick()

    assert tick.market_id == feed._current_market_id
    assert tick.settlement_pending
    assert not tick.is_expired
    assert tick.final_spot is None
    assert tick.final_spot_source == "settlement_abandoned"
    assert feed._expiry_reported is True

    next_start = now - timedelta(minutes=2)
    next_end = now + timedelta(minutes=13)
    next_market_id = f"btc-updown-15m-{int(next_start.timestamp())}"
    next_event = {
        "slug": next_market_id,
        "title": "Bitcoin Up or Down - April 15, 11:58AM-12:13PM ET",
        "startTime": next_start.isoformat().replace("+00:00", "Z"),
        "endDate": next_end.isoformat().replace("+00:00", "Z"),
        "closed": False,
    }
    monkeypatch.setattr("scripts.trader.data_feed.find_active_15m_market", lambda: next_event)
    broker.event = SimpleNamespace(
        prob_above=0.6,
        prob_below=0.4,
        model_used="ssvi_surface",
        calibration_seconds=1.0,
        spot_price=70100.0,
        spot_source="calc",
        up_best_ask=0.51,
        down_best_ask=0.49,
        entry_signals=(),
        ready_at=now,
        market_id=next_market_id,
    )

    next_tick = feed.tick()

    assert next_tick.market_id == next_market_id
    assert next_tick.is_new_market
    assert not next_tick.settlement_pending
    assert not next_tick.is_expired


# ---------------------------------------------------------------------------
# Spot fallback chain (no Chainlink — calc owns it)
# ---------------------------------------------------------------------------


def test_current_spot_falls_back_to_coinbase_exchange_when_binance_unavailable(monkeypatch):
    now = datetime(2026, 4, 15, 12, 0, tzinfo=timezone.utc)
    feed = _make_active_feed(monkeypatch, GridConfig(), now)
    _freeze_time(monkeypatch, now)
    monkeypatch.setattr("scripts.trader.data_feed.get_current_btc_price", lambda: None)
    monkeypatch.setattr("scripts.trader.data_feed.get_coinbase_exchange_ticker_price", lambda: 70333.0)

    tick = feed.tick()

    assert tick.spot_price == 70333.0
    assert tick.spot_source == "coinbase_exchange"


def test_current_spot_falls_back_to_coinbase_data_when_exchange_unavailable(monkeypatch):
    now = datetime(2026, 4, 15, 12, 0, tzinfo=timezone.utc)
    feed = _make_active_feed(monkeypatch, GridConfig(), now)
    _freeze_time(monkeypatch, now)
    monkeypatch.setattr("scripts.trader.data_feed.get_current_btc_price", lambda: None)
    monkeypatch.setattr("scripts.trader.data_feed.get_coinbase_exchange_ticker_price", lambda: None)
    monkeypatch.setattr("scripts.trader.data_feed.get_coinbase_spot_price", lambda: 70444.0)

    tick = feed.tick()

    assert tick.spot_price == 70444.0
    assert tick.spot_source == "coinbase_spot"


def test_current_spot_uses_last_good_spot_after_remote_source_outage(monkeypatch):
    now = datetime(2026, 4, 15, 12, 0, tzinfo=timezone.utc)
    feed = _make_active_feed(monkeypatch, GridConfig(), now)
    feed._last_good_spot = 70444.0
    feed._last_good_spot_source = "coinbase_exchange"
    _freeze_time(monkeypatch, now)
    monkeypatch.setattr("scripts.trader.data_feed.get_current_btc_price", lambda: None)

    tick = feed.tick()

    assert tick.spot_price == 70444.0
    assert tick.spot_source == "last_good_spot"


def test_current_spot_uses_barrier_fallback_when_all_live_sources_fail(monkeypatch):
    now = datetime(2026, 4, 15, 12, 0, tzinfo=timezone.utc)
    feed = _make_active_feed(monkeypatch, GridConfig(), now)
    _freeze_time(monkeypatch, now)
    monkeypatch.setattr("scripts.trader.data_feed.get_current_btc_price", lambda: None)
    monkeypatch.setattr("scripts.trader.data_feed.get_coinbase_exchange_ticker_price", lambda: None)
    monkeypatch.setattr("scripts.trader.data_feed.get_coinbase_spot_price", lambda: None)

    tick = feed.tick()

    assert tick.spot_price == 70000.0
    assert tick.spot_source == "barrier_fallback"


# ---------------------------------------------------------------------------
# Broker-driven barrier capture (post-centralize-chainlink architecture)
# ---------------------------------------------------------------------------


def test_data_feed_uses_calc_chainlink_barrier_when_event_present(monkeypatch):
    """Calc has published an event with barrier_source='chainlink'.
    SharedDataFeed reads the barrier off the broker and marks the source —
    `barrier_confirmed = source in ('chainlink', 'polymarket')` flips
    True for entries."""
    now = datetime(2026, 4, 15, 12, 0, 5, tzinfo=timezone.utc)
    feed = _make_capturing_feed(monkeypatch, now, window_age_s=5)
    feed._hybrid_broker.barrier_result = (70_376.82, "chainlink", 0.0)
    _freeze_time(monkeypatch, now)
    # Sanity check is on by default; provide a Binance kline within tolerance.
    monkeypatch.setattr("scripts.trader.data_feed.get_binance_price_at", lambda timestamp: 70_380.0)

    feed.tick()

    assert feed._barrier == 70_376.82
    assert feed._barrier_source == "chainlink"
    assert feed._barrier_offset_s == 0.0
    assert feed._barrier_captured is True
    assert feed._barrier_finalized is True


def test_data_feed_uses_calc_polymarket_barrier_when_event_present(monkeypatch):
    """Calc has published an event with barrier_source='polymarket' (the
    new primary post-091be1b: openPrice from /api/crypto/crypto-price).
    SharedDataFeed must accept it the same way it accepts 'chainlink' —
    `barrier_confirmed` flips True so entries unblock.

    Regression test for the bug where data_feed.py:737 hardcoded
    `source == 'chainlink'` after calc had switched its primary source
    to polymarket — every entry was silently rejected for ~1.5 hours
    across the live trader and the configured grid instances."""
    now = datetime(2026, 4, 15, 12, 0, 35, tzinfo=timezone.utc)
    feed = _make_capturing_feed(monkeypatch, now, window_age_s=35)
    feed._hybrid_broker.barrier_result = (77_972.55, "polymarket", 0.0)
    _freeze_time(monkeypatch, now)
    # Sanity check should fire for polymarket too; provide an in-tolerance kline.
    monkeypatch.setattr("scripts.trader.data_feed.get_binance_price_at", lambda timestamp: 77_980.0)

    feed.tick()

    assert feed._barrier == 77_972.55
    assert feed._barrier_source == "polymarket"
    assert feed._barrier_offset_s == 0.0
    assert feed._barrier_captured is True
    assert feed._barrier_finalized is True


def test_data_feed_binance_sanity_rejects_divergent_polymarket(monkeypatch):
    """Defense-in-depth: if the polymarket-sourced barrier disagrees
    with the local Binance kline by more than the 1% tolerance, downgrade
    to 'binance_sanity_failed' so `barrier_confirmed` stays False. The
    cross-check applies to polymarket the same way it does to chainlink."""
    now = datetime(2026, 4, 15, 12, 0, 35, tzinfo=timezone.utc)
    feed = _make_capturing_feed(monkeypatch, now, window_age_s=35)
    feed._hybrid_broker.barrier_result = (80_000.0, "polymarket", 0.0)
    _freeze_time(monkeypatch, now)
    monkeypatch.setattr("scripts.trader.data_feed.get_binance_price_at", lambda timestamp: 70_000.0)

    feed.tick()

    assert feed._barrier == 80_000.0  # value preserved for diagnostics
    assert feed._barrier_source == "binance_sanity_failed"
    assert feed._barrier_captured is True
    assert feed._barrier_finalized is True


def test_data_feed_blocks_entries_when_calc_barrier_is_binance(monkeypatch):
    """Calc fell back to its own Binance kline. The barrier_source on the
    event is 'binance' — live/grid trust the value but do not gate entries
    (`barrier_confirmed=False`)."""
    now = datetime(2026, 4, 15, 12, 0, 5, tzinfo=timezone.utc)
    feed = _make_capturing_feed(monkeypatch, now, window_age_s=5)
    feed._hybrid_broker.barrier_result = (69_950.0, "binance", None)
    _freeze_time(monkeypatch, now)
    # No sanity call expected (only chainlink triggers it). Stub just in case.
    monkeypatch.setattr("scripts.trader.data_feed.get_binance_price_at", lambda timestamp: 69_950.0)

    feed.tick()

    assert feed._barrier == 69_950.0
    assert feed._barrier_source == "binance"  # NOT 'chainlink' → blocks entries
    assert feed._barrier_captured is True


def test_data_feed_falls_back_to_binance_local_when_no_calibration_yet(monkeypatch):
    """Calc has not yet published an event for this market (early in the
    window or calc starting up). SharedDataFeed pulls a tentative Binance
    kline so the rest of the tick still populates, but tags the source
    'binance_local' so `barrier_confirmed` stays False."""
    now = datetime(2026, 4, 15, 12, 0, 2, tzinfo=timezone.utc)
    feed = _make_capturing_feed(monkeypatch, now, window_age_s=2)
    feed._hybrid_broker.barrier_result = None  # calc silent
    _freeze_time(monkeypatch, now)
    monkeypatch.setattr("scripts.trader.data_feed.get_binance_price_at", lambda timestamp: 69_950.0)

    feed.tick()

    assert feed._barrier == 69_950.0
    assert feed._barrier_source == "binance_local"
    assert feed._barrier_captured is True
    assert feed._barrier_finalized is False  # door open for calc's first event


def test_data_feed_binance_sanity_rejects_divergent_chainlink(monkeypatch):
    """Calc says chainlink barrier=$80,000 but the local Binance kline at
    window-start is $70,000 — a 14% divergence well above the 1% tolerance.
    The sanity check fails: source is downgraded to 'binance_sanity_failed'
    so `barrier_confirmed` stays False even though calc reported chainlink."""
    now = datetime(2026, 4, 15, 12, 0, 5, tzinfo=timezone.utc)
    feed = _make_capturing_feed(monkeypatch, now, window_age_s=5)
    feed._hybrid_broker.barrier_result = (80_000.0, "chainlink", 1.5)
    _freeze_time(monkeypatch, now)
    monkeypatch.setattr("scripts.trader.data_feed.get_binance_price_at", lambda timestamp: 70_000.0)

    feed.tick()

    assert feed._barrier == 80_000.0  # value preserved for diagnostics
    assert feed._barrier_source == "binance_sanity_failed"
    assert feed._barrier_captured is True
    assert feed._barrier_finalized is True


def test_data_feed_binance_sanity_disabled_passes_divergent_chainlink_through(monkeypatch):
    """Same setup as the divergence test but the operator has disabled the
    sanity check via config. Calc's chainlink barrier is accepted as-is."""
    now = datetime(2026, 4, 15, 12, 0, 5, tzinfo=timezone.utc)
    config = GridConfig(binance_sanity_check_enabled=False)
    _patch_externals(monkeypatch)
    feed = SharedDataFeed(config)
    feed._hybrid_broker = _FakeBroker()
    start = now - timedelta(seconds=5)
    feed._current_market = {
        "market_id": "btc-updown-15m-test",
        "title": "Test",
        "end_date_utc": start + timedelta(minutes=15),
        "window_start_utc": start,
        "prob_up": 0.5,
        "prob_down": 0.5,
        "token_ids": {"UP": "up", "DOWN": "down"},
    }
    feed._current_market_id = "btc-updown-15m-test"
    feed._hybrid_broker.barrier_result = (80_000.0, "chainlink", 1.5)
    _freeze_time(monkeypatch, now)
    monkeypatch.setattr("scripts.trader.data_feed.get_binance_price_at", lambda timestamp: 70_000.0)

    feed.tick()

    assert feed._barrier == 80_000.0
    assert feed._barrier_source == "chainlink"  # sanity check was bypassed
    assert feed._barrier_finalized is True


def test_data_feed_binance_sanity_passes_when_binance_unreachable(monkeypatch):
    """If Binance is unreachable (RuntimeError/None response), the sanity
    check degrades gracefully — trust calc, do NOT block entries on a
    transient Binance outage."""
    now = datetime(2026, 4, 15, 12, 0, 5, tzinfo=timezone.utc)
    feed = _make_capturing_feed(monkeypatch, now, window_age_s=5)
    feed._hybrid_broker.barrier_result = (70_376.82, "chainlink", 0.0)
    _freeze_time(monkeypatch, now)

    def _binance_explodes(_timestamp):
        raise RuntimeError("binance unreachable")

    monkeypatch.setattr("scripts.trader.data_feed.get_binance_price_at", _binance_explodes)

    feed.tick()

    assert feed._barrier_source == "chainlink"  # trusted despite Binance outage
    assert feed._barrier == 70_376.82


# ---------------------------------------------------------------------------
# Scan rate-limit + next_sleep around expiry (independent of Chainlink)
# ---------------------------------------------------------------------------


def test_market_expiry_resets_scan_rate_limit(monkeypatch):
    """When the current market expires, tick() must reset _last_market_scan
    to 0.0 so the NEXT tick scans immediately — not 30s later. Without this,
    the new market is detected late in the next window."""
    now = datetime(2026, 4, 15, 12, 0, 0, tzinfo=timezone.utc)
    feed = _make_capturing_feed(monkeypatch, now, window_age_s=300)
    feed._current_market["end_date_utc"] = now
    feed._current_market["window_start_utc"] = now - timedelta(minutes=15)
    feed._expiry_reported = False
    import time as _time
    feed._last_market_scan = _time.time() - 5.0
    _freeze_time(monkeypatch, now)
    monkeypatch.setattr("scripts.trader.data_feed.get_binance_price_at", lambda timestamp: 69950.0)

    feed.tick()

    assert feed._last_market_scan == 0.0


def test_next_sleep_short_in_scan_mode(monkeypatch):
    """Post-expiry / awaiting next market, next_sleep must return a SHORT
    value (2s, not the 30s scan interval). Otherwise the next tick fires
    too late to capture a fresh barrier from calc within the entry window."""
    now = datetime(2026, 4, 15, 12, 0, 0, tzinfo=timezone.utc)
    feed = _make_capturing_feed(monkeypatch, now, window_age_s=300)
    feed._current_market = None
    feed._expiry_reported = False
    assert feed.next_sleep() == 2.0

    feed._current_market = {
        "market_id": "btc-updown-15m-test",
        "end_date_utc": now,
        "window_start_utc": now - timedelta(minutes=15),
    }
    feed._expiry_reported = True
    assert feed.next_sleep() == 2.0


def test_next_sleep_zero_during_active_market(monkeypatch):
    """During an active market (not expired), next_sleep returns
    tick_interval_seconds (0 by default = run as fast as possible)."""
    now = datetime(2026, 4, 15, 12, 0, 0, tzinfo=timezone.utc)
    _freeze_time(monkeypatch, now)
    feed = _make_capturing_feed(monkeypatch, now, window_age_s=60)
    feed._expiry_reported = False
    assert feed.next_sleep() == feed.config.tick_interval_seconds
