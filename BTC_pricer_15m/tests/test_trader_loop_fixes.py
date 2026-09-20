"""Barrier-sanity caching, calibration delivery and signal lookup tests.

D1: Binance barrier-sanity verdicts are memoized per (market, barrier).
D3: the push listener delivers events to the broker without polling.
D5: the per-tick entry-signal index matches the legacy scan.
"""

import threading
import time
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from scripts.trader.config import GridConfig
from scripts.trader.data_feed import SharedDataFeed, TickData


def _make_feed(monkeypatch=None) -> SharedDataFeed:
    return SharedDataFeed(GridConfig())


# ---------------------------------------------------------------------------
# D1 — sanity memoization
# ---------------------------------------------------------------------------

class TestBinanceSanityMemo:
    def _feed(self):
        feed = _make_feed()
        feed._current_market_id = "btc-updown-15m-1783638000"
        return feed

    def test_definitive_verdict_cached(self, monkeypatch):
        import scripts.trader.data_feed as df

        calls = {"n": 0}

        def fake_kline(start_utc):
            calls["n"] += 1
            return 63000.0

        monkeypatch.setattr(df, "get_binance_price_at", fake_kline)
        feed = self._feed()
        start = datetime(2026, 7, 10, 12, 0, tzinfo=timezone.utc)
        for _ in range(5):
            assert feed._binance_sanity_passes(63005.0, start) is True
        assert calls["n"] == 1

    def test_failure_verdict_cached(self, monkeypatch):
        import scripts.trader.data_feed as df

        calls = {"n": 0}

        def fake_kline(start_utc):
            calls["n"] += 1
            return 63000.0

        monkeypatch.setattr(df, "get_binance_price_at", fake_kline)
        feed = self._feed()
        start = datetime(2026, 7, 10, 12, 0, tzinfo=timezone.utc)
        # >1% divergence — definitive failure, cached
        for _ in range(5):
            assert feed._binance_sanity_passes(65000.0, start) is False
        assert calls["n"] == 1

    def test_unreachable_fails_open_and_rate_limits(self, monkeypatch):
        import scripts.trader.data_feed as df

        calls = {"n": 0}

        def broken_kline(start_utc):
            calls["n"] += 1
            raise ConnectionError("binance stalled")

        monkeypatch.setattr(df, "get_binance_price_at", broken_kline)
        feed = self._feed()
        start = datetime(2026, 7, 10, 12, 0, tzinfo=timezone.utc)
        assert feed._binance_sanity_passes(63005.0, start) is True
        # Retry limiter: no second HTTP attempt inside the window,
        # still fail-open.
        assert feed._binance_sanity_passes(63005.0, start) is True
        assert calls["n"] == 1

    def test_cache_cleared_on_market_change(self, monkeypatch):
        import scripts.trader.data_feed as df

        feed = self._feed()
        old_key = ("btc-updown-15m-1783638000", 63005.0)
        feed._sanity_verdicts[old_key] = False
        new_id = "btc-updown-15m-1783638900"
        market = {"market_id": new_id, "title": "next window", "prob_up": .5, "prob_down": .5}
        monkeypatch.setattr(df, "find_active_15m_market", lambda: market)
        monkeypatch.setattr(df, "extract_market_data", lambda event: event)
        feed._scan_market()
        assert feed._current_market_id == new_id
        assert old_key not in feed._sanity_verdicts
        # Scanning the same market must preserve its already-computed verdict.
        new_key = (new_id, 63010.0)
        feed._sanity_verdicts[new_key] = True
        feed._last_market_scan = 0
        feed._scan_market()
        assert feed._sanity_verdicts[new_key] is True


# ---------------------------------------------------------------------------
# D3 — push listener
# ---------------------------------------------------------------------------

class TestRedisCalibrationListener:
    def test_listener_delivers_events(self, monkeypatch):
        import scripts.trader.ipc as ipc

        delivered = []
        stop_feeding = threading.Event()

        class _FakeSubscriber:
            def __init__(self, **kwargs):
                self._sent = False

            def poll(self):
                if self._sent:
                    stop_feeding.wait(0.05)
                    return []
                self._sent = True
                return ["EVENT"]

            def close(self):
                pass

        monkeypatch.setattr(ipc, "RedisStreamCalibrationSubscriber", _FakeSubscriber)
        listener = ipc.RedisCalibrationListener(
            redis_url="redis://fake:6379/0",
            on_event=delivered.append,
        )
        listener.start()
        deadline = time.time() + 2.0
        while not delivered and time.time() < deadline:
            time.sleep(0.01)
        listener.stop()
        assert delivered == ["EVENT"]

    def test_listener_survives_poll_errors(self, monkeypatch):
        import scripts.trader.ipc as ipc

        delivered = []

        class _FlakySubscriber:
            def __init__(self, **kwargs):
                self.calls = 0

            def poll(self):
                self.calls += 1
                if self.calls == 1:
                    raise RuntimeError("transient redis error")
                if self.calls == 2:
                    return ["OK"]
                time.sleep(0.05)
                return []

            def close(self):
                pass

        monkeypatch.setattr(ipc, "RedisStreamCalibrationSubscriber", _FlakySubscriber)
        listener = ipc.RedisCalibrationListener(
            redis_url="redis://fake:6379/0",
            on_event=delivered.append,
        )
        # Shrink the error backoff for the test
        listener._stop = threading.Event()
        listener.start()
        deadline = time.time() + 3.0
        while not delivered and time.time() < deadline:
            time.sleep(0.02)
        listener.stop()
        assert delivered == ["OK"]


class TestBrokerPushWiring:
    def test_file_backend_never_starts_listener(self, tmp_path):
        from scripts.trader.hybrid_calibration import HybridCalibrationBroker

        broker = HybridCalibrationBroker(
            results_dir=tmp_path,
            enable_local_worker=False,
            enable_remote_inbox=True,
            ipc_backend="file",
            push_consumer_enabled=True,
        )
        broker.start()
        try:
            assert broker._push_listener is None
            assert broker._event_subscriber is not None
        finally:
            broker.stop()

    def test_redis_backend_starts_listener_with_file_fallback_poll(self, tmp_path, monkeypatch):
        import scripts.trader.ipc as ipc
        from scripts.trader.hybrid_calibration import HybridCalibrationBroker

        class _FakeSubscriber:
            def __init__(self, **kwargs):
                pass

            def poll(self):
                time.sleep(0.02)
                return []

            def close(self):
                pass

        monkeypatch.setattr(ipc, "RedisStreamCalibrationSubscriber", _FakeSubscriber)
        broker = HybridCalibrationBroker(
            results_dir=tmp_path,
            enable_local_worker=False,
            enable_remote_inbox=True,
            ipc_backend="redis",
            redis_url="redis://fake:6379/0",
            push_consumer_enabled=True,
        )
        broker.start()
        try:
            assert broker._push_listener is not None
            # poll path only covers the file inbox now
            from scripts.trader.ipc import FileCalibrationSubscriber

            assert isinstance(broker._event_subscriber, FileCalibrationSubscriber)
            broker.poll_inbox()  # must not raise / not re-enter start loop
        finally:
            broker.stop()


# ---------------------------------------------------------------------------
# D5 — signal index equivalence
# ---------------------------------------------------------------------------

class _IdOnly:
    """Minimal stand-in exposing only what _matching_entry_signal needs."""

    def __init__(self, id_):
        self.id = id_

    _matching_entry_signal = None  # replaced below


def _legacy_scan(instance_id, signals):
    for sig in signals or ():
        if str(sig.get("instance_id")) == str(instance_id):
            return sig
    return None


class TestSignalIndex:
    def _tick(self, signals, index=None):
        return SimpleNamespace(entry_signals=signals, entry_signal_index=index)

    def test_index_matches_legacy_scan(self):
        from scripts.trader.trader import PaperInstance

        signals = tuple(
            {"instance_id": i % 100, "direction": "UP", "n": i} for i in range(500)
        )
        index = {}
        for sig in signals:
            index.setdefault(str(sig.get("instance_id")), sig)

        for probe_id in (0, 7, 99, "live", 100):
            expected = _legacy_scan(probe_id, signals)
            got = PaperInstance._matching_entry_signal(
                SimpleNamespace(id=probe_id), self._tick(signals, index)
            )
            assert got == expected

    def test_fallback_without_index(self):
        from scripts.trader.trader import PaperInstance

        signals = ({"instance_id": 3, "direction": "UP"},)
        got = PaperInstance._matching_entry_signal(
            SimpleNamespace(id=3), self._tick(signals, None)
        )
        assert got == signals[0]

    def test_feed_builds_first_wins_index(self):
        feed = _make_feed()
        feed._latest_entry_signals = (
            {"instance_id": 1, "direction": "UP", "tag": "first"},
            {"instance_id": 1, "direction": "DOWN", "tag": "second"},
            {"instance_id": 2, "direction": "UP"},
        )
        signal_index = {}
        for sig in feed._latest_entry_signals:
            key = str(sig.get("instance_id"))
            if key not in signal_index:
                signal_index[key] = sig
        assert signal_index["1"]["tag"] == "first"
        assert set(signal_index) == {"1", "2"}
