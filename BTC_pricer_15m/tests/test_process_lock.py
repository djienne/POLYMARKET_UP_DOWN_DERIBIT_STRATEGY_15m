from __future__ import annotations

import json
import os
import threading
import time
import uuid

import pytest

from scripts.trader.process_lock import (
    CompositeProcessLock,
    RedisLeaseLock,
    lock_owner_can_use_host_pid,
    lock_owner_is_container_namespace,
    parse_lock_owner,
)


class _FakeRedis:
    def __init__(self):
        self.store: dict[str, str] = {}

    def set(self, key, value, nx=False, px=None):
        if nx and key in self.store:
            return False
        self.store[key] = value
        return True

    def get(self, key):
        return self.store.get(key)

    def eval(self, script, num_keys, key, needle, *args):
        raw = self.store.get(key)
        if raw is None:
            return 1 if "DEL" in script else 0
        if needle not in raw:
            return 0
        if "DEL" in script:
            self.store.pop(key, None)
            return 1
        if args:
            self.store[key] = args[0]
            return 1
        return 0


def _owner(token: str, **overrides):
    data = {
        "schema_version": 2,
        "project": "POLYMARKET_UP_DOWN_15m",
        "mode": "live",
        "lock_kind": "redis_lease",
        "hostname": "container-a",
        "pid": 1,
        "container_id": "container-a",
        "boot_id": "boot-1",
        "timestamp": "2026-05-20T00:00:00Z",
        "token": token,
    }
    data.update(overrides)
    return data


def test_redis_lease_lock_is_token_safe():
    redis = _FakeRedis()
    first = RedisLeaseLock("redis://example/0", "lock:key", 1000, _owner("aaa"), client=redis)
    second = RedisLeaseLock("redis://example/0", "lock:key", 1000, _owner("bbb"), client=redis)

    assert first.acquire() is True
    assert second.acquire() is False
    assert second.release() is False
    assert parse_lock_owner(redis.store["lock:key"])["token"] == "aaa"

    assert first.renew() is True
    assert first.release() is True
    assert "lock:key" not in redis.store


def test_composite_lock_releases_redis_when_file_lock_fails():
    redis = _FakeRedis()
    redis_lock = RedisLeaseLock("redis://example/0", "lock:key", 1000, _owner("aaa"), client=redis)
    lock = CompositeProcessLock(
        redis_lock=redis_lock,
        file_acquire=lambda: False,
        file_release=lambda: None,
        renew_interval_ms=100,
    )

    assert lock.acquire() is False
    assert "lock:key" not in redis.store


def test_composite_fail_open_file_lock_is_released():
    class _BrokenRedisLock(RedisLeaseLock):
        def acquire(self):
            raise RuntimeError("redis unavailable")

        def release(self):
            raise AssertionError("redis release should not run without lease")

    acquired = []
    released = []
    lock = CompositeProcessLock(
        redis_lock=_BrokenRedisLock("redis://example/0", "lock:key", 1000, _owner("aaa"), client=_FakeRedis()),
        file_acquire=lambda: acquired.append(True) or True,
        file_release=lambda: released.append(True),
        renew_interval_ms=100,
        fail_closed=False,
    )

    assert lock.acquire() is True
    lock.release()
    assert acquired == [True]
    assert released == [True]


def test_composite_lock_reports_lost_redis_lease():
    redis = _FakeRedis()
    redis_lock = RedisLeaseLock("redis://example/0", "lock:key", 1000, _owner("aaa"), client=redis)
    released = []
    lock = CompositeProcessLock(
        redis_lock=redis_lock,
        file_acquire=lambda: True,
        file_release=lambda: released.append(True),
        renew_interval_ms=100,
    )
    lost = threading.Event()

    assert lock.acquire() is True
    lock.start_renewal(lambda reason: lost.set())
    redis.store["lock:key"] = json.dumps(_owner("other"), separators=(",", ":"))

    assert lost.wait(timeout=2.0)
    lock.release()
    assert released == [True]


def test_v2_container_lock_owner_is_not_host_pid_checked():
    owner = _owner("aaa", pid=1, container_id="abc123def456")

    assert lock_owner_is_container_namespace(owner) is True
    assert lock_owner_can_use_host_pid(owner) is False


def test_real_redis_lease_expires_and_stale_owner_cannot_renew_or_release():
    """Run against a disposable isolated Redis, never the production broker."""
    url = os.environ.get("TEST_REDIS_URL")
    if not url:
        pytest.skip("requires isolated TEST_REDIS_URL")
    import redis

    client = redis.Redis.from_url(url, decode_responses=True)
    key = f"audit:lease:{uuid.uuid4().hex}"
    first = RedisLeaseLock(url, key, 300, _owner("first"), client=client)
    second = RedisLeaseLock(url, key, 1000, _owner("second"), client=client)
    try:
        assert first.acquire()
        assert not second.acquire()
        assert first.renew()
        assert 0 < client.pttl(key) <= 300
        deadline = time.monotonic() + 3
        while client.exists(key) and time.monotonic() < deadline:
            time.sleep(.01)
        assert not client.exists(key)
        assert second.acquire()
        assert not first.renew()
        assert not first.release()
        assert second.current_owner()["token"] == "second"
        assert second.release()
        assert not client.exists(key)
    finally:
        client.delete(key)
        client.close()
