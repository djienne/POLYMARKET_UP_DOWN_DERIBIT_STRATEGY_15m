"""Process-lock helpers for Docker-safe trader concurrency guards."""

from __future__ import annotations

import json
import os
import secrets
import socket
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional


PROJECT_NAME = "POLYMARKET_UP_DOWN_15m"


def _read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8").strip()
    except OSError:
        return ""


def current_boot_id() -> str:
    return _read_text(Path("/proc/sys/kernel/random/boot_id"))


def current_container_id() -> str:
    """Best-effort stable container identity.

    Docker usually exposes the short container id as HOSTNAME and/or in
    /proc/self/cgroup. Returning an empty string on a host process keeps old
    non-Docker lock behavior unchanged.
    """
    ids: list[str] = []
    cgroup = _read_text(Path("/proc/self/cgroup"))
    for line in cgroup.splitlines():
        for chunk in line.replace("\\", "/").split("/"):
            chunk = chunk.strip()
            if len(chunk) >= 12 and all(c in "0123456789abcdef" for c in chunk.lower()):
                ids.append(chunk[:64])
    if Path("/.dockerenv").exists():
        hostname = os.getenv("HOSTNAME") or socket.gethostname()
        if hostname:
            ids.append(hostname)
    return ids[0] if ids else ""


def build_lock_owner(
    *,
    token: Optional[str] = None,
    project: str = PROJECT_NAME,
    mode: str = "trader",
    lock_kind: str = "file",
) -> dict:
    return {
        "schema_version": 2,
        "project": project,
        "mode": mode,
        "lock_kind": lock_kind,
        "hostname": socket.gethostname(),
        "pid": os.getpid(),
        "container_id": current_container_id(),
        "boot_id": current_boot_id(),
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "token": token or secrets.token_hex(16),
    }


def parse_lock_owner(content: str) -> dict:
    """Parse v2 JSON, legacy JSON, host:pid, or pid-only lock payloads."""
    content = (content or "").strip()
    if not content:
        raise ValueError("empty lock owner")
    try:
        raw = json.loads(content)
    except ValueError:
        raw = None
    if isinstance(raw, dict):
        try:
            pid = int(raw.get("pid") or 0)
        except (TypeError, ValueError):
            pid = 0
        return {
            "schema_version": int(raw.get("schema_version") or 1),
            "project": str(raw.get("project") or ""),
            "mode": str(raw.get("mode") or ""),
            "lock_kind": str(raw.get("lock_kind") or "file"),
            "hostname": str(raw.get("hostname") or raw.get("host") or ""),
            "pid": pid,
            "container_id": str(raw.get("container_id") or ""),
            "boot_id": str(raw.get("boot_id") or ""),
            "timestamp": str(raw.get("timestamp") or ""),
            "token": str(raw.get("token") or ""),
        }
    if ":" in content:
        host, pid_text = content.rsplit(":", 1)
    else:
        host, pid_text = "", content
    return {
        "schema_version": 0,
        "project": "",
        "mode": "",
        "lock_kind": "legacy",
        "hostname": host,
        "pid": int(pid_text),
        "container_id": "",
        "boot_id": "",
        "timestamp": "",
        "token": "",
    }


def read_lock_owner(path: Path) -> Optional[dict]:
    try:
        return parse_lock_owner(path.read_text(encoding="utf-8").strip())
    except (OSError, ValueError):
        return None


def lock_owner_is_current_host(owner: dict) -> bool:
    owner_container = str(owner.get("container_id") or "")
    current_container = current_container_id()
    if owner_container and current_container:
        return owner_container == current_container
    hostname = str(owner.get("hostname") or "")
    return bool(hostname and hostname == socket.gethostname())


def lock_owner_is_container_namespace(owner: dict) -> bool:
    if owner.get("container_id"):
        return True
    return int(owner.get("pid") or 0) == 1 and bool(owner.get("hostname"))


def lock_owner_can_use_host_pid(owner: dict) -> bool:
    if not owner.get("hostname") and not owner.get("container_id"):
        return True
    return not lock_owner_is_container_namespace(owner) and lock_owner_is_current_host(owner)


def _redis_from_url(redis_url: str):
    import redis

    return redis.Redis.from_url(redis_url, decode_responses=True)


@dataclass
class RedisLeaseLock:
    redis_url: str
    key: str
    ttl_ms: int
    owner: dict
    client: object | None = None

    def __post_init__(self) -> None:
        self.ttl_ms = int(self.ttl_ms)
        self.owner = dict(self.owner)
        self.owner["token"] = str(self.owner.get("token") or secrets.token_hex(16))
        self._client = self.client

    @property
    def token(self) -> str:
        return str(self.owner["token"])

    def _payload(self) -> str:
        owner = dict(self.owner)
        owner["timestamp"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        return json.dumps(owner, sort_keys=True, separators=(",", ":"))

    def _redis(self):
        if self._client is None:
            self._client = _redis_from_url(self.redis_url)
        return self._client

    def acquire(self) -> bool:
        return bool(self._redis().set(self.key, self._payload(), nx=True, px=self.ttl_ms))

    def current_owner(self) -> Optional[dict]:
        raw = self._redis().get(self.key)
        if not raw:
            return None
        try:
            return parse_lock_owner(str(raw))
        except ValueError:
            return None

    def renew(self) -> bool:
        needle = f'"token":"{self.token}"'
        script = """
local raw = redis.call('GET', KEYS[1])
if not raw then
  return 0
end
if string.find(raw, ARGV[1], 1, true) then
  redis.call('SET', KEYS[1], ARGV[2], 'PX', tonumber(ARGV[3]))
  return 1
end
return 0
"""
        return bool(self._redis().eval(script, 1, self.key, needle, self._payload(), str(self.ttl_ms)))

    def release(self) -> bool:
        needle = f'"token":"{self.token}"'
        script = """
local raw = redis.call('GET', KEYS[1])
if not raw then
  return 1
end
if string.find(raw, ARGV[1], 1, true) then
  redis.call('DEL', KEYS[1])
  return 1
end
return 0
"""
        return bool(self._redis().eval(script, 1, self.key, needle))


class CompositeProcessLock:
    """Acquire Redis first, then a local file lock; renew Redis in background."""

    def __init__(
        self,
        *,
        redis_lock: RedisLeaseLock,
        file_acquire: Callable[[], bool],
        file_release: Callable[[], None],
        renew_interval_ms: int,
        fail_closed: bool = True,
    ) -> None:
        self.redis_lock = redis_lock
        self.file_acquire = file_acquire
        self.file_release = file_release
        self.renew_interval_ms = int(renew_interval_ms)
        self.fail_closed = bool(fail_closed)
        self.acquired = False
        self._file_acquired = False
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def acquire(self) -> bool:
        try:
            if not self.redis_lock.acquire():
                return False
        except Exception:
            if self.fail_closed:
                return False
            if self.file_acquire():
                self._file_acquired = True
                return True
            return False
        if not self.file_acquire():
            try:
                self.redis_lock.release()
            except Exception:
                pass
            return False
        self._file_acquired = True
        self.acquired = True
        return True

    def start_renewal(self, on_lost: Callable[[str], None]) -> None:
        if not self.acquired or self._thread is not None:
            return

        interval = max(0.1, self.renew_interval_ms / 1000.0)

        def _run() -> None:
            while not self._stop.wait(interval):
                try:
                    ok = self.redis_lock.renew()
                except Exception as exc:
                    ok = False
                    reason = f"redis_renew_exception:{exc}"
                else:
                    reason = "redis_lease_lost"
                if not ok:
                    on_lost(reason)
                    return

        self._thread = threading.Thread(
            target=_run,
            name="redis-live-lock-renewal",
            daemon=True,
        )
        self._thread.start()

    def release(self) -> None:
        self._stop.set()
        if self._thread is not None and self._thread.is_alive():
            self._thread.join(timeout=2.0)
        if self._file_acquired:
            try:
                self.file_release()
            finally:
                self._file_acquired = False
        if self.acquired:
            try:
                self.redis_lock.release()
            except Exception:
                pass
            self.acquired = False
