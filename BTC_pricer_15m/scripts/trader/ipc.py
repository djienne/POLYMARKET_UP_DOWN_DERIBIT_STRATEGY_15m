"""Calibration IPC adapters.

The historical path used JSON files as a destructive inbox.  This module keeps
that path as a compatibility adapter and adds Redis Streams as the durable
message queue target.  Callers can dual-write during migration and switch
consumers independently.
"""

from __future__ import annotations

import json
import logging
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable, Optional, Protocol

from ._io import atomic_write_json, atomic_write_text
from .hybrid_calibration import CalibrationEvent

log = logging.getLogger(__name__)


def serialize_calibration_event(event: CalibrationEvent) -> str:
    """Canonical compact JSON serialization shared by every publish target."""
    return json.dumps(event.to_dict(), separators=(",", ":"), sort_keys=True)


class CalibrationPublisher(Protocol):
    def publish(self, event: CalibrationEvent, payload_json: Optional[str] = None) -> None:
        ...


class CalibrationSubscriber(Protocol):
    def poll(self) -> list[CalibrationEvent]:
        ...


@dataclass(frozen=True)
class FileCalibrationPublisher:
    targets: tuple[Path, ...]

    def publish(self, event: CalibrationEvent, payload_json: Optional[str] = None) -> None:
        if payload_json is None:
            payload_json = serialize_calibration_event(event)
        for target in self.targets:
            target.mkdir(parents=True, exist_ok=True)
            out = target / f"{event.event_id}.json"
            atomic_write_text(out, payload_json)


@dataclass(frozen=True)
class FileCalibrationSubscriber:
    inbox_dir: Path

    def poll(self) -> list[CalibrationEvent]:
        self.inbox_dir.mkdir(parents=True, exist_ok=True)
        for stale in self.inbox_dir.glob("*.processing"):
            try:
                if time.time() - stale.stat().st_mtime > 60:
                    stale.unlink(missing_ok=True)
            except OSError:
                pass
        events: list[CalibrationEvent] = []
        for path in sorted(self.inbox_dir.glob("*.json")):
            processing = path.with_suffix(".processing")
            try:
                path.rename(processing)
            except OSError:
                continue
            try:
                raw = json.loads(processing.read_text(encoding="utf-8"))
                events.append(CalibrationEvent.from_dict(raw))
            except Exception as exc:  # noqa: BLE001
                log.warning("dropping unreadable calibration inbox file %s: %s", path, exc)
            finally:
                processing.unlink(missing_ok=True)
        return events


class CompositeCalibrationPublisher:
    def __init__(self, publishers: Iterable[CalibrationPublisher]):
        self.publishers = tuple(publishers)

    def publish(self, event: CalibrationEvent, payload_json: Optional[str] = None) -> None:
        if payload_json is None:
            payload_json = serialize_calibration_event(event)
        published = 0
        errors: list[str] = []
        for publisher in self.publishers:
            try:
                publisher.publish(event, payload_json)
                published += 1
            except Exception as exc:  # noqa: BLE001
                errors.append(str(exc))
                log.warning("calibration publisher %s failed: %s", type(publisher).__name__, exc)
        if published == 0:
            raise RuntimeError("; ".join(errors) or "no calibration publishers configured")

    def close(self) -> None:
        for publisher in self.publishers:
            if hasattr(publisher, "close"):
                publisher.close()


class FallbackCalibrationPublisher:
    """Publish to ``primary``; write ``fallback`` only when the primary fails.

    Replaces the dual-write Composite for the redis+file configuration: the
    file inbox is a pure fallback consumers poll anyway, so writing it on
    every event just burned two extra serializations + bind-mount writes
    per cycle.
    """

    def __init__(self, primary: CalibrationPublisher, fallback: CalibrationPublisher):
        self.primary = primary
        self.fallback = fallback

    def publish(self, event: CalibrationEvent, payload_json: Optional[str] = None) -> None:
        if payload_json is None:
            payload_json = serialize_calibration_event(event)
        try:
            self.primary.publish(event, payload_json)
            return
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "primary calibration publisher %s failed (%s); writing file fallback",
                type(self.primary).__name__,
                exc,
            )
        self.fallback.publish(event, payload_json)

    def close(self) -> None:
        for publisher in (self.primary, self.fallback):
            if hasattr(publisher, "close"):
                publisher.close()


class CompositeCalibrationSubscriber:
    def __init__(self, subscribers: Iterable[CalibrationSubscriber]):
        self.subscribers = tuple(subscribers)

    def poll(self) -> list[CalibrationEvent]:
        events: list[CalibrationEvent] = []
        for subscriber in self.subscribers:
            try:
                events.extend(subscriber.poll())
            except Exception as exc:  # noqa: BLE001
                log.warning("calibration subscriber %s failed: %s", type(subscriber).__name__, exc)
        events.sort(key=lambda event: event.sort_key)
        return events

    def close(self) -> None:
        for subscriber in self.subscribers:
            if hasattr(subscriber, "close"):
                subscriber.close()


class RedisStreamCalibrationPublisher:
    def __init__(
        self,
        *,
        redis_url: str,
        stream: str = "market.calibration",
        maxlen: int = 10_000,
    ) -> None:
        self.redis_url = redis_url
        self.stream = stream
        self.maxlen = maxlen
        self._client = _redis_from_url(redis_url)

    def publish(self, event: CalibrationEvent, payload_json: Optional[str] = None) -> None:
        if payload_json is None:
            payload_json = serialize_calibration_event(event)
        self._client.xadd(
            self.stream,
            {"event_id": event.event_id, "market_id": event.market_id, "payload": payload_json},
            maxlen=self.maxlen,
            approximate=True,
        )

    def close(self) -> None:
        try:
            self._client.close()
        except Exception:  # noqa: BLE001
            pass


class RedisStreamCalibrationSubscriber:
    def __init__(
        self,
        *,
        redis_url: str,
        stream: str = "market.calibration",
        group: str,
        consumer: str,
        count: int = 50,
        block_ms: Optional[int] = None,
    ) -> None:
        self.redis_url = redis_url
        self.stream = stream
        self.group = group
        self.consumer = consumer
        self.count = count
        self.block_ms = block_ms
        self._client = _redis_from_url(redis_url)
        try:
            self._ensure_group()
        except Exception:
            try:
                self._client.close()
            except Exception:
                pass
            raise

    def _ensure_group(self) -> None:
        try:
            self._client.xgroup_create(
                self.stream,
                self.group,
                id="0-0",
                mkstream=True,
            )
        except Exception as exc:  # noqa: BLE001
            if "BUSYGROUP" not in str(exc):
                raise

    def poll(self) -> list[CalibrationEvent]:
        kwargs = {"count": self.count}
        if self.block_ms is not None:
            kwargs["block"] = self.block_ms
        response = self._client.xreadgroup(
            self.group,
            self.consumer,
            {self.stream: ">"},
            **kwargs,
        )
        events: list[CalibrationEvent] = []
        ack_ids: list[str] = []
        for _stream_name, messages in response:
            for message_id, fields in messages:
                try:
                    payload = fields.get(b"payload") or fields.get("payload")
                    if isinstance(payload, bytes):
                        payload = payload.decode("utf-8")
                    if not payload:
                        raise ValueError("missing payload field")
                    events.append(CalibrationEvent.from_dict(json.loads(str(payload))))
                    ack_ids.append(message_id)
                except Exception as exc:  # noqa: BLE001
                    log.warning("dropping unreadable redis calibration event %s: %s", message_id, exc)
                    ack_ids.append(message_id)
        if ack_ids:
            self._client.xack(self.stream, self.group, *ack_ids)
        events.sort(key=lambda event: event.sort_key)
        return events

    def close(self) -> None:
        try:
            self._client.close()
        except Exception:  # noqa: BLE001
            pass


class RedisCalibrationListener:
    """Push-based calibration consumer: a daemon thread doing blocking
    XREADGROUP reads, delivering events to ``on_event`` the moment they land.

    Replaces the once-per-tick non-blocking poll on the hot path (which added
    avg ~half a tick, worst a full tick, of dead time before the broker saw a
    published event). Mirrors the RedisSpotConsumer pattern.
    """

    def __init__(
        self,
        *,
        redis_url: str,
        stream: str = "market.calibration",
        group: str = "trader",
        consumer: str = "consumer",
        on_event: Callable[[CalibrationEvent], None],
        block_ms: int = 1000,
    ) -> None:
        self._subscriber = RedisStreamCalibrationSubscriber(
            redis_url=redis_url,
            stream=stream,
            group=group,
            consumer=consumer,
            count=50,
            block_ms=block_ms,
        )
        self._on_event = on_event
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(
            target=self._loop,
            name="calibration-listener",
            daemon=True,
        )
        self._thread.start()

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                events = self._subscriber.poll()
            except Exception as exc:  # noqa: BLE001
                log.warning("calibration listener poll failed: %s", exc)
                self._stop.wait(1.0)
                continue
            for event in events:
                if self._stop.is_set():
                    return
                try:
                    self._on_event(event)
                except Exception:  # noqa: BLE001
                    log.exception("calibration listener event handler failed")

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2.5)
            self._thread = None
        self._subscriber.close()

    close = stop


def build_calibration_publisher(
    *,
    backend: str,
    inbox_targets: Iterable[Path],
    redis_url: Optional[str] = None,
    redis_stream: str = "market.calibration",
    redis_maxlen: int = 10_000,
    file_fallback: bool = True,
) -> CalibrationPublisher:
    backend = (backend or "file").lower()
    publishers: list[CalibrationPublisher] = []
    if backend == "redis":
        if not redis_url:
            raise ValueError("redis backend requires redis_url")
        redis_publisher = RedisStreamCalibrationPublisher(
            redis_url=redis_url,
            stream=redis_stream,
            maxlen=redis_maxlen,
        )
        if file_fallback:
            # File inbox only on Redis failure — consumers poll both, so the
            # happy path skips two bind-mount writes per event.
            return FallbackCalibrationPublisher(
                redis_publisher,
                FileCalibrationPublisher(tuple(inbox_targets)),
            )
        publishers.append(redis_publisher)
    elif backend == "file":
        publishers.append(FileCalibrationPublisher(tuple(inbox_targets)))
    else:
        raise ValueError(f"unsupported calibration IPC backend: {backend}")
    return publishers[0] if len(publishers) == 1 else CompositeCalibrationPublisher(publishers)


def build_calibration_subscriber(
    *,
    backend: str,
    inbox_dir: Path,
    redis_url: Optional[str] = None,
    redis_stream: str = "market.calibration",
    redis_group: str = "trader",
    redis_consumer: str = "consumer",
    file_fallback: bool = True,
) -> CalibrationSubscriber:
    backend = (backend or "file").lower()
    subscribers: list[CalibrationSubscriber] = []
    if backend == "redis":
        if not redis_url:
            raise ValueError("redis backend requires redis_url")
        subscribers.append(
            RedisStreamCalibrationSubscriber(
                redis_url=redis_url,
                stream=redis_stream,
                group=redis_group,
                consumer=redis_consumer,
            )
        )
        if file_fallback:
            subscribers.append(FileCalibrationSubscriber(inbox_dir))
    elif backend == "file":
        subscribers.append(FileCalibrationSubscriber(inbox_dir))
    else:
        raise ValueError(f"unsupported calibration IPC backend: {backend}")
    return subscribers[0] if len(subscribers) == 1 else CompositeCalibrationSubscriber(subscribers)


def _redis_from_url(redis_url: str):
    try:
        import redis  # type: ignore
    except ImportError as exc:  # pragma: no cover - depends on optional runtime package
        raise RuntimeError(
            "Redis IPC selected but the 'redis' package is not installed. "
            "Install redis-py or use ipc.backend=file."
        ) from exc
    return redis.Redis.from_url(redis_url, decode_responses=False)
