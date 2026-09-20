"""Mirror-signal IPC and config helpers for live/grid parity control."""

from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from .config import GridConfig, InstanceParams, TraderConfig
from .ipc import _redis_from_url
from .position import TradeEvent

log = logging.getLogger(__name__)

MIRROR_SIGNAL_SCHEMA_VERSION = 1
MIRROR_SIGNAL_EVENTS = {"ENTRY", "STOP_LOSS", "TP_FILLED", "WIN_EXPIRY", "LOSS_EXPIRY"}
MIRROR_ACTION_EVENTS = {"ENTRY", "STOP_LOSS", "TP_FILLED"}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _dt_to_iso(value: Optional[datetime]) -> str:
    return value.isoformat() if value is not None else ""


def _parse_dt(value: object) -> Optional[datetime]:
    if not value:
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        try:
            parsed = datetime.fromisoformat(str(value))
        except ValueError:
            return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _float_or_none(value: object) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def live_params_from_config(config: TraderConfig) -> InstanceParams:
    return InstanceParams(
        alpha_up=float(config.edge_alpha_up),
        alpha_down=float(config.edge_alpha_down),
        floor_up=float(config.edge_floor_up),
        floor_down=float(config.edge_floor_down),
        tp_percentage=float(config.tp_percentage),
        stop_loss_pct=float(config.stop_loss_pct),
    )


def mirror_config_hash(params: InstanceParams) -> str:
    payload = {
        "alpha_up": float(params.alpha_up),
        "alpha_down": float(params.alpha_down),
        "floor_up": float(params.floor_up),
        "floor_down": float(params.floor_down),
        "tp_percentage": float(params.tp_percentage),
        "stop_loss_pct": float(params.stop_loss_pct),
    }
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def mirror_config_hash_from_trader(config: TraderConfig) -> str:
    return mirror_config_hash(live_params_from_config(config))


def _params_equal(left: InstanceParams, right: InstanceParams) -> bool:
    return (
        abs(float(left.alpha_up) - float(right.alpha_up)) < 1e-9
        and abs(float(left.alpha_down) - float(right.alpha_down)) < 1e-9
        and abs(float(left.floor_up) - float(right.floor_up)) < 1e-9
        and abs(float(left.floor_down) - float(right.floor_down)) < 1e-9
        and abs(float(left.tp_percentage) - float(right.tp_percentage)) < 1e-9
        and abs(float(left.stop_loss_pct) - float(right.stop_loss_pct)) < 1e-9
    )


def resolve_mirror_instance_id(
    *,
    grid_config: GridConfig,
    live_config: TraderConfig,
    requested: object = "auto",
) -> int:
    """Resolve an explicit or auto mirror instance id.

    Auto resolution must be exact and unique. This is intentionally stricter
    than dashboard leaderboard selection because live funds depend on this id.
    """
    text = str(requested if requested is not None else "auto").strip().lower()
    if text not in {"", "auto", "auto_from_live"}:
        try:
            value = int(str(requested))
        except (TypeError, ValueError) as exc:
            raise ValueError(f"invalid mirror instance id {requested!r}") from exc
        params = grid_config.generate_params()
        if value < 0 or value >= len(params):
            raise ValueError(
                f"mirror instance id {value} is outside generated grid range 0..{len(params)-1}"
            )
        return value

    target = live_params_from_config(live_config)
    matches = [
        idx
        for idx, params in enumerate(grid_config.generate_params())
        if _params_equal(params, target)
    ]
    if len(matches) != 1:
        if not matches:
            raise ValueError(
                "could not auto-resolve mirror instance from live params: "
                f"{target.label()}"
            )
        raise ValueError(
            "mirror instance auto-resolution is ambiguous; matches="
            + ",".join(str(m) for m in matches)
        )
    return matches[0]


def resolve_mirror_instance_from_paths(
    *,
    live_config: TraderConfig,
    requested: object = "auto",
    config_path: Path = Path("config.yaml"),
) -> int:
    grid_config = GridConfig.from_json(config_path)
    return resolve_mirror_instance_id(
        grid_config=grid_config,
        live_config=live_config,
        requested=requested,
    )


@dataclass(frozen=True)
class MirrorTradeSignal:
    schema_version: int = MIRROR_SIGNAL_SCHEMA_VERSION
    signal_id: str = ""
    source: str = "grid"
    source_instance_id: int = -1
    source_config_hash: str = ""
    market_id: str = ""
    market_title: str = ""
    direction: str = ""
    event: str = ""
    dry_entry_price: Optional[float] = None
    dry_exit_price: Optional[float] = None
    dry_stop_loss_price: Optional[float] = None
    dry_tp_price: Optional[float] = None
    dry_shares: Optional[float] = None
    dry_opened_at: Optional[datetime] = None
    dry_closed_at: Optional[datetime] = None
    dry_tick_timestamp: Optional[datetime] = None
    publish_timestamp: Optional[datetime] = None
    expires_at: Optional[datetime] = None
    trigger_bid: Optional[float] = None
    trigger_ask: Optional[float] = None
    trigger_mid: Optional[float] = None
    barrier: Optional[float] = None
    spot_price: Optional[float] = None
    reason: str = ""

    def to_dict(self) -> dict:
        return {
            "schema_version": self.schema_version,
            "signal_id": self.signal_id,
            "source": self.source,
            "source_instance_id": self.source_instance_id,
            "source_config_hash": self.source_config_hash,
            "market_id": self.market_id,
            "market_title": self.market_title,
            "direction": self.direction,
            "event": self.event,
            "dry_entry_price": self.dry_entry_price,
            "dry_exit_price": self.dry_exit_price,
            "dry_stop_loss_price": self.dry_stop_loss_price,
            "dry_tp_price": self.dry_tp_price,
            "dry_shares": self.dry_shares,
            "dry_opened_at": _dt_to_iso(self.dry_opened_at),
            "dry_closed_at": _dt_to_iso(self.dry_closed_at),
            "dry_tick_timestamp": _dt_to_iso(self.dry_tick_timestamp),
            "publish_timestamp": _dt_to_iso(self.publish_timestamp),
            "expires_at": _dt_to_iso(self.expires_at),
            "trigger_bid": self.trigger_bid,
            "trigger_ask": self.trigger_ask,
            "trigger_mid": self.trigger_mid,
            "barrier": self.barrier,
            "spot_price": self.spot_price,
            "reason": self.reason,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "MirrorTradeSignal":
        if not isinstance(data, dict):
            raise ValueError("mirror signal payload must be a mapping")
        event = str(data.get("event") or "").upper()
        if event not in MIRROR_SIGNAL_EVENTS:
            raise ValueError(f"unsupported mirror signal event {event!r}")
        return cls(
            schema_version=int(data.get("schema_version") or MIRROR_SIGNAL_SCHEMA_VERSION),
            signal_id=str(data.get("signal_id") or ""),
            source=str(data.get("source") or "grid"),
            source_instance_id=int(data.get("source_instance_id")),
            source_config_hash=str(data.get("source_config_hash") or ""),
            market_id=str(data.get("market_id") or ""),
            market_title=str(data.get("market_title") or ""),
            direction=str(data.get("direction") or "").upper(),
            event=event,
            dry_entry_price=_float_or_none(data.get("dry_entry_price")),
            dry_exit_price=_float_or_none(data.get("dry_exit_price")),
            dry_stop_loss_price=_float_or_none(data.get("dry_stop_loss_price")),
            dry_tp_price=_float_or_none(data.get("dry_tp_price")),
            dry_shares=_float_or_none(data.get("dry_shares")),
            dry_opened_at=_parse_dt(data.get("dry_opened_at")),
            dry_closed_at=_parse_dt(data.get("dry_closed_at")),
            dry_tick_timestamp=_parse_dt(data.get("dry_tick_timestamp")),
            publish_timestamp=_parse_dt(data.get("publish_timestamp")),
            expires_at=_parse_dt(data.get("expires_at")),
            trigger_bid=_float_or_none(data.get("trigger_bid")),
            trigger_ask=_float_or_none(data.get("trigger_ask")),
            trigger_mid=_float_or_none(data.get("trigger_mid")),
            barrier=_float_or_none(data.get("barrier")),
            spot_price=_float_or_none(data.get("spot_price")),
            reason=str(data.get("reason") or ""),
        )

    @classmethod
    def from_trade_event(
        cls,
        event: TradeEvent,
        *,
        market_title: str = "",
        source_config_hash: str,
        max_age_seconds: float = 30.0,
        trigger_bid: Optional[float] = None,
        trigger_ask: Optional[float] = None,
        trigger_mid: Optional[float] = None,
    ) -> "MirrorTradeSignal":
        publish_ts = _utc_now()
        expires_at = publish_ts + timedelta(seconds=max(0.0, float(max_age_seconds)))
        dry_tp = None
        if event.entry_price and event.effective_tp_pct:
            dry_tp = min(float(event.entry_price) * (1.0 + float(event.effective_tp_pct)), 0.99)
        if event.event == "TP_FILLED" and event.exit_price:
            dry_tp = float(event.exit_price)
        signal_id = deterministic_signal_id(event, source_config_hash)
        return cls(
            signal_id=signal_id,
            source="grid",
            source_instance_id=int(event.instance_id),
            source_config_hash=source_config_hash,
            market_id=event.market_id,
            market_title=market_title,
            direction=event.direction,
            event=event.event,
            dry_entry_price=float(event.entry_price or 0.0) or None,
            dry_exit_price=float(event.exit_price or 0.0) or None,
            dry_stop_loss_price=(
                float(event.exit_price) if event.event == "STOP_LOSS" and event.exit_price else None
            ),
            dry_tp_price=dry_tp,
            dry_shares=float(event.shares or 0.0) or None,
            dry_opened_at=event.timestamp if event.event == "ENTRY" else None,
            dry_closed_at=event.timestamp if event.event != "ENTRY" else None,
            dry_tick_timestamp=event.timestamp,
            publish_timestamp=publish_ts,
            expires_at=expires_at,
            trigger_bid=trigger_bid,
            trigger_ask=trigger_ask,
            trigger_mid=trigger_mid,
            barrier=float(event.barrier or 0.0) or None,
            spot_price=float(event.spot_price or 0.0) or None,
            reason=event.event,
        )


def deterministic_signal_id(event: TradeEvent, config_hash: str) -> str:
    raw = "|".join(
        [
            "grid",
            str(event.instance_id),
            event.market_id or "",
            event.event or "",
            event.direction or "",
            event.timestamp.isoformat(),
            f"{float(event.entry_price or 0.0):.8f}",
            f"{float(event.exit_price or 0.0):.8f}",
            config_hash or "",
        ]
    )
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]
    return f"mirror-{digest}"


class RedisMirrorSignalPublisher:
    def __init__(
        self,
        *,
        redis_url: str,
        stream: str = "trade.mirror.signals",
        maxlen: int = 10_000,
    ) -> None:
        self.redis_url = redis_url
        self.stream = stream
        self.maxlen = maxlen
        self._client = _redis_from_url(redis_url)

    def publish(self, signal: MirrorTradeSignal) -> None:
        payload = json.dumps(signal.to_dict(), separators=(",", ":"), sort_keys=True)
        self._client.xadd(
            self.stream,
            {
                "signal_id": signal.signal_id,
                "market_id": signal.market_id,
                "event": signal.event,
                "payload": payload,
            },
            maxlen=self.maxlen,
            approximate=True,
        )

    def publish_heartbeat(
        self,
        *,
        key: str,
        ttl_seconds: float,
        market_id: str,
        market_title: str = "",
        source_instance_id: Optional[int] = None,
        source_config_hash: str = "",
        tick_count: int = 0,
        entries_this_tick: int = 0,
        exits_this_tick: int = 0,
    ) -> None:
        payload = {
            "timestamp": _utc_now().isoformat(),
            "market_id": market_id,
            "market_title": market_title,
            "source_instance_id": source_instance_id,
            "source_config_hash": source_config_hash,
            "tick_count": int(tick_count),
            "entries_this_tick": int(entries_this_tick),
            "exits_this_tick": int(exits_this_tick),
        }
        self._client.set(
            key,
            json.dumps(payload, separators=(",", ":"), sort_keys=True),
            ex=max(1, int(ttl_seconds)),
        )

    def close(self) -> None:
        try:
            self._client.close()
        except Exception:  # noqa: BLE001
            pass


class RedisMirrorSignalSubscriber:
    def __init__(
        self,
        *,
        redis_url: str,
        stream: str = "trade.mirror.signals",
        group: str = "live-mirror",
        consumer: str = "live",
        ack_stream: str = "trade.mirror.acks",
        ack_maxlen: int = 100_000,
        count: int = 50,
        block_ms: Optional[int] = None,
    ) -> None:
        self.redis_url = redis_url
        self.stream = stream
        self.group = group
        self.consumer = consumer
        self.ack_stream = ack_stream
        self.ack_maxlen = max(1, int(ack_maxlen))
        self.count = count
        self.block_ms = block_ms
        self._client = _redis_from_url(redis_url)
        self._ensure_group()

    def _ensure_group(self) -> None:
        try:
            self._client.xgroup_create(self.stream, self.group, id="0-0", mkstream=True)
        except Exception as exc:  # noqa: BLE001
            if "BUSYGROUP" not in str(exc):
                raise

    @staticmethod
    def _decode_payload(fields: dict) -> MirrorTradeSignal:
        payload = fields.get(b"payload") or fields.get("payload")
        if isinstance(payload, bytes):
            payload = payload.decode("utf-8")
        if not payload:
            raise ValueError("missing payload field")
        return MirrorTradeSignal.from_dict(json.loads(str(payload)))

    def poll(self) -> list[MirrorTradeSignal]:
        return [
            signal
            for _message_id, signal in self.read_new()
            if signal is not None
        ]

    def read_new(
        self,
        *,
        block_ms: Optional[int] = None,
        count: Optional[int] = None,
    ) -> list[tuple[str, Optional[MirrorTradeSignal]]]:
        kwargs: dict[str, int] = {"count": count or self.count}
        effective_block = self.block_ms if block_ms is None else block_ms
        if effective_block is not None:
            kwargs["block"] = effective_block
        response = self._client.xreadgroup(
            self.group,
            self.consumer,
            {self.stream: ">"},
            **kwargs,
        )
        out: list[tuple[str, Optional[MirrorTradeSignal]]] = []
        for _stream_name, messages in response:
            for message_id, fields in messages:
                decoded_id = self._decode_message_id(message_id)
                try:
                    out.append((decoded_id, self._decode_payload(fields)))
                except Exception as exc:  # noqa: BLE001
                    log.warning("dropping unreadable mirror signal %s: %s", decoded_id, exc)
                    out.append((decoded_id, None))
        return out

    def claim_pending(
        self,
        *,
        min_idle_ms: int = 1000,
        start_id: str = "0-0",
        count: Optional[int] = None,
    ) -> list[tuple[str, Optional[MirrorTradeSignal]]]:
        try:
            response = self._client.xautoclaim(
                self.stream,
                self.group,
                self.consumer,
                min_idle_ms,
                start_id=start_id,
                count=count or self.count,
            )
        except AttributeError:
            return []
        if not response:
            return []
        messages = (
            response[1]
            if isinstance(response, (tuple, list)) and len(response) >= 2
            else []
        )
        out: list[tuple[str, Optional[MirrorTradeSignal]]] = []
        for message_id, fields in messages:
            decoded_id = self._decode_message_id(message_id)
            try:
                out.append((decoded_id, self._decode_payload(fields)))
            except Exception as exc:  # noqa: BLE001
                log.warning("dropping unreadable pending mirror signal %s: %s", decoded_id, exc)
                out.append((decoded_id, None))
        return out

    def ack_message(self, message_id: str) -> None:
        if not message_id:
            return
        self._client.xack(self.stream, self.group, message_id)

    def recent(self, *, count: int = 100) -> list[MirrorTradeSignal]:
        messages = self._client.xrevrange(self.stream, count=count)
        out: list[MirrorTradeSignal] = []
        for _message_id, fields in reversed(messages):
            try:
                out.append(self._decode_payload(fields))
            except Exception as exc:  # noqa: BLE001
                log.warning("dropping unreadable recent mirror signal: %s", exc)
        return out

    @staticmethod
    def _decode_message_id(message_id: object) -> str:
        if isinstance(message_id, bytes):
            return message_id.decode("utf-8")
        return str(message_id)

    def latest_id(self) -> str:
        messages = self._client.xrevrange(self.stream, count=1)
        if not messages:
            return "$"
        message_id, _fields = messages[0]
        return self._decode_message_id(message_id)

    def read_after(
        self,
        last_id: str,
        *,
        block_ms: Optional[int] = None,
        count: Optional[int] = None,
    ) -> list[tuple[str, Optional[MirrorTradeSignal]]]:
        kwargs: dict[str, int] = {"count": count or self.count}
        if block_ms is not None:
            kwargs["block"] = block_ms
        response = self._client.xread({self.stream: last_id}, **kwargs)
        out: list[tuple[str, Optional[MirrorTradeSignal]]] = []
        for _stream_name, messages in response:
            for message_id, fields in messages:
                decoded_id = self._decode_message_id(message_id)
                try:
                    out.append((decoded_id, self._decode_payload(fields)))
                except Exception as exc:  # noqa: BLE001
                    log.warning("dropping unreadable mirror signal %s: %s", decoded_id, exc)
                    out.append((decoded_id, None))
        return out

    def ack_signal(
        self,
        signal: MirrorTradeSignal,
        *,
        status: str,
        reason: str = "",
        details: Optional[dict] = None,
    ) -> None:
        details = details or {}
        fields = {
            "signal_id": signal.signal_id,
            "market_id": signal.market_id,
            "event": signal.event,
            "source_instance_id": str(signal.source_instance_id),
            "status": status,
            "reason": reason,
            "timestamp": _utc_now().isoformat(),
        }
        for key, value in details.items():
            if value is None:
                continue
            if isinstance(value, (dict, list, tuple)):
                fields[str(key)] = json.dumps(value, sort_keys=True, default=str)[:1000]
            else:
                fields[str(key)] = str(value)[:1000]
        self._client.xadd(
            self.ack_stream,
            fields,
            maxlen=self.ack_maxlen,
            approximate=True,
        )

    def get_json_key(self, key: str) -> Optional[dict]:
        raw = self._client.get(key)
        if raw is None:
            return None
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        return json.loads(str(raw))

    def close(self) -> None:
        try:
            self._client.close()
        except Exception:  # noqa: BLE001
            pass


def ack_is_unplaceable_rejection(ack: dict) -> bool:
    """True if a mirror ack means live could NOT place the ENTRY order.

    The near-real-time auto-amend voids the grid's phantom
    paper fill only for this case — a ``rejected`` ENTRY with no live order and no
    live position whose reason is an order/availability failure (425, post-only,
    transport, ``order_failed``/``entry_failed``). It deliberately does NOT void
    on legitimate strategy divergence (price guard, edge gone) or ``diagnostic``/
    ``duplicate`` acks, which are not parity violations.
    """
    if str(ack.get("status") or "").strip().lower() != "rejected":
        return False
    if str(ack.get("event") or "").strip().upper() != "ENTRY":
        return False
    if str(ack.get("order_id") or "").strip() or str(ack.get("live_position_id") or "").strip():
        return False
    reason = str(ack.get("reason") or "").lower()
    return "order_failed" in reason or "entry_failed" in reason


class RedisMirrorAckReader:
    """Thin reader of new mirror ack messages for the grid's auto-amend hook.

    Starts at ``$`` so it only sees acks produced after the grid connects, and
    advances its cursor as it reads. Best-effort; the grid wraps ``read_new`` in
    a try/except so a Redis hiccup never breaks the tick loop.
    """

    def __init__(self, *, redis_url: str, ack_stream: str = "trade.mirror.acks") -> None:
        self.stream = ack_stream
        self._client = _redis_from_url(redis_url)
        self._last_id = "$"

    @staticmethod
    def _decode(value: object) -> str:
        return value.decode("utf-8") if isinstance(value, bytes) else str(value)

    def read_new(self, *, count: int = 200, block_ms: Optional[int] = None) -> list[dict]:
        kwargs: dict[str, int] = {"count": count}
        if block_ms is not None:
            kwargs["block"] = block_ms
        response = self._client.xread({self.stream: self._last_id}, **kwargs)
        out: list[dict] = []
        for _stream_name, messages in response or []:
            for message_id, fields in messages:
                self._last_id = self._decode(message_id)
                out.append(
                    {self._decode(k): self._decode(v) for k, v in fields.items()}
                )
        return out

    def close(self) -> None:
        try:
            self._client.close()
        except Exception:  # noqa: BLE001
            pass


def signal_trigger_prices(event: TradeEvent, tick) -> tuple[Optional[float], Optional[float], Optional[float]]:
    book = None
    if event.direction == "UP":
        book = getattr(tick, "up_order_book", None)
    elif event.direction == "DOWN":
        book = getattr(tick, "down_order_book", None)
    bid = getattr(book, "best_bid", None) if book is not None else None
    ask = getattr(book, "best_ask", None) if book is not None else None
    mid = (float(bid) + float(ask)) / 2.0 if bid is not None and ask is not None else None
    return bid, ask, mid
