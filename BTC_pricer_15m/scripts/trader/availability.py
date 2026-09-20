"""Canonical live-tradability ledger — the historical truth feeding parity repair.

Background
----------
``clob_health.py`` keeps a *hot* self-expiring flag (``15m_clob_health.json``) for
cheap forward gating. That flag is perfect for "should I skip this entry right
now", but it is useless as a *record*: it overwrites itself, self-expires, and the
read-probe never wrote a clean recovery edge — so the append-only health log ended
up with hundreds of orphan ``outage_start`` pings and zero ``outage_cleared`` rows.

This module adds the missing piece: a normalized, scoped, confidence-tagged
*interval* ledger that answers "was live trading actually impossible at time T,
for this scope/market?" — reliably enough to (a) gate forward entries, (b) clean
historical phantom dry-run fills, and (c) be shared with the dry-run-only 5m bot.

Three layers
------------
1. **Hot flag** (``clob_health.py``)        — runtime hint, not kept here.
2. **Raw evidence** (``availability_events.csv``) — append-only observations, this
   module's :func:`record_event`. The legacy ``15m_clob_health_log.csv`` and the
   ``15m_mirror_signal_audit.csv`` are *also* evidence sources (see the
   ``events_from_*`` backfill helpers) so the ledger works on today's data with no
   new writes required.
3. **Normalized intervals** (``availability_ledger.csv``) — :func:`build_intervals`
   coalesces evidence into ``[start_ts, end_ts]`` periods with ``scope`` /
   ``confidence`` / ``evidence``. :func:`untradable_at` queries them.

Scope discipline (why the 5m bot can reuse only *some* of this)
---------------------------------------------------------------
* ``platform``  — CLOB-wide (HTTP 425 "service not ready", ``/ok`` down, scheduled
  maintenance). Applies to every market and every bot → safe to share to 5m.
* ``market``    — one market's book empty / not accepting orders. Applies only to
  the matching ``market_id``.
* ``account``   — 429 / rate-limit (API-key scoped). Not shared blindly.
* ``bot_host``  — local transport/network failure ("Request exception!"). Proves
  *this* attempt failed; does not prove another host/account could not trade.
* ``unknown``   — unclassified; conservative, report-only.

Everything here is dependency-light (stdlib only) and **fail-open**: a missing or
corrupt ledger yields "tradable" so this module can never block trading, mirroring
``clob_health.py``'s posture.
"""
from __future__ import annotations

import csv
import hashlib
import json
import logging
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, Optional, Sequence, Union

log = logging.getLogger(__name__)

PathArg = Union[str, Path, None]

# ── File names (siblings of the hot flag so a tmp flag path in tests keeps the
#    whole ledger alongside, matching clob_health.health_log_path) ────────────
DEFAULT_FLAG_PATH = Path("results/15m_clob_health.json")
EVENTS_NAME = "availability_events.csv"
LEDGER_NAME = "availability_ledger.csv"

# ── Scopes ───────────────────────────────────────────────────────────────────
SCOPE_PLATFORM = "platform"
SCOPE_MARKET = "market"
SCOPE_ACCOUNT = "account"
SCOPE_BOT_HOST = "bot_host"
SCOPE_UNKNOWN = "unknown"
ALL_SCOPES = (SCOPE_PLATFORM, SCOPE_MARKET, SCOPE_ACCOUNT, SCOPE_BOT_HOST, SCOPE_UNKNOWN)
# Scopes that are safe to publish to the shared cross-bot ledger.
SHAREABLE_SCOPES = frozenset({SCOPE_PLATFORM})

# ── Confidence (ranked for min_confidence filtering) ───────────────────────────
CONFIDENCE_INFERRED = "inferred"
CONFIDENCE_MANUAL = "manual"
CONFIDENCE_CONFIRMED = "confirmed"
_CONFIDENCE_RANK = {CONFIDENCE_INFERRED: 0, CONFIDENCE_MANUAL: 1, CONFIDENCE_CONFIRMED: 1}

# ── Event types ────────────────────────────────────────────────────────────────
EVENT_UNAVAILABLE = "unavailable_observed"
EVENT_AVAILABLE = "available_observed"
EVENT_STATUS_DOWN = "status_down"
EVENT_STATUS_UP = "status_up"
EVENT_MIRROR_REJECTED = "mirror_entry_rejected"
EVENT_MIRROR_ACCEPTED = "mirror_entry_accepted"
EVENT_MANUAL = "manual_backfill"
# Events that *open/extend* an unavailable interval vs *close* it. A mirror
# ENTRY that was *accepted* proves the platform took a real taker order at that
# instant, so it is a confirmed recovery (close) edge — same as an explicit
# available/status_up observation.
_OPEN_EVENTS = frozenset({EVENT_UNAVAILABLE, EVENT_STATUS_DOWN, EVENT_MIRROR_REJECTED})
_CLOSE_EVENTS = frozenset({EVENT_AVAILABLE, EVENT_STATUS_UP, EVENT_MIRROR_ACCEPTED})

# ── Evidence ────────────────────────────────────────────────────────────────────
EVIDENCE_ORDER_POST = "order_post"
EVIDENCE_CLOB_OK = "clob_ok_probe"
EVIDENCE_GAMMA = "gamma_market_probe"
EVIDENCE_BOOK = "book_probe"
EVIDENCE_STATUS_PAGE = "status_page"
EVIDENCE_MIRROR = "mirror_rejection"
EVIDENCE_LEGACY = "health_log_legacy"
EVIDENCE_MANUAL = "manual_backfill"
# Evidence kinds we trust enough to mark an interval ``confirmed`` (a real
# attempt or an authoritative status page — not a read-only book probe).
_CONFIRMING_EVIDENCE = frozenset({EVIDENCE_ORDER_POST, EVIDENCE_STATUS_PAGE, EVIDENCE_MIRROR})

DEFAULT_COALESCE_GAP_SECONDS = 120.0
# Cap on the backward discovery-lag span applied to a mirror rejection's
# ``effective_from`` (so a stale/garbage timing field can't reach back hours).
MIRROR_DISCOVERY_LAG_CAP_SECONDS = 300.0
# Self-expiry estimate stamped on ``polymarket_down`` evidence so an unclosed
# edge (recovery swallowed by a live-trader restart) seals at a bounded time
# instead of covering everything after it. Sized for the observed status-page
# outage durations (1-8 min) with headroom; a real ``polymarket_up`` close edge
# always wins over this estimate. The grid gates on the status page itself in
# real time, so entries during a *longer* genuine outage never happen anyway —
# the estimate only bounds how much post-recovery history a stale edge can
# falsely cover (forward gate skips / cleaner voids).
STATUS_DOWN_SELF_EXPIRY_SECONDS = 600.0

_STATUS_CODE_RE = re.compile(r"status_code\s*[=:]\s*(\d{3})")

_EVENTS_HEADER = (
    "event_id",
    "observed_at",
    "source_bot",
    "source_process",
    "event_type",
    "scope",
    "market_id",
    "reason_code",
    "status_code",
    "evidence",
    "confidence",
    "window_until",
    "effective_from",
    "effective_until",
    "related_signal_id",
    "related_order_id",
    "related_position_id",
    "dry_instance_id",
    "dry_opened_at",
    "metadata_json",
)

_LEDGER_HEADER = (
    "interval_id",
    "scope",
    "market_id",
    "start_ts",
    "end_ts",
    "status",
    "reason_code",
    "evidence",
    "confidence",
    "source_bot",
    "effective_from",
    "effective_until",
    "updated_at",
)


# ── Path helpers ───────────────────────────────────────────────────────────────
def _resolve_flag(path: PathArg) -> Path:
    return DEFAULT_FLAG_PATH if path is None else Path(path)


def events_path(flag_path: PathArg = None) -> Path:
    """Raw evidence CSV path beside the hot flag file."""
    return _resolve_flag(flag_path).with_name(EVENTS_NAME)


def ledger_path(flag_path: PathArg = None) -> Path:
    """Normalized interval ledger CSV path beside the hot flag file."""
    return _resolve_flag(flag_path).with_name(LEDGER_NAME)


# ── Datetime helpers ───────────────────────────────────────────────────────────
def _now(now: Optional[datetime]) -> datetime:
    return now or datetime.now(timezone.utc)


def parse_dt(value: object) -> Optional[datetime]:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _iso(value: Optional[datetime]) -> str:
    return value.isoformat() if value is not None else ""


def _conf_rank(confidence: object) -> int:
    return _CONFIDENCE_RANK.get(str(confidence or "").strip().lower(), 0)


# ── Scope classification ───────────────────────────────────────────────────────
def classify(
    reason: object,
    *,
    status_code: object = None,
    source: str = "",
    has_market_id: bool = False,
) -> tuple[str, str]:
    """Map a raw observation to ``(scope, confidence)``.

    Conservative by design:
    * 425 / "service not ready" / ``/ok`` down / post-only  → platform.
    * scheduled maintenance / status page                   → platform.
    * book empty / acceptingOrders / enableOrderBook         → market (if we know
      the market) else unknown.
    * 429 / rate-limit                                       → account.
    * transport / network / "request exception"             → bot_host.
    Confidence is ``confirmed`` only for an authoritative source (a real order
    POST or the status page); a read-only probe is ``inferred``.
    """
    low = str(reason or "").lower()
    code = None
    try:
        code = int(status_code) if status_code not in (None, "", "None") else None
    except (TypeError, ValueError):
        code = None
    if code is None:
        # Many error strings embed the HTTP status (e.g.
        # "PolyApiException[status_code=425| error_message=...]").
        m = _STATUS_CODE_RE.search(low)
        if m:
            try:
                code = int(m.group(1))
            except (TypeError, ValueError):
                code = None
    src = str(source or "").lower()
    confirmed_src = "order_post" in src or "status" in src

    def conf(default_inferred: bool = True) -> str:
        return CONFIDENCE_CONFIRMED if confirmed_src else (
            CONFIDENCE_INFERRED if default_inferred else CONFIDENCE_CONFIRMED
        )

    # Account-scoped throttling.
    if code == 429 or "rate limit" in low or "too many requests" in low:
        return SCOPE_ACCOUNT, conf()
    # Platform-wide refusals.
    if (
        code in (425, 503)
        or "service not ready" in low
        or "service unavailable" in low
        or "temporarily unavailable" in low
        or "post_only" in low
        or "post-only" in low
        or "post only" in low
        or "scheduled clob maintenance" in low
        or "hasissues" in low
        or "/ok" in low
        or "maintenance" in low
    ):
        return SCOPE_PLATFORM, conf()
    # Per-market book/acceptance problems. "not accepting" covers the read
    # probe's human label ("market not accepting orders"), which carries
    # neither the camelCase gamma field name nor the word "book".
    if (
        "book" in low
        or "acceptingorders" in low
        or "enableorderbook" in low
        or "order book" in low
        or "not accepting" in low
    ):
        return (SCOPE_MARKET if has_market_id else SCOPE_UNKNOWN), conf()
    # Host-local transport failures.
    if (
        "request exception" in low
        or "connection" in low
        or "timed out" in low
        or "timeout" in low
        or "max retries" in low
        or "remote end closed" in low
        or "network" in low
    ):
        return SCOPE_BOT_HOST, conf()
    return SCOPE_UNKNOWN, conf()


# ── Evidence event ─────────────────────────────────────────────────────────────
@dataclass
class AvailabilityEvent:
    observed_at: datetime
    event_type: str
    scope: str
    confidence: str
    evidence: str
    reason_code: str = ""
    market_id: str = ""
    status_code: Optional[int] = None
    source_bot: str = "15m"
    source_process: str = ""
    window_until: Optional[datetime] = None
    effective_from: Optional[datetime] = None
    effective_until: Optional[datetime] = None
    related_signal_id: str = ""
    related_order_id: str = ""
    related_position_id: str = ""
    dry_instance_id: str = ""
    dry_opened_at: Optional[datetime] = None
    metadata: dict = field(default_factory=dict)

    def event_id(self) -> str:
        raw = "|".join(
            [
                _iso(self.observed_at),
                self.event_type,
                self.scope,
                self.market_id,
                self.reason_code,
                self.related_signal_id,
                self.source_bot,
            ]
        )
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]

    def to_row(self) -> dict:
        return {
            "event_id": self.event_id(),
            "observed_at": _iso(self.observed_at),
            "source_bot": self.source_bot,
            "source_process": self.source_process,
            "event_type": self.event_type,
            "scope": self.scope,
            "market_id": self.market_id,
            "reason_code": self.reason_code,
            "status_code": "" if self.status_code is None else self.status_code,
            "evidence": self.evidence,
            "confidence": self.confidence,
            "window_until": _iso(self.window_until),
            "effective_from": _iso(self.effective_from),
            "effective_until": _iso(self.effective_until),
            "related_signal_id": self.related_signal_id,
            "related_order_id": self.related_order_id,
            "related_position_id": self.related_position_id,
            "dry_instance_id": self.dry_instance_id,
            "dry_opened_at": _iso(self.dry_opened_at),
            "metadata_json": json.dumps(self.metadata, sort_keys=True, default=str)
            if self.metadata
            else "",
        }

    @classmethod
    def from_row(cls, row: dict) -> Optional["AvailabilityEvent"]:
        observed = parse_dt(row.get("observed_at"))
        if observed is None:
            return None
        meta_raw = row.get("metadata_json") or ""
        try:
            meta = json.loads(meta_raw) if meta_raw else {}
            if not isinstance(meta, dict):
                meta = {}
        except (ValueError, TypeError):
            meta = {}
        status_code: Optional[int]
        try:
            sc = row.get("status_code")
            status_code = int(sc) if sc not in (None, "", "None") else None
        except (TypeError, ValueError):
            status_code = None
        return cls(
            observed_at=observed,
            event_type=str(row.get("event_type") or "").strip(),
            scope=str(row.get("scope") or SCOPE_UNKNOWN).strip(),
            confidence=str(row.get("confidence") or CONFIDENCE_INFERRED).strip(),
            evidence=str(row.get("evidence") or "").strip(),
            reason_code=str(row.get("reason_code") or "").strip(),
            market_id=str(row.get("market_id") or "").strip(),
            status_code=status_code,
            source_bot=str(row.get("source_bot") or "15m").strip(),
            source_process=str(row.get("source_process") or "").strip(),
            window_until=parse_dt(row.get("window_until")),
            effective_from=parse_dt(row.get("effective_from")),
            effective_until=parse_dt(row.get("effective_until")),
            related_signal_id=str(row.get("related_signal_id") or "").strip(),
            related_order_id=str(row.get("related_order_id") or "").strip(),
            related_position_id=str(row.get("related_position_id") or "").strip(),
            dry_instance_id=str(row.get("dry_instance_id") or "").strip(),
            dry_opened_at=parse_dt(row.get("dry_opened_at")),
            metadata=meta,
        )


# ── Normalized interval ─────────────────────────────────────────────────────────
@dataclass
class AvailabilityInterval:
    scope: str
    market_id: str
    start_ts: datetime
    end_ts: Optional[datetime]
    status: str  # "open" | "closed"
    reason_code: str
    evidence: str
    confidence: str
    source_bot: str
    effective_from: Optional[datetime] = None
    effective_until: Optional[datetime] = None

    def interval_id(self) -> str:
        raw = f"{self.scope}|{self.market_id}|{_iso(self.start_ts)}|{self.source_bot}"
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]

    def applicability_window(self) -> tuple[datetime, Optional[datetime]]:
        """``(lo, hi)`` of the cleanup/gating applicability window.

        Uses ``effective_from/until`` when present (so a discovery-lag
        mirror rejection whose dry fill *predates* the observed outage still
        covers that fill) else falls back to ``start_ts/end_ts``.
        """
        lo = self.effective_from or self.start_ts
        hi = self.effective_until or self.end_ts
        return lo, hi

    def covers(self, ts: datetime) -> bool:
        lo, hi = self.applicability_window()
        if ts < lo:
            return False
        if hi is None:  # open interval with no expiry estimate — applies onward
            return True
        return ts <= hi

    def to_row(self, *, updated_at: Optional[datetime] = None) -> dict:
        return {
            "interval_id": self.interval_id(),
            "scope": self.scope,
            "market_id": self.market_id,
            "start_ts": _iso(self.start_ts),
            "end_ts": _iso(self.end_ts),
            "status": self.status,
            "reason_code": self.reason_code,
            "evidence": self.evidence,
            "confidence": self.confidence,
            "source_bot": self.source_bot,
            "effective_from": _iso(self.effective_from),
            "effective_until": _iso(self.effective_until),
            "updated_at": _iso(updated_at) if updated_at else "",
        }

    @classmethod
    def from_row(cls, row: dict) -> Optional["AvailabilityInterval"]:
        start = parse_dt(row.get("start_ts"))
        if start is None:
            return None
        return cls(
            scope=str(row.get("scope") or SCOPE_UNKNOWN).strip(),
            market_id=str(row.get("market_id") or "").strip(),
            start_ts=start,
            end_ts=parse_dt(row.get("end_ts")),
            status=str(row.get("status") or "closed").strip(),
            reason_code=str(row.get("reason_code") or "").strip(),
            evidence=str(row.get("evidence") or "").strip(),
            confidence=str(row.get("confidence") or CONFIDENCE_INFERRED).strip(),
            source_bot=str(row.get("source_bot") or "15m").strip(),
            effective_from=parse_dt(row.get("effective_from")),
            effective_until=parse_dt(row.get("effective_until")),
        )


# ── Interval compaction ─────────────────────────────────────────────────────────
def build_intervals(
    events: Iterable[AvailabilityEvent],
    *,
    coalesce_gap_seconds: float = DEFAULT_COALESCE_GAP_SECONDS,
) -> list[AvailabilityInterval]:
    """Coalesce raw evidence into normalized unavailability intervals.

    Algorithm (per ``(scope, market_id)`` group, chronological):
    * an *open* event (unavailable/status_down/mirror_rejected) starts a new
      interval or extends the current one if it falls within
      ``coalesce_gap_seconds`` of the running end; each event contributes a
      ``[observed_at, window_until]`` window (``window_until`` defaults to
      ``observed_at`` when absent);
    * a *close* event (available/status_up/mirror_accepted) seals the current
      open interval at its ``observed_at`` and marks it ``status="closed"`` (a
      confirmed recovery edge);
    * an interval never sealed by a recovery edge is ``status="open"``: it ends
      at its last ``window_until`` (the self-expiry estimate) when any member
      carried one, else ``end_ts=None`` (genuinely ongoing — e.g. a status-page
      ``status_down`` with no expiry and no ``status_up`` yet — so ``covers``
      applies it onward instead of collapsing it to a near-instant point).
    Interval confidence is the strongest member confidence; effective_from/until
    span the widest member effective window. Mirror-rejection events whose
    ``effective_from`` predates ``start_ts`` widen the applicability window so a
    discovery-lag phantom fill is still covered.
    """
    gap = timedelta(seconds=max(0.0, float(coalesce_gap_seconds)))
    by_group: dict[tuple[str, str], list[AvailabilityEvent]] = {}
    for ev in events:
        if ev is None or ev.event_type not in (_OPEN_EVENTS | _CLOSE_EVENTS):
            continue
        by_group.setdefault((ev.scope, ev.market_id), []).append(ev)

    intervals: list[AvailabilityInterval] = []
    for (scope, market_id), group in by_group.items():
        group.sort(key=lambda e: e.observed_at)
        cur: Optional[dict] = None

        def _open_end(state: dict) -> Optional[datetime]:
            """End timestamp for an UNCLOSED interval (no recovery edge seen).

            If any member contributed an explicit ``window_until`` (a self-expiry
            estimate, e.g. the hot flag's cooldown), seal at that finite estimate
            — historical behavior. Otherwise the evidence carries no expiry (e.g.
            a status-page ``status_down`` with no ``status_up`` yet), so the
            interval is genuinely ongoing: end ``None`` so ``covers`` applies it
            onward instead of collapsing it to a near-instant point.
            """
            return state["end"] if state["had_expiry"] else None

        def _seal(state: dict, *, status: str, end_ts: Optional[datetime]) -> None:
            if end_ts is None:
                eff_until = state["eff_until"]  # open-ended → applicability open too
            elif state["eff_until"] is None:
                eff_until = end_ts
            else:
                eff_until = max(state["eff_until"], end_ts)
            intervals.append(
                AvailabilityInterval(
                    scope=scope,
                    market_id=market_id,
                    start_ts=state["start"],
                    end_ts=end_ts,
                    status=status,
                    reason_code=state["reason"],
                    evidence=state["evidence"],
                    confidence=state["confidence"],
                    source_bot=state["source_bot"],
                    effective_from=min(state["eff_from"], state["start"]),
                    effective_until=eff_until,
                )
            )

        def _new_state(ev: AvailabilityEvent) -> dict:
            return {
                "start": ev.observed_at,
                "end": ev.window_until or ev.observed_at,
                "reason": ev.reason_code,
                "evidence": ev.evidence,
                "confidence": ev.confidence,
                "source_bot": ev.source_bot,
                "eff_from": ev.effective_from or ev.observed_at,
                "eff_until": ev.effective_until,
                # Track whether any member gave an explicit expiry estimate; an
                # unclosed interval with none is sealed open-ended (see _open_end).
                "had_expiry": ev.window_until is not None,
            }

        for ev in group:
            if ev.event_type in _CLOSE_EVENTS:
                if cur is not None and ev.observed_at >= cur["start"]:
                    _seal(cur, status="closed", end_ts=ev.observed_at)
                    cur = None
                continue
            win_end = ev.window_until or ev.observed_at
            eff_from = ev.effective_from or ev.observed_at
            eff_until = ev.effective_until
            if cur is None:
                cur = _new_state(ev)
                continue
            if ev.observed_at <= cur["end"] + gap:
                # extend
                cur["end"] = max(cur["end"], win_end)
                cur["had_expiry"] = cur["had_expiry"] or (ev.window_until is not None)
                if _conf_rank(ev.confidence) > _conf_rank(cur["confidence"]):
                    cur["confidence"] = ev.confidence
                    cur["reason"] = ev.reason_code
                    cur["evidence"] = ev.evidence
                cur["eff_from"] = min(cur["eff_from"], eff_from)
                if eff_until is not None:
                    cur["eff_until"] = (
                        eff_until if cur["eff_until"] is None else max(cur["eff_until"], eff_until)
                    )
            else:
                _seal(cur, status="open", end_ts=_open_end(cur))
                cur = _new_state(ev)
        if cur is not None:
            _seal(cur, status="open", end_ts=_open_end(cur))

    intervals.sort(key=lambda iv: (iv.start_ts, iv.scope, iv.market_id))
    return intervals


# ── Persistence ─────────────────────────────────────────────────────────────────
def record_event(
    event: AvailabilityEvent,
    *,
    flag_path: PathArg = None,
    events_file: PathArg = None,
    shared_path: PathArg = None,
) -> None:
    """Append one evidence row (best-effort, never raises).

    Always writes to the per-bot ``availability_events.csv``. When ``shared_path``
    is given *and* the event scope is shareable (platform), also append to the
    cross-bot shared ledger so the 5m bot can consume platform outages it cannot
    observe itself.
    """
    target = Path(events_file) if events_file else events_path(flag_path)
    _append_events(target, [event])
    if shared_path and event.scope in SHAREABLE_SCOPES:
        _append_events(Path(shared_path), [event])


def _append_events(path: Path, events: Sequence[AvailabilityEvent]) -> None:
    rows = [e.to_row() for e in events if e is not None]
    if not rows:
        return
    try:
        new_file = not path.exists() or path.stat().st_size == 0
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=_EVENTS_HEADER)
            if new_file:
                writer.writeheader()
            for row in rows:
                writer.writerow({k: row.get(k, "") for k in _EVENTS_HEADER})
    except OSError as exc:  # pragma: no cover - disk/lock edge
        log.warning("availability: failed to append events to %s: %s", path, exc)


def load_events(path: PathArg) -> list[AvailabilityEvent]:
    p = _resolve_events_arg(path)
    if p is None or not p.exists():
        return []
    out: list[AvailabilityEvent] = []
    try:
        with p.open("r", encoding="utf-8", newline="") as fh:
            for row in csv.DictReader(fh):
                ev = AvailabilityEvent.from_row(row)
                if ev is not None:
                    out.append(ev)
    except (OSError, ValueError) as exc:
        log.warning("availability: failed to read events %s: %s", p, exc)
        return []
    return out


def _resolve_events_arg(path: PathArg) -> Optional[Path]:
    if path is None:
        return None
    return Path(path)


def write_ledger(
    intervals: Sequence[AvailabilityInterval],
    *,
    flag_path: PathArg = None,
    ledger_file: PathArg = None,
    now: Optional[datetime] = None,
) -> Path:
    """Atomically replace the interval ledger CSV. Returns the written path."""
    from ._io import atomic_write_csv_rows

    target = Path(ledger_file) if ledger_file else ledger_path(flag_path)
    ts = _now(now)
    rows = [
        [iv.to_row(updated_at=ts).get(col, "") for col in _LEDGER_HEADER]
        for iv in intervals
    ]
    atomic_write_csv_rows(target, list(_LEDGER_HEADER), rows)
    _ledger_cache.pop(str(target), None)  # invalidate read cache after a write
    return target


def load_intervals(path: PathArg) -> list[AvailabilityInterval]:
    p = _resolve_events_arg(path)
    if p is None or not p.exists():
        return []
    out: list[AvailabilityInterval] = []
    try:
        with p.open("r", encoding="utf-8", newline="") as fh:
            for row in csv.DictReader(fh):
                iv = AvailabilityInterval.from_row(row)
                if iv is not None:
                    out.append(iv)
    except (OSError, ValueError) as exc:
        log.warning("availability: failed to read ledger %s: %s", p, exc)
        return []
    return out


# Share ledger reads across grid instances for one TTL. The canonical config
# enables the gate; its interval verdict is still evaluated against current time.
READ_CACHE_TTL_SECONDS = 1.0
_ledger_cache: dict[str, tuple[float, list]] = {}


def load_intervals_cached(path: PathArg, *, ttl: float = READ_CACHE_TTL_SECONDS) -> list[AvailabilityInterval]:
    p = _resolve_events_arg(path)
    if p is None:
        return []
    key = str(p)
    mono = time.monotonic()
    cached = _ledger_cache.get(key)
    if cached is not None and (mono - cached[0]) < ttl:
        return cached[1]
    intervals = load_intervals(p)
    _ledger_cache[key] = (mono, intervals)
    return intervals


def reset_cache() -> None:
    """Clear the module ledger read cache (test helper / post-write refresh)."""
    _ledger_cache.clear()


def rebuild_ledger(
    *,
    flag_path: PathArg = None,
    events_file: PathArg = None,
    ledger_file: PathArg = None,
    extra_events: Iterable[AvailabilityEvent] = (),
    coalesce_gap_seconds: float = DEFAULT_COALESCE_GAP_SECONDS,
    now: Optional[datetime] = None,
) -> list[AvailabilityInterval]:
    """Read evidence (events file + ``extra_events``), compact, and write the ledger."""
    src = Path(events_file) if events_file else events_path(flag_path)
    events = load_events(src)
    events.extend(e for e in extra_events if e is not None)
    intervals = build_intervals(events, coalesce_gap_seconds=coalesce_gap_seconds)
    write_ledger(intervals, flag_path=flag_path, ledger_file=ledger_file, now=now)
    return intervals


# ── Query ────────────────────────────────────────────────────────────────────────
# Scopes the forward gate / cross-bot consumer honors by default: platform
# (CLOB-wide, applies everywhere) + market (only the matching market). Account /
# bot_host / unknown are deliberately excluded here so the dry-run-only 5m bot
# never blocks on a 15m host/account/unclassified failure. Self-cleaning passes
# ``include_scopes=ALL_SCOPES`` because every scope a bot observed applies to its
# own history.
GATE_SCOPES = (SCOPE_PLATFORM, SCOPE_MARKET)


def _scope_applies(
    interval: AvailabilityInterval,
    market_id: Optional[str],
    include_scopes,
) -> bool:
    if interval.scope == SCOPE_MARKET:
        return (
            SCOPE_MARKET in include_scopes
            and bool(market_id)
            and interval.market_id == str(market_id)
        )
    return interval.scope in include_scopes


def untradable_at(
    ts: datetime,
    *,
    market_id: Optional[str] = None,
    ledger_path: PathArg,
    min_confidence: str = CONFIDENCE_CONFIRMED,
    include_scopes=GATE_SCOPES,
    intervals: Optional[Sequence[AvailabilityInterval]] = None,
) -> tuple[bool, Optional[str], Optional[AvailabilityInterval]]:
    """Was live trading impossible at ``ts``?

    Returns ``(active, reason, interval)``. **Fail-open**: a missing/corrupt
    ledger, or any read error, yields ``(False, None, None)`` so this can never
    block trading. ``include_scopes`` defaults to ``(platform, market)`` — exactly
    what the forward gate and the dry-run-only 5m bot should consume from a shared
    ledger (platform applies everywhere; market only to the matching market). Pass
    ``include_scopes=ALL_SCOPES`` for same-bot self-cleaning.
    """
    ts = ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
    rank_min = _conf_rank(min_confidence)
    include_scopes = frozenset(include_scopes)
    try:
        ivs = list(intervals) if intervals is not None else load_intervals_cached(ledger_path)
    except Exception:  # noqa: BLE001 - fail open on any read/parse error
        return (False, None, None)
    for iv in ivs:
        if _conf_rank(iv.confidence) < rank_min:
            continue
        if not _scope_applies(iv, market_id, include_scopes):
            continue
        if iv.covers(ts):
            return (True, iv.reason_code or "untradable", iv)
    return (False, None, None)


# ── Backfill from existing evidence sources ───────────────────────────────────────
def events_from_health_log(
    health_log_path: PathArg,
    *,
    source_bot: str = "15m",
) -> list[AvailabilityEvent]:
    """Derive evidence events from the legacy ``15m_clob_health_log.csv``.

    ``outage_start`` rows carry an ``unhealthy_until`` self-expiry, used as the
    per-ping ``window_until`` so :func:`build_intervals` can coalesce hundreds of
    orphan starts into a handful of real intervals. ``outage_cleared`` and
    ``polymarket_up`` become close edges; ``polymarket_down`` a platform
    status-down edge. The legacy log has no ``market_id`` column, so read-probe
    "books empty" rows classify to ``unknown`` (inferred / report-only).
    """
    p = _resolve_events_arg(health_log_path)
    if p is None or not p.exists():
        return []
    out: list[AvailabilityEvent] = []
    # The legacy health flag is a single GLOBAL file (one outage at a time), so an
    # ``outage_cleared`` edge closes whatever outage preceded it. Carry the last
    # open outage's scope/market onto the clear so they coalesce into the same
    # interval group (otherwise a reads/unknown outage would never get closed).
    last_open_scope = SCOPE_PLATFORM
    last_open_market = ""
    try:
        with p.open("r", encoding="utf-8", newline="") as fh:
            for row in csv.DictReader(fh):
                event = (row.get("event") or "").strip()
                observed = parse_dt(row.get("timestamp"))
                if observed is None:
                    continue
                reason = (row.get("reason") or "").strip()
                source = (row.get("source") or "").strip()
                status_code = row.get("status_code") or None
                if event == "outage_start":
                    scope, confidence = classify(
                        reason, status_code=status_code, source=source, has_market_id=False
                    )
                    last_open_scope, last_open_market = scope, ""
                    out.append(
                        AvailabilityEvent(
                            observed_at=observed,
                            event_type=EVENT_UNAVAILABLE,
                            scope=scope,
                            confidence=confidence,
                            evidence=EVIDENCE_LEGACY,
                            reason_code=reason,
                            source_bot=source_bot,
                            source_process=source,
                            window_until=parse_dt(row.get("unhealthy_until")),
                        )
                    )
                elif event == "outage_cleared":
                    out.append(
                        AvailabilityEvent(
                            observed_at=observed,
                            event_type=EVENT_AVAILABLE,
                            scope=last_open_scope,
                            confidence=CONFIDENCE_CONFIRMED,
                            evidence=EVIDENCE_LEGACY,
                            reason_code=reason or "cleared",
                            market_id=last_open_market,
                            source_bot=source_bot,
                            source_process=source,
                        )
                    )
                elif event == "polymarket_down":
                    out.append(
                        AvailabilityEvent(
                            observed_at=observed,
                            event_type=EVENT_STATUS_DOWN,
                            scope=SCOPE_PLATFORM,
                            confidence=CONFIDENCE_CONFIRMED,
                            evidence=EVIDENCE_STATUS_PAGE,
                            reason_code=reason or "polymarket_down",
                            source_bot=source_bot,
                            source_process=source,
                            # Self-expiry estimate, mirroring the hot flag's
                            # cooldown on outage_start rows: if the matching
                            # ``polymarket_up`` edge is never written (live
                            # trader restarted mid-outage — the edge-triggered
                            # recorder's in-memory state resets), the interval
                            # seals here instead of staying open-ended. An
                            # open-ended confirmed platform interval covers
                            # every later timestamp: it would wedge the forward
                            # gate and let the cleaner void the entire history
                            # after it. A real ``polymarket_up`` close edge
                            # still seals the interval at its exact time.
                            window_until=observed
                            + timedelta(seconds=STATUS_DOWN_SELF_EXPIRY_SECONDS),
                        )
                    )
                elif event == "polymarket_up":
                    out.append(
                        AvailabilityEvent(
                            observed_at=observed,
                            event_type=EVENT_STATUS_UP,
                            scope=SCOPE_PLATFORM,
                            confidence=CONFIDENCE_CONFIRMED,
                            evidence=EVIDENCE_STATUS_PAGE,
                            reason_code=reason or "operational",
                            source_bot=source_bot,
                            source_process=source,
                        )
                    )
    except (OSError, ValueError) as exc:
        log.warning("availability: failed to read health log %s: %s", p, exc)
        return []
    return out


def _mirror_lag_seconds(row: dict) -> float:
    """Estimate the dry-open → live-attempt lag (s) for a mirror audit row.

    The audit CSV has no ``dry_opened_at`` column, so approximate it from the
    row's own timing fields: ``signal_age_s`` (age of the dry signal at the live
    attempt ≈ time since the dry fill) preferred, then ``publish_to_attempt_s``;
    capped to :data:`MIRROR_DISCOVERY_LAG_CAP_SECONDS`. Falls back to the coalesce
    gap when neither is a positive number. Used to set ``effective_from`` so a
    discovery-lag phantom fill that *predates* the rejection is still covered.
    """
    for key in ("signal_age_s", "publish_to_attempt_s"):
        raw = str(row.get(key) or "").strip()
        if not raw:
            continue
        try:
            val = float(raw)
        except (TypeError, ValueError):
            continue
        if val > 0:
            return min(val, MIRROR_DISCOVERY_LAG_CAP_SECONDS)
    return DEFAULT_COALESCE_GAP_SECONDS


def events_from_mirror_audit(
    audit_path: PathArg,
    *,
    instance_id: Optional[int] = None,
    source_bot: str = "15m",
) -> list[AvailabilityEvent]:
    """Derive mirror ENTRY accept/reject evidence from ``15m_mirror_signal_audit.csv``.

    A rejected ENTRY (``stage=attempt_result, status=rejected``) with no live
    order/position is the *strongest* cleanup evidence (Tier A) — and its
    ``effective_from`` is the dry-run open timestamp so a discovery-lag fill that
    predates the observed outage is still covered. Scope is classified from the
    rejection error (425 → platform; "Request exception" → bot_host, per
    Decision 3). Accepted ENTRYs are recorded as ``available`` close edges.
    """
    p = _resolve_events_arg(audit_path)
    if p is None or not p.exists():
        return []
    out: list[AvailabilityEvent] = []
    try:
        with p.open("r", encoding="utf-8", newline="") as fh:
            for row in csv.DictReader(fh):
                if (row.get("stage") or "").strip() != "attempt_result":
                    continue
                if (row.get("event") or "").strip().upper() != "ENTRY":
                    continue
                try:
                    src_iid = int(row.get("source_instance_id") or -1)
                except (TypeError, ValueError):
                    continue
                if instance_id is not None and src_iid != int(instance_id):
                    continue
                observed = parse_dt(row.get("timestamp"))
                if observed is None:
                    continue
                status = (row.get("status") or "").strip().lower()
                market_id = (row.get("market_id") or "").strip()
                details = (row.get("details") or "")
                order_id = (row.get("order_id") or "").strip()
                position_id = (row.get("live_position_id") or "").strip()
                signal_id = (row.get("signal_id") or "").strip()
                if status == "rejected" and not order_id and not position_id:
                    scope, _conf = classify(
                        details, source="order_post", has_market_id=bool(market_id)
                    )
                    # Discovery-lag span (MEGA WS3 / §2.3): the dry fill that
                    # triggered this mirror signal opened BEFORE live discovered
                    # the rejection, so the applicability window must reach back to
                    # ~dry_opened_at. window_until is pinned to observed_at — the
                    # rejection proves untradable at that instant (backward cover
                    # comes from effective_from), and it gives the interval an
                    # explicit expiry so a lone rejection stays finite rather than
                    # coalescing into an open-ended interval.
                    lag = _mirror_lag_seconds(row)
                    out.append(
                        AvailabilityEvent(
                            observed_at=observed,
                            event_type=EVENT_MIRROR_REJECTED,
                            scope=scope,
                            confidence=CONFIDENCE_CONFIRMED,
                            evidence=EVIDENCE_MIRROR,
                            reason_code=(row.get("reason") or "order_failed").strip(),
                            market_id=market_id,
                            source_bot=source_bot,
                            source_process="mirror_audit",
                            related_signal_id=signal_id,
                            dry_instance_id=str(src_iid),
                            window_until=observed,
                            effective_from=observed - timedelta(seconds=lag),
                            metadata={"details": details[:400]} if details else {},
                        )
                    )
                elif status == "accepted":
                    out.append(
                        AvailabilityEvent(
                            observed_at=observed,
                            event_type=EVENT_MIRROR_ACCEPTED,
                            scope=SCOPE_PLATFORM,
                            confidence=CONFIDENCE_CONFIRMED,
                            evidence=EVIDENCE_MIRROR,
                            reason_code="entered",
                            market_id=market_id,
                            source_bot=source_bot,
                            source_process="mirror_audit",
                            related_signal_id=signal_id,
                            dry_instance_id=str(src_iid),
                        )
                    )
    except (OSError, ValueError) as exc:
        log.warning("availability: failed to read mirror audit %s: %s", p, exc)
        return []
    return out
