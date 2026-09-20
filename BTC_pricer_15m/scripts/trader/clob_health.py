"""Shared CLOB *taker-health* flag — keeps dry-run (grid) and live entries matched.

Background: the dry-run grid records a fill instantly at the model price with no
real order, while the live trader places a real taker order that the Polymarket
CLOB can reject (``post_only_mode``, HTTP 425 "service not ready", or a transport
failure on ``POST /order``). When that happens the grid still "wins" the trade but
live gets nothing, so the leaderboard diverges from what is actually achievable.

Only the live trader can observe a taker-order rejection — those failure modes are
invisible to read-only probes (``acceptingOrders`` stays true, ``/book`` stays up).
So the live trader writes a short, self-expiring "taker outage" flag to a JSON file
in the shared ``results/`` directory; both the grid (all instances) and the live
trader read it before entering and skip the same windows.

The flag is intentionally a tiny file-based signal (no Redis, no network) so it can
be read from the grid's hot per-instance tick path cheaply (module-level cache) and
survives either process restarting. ``unhealthy_until`` is an *absolute* timestamp,
so a stale file left by a dead writer simply expires to healthy — i.e. fail-open,
trading is never blocked indefinitely by this module.

This mirrors the standalone, dependency-light style of ``polymarket_status.py`` so
it can be imported from grid tick paths, the single-trader, and tests alike.
"""
from __future__ import annotations

import csv
import json
import logging
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, Union

from ._io import atomic_write_json, ensure_trailing_newline

log = logging.getLogger(__name__)

DEFAULT_HEALTH_PATH = Path("results/15m_clob_health.json")
DEFAULT_COOLDOWN_SECONDS = 90.0
DEFAULT_MAX_COOLDOWN_SECONDS = 120.0
READ_CACHE_TTL_SECONDS = 1.0

# Append-only audit trail of every period CLOB / trading was unavailable. Lives
# next to the flag file (sibling) so a tmp flag path in tests keeps its log
# alongside. Unlike the self-overwriting JSON flag, this is a permanent record.
DEFAULT_HEALTH_LOG_NAME = "15m_clob_health_log.csv"
_HEALTH_LOG_HEADER = (
    "event",
    "timestamp",
    "reason",
    "source",
    "status_code",
    "retry_after_seconds",
    "cooldown_seconds",
    "unhealthy_until",
    "duration_seconds",
)

PathArg = Union[str, Path, None]

# Order-POST error fragments that mean "the exchange is refusing taker orders
# right now" — i.e. retrying immediately is pointless and the dry-run grid should
# also skip. Matched case-insensitively against the executor error message.
_TAKER_BLOCK_TOKENS = (
    "post_only_mode",
    "post-only mode",
    "post only mode",
    "service not ready",          # HTTP 425
    "status_code=425",
    " 425",
    "status_code=503",
    " 503",
    "service unavailable",
    "temporarily unavailable",
    "request exception",          # status_code=None transport failure
    "max retries exceeded",
    "connection reset",
    "connection aborted",
    "connection error",
    "remote end closed",
    "timed out",
    "timeout",
    "rate limit",
    "too many requests",
)

# Subset of the above that means the exchange explicitly told us "not now" with a
# tens-of-seconds backoff — retrying inside the entry window is hopeless, so the
# live retry loop should stop immediately rather than block on doomed attempts.
# Transient blips (timeout/connection/rate-limit) are deliberately excluded so
# the normal retry path can still catch a quick recovery.
_HARD_OUTAGE_TOKENS = (
    "post_only_mode",
    "post-only mode",
    "post only mode",
    "service not ready",
    "status_code=425",
    " 425",
    "status_code=503",
    " 503",
    "service unavailable",
    "temporarily unavailable",
)

_RETRY_AFTER_RE = re.compile(r"retry_after_seconds['\"]?\s*[:=]\s*(\d+(?:\.\d+)?)")
# Pulls an HTTP status code out of an executor error string so the availability
# ledger can classify the outage scope (425/503 → platform, 429 → account).
_STATUS_CODE_RE = re.compile(r"status_code['\"]?\s*[:=]\s*(\d{3})")
_BARE_CODE_RE = re.compile(r"(?<!\d)(425|503|429)(?!\d)")

# Module-level read cache keyed by resolved path. Avoids every grid instance
# hitting the disk on every tick; the expiry decision is still evaluated live
# against ``now`` so the 1s cache never makes a stale verdict.
_read_cache: dict[str, tuple[float, dict]] = {}


def _resolve_path(path: PathArg) -> Path:
    return DEFAULT_HEALTH_PATH if path is None else Path(path)


def health_log_path(path: PathArg = None) -> Path:
    """Path of the append-only unavailability audit CSV beside the flag file."""
    return _resolve_path(path).with_name(DEFAULT_HEALTH_LOG_NAME)


def _read_fresh(p: Path) -> dict:
    """Read the flag file straight from disk (bypassing the read cache).

    Used to detect healthy<->unhealthy *transitions* so the audit log records
    one row per outage period rather than one per re-mark.
    """
    try:
        parsed = json.loads(p.read_text(encoding="utf-8"))
        return parsed if isinstance(parsed, dict) else {}
    except (OSError, ValueError):
        return {}


def _outage_active_payload(data: dict, now: datetime) -> tuple[bool, Optional[datetime]]:
    """Return ``(active, since)`` for a raw flag payload at ``now``."""
    if not data or data.get("healthy", True):
        return (False, None)
    until = _parse_dt(data.get("unhealthy_until"))
    if until is None or now >= until:
        return (False, _parse_dt(data.get("since")))
    return (True, _parse_dt(data.get("since")))


def _append_health_log(health_path: Path, row: dict) -> None:
    """Append one row to the unavailability audit CSV (creating the header once).

    Best-effort and append-only: a write failure is logged, never raised, so it
    can never break the order path (same posture as the flag write).
    """
    log_path = health_log_path(health_path)
    try:
        new_file = not log_path.exists()
        log_path.parent.mkdir(parents=True, exist_ok=True)
        ensure_trailing_newline(log_path)
        with log_path.open("a", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=_HEALTH_LOG_HEADER)
            if new_file:
                writer.writeheader()
            writer.writerow({k: row.get(k, "") for k in _HEALTH_LOG_HEADER})
    except OSError as exc:  # pragma: no cover - disk/lock edge
        log.warning("clob_health: failed to append audit log %s: %s", log_path, exc)


def log_availability_event(
    event: str,
    *,
    path: PathArg = None,
    reason: str = "",
    source: str = "polymarket_status",
    duration_seconds: Optional[float] = None,
    now: Optional[datetime] = None,
) -> None:
    """Record a non-CLOB availability transition (e.g. Polymarket maintenance).

    Lets callers add ``polymarket_down`` / ``polymarket_up`` edges to the same
    audit trail as CLOB taker outages. Best-effort; never raises.
    """
    ts = _now(now)
    _append_health_log(
        _resolve_path(path),
        {
            "event": event,
            "timestamp": ts.isoformat(),
            "reason": reason,
            "source": source,
            "duration_seconds": ""
            if duration_seconds is None
            else f"{float(duration_seconds):.3f}",
        },
    )


def _now(now: Optional[datetime]) -> datetime:
    return now or datetime.now(timezone.utc)


def _parse_dt(value: object) -> Optional[datetime]:
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def is_taker_blocking_error(text: object) -> bool:
    """True if an executor error message signals the CLOB is refusing takers."""
    if not text:
        return False
    low = str(text).lower()
    return any(tok in low for tok in _TAKER_BLOCK_TOKENS)


def is_hard_taker_outage(text: object) -> bool:
    """True for sustained "not now" rejections (post-only / 425 / 503) where the
    retry loop should fast-fail instead of hammering doomed attempts."""
    if not text:
        return False
    low = str(text).lower()
    return any(tok in low for tok in _HARD_OUTAGE_TOKENS)


def parse_retry_after(text: object) -> Optional[float]:
    """Extract ``retry_after_seconds`` from a 503 post-only error body, if present."""
    if not text:
        return None
    match = _RETRY_AFTER_RE.search(str(text))
    if not match:
        return None
    try:
        return float(match.group(1))
    except ValueError:
        return None


def parse_status_code(text: object) -> Optional[int]:
    """Extract an HTTP status code from an executor error message, if present.

    Recognizes the explicit ``status_code=NNN`` form first (e.g.
    ``PolyApiException[status_code=425| error_message=service not ready]``),
    then falls back to a bare ``425``/``503``/``429`` token. Returns ``None``
    when no code is present (e.g. a transport ``Request exception!`` carrying
    ``status_code=None``) so the caller leaves scope classification to the
    error text. Mirrors :func:`parse_retry_after`.
    """
    if not text:
        return None
    s = str(text)
    for pat in (_STATUS_CODE_RE, _BARE_CODE_RE):
        m = pat.search(s)
        if m:
            try:
                return int(m.group(1))
            except ValueError:
                return None
    return None


def _clamp_cooldown(
    retry_after: Optional[float],
    default_cooldown: float,
    max_cooldown: float,
) -> float:
    cooldown = default_cooldown
    if retry_after is not None and retry_after > 0:
        cooldown = max(default_cooldown, float(retry_after))
    if max_cooldown and max_cooldown > 0:
        cooldown = min(cooldown, max(default_cooldown, max_cooldown))
    return cooldown


def mark_unhealthy(
    reason: str,
    *,
    path: PathArg = None,
    retry_after: Optional[float] = None,
    status_code: Optional[int] = None,
    source: str = "live_order_post",
    default_cooldown: float = DEFAULT_COOLDOWN_SECONDS,
    max_cooldown: float = DEFAULT_MAX_COOLDOWN_SECONDS,
    now: Optional[datetime] = None,
) -> datetime:
    """Record a taker-order outage; returns the ``unhealthy_until`` timestamp.

    ``unhealthy_until = now + clamp(retry_after, default_cooldown, max_cooldown)``.
    Writes are best-effort: a failure to persist is logged, not raised, so it can
    never break the order path.
    """
    p = _resolve_path(path)
    ts = _now(now)
    # Detect a genuine healthy->unhealthy transition so the audit log records one
    # row per outage period, not one per re-mark during a sustained outage.
    was_active, _since = _outage_active_payload(_read_fresh(p), ts)
    cooldown = _clamp_cooldown(retry_after, default_cooldown, max_cooldown)
    unhealthy_until = ts + timedelta(seconds=cooldown)
    # Best-effort scope classification (platform/market/account/bot_host/unknown)
    # so cross-bot readers can tell a CLOB-wide outage from a condition that only
    # concerns *this* bot's market — the dry-run-only 5m ignores ``market`` flags
    # (its markets are different; it probes them itself). The read probe's
    # gamma/book checks always concern the live trader's current market, hence
    # has_market_id for that source. Metadata only: 15m readers ignore it, and a
    # classification failure must never break the order path.
    scope: Optional[str] = None
    try:
        from .availability import classify as _classify_scope

        scope, _ = _classify_scope(
            reason,
            status_code=status_code,
            source=source,
            has_market_id=(source == "live_reads_probe"),
        )
    except Exception:  # noqa: BLE001 - scope is optional metadata
        scope = None
    payload = {
        "healthy": False,
        "reason": reason,
        "source": source,
        "scope": scope,
        "status_code": status_code,
        "retry_after_seconds": retry_after,
        "cooldown_seconds": cooldown,
        "since": ts.isoformat(),
        "unhealthy_until": unhealthy_until.isoformat(),
        "updated_at": ts.isoformat(),
    }
    try:
        atomic_write_json(p, payload)
    except OSError as exc:  # pragma: no cover - disk/lock edge
        log.warning("clob_health: failed to write unhealthy flag to %s: %s", p, exc)
    _read_cache.pop(str(p), None)
    if not was_active:
        _append_health_log(
            p,
            {
                "event": "outage_start",
                "timestamp": ts.isoformat(),
                "reason": reason,
                "source": source,
                "status_code": "" if status_code is None else status_code,
                "retry_after_seconds": "" if retry_after is None else retry_after,
                "cooldown_seconds": cooldown,
                "unhealthy_until": unhealthy_until.isoformat(),
            },
        )
    return unhealthy_until


def mark_healthy(
    *,
    path: PathArg = None,
    source: str = "live_order_post",
    reason: str = "ok",
    now: Optional[datetime] = None,
) -> None:
    """Clear the taker-outage flag after a successful order / probe."""
    p = _resolve_path(path)
    ts = _now(now)
    # Was an outage active right before this clear? If so, log the period's end.
    was_active, prior_since = _outage_active_payload(_read_fresh(p), ts)
    payload = {
        "healthy": True,
        "reason": reason,
        "source": source,
        "status_code": None,
        "retry_after_seconds": None,
        "cooldown_seconds": 0.0,
        "since": ts.isoformat(),
        "unhealthy_until": None,
        "updated_at": ts.isoformat(),
    }
    try:
        atomic_write_json(p, payload)
    except OSError as exc:  # pragma: no cover - disk/lock edge
        log.warning("clob_health: failed to write healthy flag to %s: %s", p, exc)
    _read_cache.pop(str(p), None)
    if was_active:
        duration = (ts - prior_since).total_seconds() if prior_since else None
        _append_health_log(
            p,
            {
                "event": "outage_cleared",
                "timestamp": ts.isoformat(),
                "reason": reason,
                "source": source,
                "duration_seconds": "" if duration is None else f"{duration:.3f}",
            },
        )


def _read_raw(p: Path) -> dict:
    key = str(p)
    mono = time.monotonic()
    cached = _read_cache.get(key)
    if cached is not None and (mono - cached[0]) < READ_CACHE_TTL_SECONDS:
        return cached[1]
    data: dict = {}
    try:
        parsed = json.loads(p.read_text(encoding="utf-8"))
        if isinstance(parsed, dict):
            data = parsed
    except FileNotFoundError:
        data = {}
    except (OSError, ValueError) as exc:
        log.debug("clob_health: unreadable flag %s (%s); treating as healthy", p, exc)
        data = {}
    _read_cache[key] = (mono, data)
    return data


def taker_outage_active(
    path: PathArg = None,
    *,
    now: Optional[datetime] = None,
) -> tuple[bool, Optional[str], Optional[datetime]]:
    """Return ``(outage_active, reason, unhealthy_until)``.

    Fail-open: a missing/corrupt file, a healthy flag, or an expired/absent
    ``unhealthy_until`` all yield ``(False, None, ...)``. Because the flag carries
    an absolute expiry, a stale file from a crashed writer never blocks forever.
    """
    p = _resolve_path(path)
    data = _read_raw(p)
    if not data or data.get("healthy", True):
        return (False, None, None)
    until = _parse_dt(data.get("unhealthy_until"))
    if until is None:
        return (False, None, None)
    if _now(now) >= until:
        return (False, None, until)
    return (True, str(data.get("reason") or "clob_taker_outage"), until)


def reset_cache() -> None:
    """Clear the module read cache (test helper)."""
    _read_cache.clear()
