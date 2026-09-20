from __future__ import annotations

import asyncio
import csv
import hashlib
import io
import logging
import time
from collections import Counter, deque
from pathlib import Path
from threading import RLock
from typing import Deque, NamedTuple, Optional

from ..config import settings
from ..events.bus import bus
from ..models import TodaySummary, TradeEvent, ViewSource
from ..time_utils import paris_date_key
from . import entry_registry

log = logging.getLogger(__name__)

MAX_RECENT_PER_INSTANCE = 200
# Per-instance cap on retained realized closes. The realized buffer feeds the
# equity timeseries (the left equity plot) + today-PnL math, and was
# previously an unbounded list: seeding the full trades.csv into per-instance
# lists of pydantic TradeEvent objects was the cause of the dashboard's
# multi-GB RSS. We now store a lightweight RealizedRecord in a span-preserving
# downsampling buffer (see _RealizedBuffer): when it fills, it halves
# resolution by merging older closes instead of dropping them, so the equity
# curve keeps its FULL time span (just coarser for old data) while memory
# stays bounded.
MAX_REALIZED_PER_INSTANCE = 4000
# Cap on the list seed() returns. Seed events are NOT replayed to clients
# (run_trades_loop suppresses them via trades.reset); the return value is only
# used for a post-reseed log count and small-input tests. Bounding it stops a
# large initial seed (a 185-318 MB trades.csv = 1-2M rows) from transiently
# materializing the entire file as pydantic TradeEvents — a startup RSS spike
# that could trip the container memory cap before steady state settles.
SEED_RETURN_CAP = 5000
# Cap on retained grid de-dup keys (instance_id, market_id, direction). These
# sets/dict suppress duplicate grid rows; they used to grow with every market
# ever written to trades.csv, so dashboard RSS crept up with the file. A
# market's entry->close window is minutes; once it settles the key never
# recurs in the append stream, so FIFO-evicting the oldest keys past this cap
# bounds memory without dropping any live de-dup. 60k keys covers many hours of
# markets across all instances.
MAX_SEEN_GRID_KEYS = 60000
TAIL_REWRITE_HASH_BYTES = 5 * 1024 * 1024
SLOW_POLL_WARN_MS = 250.0
REALIZED_EVENTS = {
    "TP_FILLED",
    "TP_SIGNAL_MARKET_EXIT",
    "WIN_EXPIRY",
    "LOSS_EXPIRY",
    "STOP_LOSS",
    "UNRESOLVED_RESTART",
}
WIN_EVENTS = {"TP_FILLED", "WIN_EXPIRY"}
LOSS_EVENTS = {"STOP_LOSS", "LOSS_EXPIRY"}


class RealizedRecord(NamedTuple):
    """Minimal projection of a realized TradeEvent kept for the equity curve
    and today-PnL math (``state_hub`` consumes only these four fields via
    attribute access). Storing this instead of the full pydantic TradeEvent
    cuts per-row memory several-fold; the recent-trades table still uses the
    full TradeEvent via ``_recent_per_instance``.

    NOTE: P&L mean/SD, max-drawdown, win-rate and other upper-panel statistics are
    NOT derived from this buffer — they come from the grid's full per-trade
    PnL history in ``state_snapshot.json`` / ``leaderboard.csv`` (see
    ``state_reader._pnl_mean_to_sd`` and ``state_hub`` leaderboard context), so
    downsampling here never affects them."""

    event: str
    timestamp: str
    pnl: Optional[float]
    capital: Optional[float]


def _add_opt(a: Optional[float], b: Optional[float]) -> Optional[float]:
    """Sum two optional floats, treating ``None`` as 0 but returning ``None``
    only when both are ``None`` (so merged records keep cumulative-PnL math)."""
    if a is None and b is None:
        return None
    return (a or 0.0) + (b or 0.0)


class _RealizedBuffer:
    """Span-preserving, memory-bounded store of one instance's realized closes.

    Feeds the equity timeseries (the left equity plot) and today-PnL math.
    Holds at most ``cap`` RealizedRecords; when it fills it *halves resolution*
    by merging adjacent interior closes (summing their PnL, keeping the later
    close's capital + timestamp) rather than dropping them. The first and last
    records are always kept exact, so the equity curve always spans the full
    history (start..latest) and its endpoints — curve start time and realized
    capital — stay precise. Only the interior resolution coarsens for very long
    histories, which is acceptable for a chart and keeps RSS bounded."""

    __slots__ = ("_cap", "_records")

    def __init__(self, cap: int) -> None:
        # >= 4 so keep-first/keep-last + interior merge always makes progress.
        self._cap = max(4, cap)
        self._records: list[RealizedRecord] = []

    def append(self, rec: RealizedRecord) -> None:
        self._records.append(rec)
        if len(self._records) > self._cap:
            self._halve()

    def _halve(self) -> None:
        recs = self._records
        last = len(recs) - 1
        merged: list[RealizedRecord] = [recs[0]]  # keep curve start exact
        i = 1
        while i < last:
            a = recs[i]
            if i + 1 < last:
                b = recs[i + 1]
                merged.append(
                    RealizedRecord(b.event, b.timestamp, _add_opt(a.pnl, b.pnl), b.capital)
                )
                i += 2
            else:
                merged.append(a)
                i += 1
        merged.append(recs[last])  # keep latest point (realized capital) exact
        self._records = merged

    def __len__(self) -> int:
        return len(self._records)

    def records(self) -> list[RealizedRecord]:
        return list(self._records)


class _BoundedKeySet:
    """Insertion-ordered set with FIFO eviction at ``cap`` (oldest key first).

    Backs the grid de-dup ``seen`` sets. Only long-settled keys are ever
    evicted, and settled markets never reappear in the append stream, so
    eviction cannot let a real duplicate through."""

    __slots__ = ("_cap", "_d")

    def __init__(self, cap: int) -> None:
        self._cap = max(1, cap)
        self._d: dict = {}

    def __contains__(self, key) -> bool:
        return key in self._d

    def add(self, key) -> None:
        d = self._d
        if key in d:
            return
        d[key] = None
        if len(d) > self._cap:
            del d[next(iter(d))]

    def clear(self) -> None:
        self._d.clear()

    def __len__(self) -> int:
        return len(self._d)


class _BoundedKeyDict:
    """Insertion-ordered mapping with FIFO eviction at ``cap`` (see
    _BoundedKeySet). Backs per-key grid entry prices."""

    __slots__ = ("_cap", "_d")

    def __init__(self, cap: int) -> None:
        self._cap = max(1, cap)
        self._d: dict = {}

    def __contains__(self, key) -> bool:
        return key in self._d

    def get(self, key, default=None):
        return self._d.get(key, default)

    def __setitem__(self, key, value) -> None:
        d = self._d
        if key not in d and len(d) >= self._cap:
            del d[next(iter(d))]
        d[key] = value

    def clear(self) -> None:
        self._d.clear()

    def __len__(self) -> int:
        return len(self._d)


def _duplicate_counts_text(counts: Counter[str]) -> str:
    return " ".join(f"{event}={count}" for event, count in sorted(counts.items()))


def _to_float(v: str) -> Optional[float]:
    if v == "" or v is None:
        return None
    try:
        return float(v)
    except ValueError:
        return None


def _to_int(v: str) -> Optional[int]:
    try:
        return int(v)
    except (ValueError, TypeError):
        return None


def _single_trader_instance_id() -> int:
    """Instance id used for single-trader CSV rows.

    Single-trader CSVs (live/paper 15m) do not carry an ``instance_id``. Bind
    them to the configured default selection so bootstrap ``equity_series`` and
    websocket trade events land in the same bucket the frontend is viewing.
    """
    return int(settings.default_instance_id)


def _row_to_events(
    row: dict,
    run_source: ViewSource,
    single_trader_instance_id: int,
) -> list[TradeEvent]:
    """Parse one CSV row into 0, 1, or 2 TradeEvents.

    Two schemas supported:
    - Grid per-event (instance_id, timestamp, event, ...) → 1 event
    - Single-trader per-position (id, opened_at, closed_at, result, ...) →
      ENTRY at opened_at + close event at closed_at
    """
    iid = _to_int(row.get("instance_id", ""))
    if iid is not None:
        ev = TradeEvent(
            instance_id=iid,
            timestamp=row.get("timestamp", ""),
            event=row.get("event", ""),
            run_source=run_source,
            direction=row.get("direction") or None,
            market_id=row.get("market_id") or None,
            entry_price=_to_float(row.get("entry_price", "")),
            exit_price=_to_float(row.get("exit_price", "")),
            shares=_to_float(row.get("shares", "")),
            pnl=_to_float(row.get("pnl", "")),
            pnl_pct=_to_float(row.get("pnl_pct", "")),
            capital=_to_float(row.get("capital", "")),
            model_prob=_to_float(row.get("model_prob", "")),
            poly_prob=_to_float(row.get("poly_prob", "")),
            spot_price=_to_float(row.get("spot_price", "")),
            barrier=_to_float(row.get("barrier", "")),
        )
        return [ev]

    opened_at = row.get("opened_at", "") or ""
    if not opened_at:
        return []
    direction = row.get("direction") or None
    entry_price = _to_float(row.get("entry_price", ""))
    exit_price = _to_float(row.get("exit_price", ""))
    shares = _to_float(row.get("shares", ""))
    pnl = _to_float(row.get("pnl", ""))
    pnl_pct = _to_float(row.get("pnl_pct", ""))
    model_prob = _to_float(row.get("model_prob", ""))
    poly_prob = _to_float(row.get("poly_price", ""))
    spot_price = _to_float(row.get("spot_price", ""))
    barrier = _to_float(row.get("reference_price", ""))
    closed_at = row.get("closed_at", "") or ""
    result = (row.get("result", "") or "").strip()

    entry_ev = TradeEvent(
        instance_id=single_trader_instance_id,
        timestamp=opened_at,
        event="ENTRY",
        run_source=run_source,
        direction=direction,
        market_id=None,
        entry_price=entry_price,
        exit_price=None,
        shares=shares,
        pnl=None,
        pnl_pct=None,
        capital=None,
        model_prob=model_prob,
        poly_prob=poly_prob,
        spot_price=spot_price,
        barrier=barrier,
    )
    if not closed_at or not result:
        return [entry_ev]
    close_ev = TradeEvent(
        instance_id=single_trader_instance_id,
        timestamp=closed_at,
        event=result,
        run_source=run_source,
        direction=direction,
        market_id=None,
        entry_price=entry_price,
        exit_price=exit_price,
        shares=shares,
        pnl=pnl,
        pnl_pct=pnl_pct,
        capital=None,
        model_prob=model_prob,
        poly_prob=poly_prob,
        spot_price=spot_price,
        barrier=barrier,
    )
    return [entry_ev, close_ev]


class TradesTail:
    def __init__(
        self,
        path_fn,
        run_source: ViewSource = "dry_run",
        single_trader_instance_id_fn=None,
    ) -> None:
        self._path_fn = path_fn
        self.run_source: ViewSource = run_source
        self._single_trader_instance_id_fn = single_trader_instance_id_fn
        self._last_path: Optional[Path] = None
        self._offset: int = 0
        self._header: Optional[list[str]] = None
        self._recent_per_instance: dict[int, Deque[TradeEvent]] = {}
        self._realized_per_instance: dict[int, _RealizedBuffer] = {}
        self._daily_per_instance: dict[int, dict[str, TodaySummary]] = {}
        self._last_size: int = 0
        # Hash of file content as of the last read. Lets us detect
        # in-place rewrites (size unchanged but content modified) — e.g.
        # when the live trader retracts a phantom TP_FILLED row by
        # rewriting it as VOIDED_PHANTOM_TP. Without this, append-only
        # tailing would keep showing the phantom row indefinitely.
        self._last_content_hash: Optional[str] = None
        self._last_tail_hash: Optional[str] = None
        self._last_mtime_ns: Optional[int] = None
        self._last_identity: Optional[tuple[int, int]] = None
        # Set when poll() detects an in-place rewrite and re-seeds. The
        # WS publisher checks-and-clears it to emit a `trades.reset`
        # envelope (full freshly-parsed list) so the frontend store can
        # discard its cached events and re-apply the new bootstrap.
        self._pending_reset: bool = False
        self._seen_grid_entries = _BoundedKeySet(MAX_SEEN_GRID_KEYS)
        self._seen_grid_closes = _BoundedKeySet(MAX_SEEN_GRID_KEYS)
        self._grid_entry_prices = _BoundedKeyDict(MAX_SEEN_GRID_KEYS)
        self._lock = RLock()
        self._busy: bool = False
        self._seed_requested: bool = False
        self._seed_reason: Optional[str] = None
        self._last_poll_ms: Optional[float] = None
        self._last_seed_ms: Optional[float] = None
        self._last_rewrite_check_ms: Optional[float] = None
        self._last_file_size_bytes: int = 0
        self._last_reset_reason: Optional[str] = None
        # Once the file has been fully read once, don't re-seed on subsequent
        # queries. Single-trader rows have market_id=None and bypass the
        # grid-only dedup, so re-seeding the same file would duplicate them
        # into the [100] deque on every miss-keyed lookup.
        self._seeded: bool = False

    @property
    def busy(self) -> bool:
        return self._busy

    def status(self) -> dict:
        return {
            "source": self.run_source,
            "path": str(self._last_path or self.path),
            "busy": self._busy,
            "seeded": self._seeded,
            "seed_requested": self._seed_requested,
            "seed_reason": self._seed_reason,
            "last_poll_ms": self._last_poll_ms,
            "last_seed_ms": self._last_seed_ms,
            "last_rewrite_check_ms": self._last_rewrite_check_ms,
            "file_size_bytes": self._last_file_size_bytes,
            "pending_reset": self._pending_reset,
            "last_reset_reason": self._last_reset_reason,
        }

    def request_seed(self, reason: str) -> None:
        with self._lock:
            if reason == "lazy_lookup" and (
                self._seeded or self._busy or self._seed_requested
            ):
                return
            self._seed_requested = True
            self._seed_reason = reason

    def take_seed_request(self) -> Optional[str]:
        with self._lock:
            if not self._seed_requested:
                return None
            reason = self._seed_reason or "requested"
            self._seed_requested = False
            self._seed_reason = None
            return reason

    def _single_trader_instance_id(self) -> int:
        if self._single_trader_instance_id_fn is not None:
            try:
                return int(self._single_trader_instance_id_fn())
            except (TypeError, ValueError):
                pass
        return _single_trader_instance_id()

    def _ensure_seeded_for_instance(self, instance_id: int) -> None:
        with self._lock:
            needs_seed = not self._seeded and not self._busy and not self._seed_requested
        if needs_seed:
            self.request_seed("lazy_lookup")

    @property
    def path(self) -> Path:
        p = self._path_fn()
        if self._last_path is not None and p != self._last_path:
            log.info("trades path changed %s -> %s; re-seeding", self._last_path.name, p.name)
            self._reset_state()
            self._seeded = False
        self._last_path = p
        return p

    def _is_duplicate_grid_event(self, event: TradeEvent) -> bool:
        return self._is_duplicate_grid_event_for(
            event,
            self._seen_grid_entries,
            self._seen_grid_closes,
            self._grid_entry_prices,
        )

    @staticmethod
    def _is_duplicate_grid_event_for(
        event: TradeEvent,
        seen_grid_entries: _BoundedKeySet,
        seen_grid_closes: _BoundedKeySet,
        grid_entry_prices: _BoundedKeyDict,
    ) -> bool:
        if not event.market_id:
            return False
        key = (event.instance_id, event.market_id, event.direction or "")
        if event.event == "ENTRY":
            if key in seen_grid_entries or key in seen_grid_closes:
                return True
            seen_grid_entries.add(key)
            if event.entry_price is not None:
                grid_entry_prices[key] = event.entry_price
            return False
        if event.event in REALIZED_EVENTS:
            if key in seen_grid_entries:
                if key in seen_grid_closes:
                    return True
                entry_price = grid_entry_prices.get(key)
                if (
                    entry_price is not None
                    and event.entry_price is not None
                    and abs(entry_price - event.entry_price) > 1e-9
                ):
                    return True
                seen_grid_closes.add(key)
        return False

    def _log_duplicate_summary(
        self,
        action: str,
        path: Path,
        duplicate_counts: Counter[str],
    ) -> None:
        total = sum(duplicate_counts.values())
        if total <= 0:
            return
        log.info(
            "trades_tail skipped %d duplicate grid event(s) while %s %s %s: %s",
            total,
            action,
            self.run_source,
            path.name,
            _duplicate_counts_text(duplicate_counts),
        )

    def _push(
        self,
        event: TradeEvent,
        duplicate_counts: Optional[Counter[str]] = None,
    ) -> bool:
        return self._push_to_state(
            event,
            self._recent_per_instance,
            self._realized_per_instance,
            self._daily_per_instance,
            self._seen_grid_entries,
            self._seen_grid_closes,
            self._grid_entry_prices,
            duplicate_counts,
        )

    def _push_to_state(
        self,
        event: TradeEvent,
        recent_per_instance: dict[int, Deque[TradeEvent]],
        realized_per_instance: dict[int, _RealizedBuffer],
        daily_per_instance: dict[int, dict[str, TodaySummary]],
        seen_grid_entries: _BoundedKeySet,
        seen_grid_closes: _BoundedKeySet,
        grid_entry_prices: _BoundedKeyDict,
        duplicate_counts: Optional[Counter[str]] = None,
    ) -> bool:
        if self._is_duplicate_grid_event_for(
            event,
            seen_grid_entries,
            seen_grid_closes,
            grid_entry_prices,
        ):
            if duplicate_counts is not None:
                duplicate_counts[event.event or "(blank)"] += 1
            else:
                log.debug(
                    "trades_tail skipped duplicate grid %s instance=%s market=%s",
                    event.event,
                    event.instance_id,
                    event.market_id,
            )
            return False
        dq = recent_per_instance.setdefault(
            event.instance_id,
            deque(maxlen=MAX_RECENT_PER_INSTANCE),
        )
        dq.append(event)
        if event.event in REALIZED_EVENTS and (
            event.capital is not None or event.pnl is not None
        ):
            history = realized_per_instance.setdefault(
                event.instance_id,
                _RealizedBuffer(MAX_REALIZED_PER_INSTANCE),
            )
            history.append(
                RealizedRecord(
                    event.event,
                    event.timestamp,
                    event.pnl,
                    event.capital,
                )
            )
        day_key = paris_date_key(event.timestamp)
        if day_key:
            by_day = daily_per_instance.setdefault(event.instance_id, {})
            summary = by_day.setdefault(day_key, TodaySummary())
            if event.event == "ENTRY":
                summary.entries += 1
            if event.event in REALIZED_EVENTS:
                summary.closed += 1
                if event.pnl is not None:
                    summary.pnl += event.pnl
                if event.event in WIN_EVENTS:
                    summary.wins += 1
                elif event.event in LOSS_EVENTS:
                    summary.losses += 1
        return True

    @staticmethod
    def _tail_hash(path: Path, size: int) -> Optional[str]:
        if size <= 0:
            return None
        h = hashlib.sha256()
        start = max(0, size - TAIL_REWRITE_HASH_BYTES)
        remaining = size - start
        try:
            with path.open("rb") as f:
                f.seek(start)
                while remaining > 0:
                    chunk = f.read(min(1024 * 1024, remaining))
                    if not chunk:
                        break
                    h.update(chunk)
                    remaining -= len(chunk)
        except OSError:
            return None
        return h.hexdigest()

    @staticmethod
    def _stat(path: Path) -> Optional[tuple[int, int]]:
        try:
            st = path.stat()
        except OSError:
            return None
        return st.st_size, st.st_mtime_ns

    @staticmethod
    def _identity(path: Path) -> Optional[tuple[int, int]]:
        try:
            st = path.stat()
            return st.st_dev, st.st_ino
        except OSError:
            return None

    @staticmethod
    def _voided_grid_keys(path: Path) -> set[tuple[int, str, str]]:
        """Stream correction markers before replaying their original rows.

        Grid logs have one physical line per event. Only sparse correction rows
        need CSV parsing here; ordinary history is parsed once by seed().
        """
        voided = set()
        with path.open("r", encoding="utf-8", newline="") as file:
            header = next(csv.reader(file), [])
            for line in file:
                if ",ENTRY_VOID," not in line or not line.endswith("\n"):
                    continue
                row = next(csv.DictReader([line], fieldnames=header))
                instance_id = _to_int(row.get("instance_id"))
                if row.get("event") == "ENTRY_VOID" and instance_id is not None and row.get("market_id"):
                    voided.add((instance_id, row["market_id"], row.get("direction") or ""))
        return voided

    def seed(self, per_instance_limit: int = MAX_RECENT_PER_INSTANCE) -> list[TradeEvent]:
        """Replay complete rows with corrections, then advance the consumed offset."""
        started = time.perf_counter()
        p = self.path
        if not p.exists():
            return []
        with self._lock:
            self._busy = True
        new_events: Deque[TradeEvent] = deque(maxlen=SEED_RETURN_CAP)
        duplicate_counts: Counter[str] = Counter()
        recent_per_instance: dict[int, Deque[TradeEvent]] = {}
        realized_per_instance: dict[int, _RealizedBuffer] = {}
        daily_per_instance: dict[int, dict[str, TodaySummary]] = {}
        seen_grid_entries = _BoundedKeySet(MAX_SEEN_GRID_KEYS)
        seen_grid_closes = _BoundedKeySet(MAX_SEEN_GRID_KEYS)
        grid_entry_prices = _BoundedKeyDict(MAX_SEEN_GRID_KEYS)
        header: Optional[list[str]] = None
        parsed_offset = 0
        stat = self._stat(p)
        identity = self._identity(p)
        try:
            voided = self._voided_grid_keys(p) if self.run_source == "dry_run" else set()
            with p.open("rb") as f:
                def complete_lines():
                    nonlocal parsed_offset
                    for line in f:
                        if not line.endswith(b"\n"):
                            return
                        parsed_offset = f.tell()
                        yield line.decode("utf-8")

                reader = csv.DictReader(complete_lines(), strict=True)
                header = reader.fieldnames
                for row in reader:
                    for ev in _row_to_events(
                        row,
                        self.run_source,
                        self._single_trader_instance_id(),
                    ):
                        if ev.event != "ENTRY_VOID" and (ev.instance_id, ev.market_id, ev.direction or "") in voided:
                            continue
                        if self._push_to_state(
                            ev,
                            recent_per_instance,
                            realized_per_instance,
                            daily_per_instance,
                            seen_grid_entries,
                            seen_grid_closes,
                            grid_entry_prices,
                            duplicate_counts,
                        ):
                            new_events.append(ev)
        except OSError:
            with self._lock:
                self._busy = False
            return []
        except Exception:
            with self._lock:
                self._busy = False
            raise
        stat = self._stat(p) or stat
        if self._identity(p) != identity:
            # An atomic correction replaced the file while this seed was in
            # flight. Keep the previous coherent snapshot and retry the new file.
            with self._lock:
                self._busy = False
            self._mark_reset_requested("replaced_during_seed")
            return []
        size = stat[0] if stat else parsed_offset
        mtime_ns = stat[1] if stat else None
        tail_hash = self._tail_hash(p, parsed_offset)
        with self._lock:
            self._header = header
            self._recent_per_instance = recent_per_instance
            self._realized_per_instance = realized_per_instance
            self._daily_per_instance = daily_per_instance
            self._seen_grid_entries = seen_grid_entries
            self._seen_grid_closes = seen_grid_closes
            self._grid_entry_prices = grid_entry_prices
            self._offset = parsed_offset
            self._last_size = parsed_offset
            self._last_mtime_ns = mtime_ns
            self._last_identity = identity
            self._last_tail_hash = tail_hash
            self._last_content_hash = tail_hash
            self._last_file_size_bytes = size
            self._seeded = True
            self._busy = False
            self._seed_requested = False
            self._seed_reason = None
            self._last_seed_ms = (time.perf_counter() - started) * 1000.0
        self._log_duplicate_summary("seeding", p, duplicate_counts)
        return list(new_events)

    def poll(self) -> list[TradeEvent]:
        started = time.perf_counter()
        p = self.path
        if not p.exists():
            return []
        stat = self._stat(p)
        if stat is None:
            return []
        size, mtime_ns = stat
        identity = self._identity(p)
        with self._lock:
            last_size = self._last_size
            last_mtime_ns = self._last_mtime_ns
            last_tail_hash = self._last_tail_hash
            offset = self._offset
            header = self._header
            last_identity = self._last_identity
        self._last_file_size_bytes = size
        if last_identity is not None and identity != last_identity:
            self._mark_reset_requested("replaced")
            self._record_poll_ms(started)
            return []
        if size < offset:
            # Truncation/rotation — re-seed
            log.info("trades.csv truncated; re-seeding")
            self._mark_reset_requested("truncated")
            self._record_poll_ms(started)
            return []
        if size == last_size and mtime_ns == last_mtime_ns:
            self._record_poll_ms(started)
            return []
        if offset and last_tail_hash is not None:
            # Growth can be a correction, not just append. Compare the bytes
            # already consumed, bounded to the same tail used for the baseline.
            check_started = time.perf_counter()
            tail_hash = self._tail_hash(p, offset)
            self._last_rewrite_check_ms = (time.perf_counter() - check_started) * 1000.0
            if tail_hash != last_tail_hash:
                self._mark_reset_requested("consumed_tail_rewrite")
                self._record_poll_ms(started)
                return []
            if size == last_size:
                with self._lock:
                    self._last_mtime_ns = mtime_ns
                self._record_poll_ms(started)
                return []

        new_events: list[TradeEvent] = []
        duplicate_counts: Counter[str] = Counter()
        try:
            with p.open("r", encoding="utf-8", newline="") as f:
                if header is None:
                    self.request_seed("file_created")
                    self._record_poll_ms(started)
                    return []
                f.seek(offset)
                # Only process complete lines
                remainder = f.read()
                if "\n" not in remainder:
                    self._record_poll_ms(started)
                    return []
                last_nl = remainder.rfind("\n")
                complete = remainder[: last_nl + 1]
                consumed = len(complete.encode("utf-8"))
                reader = csv.DictReader(io.StringIO(complete), fieldnames=header)
                for row in reader:
                    new_events.extend(_row_to_events(
                        row,
                        self.run_source,
                        self._single_trader_instance_id(),
                    ))
        except OSError:
            self._record_poll_ms(started)
            return []
        if self._identity(p) != identity:
            self._mark_reset_requested("replaced_during_poll")
            self._record_poll_ms(started)
            return []
        if self.run_source == "dry_run" and any(ev.event == "ENTRY_VOID" for ev in new_events):
            self._mark_reset_requested("entry_void")
            self._record_poll_ms(started)
            return []
        tail_hash = self._tail_hash(p, offset + consumed)
        with self._lock:
            new_events = [ev for ev in new_events if self._push(ev, duplicate_counts)]
            self._offset = offset + consumed
            self._last_size = size
            self._last_mtime_ns = mtime_ns
            self._last_identity = identity
            self._last_tail_hash = tail_hash
            self._last_content_hash = tail_hash
            self._last_file_size_bytes = size
        self._log_duplicate_summary("polling", p, duplicate_counts)
        self._record_poll_ms(started)
        return new_events

    def _record_poll_ms(self, started: float) -> None:
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        self._last_poll_ms = elapsed_ms
        if elapsed_ms > SLOW_POLL_WARN_MS:
            log.warning(
                "trades_tail slow poll source=%s path=%s ms=%.1f size_mb=%.1f",
                self.run_source,
                self._last_path.name if self._last_path else "?",
                elapsed_ms,
                self._last_file_size_bytes / (1024 * 1024),
            )

    def _mark_reset_requested(self, reason: str) -> None:
        with self._lock:
            self._pending_reset = True
            self._last_reset_reason = reason
            self._seed_requested = True
            self._seed_reason = reason

    def _reset_state(self) -> None:
        self._offset = 0
        self._last_size = 0
        self._last_content_hash = None
        self._last_tail_hash = None
        self._last_mtime_ns = None
        self._last_identity = None
        self._recent_per_instance.clear()
        self._realized_per_instance.clear()
        self._daily_per_instance.clear()
        self._seen_grid_entries.clear()
        self._seen_grid_closes.clear()
        self._grid_entry_prices.clear()
        self._seeded = False

    def take_pending_reset(self) -> bool:
        """Return True (and clear the flag) if a re-seed just happened
        because of an in-place CSV rewrite. The WS publisher uses this
        to emit a ``trades.reset`` envelope so the frontend store can
        clear its cached events and re-apply the freshly-parsed list.
        """
        if self._pending_reset:
            self._pending_reset = False
            return True
        return False

    def snapshot_all_events(self) -> list[dict]:
        """Return every event currently held, oldest-first, for re-seed
        broadcasts. Mirrors what ``recent`` would publish but across
        all instances and unbounded in size."""
        events: list[TradeEvent] = []
        for dq in self._recent_per_instance.values():
            events.extend(dq)
        events.sort(key=lambda e: e.timestamp or "")
        return [e.model_dump() for e in events]

    def snapshot_events_for_instance(
        self,
        instance_id: int,
        n: int = MAX_RECENT_PER_INSTANCE,
    ) -> list[dict]:
        return [ev.model_dump() for ev in reversed(self.recent(instance_id, n=n))]

    def recent(self, instance_id: int, n: int = 50) -> list[TradeEvent]:
        self._ensure_seeded_for_instance(instance_id)
        dq = self._recent_per_instance.get(instance_id)
        if not dq:
            return []
        items = list(dq)
        items.reverse()
        return items[:n]

    def chronological(self, instance_id: int) -> list[RealizedRecord]:
        return self.realized_history(instance_id)

    def realized_history(self, instance_id: int) -> list[RealizedRecord]:
        self._ensure_seeded_for_instance(instance_id)
        buf = self._realized_per_instance.get(instance_id)
        return buf.records() if buf is not None else []

    def today_summary(self, instance_id: int, day_key: str) -> TodaySummary:
        self._ensure_seeded_for_instance(instance_id)
        summary = self._daily_per_instance.get(instance_id, {}).get(day_key)
        return summary.model_copy() if summary else TodaySummary()


async def run_trades_loop(tail: "TradesTail", stop: asyncio.Event) -> None:
    while not stop.is_set():
        seed_reason = tail.take_seed_request()
        if seed_reason is not None:
            try:
                new_events = await asyncio.to_thread(tail.seed)
            except Exception:  # noqa: BLE001
                log.exception("trades seed error")
                new_events = []
            if tail.take_pending_reset():
                await bus.publish(
                    "trades.reset",
                    {
                        "source": tail.run_source,
                        "reason": seed_reason,
                        "events": None,
                    },
                )
                if new_events:
                    log.info(
                        "trades_tail suppressed %d replayed event(s) after %s re-seed",
                        len(new_events),
                        tail.run_source,
                    )
            try:
                await asyncio.wait_for(stop.wait(), timeout=settings.poll_interval_seconds)
            except asyncio.TimeoutError:
                pass
            continue
        try:
            new_events = await asyncio.to_thread(tail.poll)
        except Exception:  # noqa: BLE001
            log.exception("trades poll error")
            new_events = []

        for ev in new_events:
            # Suppress the retroactive ENTRY that comes with a closed-row
            # pair if state_reader already published one at open time.
            # Internal bookkeeping above still consumed the event so the
            # bootstrap markers / today-counter remain accurate.
            ev_source = ev.run_source or tail.run_source
            if ev.event == "ENTRY" and entry_registry.was_emitted(
                ev_source,
                ev.timestamp,
            ):
                log.info(
                    "trades_tail skipped duplicate ENTRY opened_at=%s "
                    "(state_reader already emitted)",
                    ev.timestamp,
                )
                continue
            await bus.publish("trade.append", ev.model_dump())
        try:
            await asyncio.wait_for(stop.wait(), timeout=settings.poll_interval_seconds)
        except asyncio.TimeoutError:
            pass
