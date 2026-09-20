"""Dedup ENTRY trade events across the two emitters.

- state_reader publishes an ENTRY via ``trade.append`` the instant the live
  state file flips from no-position to position-open. This is the fast path
  that makes the entry GIF and entry chart marker appear at open time.
- trades_tail reads rows from ``15m_live_trades.csv``. Single-trader rows
  only appear at close time, but they contain both ``opened_at`` and
  ``closed_at``, so ``_row_to_events`` naturally splits into
  [ENTRY, CLOSE]. That ENTRY is retroactive — minutes late — and would
  re-fire the GIF alongside the WIN/LOSS gif if not suppressed.

This registry is the handshake: whichever path emits ENTRY first marks the
``(run_source, opened_at)`` here, and the other skips only within the same
source. Strings are compared verbatim, so both sides must read ``opened_at``
from the same source shape (they do — live trader writes ISO-8601 with
microseconds from a single datetime instance for both state.json and
trades.csv).
"""
from __future__ import annotations

_emitted: set[tuple[str, str]] = set()


def _key(run_source: str | None, opened_at: str) -> tuple[str, str]:
    source = "live" if run_source == "live" else "dry_run"
    return source, opened_at


def mark_emitted(run_source: str | None, opened_at: str) -> None:
    if not opened_at:
        return
    _emitted.add(_key(run_source, opened_at))


def was_emitted(run_source: str | None, opened_at: str) -> bool:
    if not opened_at:
        return False
    return _key(run_source, opened_at) in _emitted


def reset() -> None:
    _emitted.clear()
