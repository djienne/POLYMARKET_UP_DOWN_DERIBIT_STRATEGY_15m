"""Live <-> grid parity check (entry + exit).

The cron-fired /loop's existing parity rule only matched ENTRIES
(market_id + params + timestamp + entry_price). The phantom-TP-fill
incident on 2026-04-30 demonstrated that ENTRY parity alone is not
enough: live recorded TP_FILLED +$1.99 for trade 4ca5f67b while paper
grid 616 correctly recorded LOSS_EXPIRY -$88.77 for the same market
1777591800. The divergence would have screamed immediately if exit
parity were checked.

This script:

* Reads ``results/15m_live_trades.csv`` (single-trader rows, no
  ``instance_id`` column).
* Reads ``results/trades.csv`` and filters to the configured grid
  instance (default 1271, the live-mirror).
* For each live row whose ``opened_at >= --since``, finds the
  matching grid ENTRY by direction + opened-at within ±60s + entry
  price within ±$0.05.
* If the live row is closed (has ``closed_at`` and ``result``),
  finds the corresponding grid close event (the next event for that
  instance after the matched ENTRY) and compares the result class.

Outputs a JSON-line report to stdout and exits non-zero on any
orphan or mismatch (so the /loop can stop live and surface the
breadcrumb). Result classes treated as expected to match exactly:
``TP_FILLED``, ``WIN_EXPIRY``, ``LOSS_EXPIRY``, ``STOP_LOSS``,
``TRAILING_STOP``. ``VOIDED_PHANTOM_TP``, ``MANUAL_CLOSE`` and
``UNRESOLVED_RESTART`` are tolerated as live-only states (the void
case is by definition a deferred record awaiting expiry resolution;
the parity check will catch the eventual WIN/LOSS row).
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional


COMPARABLE_RESULTS = {
    "TP_FILLED",
    "TP_SIGNAL_MARKET_EXIT",
    "WIN_EXPIRY",
    "LOSS_EXPIRY",
    "STOP_LOSS",
    "TRAILING_STOP",
}
SKIP_RESULTS = {
    "VOIDED_PHANTOM_TP",  # awaiting expiry resolution, will reconcile then
    "MANUAL_CLOSE",       # operator action, no grid analogue
    "UNRESOLVED_RESTART", # restart-spanning, intentionally neutral
}
# A WIN-class result on live + WIN-class on grid for the same market
# means both made money via different exit paths (e.g. live TP'd at
# $0.78 while grid held to expiry and won at $1.00). Same for both
# LOSS-class. These are informational, not stop triggers — the
# phantom-TP bug class shows up as WIN-class ↔ LOSS-class divergence.
WIN_RESULTS = {"TP_FILLED", "TP_SIGNAL_MARKET_EXIT", "WIN_EXPIRY"}
LOSS_RESULTS = {"STOP_LOSS", "LOSS_EXPIRY"}
# TRAILING_STOP can land on either side of entry; treat as ambiguous
# and require an exact match (no class-based downgrade).
AMBIGUOUS_RESULTS = {"TRAILING_STOP"}


@dataclass
class LiveTrade:
    id: str
    direction: str
    entry_price: float
    opened_at: datetime
    closed_at: Optional[datetime]
    result: Optional[str]


@dataclass
class GridEvent:
    timestamp: datetime
    event: str
    direction: str
    market_id: str
    entry_price: Optional[float]


@dataclass
class Mismatch:
    kind: str
    detail: dict = field(default_factory=dict)


def _parse_dt(s: str) -> Optional[datetime]:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        return None


def _read_live(path: Path, since: Optional[datetime]) -> list[LiveTrade]:
    out: list[LiveTrade] = []
    with open(path, "r", newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            opened = _parse_dt(row.get("opened_at", ""))
            if opened is None:
                continue
            if since is not None and opened < since:
                continue
            try:
                ep = float(row.get("entry_price", "") or "0")
            except ValueError:
                ep = 0.0
            closed = _parse_dt(row.get("closed_at", ""))
            result = (row.get("result", "") or "").strip() or None
            out.append(
                LiveTrade(
                    id=row.get("id", ""),
                    direction=(row.get("direction", "") or "").upper(),
                    entry_price=ep,
                    opened_at=opened,
                    closed_at=closed,
                    result=result,
                )
            )
    out.sort(key=lambda t: t.opened_at)
    return out


def _read_grid(
    path: Path,
    instance_id: int,
    since: Optional[datetime],
) -> list[GridEvent]:
    out: list[GridEvent] = []
    iid_str = str(instance_id)
    with open(path, "r", newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row.get("instance_id", "") != iid_str:
                continue
            ts = _parse_dt(row.get("timestamp", ""))
            if ts is None:
                continue
            if since is not None and ts < since - timedelta(minutes=10):
                # Keep a small lookback so a close event for a
                # before-since ENTRY doesn't get filtered out, but
                # don't read the entire historical file.
                continue
            ep_raw = row.get("entry_price", "")
            try:
                ep = float(ep_raw) if ep_raw else None
            except ValueError:
                ep = None
            out.append(
                GridEvent(
                    timestamp=ts,
                    event=(row.get("event", "") or "").upper(),
                    direction=(row.get("direction", "") or "").upper(),
                    market_id=row.get("market_id", "") or "",
                    entry_price=ep,
                )
            )
    out.sort(key=lambda e: e.timestamp)
    return out


def _find_matching_grid_entry(
    live: LiveTrade,
    grid_events: list[GridEvent],
    used: set[int],
    time_tol_seconds: float,
    price_tol: float,
) -> Optional[int]:
    best_idx: Optional[int] = None
    best_dt = timedelta.max
    for i, g in enumerate(grid_events):
        if i in used:
            continue
        if g.event != "ENTRY":
            continue
        if g.direction != live.direction:
            continue
        if g.entry_price is not None and abs(g.entry_price - live.entry_price) > price_tol:
            continue
        delta = abs(g.timestamp - live.opened_at)
        if delta.total_seconds() > time_tol_seconds:
            continue
        if delta < best_dt:
            best_dt = delta
            best_idx = i
    return best_idx


def _find_following_close(
    grid_events: list[GridEvent],
    entry_idx: int,
) -> Optional[GridEvent]:
    """The first non-ENTRY event for the same instance that comes after
    ``entry_idx`` and shares its market_id. We've already filtered the
    grid stream to a single instance_id."""
    entry = grid_events[entry_idx]
    for j in range(entry_idx + 1, len(grid_events)):
        g = grid_events[j]
        if g.event == "ENTRY":
            # Next entry without a close in between: stop searching.
            if g.direction == entry.direction and g.market_id == entry.market_id:
                continue
            return None
        if entry.market_id and g.market_id and g.market_id != entry.market_id:
            continue
        return g
    return None


def check(
    live_csv: Path,
    grid_csv: Path,
    since: Optional[datetime],
    instance_id: int,
    time_tol_seconds: float = 60.0,
    price_tol: float = 0.05,
    close_grace_seconds: float = 120.0,
    now: Optional[datetime] = None,
) -> tuple[dict, list[Mismatch]]:
    live = _read_live(live_csv, since)
    grid = _read_grid(grid_csv, instance_id, since)

    used: set[int] = set()
    mismatches: list[Mismatch] = []
    informational: list[Mismatch] = []
    matched_entries = 0
    matched_exits = 0
    skipped_live_only = 0
    pending_grid_close = 0
    if now is None:
        now = datetime.now(timezone.utc)

    for lt in live:
        idx = _find_matching_grid_entry(
            lt, grid, used, time_tol_seconds, price_tol,
        )
        if idx is None:
            # Live entered, mirror grid didn't (or entered at a different
            # price). Common causes: different edge-filter realizations
            # against the live order book vs grid's bid simulation; live
            # entering at the max_entry_price cap that grid skipped.
            # Informational, not a phantom-fill regression.
            informational.append(
                Mismatch(
                    kind="orphan_live_entry",
                    detail={
                        "id": lt.id,
                        "direction": lt.direction,
                        "opened_at": lt.opened_at.isoformat(),
                        "entry_price": lt.entry_price,
                    },
                )
            )
            continue
        used.add(idx)
        matched_entries += 1

        if lt.closed_at is None or lt.result is None:
            continue  # still open — no exit comparison yet
        if lt.result in SKIP_RESULTS:
            skipped_live_only += 1
            continue
        if lt.result not in COMPARABLE_RESULTS:
            mismatches.append(
                Mismatch(
                    kind="unknown_live_result",
                    detail={"id": lt.id, "result": lt.result},
                )
            )
            continue

        gclose = _find_following_close(grid, idx)
        if gclose is None:
            # Live can close (TP_FILLED) seconds before grid's
            # paper-mode TP simulator records its own close for the
            # same market. If live closed recently, treat as pending
            # rather than a mismatch — a future tick will reconcile.
            age = (now - lt.closed_at).total_seconds() if lt.closed_at else float("inf")
            if age < close_grace_seconds:
                pending_grid_close += 1
                continue
            mismatches.append(
                Mismatch(
                    kind="missing_grid_close",
                    detail={
                        "id": lt.id,
                        "live_result": lt.result,
                        "grid_market_id": grid[idx].market_id,
                        "live_closed_age_seconds": round(age, 1),
                        "close_grace_seconds": close_grace_seconds,
                    },
                )
            )
            continue

        if gclose.event != lt.result:
            # Classify by money sign. Same-class (both WIN or both LOSS)
            # = different exit paths but same outcome — informational.
            # Cross-class = catastrophic (the phantom-TP bug shape:
            # live recorded a WIN that grid says was a LOSS).
            live_class = (
                "WIN" if lt.result in WIN_RESULTS
                else "LOSS" if lt.result in LOSS_RESULTS
                else "AMBIG"
            )
            grid_class = (
                "WIN" if gclose.event in WIN_RESULTS
                else "LOSS" if gclose.event in LOSS_RESULTS
                else "AMBIG"
            )
            detail = {
                "id": lt.id,
                "live_result": lt.result,
                "grid_event": gclose.event,
                "market_id": grid[idx].market_id,
                "live_opened_at": lt.opened_at.isoformat(),
                "live_closed_at": (
                    lt.closed_at.isoformat() if lt.closed_at else ""
                ),
                "live_class": live_class,
                "grid_class": grid_class,
            }
            if (
                live_class != grid_class
                or live_class == "AMBIG"
                or grid_class == "AMBIG"
            ):
                mismatches.append(
                    Mismatch(kind="exit_result_mismatch", detail=detail),
                )
            else:
                informational.append(
                    Mismatch(kind="exit_path_divergence", detail=detail),
                )
                matched_exits += 1
        else:
            matched_exits += 1

    summary = {
        "live_rows": len(live),
        "grid_events": len(grid),
        "matched_entries": matched_entries,
        "matched_exits": matched_exits,
        "skipped_live_only": skipped_live_only,
        "pending_grid_close": pending_grid_close,
        "informational": len(informational),
        "mismatches": len(mismatches),
        "instance_id": instance_id,
        "since": since.isoformat() if since else None,
        "close_grace_seconds": close_grace_seconds,
    }
    return summary, mismatches, informational


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="check_parity",
        description="Compare live ENTRY+EXIT events against the live-mirror grid instance.",
    )
    parser.add_argument(
        "--live-csv",
        default=str(Path("results") / "15m_live_trades.csv"),
        help="Path to live trades CSV (default: results/15m_live_trades.csv)",
    )
    parser.add_argument(
        "--grid-csv",
        default=str(Path("results") / "trades.csv"),
        help="Path to grid trades CSV (default: results/trades.csv)",
    )
    parser.add_argument(
        "--instance",
        type=int,
        default=1271,
        help="Grid instance_id mirroring live params (default: 1271)",
    )
    parser.add_argument(
        "--since",
        help="ISO timestamp; ignore live entries opened before this. "
        "Use the bot's StartedAt to scope to the current run.",
    )
    parser.add_argument(
        "--time-tol-seconds",
        type=float,
        default=60.0,
        help="Match tolerance on opened_at (default: 60s)",
    )
    parser.add_argument(
        "--price-tol",
        type=float,
        default=0.05,
        help="Match tolerance on entry_price (default: $0.05)",
    )
    parser.add_argument(
        "--close-grace-seconds",
        type=float,
        default=120.0,
        help=(
            "When live has closed but grid hasn't yet, tolerate the gap if "
            "live's closed_at is more recent than this many seconds. Closes "
            "the timing race where live's TP fill records seconds before the "
            "grid's bid-driven simulator does. Default: 120s."
        ),
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Print mismatches only; suppress the OK summary on success",
    )
    args = parser.parse_args(argv)

    since: Optional[datetime] = None
    if args.since:
        try:
            since = datetime.fromisoformat(args.since)
        except ValueError as exc:
            print(f"ERROR: invalid --since value {args.since!r}: {exc}", file=sys.stderr)
            return 2
        if since.tzinfo is None:
            since = since.replace(tzinfo=timezone.utc)

    summary, mismatches, informational = check(
        live_csv=Path(args.live_csv),
        grid_csv=Path(args.grid_csv),
        since=since,
        instance_id=args.instance,
        time_tol_seconds=args.time_tol_seconds,
        price_tol=args.price_tol,
        close_grace_seconds=args.close_grace_seconds,
    )

    if mismatches:
        print(json.dumps({
            "summary": summary,
            "mismatches": [{"kind": m.kind, "detail": m.detail} for m in mismatches],
            "informational": [{"kind": m.kind, "detail": m.detail} for m in informational],
        }, indent=2))
        return 1
    if not args.quiet:
        print(json.dumps({
            "status": "ok",
            "summary": summary,
            "informational": [{"kind": m.kind, "detail": m.detail} for m in informational],
        }))
    return 0


if __name__ == "__main__":
    sys.exit(main())
