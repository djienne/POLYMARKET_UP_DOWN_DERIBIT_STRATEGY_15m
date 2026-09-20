"""Generic offline cleaner for phantom dry-run grid fills.

Replaces the hard-coded ``scripts/cleanup_phantom_1271.py`` one-off. Identifies
grid (paper) ENTRYs that the live trader could not actually place, and corrects
them with append-only ``ENTRY_VOID`` markers so the rebuilt leaderboard reflects
live-achievable fills. Report-only by default; ``--apply`` to write.

Evidence tiers
--------------
* **Tier A — direct mirror rejection** (15m only): the ``15m_mirror_signal_audit``
  shows a ``rejected`` ENTRY for this ``source_instance_id`` + ``market_id`` with
  no live order and no live position, and *no* later accepted entry for that
  market. Strongest evidence — solves the discovery-lag case where the dry fill
  predates the observed outage. Auto-applied.
* **Tier B — confirmed availability interval**: the dry entry timestamp falls
  inside a ``confirmed`` unavailability interval whose scope applies (platform
  for any instance/market; market for the matching market). Auto-applied.
* **Tier C — inferred interval**: entry inside an inferred-only interval. Reported
  only; requires ``--include-inferred`` to apply.
* **Tier D — none**: reported only, never applied.

The 5m bot (``--bot 5m``) is dry-run only and has no live mirror verdicts, so
Tier A is skipped; it uses Tier B from the 15m-exported shared platform ledger
(``--shared-ledger``) and Tier C from its own grid read-probe health log.

Safety
------
* report-only unless ``--apply``;
* gzip backup of the trades CSV before any write;
* refuses if the trades CSV was modified within ``--min-quiet-seconds`` (default
  30s) unless ``--force`` — don't fight a running grid writer;
* append-only ``ENTRY_VOID`` markers by default (audit-preserving); physical row
  deletion only with ``--physical-delete`` for emergency stopped-grid repair;
* writes a ``*_void_audit.csv`` trail and (with ``--rebuild-ledger``) refreshes
  the availability ledger from the evidence sources.

Examples
--------
    python -m scripts.trader.cli.clean_unavailable_dry_trades --bot 15m --instance 1271 --since 2026-06-01
    python -m scripts.trader.cli.clean_unavailable_dry_trades --bot 15m --instance 1271 --since 2026-06-01 --apply
    python -m scripts.trader.cli.clean_unavailable_dry_trades --bot 5m --all --since 2026-06-01
"""
from __future__ import annotations

import argparse
import csv
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from scripts.trader import availability as av
from scripts.trader._io import ensure_trailing_newline
from scripts.trader.cli._shared import (
    backup_trades as _backup_trades,
    grid_lock_is_live as _grid_lock_is_live,
    read_csv_header as _read_header,
)

DEFAULT_INSTANCE = {"15m": 1271, "5m": None}
DEFAULT_MIN_QUIET_SECONDS = 30.0
GRID_CLOSE_EVENTS = {
    "TP_FILLED", "TP_SIGNAL_MARKET_EXIT", "WIN_EXPIRY", "LOSS_EXPIRY",
    "STOP_LOSS", "TRAILING_STOP", "UNRESOLVED_RESTART",
}
VOID_EVENT = "ENTRY_VOID"


@dataclass
class DryEntry:
    instance_id: int
    market_id: str
    direction: str
    entry_price: float
    timestamp: datetime
    close_event: Optional[str] = None
    close_pnl: float = 0.0
    raw: dict = field(default_factory=dict)


@dataclass
class Verdict:
    entry: DryEntry
    tier: str  # A | B | C | D
    reason: str
    scope: str = ""
    confidence: str = ""
    # The matched interval has neither an end nor an expiry estimate (e.g. an
    # unclosed ``polymarket_down`` edge after a live-trader restart). Such an
    # interval covers *every* later timestamp, so auto-applying it would void
    # the entire history after its start — never auto-apply without an explicit
    # ``--allow-open-intervals``.
    unbounded: bool = False

    @property
    def applicable(self) -> bool:
        return self.tier in ("A", "B")


# ── default paths per bot ─────────────────────────────────────────────────────
def _defaults(bot: str, results: Path) -> dict:
    prefix = "15m" if bot == "15m" else "5m"
    return {
        "trades": results / "trades.csv",
        "mirror_audit": results / f"{prefix}_mirror_signal_audit.csv",
        "health_log": results / f"{prefix}_clob_health_log.csv",
        "events": results / "availability_events.csv",
        "ledger": results / "availability_ledger.csv",
        "shared_ledger": None,
    }


# ── grid trades.csv reading ───────────────────────────────────────────────────
def _parse_dt(value: object) -> Optional[datetime]:
    return av.parse_dt(value)


def _float(value: object) -> float:
    if value in (None, ""):
        return 0.0
    try:
        return float(str(value).rstrip("%"))
    except (TypeError, ValueError):
        return 0.0


def read_grid_entries(
    trades_path: Path,
    *,
    instance_ids: Optional[set[int]],
    since: Optional[datetime],
    until: Optional[datetime],
) -> tuple[list[DryEntry], set[tuple[int, str]]]:
    """Return (open-or-closed dry entries in range, already-voided keys).

    Pairs each ENTRY with its close (by instance+market+direction+entry_price) so
    the report can show the PnL each void removes. An instance enters each market
    at most once, so (instance_id, market_id) uniquely keys an entry.
    """
    if not trades_path.exists():
        raise SystemExit(f"trades file not found: {trades_path}")
    entries: dict[tuple[int, str], DryEntry] = {}
    voided: set[tuple[int, str]] = set()
    closes: dict[tuple[int, str], tuple[str, float]] = {}
    with trades_path.open("r", newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            try:
                iid = int(row.get("instance_id") or -1)
            except (TypeError, ValueError):
                continue
            if iid < 0:
                continue
            if instance_ids is not None and iid not in instance_ids:
                continue
            event = (row.get("event") or "").strip()
            market_id = (row.get("market_id") or "").strip()
            key = (iid, market_id)
            if event == VOID_EVENT:
                voided.add(key)
                continue
            if event == "ENTRY":
                ts = _parse_dt(row.get("timestamp"))
                if ts is None:
                    continue
                if since is not None and ts < since:
                    continue
                if until is not None and ts > until:
                    continue
                entries[key] = DryEntry(
                    instance_id=iid,
                    market_id=market_id,
                    direction=(row.get("direction") or "").strip().upper(),
                    entry_price=_float(row.get("entry_price")),
                    timestamp=ts,
                    raw=dict(row),
                )
            elif event in GRID_CLOSE_EVENTS:
                closes[key] = (event, _float(row.get("pnl")))
    for key, entry in entries.items():
        if key in closes:
            entry.close_event, entry.close_pnl = closes[key]
    # Drop already-voided entries from consideration (idempotent re-run).
    live_entries = [e for k, e in entries.items() if k not in voided]
    live_entries.sort(key=lambda e: (e.instance_id, e.timestamp))
    return live_entries, voided


# ── evidence assembly ──────────────────────────────────────────────────────────
def build_evidence(
    *,
    bot: str,
    paths: dict,
    instance_ids: Optional[set[int]],
    coalesce_gap_seconds: float,
) -> tuple[list[av.AvailabilityInterval], dict[tuple[int, str], str]]:
    """Build (intervals, tier-A rejection map).

    The tier-A map is ``{(instance_id, market_id): reason}`` for markets a live
    mirror ENTRY was rejected on with no order/position and no later acceptance.
    Intervals are compacted from the health log + mirror audit + any persisted
    availability events.
    """
    events: list[av.AvailabilityEvent] = []
    if paths.get("health_log"):
        events.extend(av.events_from_health_log(paths["health_log"], source_bot=bot))
    rejected: dict[tuple[int, str], str] = {}
    if bot == "15m" and paths.get("mirror_audit"):
        # Tier A applies per-instance; gather rejections/acceptances for the
        # selected instances (or all when instance_ids is None).
        targets = instance_ids if instance_ids is not None else {None}
        accepted_markets: set[tuple[int, str]] = set()
        for iid in targets:
            audit_events = av.events_from_mirror_audit(
                paths["mirror_audit"], instance_id=iid, source_bot=bot
            )
            events.extend(audit_events)
            for ev in audit_events:
                try:
                    eid = int(ev.dry_instance_id)
                except (TypeError, ValueError):
                    continue
                k = (eid, ev.market_id)
                if ev.event_type == av.EVENT_MIRROR_ACCEPTED:
                    accepted_markets.add(k)
                elif ev.event_type == av.EVENT_MIRROR_REJECTED:
                    rejected[k] = ev.reason_code or "mirror_entry_rejected"
        # A market that was eventually accepted is NOT a phantom.
        for k in list(rejected):
            if k in accepted_markets:
                rejected.pop(k, None)
    if paths.get("events") and Path(paths["events"]).exists():
        events.extend(av.load_events(paths["events"]))
    intervals = av.build_intervals(events, coalesce_gap_seconds=coalesce_gap_seconds)
    # Merge a pre-built shared ledger (e.g. the 15m-exported platform intervals)
    # so the dry-run-only 5m bot can consume CLOB-wide outages it cannot observe
    # itself. Only platform-scope intervals are honored from a shared ledger.
    if paths.get("shared_ledger") and Path(paths["shared_ledger"]).exists():
        shared = [
            iv for iv in av.load_intervals(paths["shared_ledger"])
            if iv.scope == av.SCOPE_PLATFORM
        ]
        intervals.extend(shared)
    return intervals, rejected


def _interval_unbounded(iv: av.AvailabilityInterval) -> bool:
    """True when the interval has neither an end nor any expiry estimate.

    ``covers()`` matches every timestamp from ``start_ts`` onward for such an
    interval, so one bad unclosed edge (e.g. ``polymarket_down`` whose recovery
    was swallowed by a live-trader restart) would otherwise void everything
    that traded after it.
    """
    return iv.end_ts is None and iv.effective_until is None


def classify_entry(
    entry: DryEntry,
    *,
    bot: str,
    intervals: list[av.AvailabilityInterval],
    rejected: dict[tuple[int, str], str],
) -> Verdict:
    key = (entry.instance_id, entry.market_id)
    # Tier A — direct mirror rejection (15m only).
    if bot == "15m" and key in rejected:
        return Verdict(entry, "A", f"mirror_rejected:{rejected[key]}", confidence=av.CONFIDENCE_CONFIRMED)
    # Tier B — confirmed interval overlap. Self-cleaning honors every scope this
    # bot observed (platform/account/bot_host/unknown + matching market).
    active, reason, iv = av.untradable_at(
        entry.timestamp,
        market_id=entry.market_id,
        ledger_path=None,
        min_confidence=av.CONFIDENCE_CONFIRMED,
        include_scopes=av.ALL_SCOPES,
        intervals=intervals,
    )
    if active and iv is not None:
        return Verdict(
            entry, "B", f"confirmed_interval:{reason}", scope=iv.scope,
            confidence=iv.confidence, unbounded=_interval_unbounded(iv),
        )
    # Tier C — inferred interval overlap (report-only by default).
    active_inf, reason_inf, iv_inf = av.untradable_at(
        entry.timestamp,
        market_id=entry.market_id,
        ledger_path=None,
        min_confidence=av.CONFIDENCE_INFERRED,
        include_scopes=av.ALL_SCOPES,
        intervals=intervals,
    )
    if active_inf and iv_inf is not None:
        return Verdict(
            entry, "C", f"inferred_interval:{reason_inf}", scope=iv_inf.scope,
            confidence=iv_inf.confidence, unbounded=_interval_unbounded(iv_inf),
        )
    return Verdict(entry, "D", "no_evidence")


# ── writing voids ──────────────────────────────────────────────────────────────
def _void_row(header: list[str], entry: DryEntry, now: datetime) -> list[str]:
    values = {
        "instance_id": str(entry.instance_id),
        "timestamp": now.isoformat(),
        "event": VOID_EVENT,
        "direction": entry.direction,
        "market_id": entry.market_id,
        "entry_price": f"{entry.entry_price:.4f}",
        "shares": entry.raw.get("shares", ""),
        "pnl": "0.00",
        "capital": entry.raw.get("capital", ""),
        "experiment_id": entry.raw.get("experiment_id") or "legacy",
        "cost_basis": entry.raw.get("cost_basis", ""),
        "fee_schedule": entry.raw.get("fee_schedule", ""),
        "contract_metadata": entry.raw.get("contract_metadata", ""),
        "decision_at": entry.raw.get("decision_at", ""),
    }
    return [values.get(col, "") for col in header]


def append_voids(trades_path: Path, header: list[str], verdicts: list[Verdict], now: datetime) -> int:
    rows = [_void_row(header, v.entry, now) for v in verdicts]
    if not rows:
        return 0
    ensure_trailing_newline(trades_path)
    with trades_path.open("a", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        for row in rows:
            writer.writerow(row)
    return len(rows)


def physical_delete(trades_path: Path, verdicts: list[Verdict]) -> int:
    """Emergency stopped-grid mode: physically drop the ENTRY and its paired
    close rows for each verdict. Rewrites the whole file atomically."""
    from scripts.trader._io import atomic_write_csv_rows

    void_keys = {(v.entry.instance_id, v.entry.market_id) for v in verdicts}
    header: list[str] = []
    kept: list[list[str]] = []
    removed = 0
    with trades_path.open("r", newline="", encoding="utf-8") as fh:
        reader = csv.reader(fh)
        header = next(reader)
        idx = {name: i for i, name in enumerate(header)}
        for row in reader:
            try:
                iid = int(row[idx["instance_id"]])
            except (ValueError, IndexError, KeyError):
                kept.append(row)
                continue
            market = row[idx["market_id"]] if "market_id" in idx else ""
            event = row[idx["event"]] if "event" in idx else ""
            if (iid, market) in void_keys and event in (GRID_CLOSE_EVENTS | {"ENTRY"}):
                removed += 1
                continue
            kept.append(row)
    atomic_write_csv_rows(trades_path, header, kept)
    return removed


def _append_void_audit(audit_path: Path, verdicts: list[Verdict], now: datetime, mode: str) -> None:
    fields = [
        "voided_at", "mode", "instance_id", "market_id", "direction", "entry_price",
        "entry_timestamp", "close_event", "close_pnl", "tier", "confidence", "reason",
    ]
    new = not audit_path.exists()
    ensure_trailing_newline(audit_path)
    with audit_path.open("a", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=fields)
        if new:
            w.writeheader()
        for v in verdicts:
            e = v.entry
            w.writerow({
                "voided_at": now.isoformat(),
                "mode": mode,
                "instance_id": e.instance_id,
                "market_id": e.market_id,
                "direction": e.direction,
                "entry_price": f"{e.entry_price:.4f}",
                "entry_timestamp": e.timestamp.isoformat(),
                "close_event": e.close_event or "",
                "close_pnl": f"{e.close_pnl:.2f}",
                "tier": v.tier,
                "confidence": v.confidence,
                "reason": v.reason,
            })


# ── reporting ─────────────────────────────────────────────────────────────────
def _print_report(
    verdicts: list[Verdict],
    *,
    include_inferred: bool,
    allow_open_intervals: bool = False,
) -> tuple[list[Verdict], float]:
    by_tier: dict[str, list[Verdict]] = {"A": [], "B": [], "C": [], "D": []}
    for v in verdicts:
        by_tier[v.tier].append(v)

    def _selectable(v: Verdict) -> bool:
        if v.unbounded and not allow_open_intervals:
            return False
        return v.applicable or (include_inferred and v.tier == "C")

    selected = [v for v in verdicts if _selectable(v)]
    excluded_unbounded = [
        v for v in verdicts
        if v.unbounded and not allow_open_intervals
        and (v.applicable or (include_inferred and v.tier == "C"))
    ]
    pnl_removed = sum(v.entry.close_pnl for v in selected)
    print("=" * 78)
    print("Phantom dry-run cleanup report")
    print("=" * 78)
    for tier, label in (("A", "direct mirror rejection"), ("B", "confirmed interval"),
                        ("C", "inferred interval (report-only)"), ("D", "no evidence")):
        rows = by_tier[tier]
        if not rows:
            continue
        print(f"\nTier {tier} — {label}: {len(rows)}")
        for v in rows:
            e = v.entry
            mark = "VOID" if _selectable(v) else "keep"
            note = " [open-ended interval]" if v.unbounded else ""
            print(
                f"  [{mark}] inst={e.instance_id} {e.market_id} {e.direction} "
                f"entry={e.entry_price:.4f} @ {e.timestamp.isoformat()} "
                f"close={e.close_event or '-'} pnl={e.close_pnl:+.2f}  ({v.reason}){note}"
            )
    if excluded_unbounded:
        print(
            f"\nNOTE: {len(excluded_unbounded)} match(es) excluded — their interval has no "
            "end/expiry (an unclosed outage edge covers everything after it). Inspect the "
            "ledger; pass --allow-open-intervals only if the outage genuinely never recovered."
        )
    print("\n" + "-" * 78)
    print(f"Selected for void: {len(selected)}   net PnL removed: {pnl_removed:+.2f}")
    print("-" * 78)
    return selected, pnl_removed


def run(args: argparse.Namespace) -> int:
    results = Path(args.results)
    paths = _defaults(args.bot, results)
    for key in ("trades", "mirror_audit", "health_log", "events", "ledger"):
        override = getattr(args, key, None)
        if override:
            paths[key] = Path(override)
    if args.shared_ledger:
        paths["shared_ledger"] = Path(args.shared_ledger)

    instance_ids: Optional[set[int]]
    if args.all:
        instance_ids = None
    elif args.instance is not None:
        instance_ids = {int(args.instance)}
    else:
        default = DEFAULT_INSTANCE.get(args.bot)
        if default is None:
            raise SystemExit(f"--instance or --all required for bot {args.bot}")
        instance_ids = {default}

    since = _parse_dt(args.since) if args.since else None
    until = _parse_dt(args.until) if args.until else None

    entries, already_voided = read_grid_entries(
        paths["trades"], instance_ids=instance_ids, since=since, until=until,
    )
    intervals, rejected = build_evidence(
        bot=args.bot, paths=paths, instance_ids=instance_ids,
        coalesce_gap_seconds=args.coalesce_gap_seconds,
    )
    if args.rebuild_ledger:
        av.write_ledger(intervals, ledger_file=paths["ledger"])
        print(f"Rebuilt ledger ({len(intervals)} intervals) -> {paths['ledger']}")

    verdicts = [
        classify_entry(e, bot=args.bot, intervals=intervals, rejected=rejected)
        for e in entries
    ]
    if already_voided:
        print(f"({len(already_voided)} entries already voided — skipped)")
    selected, pnl_removed = _print_report(
        verdicts,
        include_inferred=args.include_inferred,
        allow_open_intervals=args.allow_open_intervals,
    )

    if not args.apply:
        print("\nReport-only. Re-run with --apply to write ENTRY_VOID markers.")
        return 0
    if not selected:
        print("\nNothing to apply.")
        return 0

    # Safety: refuse while a grid writer is plausibly alive (lock heartbeat /
    # host PID). The mtime quiet-period check below stays as a second line of
    # defense against a writer that holds no lock.
    live, why = _grid_lock_is_live(paths["trades"])
    if live and not args.force:
        print(f"\nABORT: grid appears to be running ({why}). Stop the grid or pass --force.")
        return 2

    # Safety: refuse if the writer touched the file very recently.
    age = time.time() - paths["trades"].stat().st_mtime
    if age < args.min_quiet_seconds and not args.force:
        print(
            f"\nABORT: {paths['trades'].name} modified {age:.1f}s ago "
            f"(< {args.min_quiet_seconds}s). Stop the grid or pass --force."
        )
        return 2

    now = datetime.now(timezone.utc)
    backup = _backup_trades(paths["trades"])
    print(f"\nBacked up trades -> {backup.name}")
    header = _read_header(paths["trades"])
    if args.physical_delete:
        removed = physical_delete(paths["trades"], selected)
        mode = "physical_delete"
        print(f"Physically removed {removed} rows for {len(selected)} voided entries.")
    else:
        written = append_voids(paths["trades"], header, selected, now)
        mode = "entry_void"
        print(f"Appended {written} ENTRY_VOID markers.")
    audit_path = paths["trades"].with_name(f"{paths['trades'].stem}_void_audit.csv")
    _append_void_audit(audit_path, selected, now, mode)
    print(f"Void audit -> {audit_path.name}")
    print(f"Net PnL removed from instance history: {pnl_removed:+.2f}")
    print("Restart/recover the grid so the leaderboard rebuilds from corrected history.")
    return 0


def main(argv: Optional[list[str]] = None) -> int:
    p = argparse.ArgumentParser(
        prog="clean_unavailable_dry_trades",
        description="Void phantom dry-run grid fills live could not place (report-only by default).",
    )
    p.add_argument("--bot", choices=["15m", "5m"], default="15m")
    p.add_argument("--results", default="results", help="results dir (default: results)")
    p.add_argument("--instance", type=int, help="grid instance_id (default: live mirror, 1271 for 15m)")
    p.add_argument("--all", action="store_true", help="all instances (report scope; use with care)")
    p.add_argument("--since", help="ISO ts; ignore entries opened before this")
    p.add_argument("--until", help="ISO ts; ignore entries opened after this")
    p.add_argument("--coalesce-gap-seconds", type=float, default=av.DEFAULT_COALESCE_GAP_SECONDS)
    p.add_argument("--include-inferred", action="store_true", help="also apply Tier C inferred intervals")
    p.add_argument("--allow-open-intervals", action="store_true",
                   help="let intervals with no end/expiry select entries for voiding "
                        "(dangerous: such an interval covers everything after its start)")
    p.add_argument("--apply", action="store_true", help="write changes (default: report only)")
    p.add_argument("--physical-delete", action="store_true",
                   help="emergency stopped-grid: physically delete rows instead of ENTRY_VOID markers")
    p.add_argument("--rebuild-ledger", action="store_true", help="also (re)write availability_ledger.csv")
    p.add_argument("--force", action="store_true", help="bypass the recent-writer safety check")
    p.add_argument("--min-quiet-seconds", type=float, default=DEFAULT_MIN_QUIET_SECONDS)
    # path overrides
    p.add_argument("--trades")
    p.add_argument("--mirror-audit", dest="mirror_audit")
    p.add_argument("--health-log", dest="health_log")
    p.add_argument("--events")
    p.add_argument("--ledger")
    p.add_argument("--shared-ledger", dest="shared_ledger")
    args = p.parse_args(argv)
    return run(args)


if __name__ == "__main__":
    sys.exit(main())
