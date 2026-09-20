"""Retroactive cleanup tool for phantom rows in 15m_live_trades.csv
*and* the runtime state snapshot (15m_live_state.json).

Companion to the in-trader auto-correction shipped after the
2026-04-30 phantom-TP-fill incident. The trader now rewrites phantom
rows the moment it detects them on chain, but rows already flushed
before that fix landed need a manual sweep — and we don't want to
hand-edit, since that bypasses the audit trail.

Two actions:

* ``void-as-loss <id>`` — atomically rewrite the matching row as
  ``LOSS_EXPIRY``: ``exit_price=0``, ``proceeds=0``,
  ``pnl=-cost_basis``, ``closed_at=now``. Use when the underlying
  position actually expired worthless but a phantom ``TP_FILLED``
  was recorded.

* ``delete <id>`` — drop the row entirely. Use for placeholder rows
  that never reflected a real position (e.g. the $0.01 ghost
  ``MANUAL_CLOSE`` left behind by a residual-sell phantom).

Both actions touch BOTH stores so the dashboard's "All time" widget
(reads the JSON snapshot via ``state_reader``) and the trade feed
(reads the CSV via ``trades_tail``) stay in sync. Each store has its
own audit trail:

* ``15m_live_trades_voided_audit.csv`` — original CSV row preserved.
* ``15m_live_state_voided_audit.jsonl`` — original JSON entry preserved
  (one record per line so multiple voids accumulate cleanly).

Examples (after the 2026-04-30 incident; market 1777591800):

    python -m scripts.trader.cli.void_phantom void-as-loss 4ca5f67b
    python -m scripts.trader.cli.void_phantom delete 2071245d

Use ``--skip-csv`` or ``--skip-state`` to operate on a single store
when the other is already correct (idempotent re-run after an
earlier partial sweep).
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from scripts.trader._io import (
    atomic_write_bytes as _shared_atomic_write_bytes,
    atomic_write_csv_rows,
)


def _resolve_csv_path(override: Optional[str]) -> Path:
    if override:
        return Path(override)
    # Lazy import so this CLI works without the runtime config (e.g.
    # when invoked from the host with only the repo on PYTHONPATH).
    from scripts.trader.config import TraderConfig

    cfg = TraderConfig(paper_mode=False)
    return cfg.get_trades_file()


def _resolve_state_path(override: Optional[str]) -> Path:
    if override:
        return Path(override)
    from scripts.trader.config import TraderConfig

    cfg = TraderConfig(paper_mode=False)
    return cfg.get_state_file()


def _atomic_write_bytes(path: Path, payload: bytes) -> None:
    _shared_atomic_write_bytes(path, payload)


def _state_audit_path(state_path: Path) -> Path:
    return state_path.parent / f"{state_path.stem}_voided_audit.jsonl"


def _append_state_audit(audit_path: Path, action: str, original: Any) -> None:
    line = json.dumps(
        {
            "voided_at": datetime.now(timezone.utc).isoformat(),
            "action": action,
            "entry": original,
        },
        default=str,
    ) + "\n"
    audit_path.parent.mkdir(parents=True, exist_ok=True)
    with open(audit_path, "a", encoding="utf-8") as f:
        f.write(line)


def _bump_mtime_path(path: Path) -> None:
    try:
        ts = time.time()
        os.utime(path, (ts, ts))
    except OSError:
        pass


def _recompute_state_pnls(state: dict) -> None:
    """Rebuild ``trade_pnls`` and ``capital.total_pnl`` from
    ``closed_positions`` so the persisted aggregates match the
    mutated list. ``capital.current`` is on-chain truth and is left
    alone — the next live trader run resyncs it from the wallet."""
    closed = state.get("closed_positions") or []
    pnls: list[float] = []
    for pos in closed:
        if not isinstance(pos, dict):
            continue
        v = pos.get("pnl")
        if isinstance(v, (int, float)):
            pnls.append(float(v))
    state["trade_pnls"] = pnls
    cap = state.get("capital")
    if isinstance(cap, dict):
        cap["total_pnl"] = sum(pnls)
    state["total_pnl"] = sum(pnls)


def _mutate_state_for_void_as_loss(state: dict, position_id: str) -> tuple[Optional[dict], Optional[float]]:
    """Locate the closed_positions entry by id and rewrite it as a
    LOSS_EXPIRY (proceeds=0, pnl=-cost_basis). Returns (original, pnl)
    or (None, None) if not found."""
    closed = state.get("closed_positions") or []
    for pos in closed:
        if isinstance(pos, dict) and pos.get("id") == position_id:
            original = json.loads(json.dumps(pos))  # deep copy via JSON roundtrip
            already_voided = pos.get("result") in ("LOSS_EXPIRY", "VOIDED_PHANTOM_TP")
            cost_basis = float(pos.get("cost_basis") or 0.0)
            pnl = -cost_basis
            pos["result"] = "LOSS_EXPIRY"
            pos["exit_price"] = 0.0
            pos["proceeds"] = 0.0
            pos["pnl"] = pnl
            pos["pnl_pct"] = -100.0 if cost_basis > 0 else 0.0
            if not already_voided:
                pos["closed_at"] = datetime.now(timezone.utc).isoformat()
            if "final_btc_price" in pos:
                pos["final_btc_price"] = None
            return original, pnl
    return None, None


def _mutate_state_for_delete(state: dict, position_id: str) -> Optional[dict]:
    closed = state.get("closed_positions") or []
    for i, pos in enumerate(closed):
        if isinstance(pos, dict) and pos.get("id") == position_id:
            original = json.loads(json.dumps(pos))
            del closed[i]
            state["closed_positions"] = closed
            return original
    return None


def _apply_state_change(
    state_path: Path,
    position_id: str,
    action: str,
) -> bool:
    if not state_path.exists():
        print(f"  [state] {state_path} does not exist; skipping JSON sync")
        return False
    raw = state_path.read_bytes()
    try:
        state = json.loads(raw.decode("utf-8"))
    except Exception as exc:
        print(f"  [state] could not parse {state_path}: {exc}; skipping")
        return False

    if action == "void-as-loss":
        original, pnl = _mutate_state_for_void_as_loss(state, position_id)
    else:
        original = _mutate_state_for_delete(state, position_id)
        pnl = None

    if original is None:
        print(f"  [state] no closed_positions entry for id={position_id}; nothing to do in JSON")
        return False

    _recompute_state_pnls(state)

    audit_path = _state_audit_path(state_path)
    _append_state_audit(audit_path, action, original)

    new_payload = json.dumps(state, indent=2, default=str).encode("utf-8")
    _atomic_write_bytes(state_path, new_payload)
    _bump_mtime_path(state_path)

    if action == "void-as-loss" and pnl is not None:
        print(
            f"  [state] {state_path.name}: id={position_id} -> LOSS_EXPIRY "
            f"(pnl={pnl:+.2f}). Audit -> {audit_path.name}"
        )
    else:
        print(
            f"  [state] {state_path.name}: deleted id={position_id}. "
            f"Audit -> {audit_path.name}"
        )
    return True


def _read_rows(path: Path) -> tuple[list[str], list[list[str]]]:
    with open(path, "r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        rows = list(reader)
    if len(rows) < 2:
        raise SystemExit(f"{path} has no data rows; nothing to do")
    return rows[0], rows[1:]


def _atomic_write(path: Path, header: list[str], rows: list[list[str]]) -> None:
    atomic_write_csv_rows(path, header, rows)


def _append_audit(audit_path: Path, header: list[str], original: list[str], action: str) -> None:
    first = not audit_path.exists()
    with open(audit_path, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if first:
            w.writerow(["voided_at", "action", *header])
        w.writerow([datetime.now(timezone.utc).isoformat(), action, *original])


def _bump_mtime(path: Path) -> None:
    try:
        ts = time.time()
        os.utime(path, (ts, ts))
    except OSError:
        pass


def _find_row_index(rows: list[list[str]], header: list[str], position_id: str) -> int:
    try:
        id_idx = header.index("id")
    except ValueError as exc:
        raise SystemExit(f"CSV missing 'id' column: {exc}")
    for i in range(len(rows) - 1, -1, -1):
        if rows[i] and len(rows[i]) > id_idx and rows[i][id_idx] == position_id:
            return i
    raise SystemExit(f"No row found with id={position_id!r}")


def _do_csv_void_as_loss(csv_path: Path, position_id: str, *, force: bool = False) -> bool:
    """Returns True if a row was rewritten, False if no matching row."""
    if not csv_path.exists():
        print(f"  [csv]   {csv_path} does not exist; skipping CSV sync")
        return False
    audit_path = csv_path.parent / f"{csv_path.stem}_voided_audit.csv"
    header, rows = _read_rows(csv_path)
    try:
        result_idx = header.index("result")
    except ValueError as exc:
        raise SystemExit(f"CSV missing 'result' column: {exc}")
    # Skip if the row is already non-TP_FILLED for this id (idempotent).
    already_voided = False
    if not force:
        for i in range(len(rows) - 1, -1, -1):
            if rows[i] and len(rows[i]) > header.index("id") and rows[i][header.index("id")] == position_id:
                current_result = rows[i][result_idx]
                if current_result in ("LOSS_EXPIRY", "VOIDED_PHANTOM_TP"):
                    print(f"  [csv]   {csv_path.name}: id={position_id} already {current_result}; CSV skipped")
                    return False
                break
    else:
        for i in range(len(rows) - 1, -1, -1):
            if rows[i] and len(rows[i]) > header.index("id") and rows[i][header.index("id")] == position_id:
                already_voided = rows[i][result_idx] in ("LOSS_EXPIRY", "VOIDED_PHANTOM_TP")
                break
    idx = _find_row_index(rows, header, position_id)
    original = list(rows[idx])
    cb_idx = header.index("cost_basis")
    try:
        cost_basis = float(rows[idx][cb_idx])
    except (ValueError, IndexError) as exc:
        raise SystemExit(f"Could not parse cost_basis on row {position_id}: {exc}")
    pnl = -cost_basis
    pnl_pct = -100.0 if cost_basis > 0 else 0.0
    rows[idx][result_idx] = "LOSS_EXPIRY"
    rows[idx][header.index("exit_price")] = "0.0000"
    rows[idx][header.index("proceeds")] = "0.00"
    rows[idx][header.index("pnl")] = f"{pnl:.2f}"
    rows[idx][header.index("pnl_pct")] = f"{pnl_pct:.2f}%"
    if not already_voided:
        rows[idx][header.index("closed_at")] = datetime.now(timezone.utc).isoformat()
    if "final_btc_price" in header:
        rows[idx][header.index("final_btc_price")] = ""
    _atomic_write(csv_path, header, rows)
    _append_audit(audit_path, header, original, "void-as-loss")
    _bump_mtime(csv_path)
    print(
        f"  [csv]   {csv_path.name}: id={position_id} -> LOSS_EXPIRY "
        f"(pnl={pnl:+.2f}). Audit -> {audit_path.name}"
    )
    return True


def _do_csv_delete(csv_path: Path, position_id: str) -> bool:
    if not csv_path.exists():
        print(f"  [csv]   {csv_path} does not exist; skipping CSV sync")
        return False
    audit_path = csv_path.parent / f"{csv_path.stem}_voided_audit.csv"
    header, rows = _read_rows(csv_path)
    id_idx = header.index("id")
    target_idx: Optional[int] = None
    for i in range(len(rows) - 1, -1, -1):
        if rows[i] and len(rows[i]) > id_idx and rows[i][id_idx] == position_id:
            target_idx = i
            break
    if target_idx is None:
        print(f"  [csv]   {csv_path.name}: id={position_id} not in CSV; skipped")
        return False
    original = list(rows[target_idx])
    del rows[target_idx]
    _atomic_write(csv_path, header, rows)
    _append_audit(audit_path, header, original, "delete")
    _bump_mtime(csv_path)
    print(
        f"  [csv]   {csv_path.name}: deleted id={position_id}. "
        f"Audit -> {audit_path.name}"
    )
    return True


def cmd_void_as_loss(args: argparse.Namespace) -> int:
    print(f"void-as-loss id={args.position_id}{' (--force)' if args.force else ''}")
    csv_changed = state_changed = False
    if not args.skip_csv:
        csv_changed = _do_csv_void_as_loss(
            _resolve_csv_path(args.csv), args.position_id, force=args.force,
        )
    if not args.skip_state:
        state_changed = _apply_state_change(
            _resolve_state_path(args.state),
            args.position_id,
            "void-as-loss",
        )
    if not (csv_changed or state_changed):
        print("  WARN: no changes applied (id not found in either store)")
        return 1
    return 0


def _do_csv_reconcile(
    csv_path: Path,
    position_id: str,
    proceeds: float,
) -> bool:
    """Update CSV row's proceeds/pnl/pnl_pct/exit_price/proceeds_source
    in place, preserving everything else. Mirrors the runtime
    polymarket_history_reconciler's CSV update."""
    if not csv_path.exists():
        print(f"  [csv]   {csv_path} does not exist; skipping CSV sync")
        return False
    audit_path = csv_path.parent / f"{csv_path.stem}_reconciled_audit.csv"
    header, rows = _read_rows(csv_path)
    id_idx = header.index("id")
    target: Optional[list[str]] = None
    target_idx = -1
    for i in range(len(rows) - 1, -1, -1):
        if rows[i] and len(rows[i]) > id_idx and rows[i][id_idx] == position_id:
            target = list(rows[i])
            target_idx = i
            break
    if target is None:
        print(f"  [csv]   {csv_path.name}: id={position_id} not in CSV; skipped")
        return False
    try:
        cb_idx = header.index("cost_basis")
        cost_basis = float(rows[target_idx][cb_idx])
    except (ValueError, IndexError):
        cost_basis = 0.0
    pnl = proceeds - cost_basis
    pnl_pct = (pnl / cost_basis) * 100.0 if cost_basis > 0 else 0.0
    shares_idx = header.index("shares")
    try:
        shares = float(rows[target_idx][shares_idx])
    except (ValueError, IndexError):
        shares = 0.0
    exit_price = (proceeds / shares) if shares > 0 else 0.0

    rows[target_idx][header.index("exit_price")] = f"{exit_price:.4f}"
    rows[target_idx][header.index("proceeds")] = f"{proceeds:.2f}"
    rows[target_idx][header.index("pnl")] = f"{pnl:.2f}"
    rows[target_idx][header.index("pnl_pct")] = f"{pnl_pct:.2f}%"
    if "proceeds_source" in header:
        rows[target_idx][header.index("proceeds_source")] = "polymarket_history"
    if "result" in header:
        # Don't downgrade a meaningful result; only repair UNRESOLVED_PROCEEDS
        # rows that the trader wrote with proceeds=0.
        if rows[target_idx][header.index("result")] == "UNRESOLVED_PROCEEDS":
            rows[target_idx][header.index("result")] = "TP_FILLED"

    _atomic_write(csv_path, header, rows)
    _append_audit(audit_path, header, target, "reconcile-from-history")
    _bump_mtime(csv_path)
    print(
        f"  [csv]   {csv_path.name}: id={position_id} reconciled "
        f"(proceeds={proceeds:.2f}, pnl={pnl:+.2f}). "
        f"Audit -> {audit_path.name}"
    )
    return True


def _apply_state_reconcile(
    state_path: Path,
    position_id: str,
    proceeds: float,
) -> bool:
    if not state_path.exists():
        print(f"  [state] {state_path} does not exist; skipping JSON sync")
        return False
    raw = state_path.read_bytes()
    try:
        state = json.loads(raw.decode("utf-8-sig"))
    except Exception as exc:
        print(f"  [state] could not parse {state_path}: {exc}; skipping")
        return False

    closed = state.get("closed_positions") or []
    target = None
    for pos in closed:
        if isinstance(pos, dict) and pos.get("id") == position_id:
            target = pos
            break
    if target is None:
        print(f"  [state] no closed_positions entry for id={position_id}; nothing to do")
        return False

    original = json.loads(json.dumps(target))
    cost_basis = float(target.get("cost_basis") or 0.0)
    shares = float(target.get("shares") or 0.0)
    pnl = proceeds - cost_basis
    pnl_pct = (pnl / cost_basis) * 100.0 if cost_basis > 0 else 0.0

    target["proceeds"] = float(proceeds)
    target["pnl"] = float(pnl)
    target["pnl_pct"] = float(pnl_pct)
    target["exit_price"] = (proceeds / shares) if shares > 0 else 0.0
    target["proceeds_source"] = "polymarket_history"
    if target.get("result") == "UNRESOLVED_PROCEEDS":
        target["result"] = "TP_FILLED"

    pnls = []
    for p in closed:
        if isinstance(p, dict):
            v = p.get("pnl")
            if isinstance(v, (int, float)):
                pnls.append(float(v))
    state["trade_pnls"] = pnls
    cap = state.get("capital")
    if isinstance(cap, dict):
        cap["total_pnl"] = sum(pnls)
    state["total_pnl"] = sum(pnls)

    audit_path = _state_audit_path(state_path).with_name(
        state_path.stem + "_reconciled_audit.jsonl"
    )
    _append_state_audit(audit_path, "reconcile-from-history", original)

    payload = json.dumps(state, indent=2, default=str).encode("utf-8")
    _atomic_write_bytes(state_path, payload)
    _bump_mtime_path(state_path)

    print(
        f"  [state] {state_path.name}: id={position_id} reconciled "
        f"(proceeds={proceeds:.4f}, pnl={pnl:+.4f}). Audit -> {audit_path.name}"
    )
    return True


def _query_reconcile_history_proceeds(
    state_path: Path,
    position_id: str,
) -> Optional[float]:
    """Return Polymarket SELL proceeds using the full history reconciler."""
    try:
        from scripts import reconcile_live_history as history
    except ImportError as exc:
        print(f"  ERROR: cannot import reconcile_live_history: {exc}")
        return None

    try:
        state = json.loads(state_path.read_text(encoding="utf-8-sig"))
    except Exception as exc:
        print(f"  ERROR: cannot read state: {exc}")
        return None

    try:
        client, funder = history._init_client()
        pm_trades, _our_uuid = history._fetch_all_trades(client, funder)
        reconciled = history._reconcile(state.get("closed_positions") or [], pm_trades)
    except SystemExit:
        raise
    except Exception as exc:
        print(f"  ERROR: reconcile_live_history query failed: {exc}")
        return None

    for row in reconciled:
        if row.position_id != position_id:
            continue
        if row.pm_proceeds is None:
            print(f"  ERROR: Polymarket history matched no proceeds for id={position_id}")
            return None
        notes = "; ".join(row.notes) if row.notes else "matched"
        print(
            f"  Polymarket history: proceeds=${row.pm_proceeds:.6f} "
            f"(cost=${(row.pm_cost or 0.0):.6f}, pnl=${(row.pm_pnl or 0.0):+.6f}; {notes})"
        )
        return float(row.pm_proceeds)

    print(f"  ERROR: id={position_id} not found in reconciled history")
    return None


def cmd_reconcile_from_history(args: argparse.Namespace) -> int:
    """Update an existing trade row's proceeds (and derived pnl /
    pnl_pct / exit_price / proceeds_source) using either an explicit
    --proceeds value (e.g. from the user's Polymarket export download)
    or a fresh /data/trades query through the live executor.

    Use case 1: today's e312391d-p (recorded as $0 because reconcile_position
    fell back to entry_price). User downloaded Polymarket history,
    summed sells: $5.5195 + $3.310798 = $8.8303. Run:
       reconcile-from-history e312391d-p --proceeds 8.83
    Use case 2: any tentative row where /data/trades has now indexed:
       reconcile-from-history <id>     # auto-queries
    """
    print(f"reconcile-from-history id={args.position_id}")
    proceeds = args.proceeds
    if proceeds is None:
        state_path = _resolve_state_path(args.state)
        proceeds = _query_reconcile_history_proceeds(state_path, args.position_id)
        if proceeds is None or proceeds <= 0:
            print("  Polymarket history returned no proceeds; no changes applied.")
            return 1
    csv_changed = state_changed = False
    if not args.skip_csv:
        csv_changed = _do_csv_reconcile(
            _resolve_csv_path(args.csv), args.position_id, float(proceeds),
        )
    if not args.skip_state:
        state_changed = _apply_state_reconcile(
            _resolve_state_path(args.state), args.position_id, float(proceeds),
        )
    if not (csv_changed or state_changed):
        print("  WARN: no changes applied (id not found in either store)")
        return 1
    return 0


def cmd_delete(args: argparse.Namespace) -> int:
    print(f"delete id={args.position_id}")
    csv_changed = state_changed = False
    if not args.skip_csv:
        csv_changed = _do_csv_delete(_resolve_csv_path(args.csv), args.position_id)
    if not args.skip_state:
        state_changed = _apply_state_change(
            _resolve_state_path(args.state),
            args.position_id,
            "delete",
        )
    if not (csv_changed or state_changed):
        print("  WARN: no changes applied (id not found in either store)")
        return 1
    return 0


def cmd_rebuild_capital(args: argparse.Namespace) -> int:
    """Recalculate capital totals after reconciliation."""
    state_path = _resolve_state_path(args.state)
    if not state_path.exists():
        print(f"  [state] {state_path} does not exist")
        return 1
    raw = state_path.read_bytes()
    try:
        state = json.loads(raw.decode("utf-8"))
    except Exception as exc:
        print(f"  [state] could not parse {state_path}: {exc}")
        return 1

    _recompute_state_pnls(state)
    cap = state.get("capital")
    if not isinstance(cap, dict):
        print("  [state] no capital object in state; nothing to do")
        return 1
    old_current = cap.get("current", 0.0)
    starting = cap.get("starting", 0.0)
    total_pnl = cap.get("total_pnl", 0.0)
    if args.anchor_current:
        new_starting = float(old_current) - float(total_pnl)
        cap["starting"] = new_starting
        print(f"  [state] capital.starting: {starting:.6f} -> {new_starting:.6f}")
        print(f"          (current={float(old_current):.6f} - total_pnl={float(total_pnl):.6f})")
    else:
        new_current = starting + total_pnl
        cap["current"] = new_current
        print(f"  [state] capital.current: {old_current:.6f} -> {new_current:.6f}")
        print(f"          (starting={starting:.6f} + total_pnl={total_pnl:.6f})")

    payload = json.dumps(state, indent=2, default=str).encode("utf-8")
    _atomic_write_bytes(state_path, payload)
    _bump_mtime_path(state_path)
    return 0


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="void_phantom",
        description=(
            "Retroactively void or delete phantom rows in the live trades "
            "CSV and the runtime state snapshot, with audit trail."
        ),
    )
    parser.add_argument(
        "--csv",
        help="Path to the trades CSV (defaults to TraderConfig live trades file)",
    )
    parser.add_argument(
        "--state",
        help="Path to the live state JSON (defaults to TraderConfig live_state_file)",
    )
    parser.add_argument(
        "--skip-csv",
        action="store_true",
        help="Don't touch the CSV (use when CSV is already correct)",
    )
    parser.add_argument(
        "--skip-state",
        action="store_true",
        help="Don't touch the state JSON (use when state is already correct)",
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_void = sub.add_parser(
        "void-as-loss",
        help="Mark row as LOSS_EXPIRY with exit_price=0 / pnl=-cost_basis",
    )
    p_void.add_argument("position_id")
    p_void.add_argument(
        "--force",
        action="store_true",
        help="Re-void even if already LOSS_EXPIRY (for rows with wrong proceeds)",
    )
    p_void.set_defaults(func=cmd_void_as_loss)

    p_del = sub.add_parser("delete", help="Remove the row entirely")
    p_del.add_argument("position_id")
    p_del.set_defaults(func=cmd_delete)

    p_rec = sub.add_parser(
        "reconcile-from-history",
        help=(
            "Update an existing trade's proceeds using Polymarket history. "
            "Pass --proceeds X to set explicitly (e.g. from the Polymarket "
            "CSV export); omit to auto-query /data/trades via the live "
            "executor (only works if the trade has a stored exit_order_id)."
        ),
    )
    p_rec.add_argument("position_id")
    p_rec.add_argument(
        "--proceeds",
        type=float,
        help=(
            "Explicit proceeds value in USDC. Overrides /data/trades query. "
            "For e312391d (today's split TP): sum of sells from Polymarket "
            "export = 5.5195 + 3.310798 = 8.83."
        ),
    )
    p_rec.set_defaults(func=cmd_reconcile_from_history)

    p_rebuild = sub.add_parser(
        "rebuild-capital",
        help="Recalculate capital.current = starting + total_pnl",
    )
    p_rebuild.add_argument(
        "--anchor-current",
        action="store_true",
        help="Preserve capital.current and set starting = current - total_pnl",
    )
    p_rebuild.set_defaults(func=cmd_rebuild_capital)

    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
