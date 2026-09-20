#!/usr/bin/env python3
"""Reconcile local trade history against Polymarket on-chain record.

For each closed_position in ``results/15m_live_state.json`` the script finds
the actual Polymarket BUY trade that opened it (closest ``match_time`` to
``opened_at`` with size within tolerance, filtered to the bot's size range
so other wallet activity doesn't pollute the match). From that BUY's
``asset_id`` it then finds the matching SELL fill(s) (for early-exit
TP/SL) or applies the market resolution payout (for WIN_EXPIRY /
LOSS_EXPIRY, where no SELL exists). Actual cost_basis / proceeds / pnl
are recomputed from ``size × price`` on the real fills — closing the
drift between ``starting + Σ local_pnls`` and on-chain USDC caused by
per-fill slippage and fees the trader's book formula ignores.

Default mode is dry-run (prints a per-position diff + totals, writes
nothing). ``--apply`` atomically rewrites state.json + the two CSVs,
stashing originals in ``results/pre_reconcile_<ts>/``.

Run from ``BTC_pricer_15m/``::

    python scripts/reconcile_live_history.py               # dry-run
    python scripts/reconcile_live_history.py --apply       # write

Reads the same env vars the live trader uses::

    POLYMARKET_PRIVATE_KEY      L1 signer key
    POLYMARKET_FUNDER           funder address (required for sig_type != 0)
    POLYMARKET_SIGNATURE_TYPE   optional, default 1
"""
from __future__ import annotations

import argparse
import csv
import json
import shutil
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from scripts.trader._io import atomic_write_dicts_csv, atomic_write_json
from scripts.trader._polymarket import create_clob_client

REPO_ROOT = Path(__file__).resolve().parent.parent
RESULTS_DIR = REPO_ROOT / "results"
STATE_PATH = RESULTS_DIR / "15m_live_state.json"
TRADES_CSV = RESULTS_DIR / "15m_live_trades.csv"
EQUITY_CSV = RESULTS_DIR / "15m_live_equity.csv"

# The bot trades small sizes ($3-$30 per fill). Restricting the candidate
# pool to this notional range prevents cross-contamination from unrelated
# trades on the same Polymarket wallet (other bots, manual activity).
BOT_FILL_MIN_USDC = 2.0
BOT_FILL_MAX_USDC = 30.0

# Time window for pairing a local opened_at / closed_at to a specific
# Polymarket trade. The live trader writes opened_at immediately on fill
# confirmation, so the actual Polymarket match_time is typically within a
# second; 60 s is generous but still well below the ~15 min between our
# sequential positions.
FILL_TIME_WINDOW_S = 60.0

# Relative tolerance between a Polymarket fill's `size` and the local
# `shares` field. Polymarket's size is the gross matched amount; the
# trader's `shares` is what the trader observed post-fill. Investigation
# showed the gap is usually 1-3% but ranges up to ~4%, so 5% is safely
# inside the noise band without being so loose it lets a wrong fill in.
SIZE_REL_TOLERANCE = 0.05

# Polymarket resolution payout: winners get 1.0 USDC/share, losers 0.
# If a core-CLOB resolution fee is ever introduced, adjust here.
RESOLUTION_FEE_RATE = 0.00
WIN_PAYOUT_PER_SHARE = 1.0 * (1.0 - RESOLUTION_FEE_RATE)
LOSS_PAYOUT_PER_SHARE = 0.0

RESOLVED_RESULTS = {
    "TP_FILLED",
    "TP_SIGNAL_MARKET_EXIT",
    "WIN_EXPIRY",
    "LOSS_EXPIRY",
    "STOP_LOSS",
    "UNRESOLVED_RESTART",
    "MANUAL_CLOSE",
}
EXPIRY_RESULTS = {"WIN_EXPIRY", "LOSS_EXPIRY"}


# ---------------------------------------------------------------------------
# Polymarket client
# ---------------------------------------------------------------------------

def _init_client():
    try:
        ctx = create_clob_client(allow_legacy=True)
    except (ImportError, ValueError, AttributeError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(2)
    return ctx.client, ctx.funder


def _trade_params_cls():
    try:
        from py_clob_client_v2 import TradeParams
    except ImportError:
        from py_clob_client.clob_types import TradeParams
    return TradeParams


def _fetch_all_trades(client, funder: Optional[str]) -> tuple[list[dict], Optional[str]]:
    """Fetch trade history + derive our owner UUID. Returns (normalized, uuid)."""
    TradeParams = _trade_params_cls()
    params = TradeParams(maker_address=funder) if funder else TradeParams()
    raw = client.get_trades(params)

    # Our owner UUID = top-level `owner` on any trade where we are TAKER
    # (top-level owner is always the taker's wallet UUID). Use this to
    # pick OUR maker_orders entry inside MAKER trades.
    our_uuid: Optional[str] = None
    for t in raw:
        d = t if isinstance(t, dict) else t.__dict__
        if str(d.get("trader_side", "")).upper() == "TAKER":
            our_uuid = str(d.get("owner", "") or "") or None
            if our_uuid:
                break

    out: list[dict] = []
    for t in raw:
        d = t if isinstance(t, dict) else t.__dict__
        trader_side = str(d.get("trader_side", "TAKER")).upper()

        # Extract OUR portion of the match — top-level for takers, our
        # specific maker_orders entry for makers. Polymarket trade.size
        # is the total matched volume (not our personal contribution)
        # so for MAKER trades we MUST read matched_amount from our entry.
        try:
            if trader_side == "MAKER":
                our_mo = None
                for mo in (d.get("maker_orders") or []):
                    if our_uuid and str(mo.get("owner", "")) == our_uuid:
                        our_mo = mo
                        break
                if our_mo is None:
                    continue  # not our order — defensive, shouldn't happen
                size = float(our_mo.get("matched_amount", 0) or 0)
                price = float(our_mo.get("price", 0) or 0)
                asset_id = str(our_mo.get("asset_id", ""))
                # Maker's own `side` is OUR side directly — no flip needed.
                our_side = str(our_mo.get("side", "")).upper()
            else:
                size = float(d.get("size", 0) or 0)
                price = float(d.get("price", 0) or 0)
                asset_id = str(d.get("asset_id", ""))
                our_side = str(d.get("side", "")).upper()
        except (TypeError, ValueError):
            continue

        match_time_raw = d.get("match_time", "")
        try:
            if isinstance(match_time_raw, (int, float)):
                mt = float(match_time_raw)
            else:
                s = str(match_time_raw).strip()
                mt = float(s) if s.isdigit() else datetime.fromisoformat(s).timestamp()
        except Exception:
            mt = 0.0

        out.append({
            "id": str(d.get("id", "")),
            "taker_order_id": str(d.get("taker_order_id", "")),
            "asset_id": asset_id,
            "side": our_side,
            "trader_side": trader_side,
            "size": size,
            "price": price,
            "usdc": round(size * price, 6),
            "match_time": mt,
            "transaction_hash": str(d.get("transaction_hash", "")),
        })
    return out, our_uuid


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _iso_to_ts(ts: str) -> float:
    if not ts:
        return 0.0
    try:
        return datetime.fromisoformat(ts).timestamp()
    except Exception:
        return 0.0


def _pct(pnl: float, cost: float) -> float:
    if cost <= 0:
        return 0.0
    return (pnl / cost) * 100.0


# ---------------------------------------------------------------------------
# Per-position matching
# ---------------------------------------------------------------------------

@dataclass
class Recon:
    position_id: str
    opened_at: str
    closed_at: str
    direction: str
    result: str
    shares: float
    local_cost: float
    local_proceeds: float
    local_pnl: float
    # Matched trades
    asset_id: Optional[str] = None
    buy_trade_ids: list[str] = field(default_factory=list)
    sell_trade_ids: list[str] = field(default_factory=list)
    pm_cost: Optional[float] = None
    pm_proceeds: Optional[float] = None
    pm_pnl: Optional[float] = None
    notes: list[str] = field(default_factory=list)

    @property
    def delta(self) -> Optional[float]:
        if self.pm_pnl is None:
            return None
        return round(self.pm_pnl - self.local_pnl, 6)

    @property
    def matched(self) -> bool:
        return self.pm_pnl is not None


def _find_buy(trades: list[dict], claimed: set[str], *, target_time: float, target_size: float) -> Optional[dict]:
    """Return the Polymarket BUY trade best matching this position's entry."""
    best: Optional[dict] = None
    best_dt = FILL_TIME_WINDOW_S + 1
    for t in trades:
        if t["id"] in claimed:
            continue
        if t["side"] != "BUY":
            continue
        usdc = t["usdc"]
        if not (BOT_FILL_MIN_USDC <= usdc <= BOT_FILL_MAX_USDC):
            continue
        dt = abs(t["match_time"] - target_time)
        if dt > FILL_TIME_WINDOW_S:
            continue
        sz_err = abs(t["size"] - target_size) / max(target_size, 1e-9)
        if sz_err > SIZE_REL_TOLERANCE:
            continue
        if dt < best_dt:
            best_dt = dt
            best = t
    return best


def _find_sells(trades: list[dict], claimed: set[str], *, asset_id: str, after_time: float, buy_size: float) -> list[dict]:
    """Return SELL fill(s) on this asset_id that close our specific buy.

    The same Polymarket token (asset_id) is sometimes traded by other
    bots on the same wallet with much larger sizes, so we can't just
    take every same-asset SELL. Cap the accumulation at ~buy_size — we
    can't sell more than we bought. Split fills (the common maker-TP
    case) are still captured cleanly because the order we walk in is
    chronological and small fills add up well under the cap.
    """
    candidates = [
        t for t in trades
        if t["id"] not in claimed
        and t["side"] == "SELL"
        and t["asset_id"] == asset_id
        and t["match_time"] >= after_time - 5.0  # small slack for clock skew
    ]
    candidates.sort(key=lambda t: t["match_time"])
    cap = buy_size * (1.0 + SIZE_REL_TOLERANCE)
    selected: list[dict] = []
    acc = 0.0
    for t in candidates:
        if acc + t["size"] > cap:
            # A single fill that alone blows the cap is almost certainly
            # from another bot on the same wallet — skip it and continue
            # scanning in case a smaller one comes next.
            if t["size"] > cap:
                continue
            break
        selected.append(t)
        acc += t["size"]
    return selected


def _reconcile(closed_positions: list[dict], pm_trades: list[dict]) -> list[Recon]:
    # Process positions in chronological order so earlier ones claim their
    # fills before later ones can accidentally steal them.
    ordered = sorted(closed_positions, key=lambda p: _iso_to_ts(str(p.get("opened_at", ""))))
    claimed: set[str] = set()  # trade ids already assigned
    results: list[Recon] = []

    for local in ordered:
        r = Recon(
            position_id=str(local.get("id", "")),
            opened_at=str(local.get("opened_at", "")),
            closed_at=str(local.get("closed_at", "")),
            direction=str(local.get("direction", "")),
            result=str(local.get("result", "")),
            shares=float(local.get("shares", 0.0) or 0.0),
            local_cost=float(local.get("cost_basis", 0.0) or 0.0),
            local_proceeds=float(local.get("proceeds", 0.0) or 0.0),
            local_pnl=float(local.get("pnl", 0.0) or 0.0),
        )

        # Skip dust rows (paired-position placeholders with near-zero
        # shares) — they don't have their own Polymarket fills.
        if r.shares < 0.1:
            r.notes.append(f"dust row ({r.shares:.4f} shares) — skipped")
            results.append(r)
            continue

        t_open = _iso_to_ts(r.opened_at)

        # Fast-path: if the local closed_position carries entry_order_id
        # (written by post-Part-1 trader), match by taker_order_id exactly.
        entry_oid = str(local.get("entry_order_id", "") or "") or None
        buy = None
        if entry_oid:
            for t in pm_trades:
                if t["id"] in claimed:
                    continue
                if t["side"] != "BUY":
                    continue
                if t.get("taker_order_id") == entry_oid:
                    buy = t
                    r.notes.append("matched by entry_order_id")
                    break

        if buy is None:
            buy = _find_buy(pm_trades, claimed, target_time=t_open, target_size=r.shares)
        if buy is None:
            r.notes.append(f"no Polymarket BUY matched within ±{FILL_TIME_WINDOW_S:.0f}s, size ±{SIZE_REL_TOLERANCE*100:.0f}%")
            results.append(r)
            continue

        claimed.add(buy["id"])
        r.asset_id = buy["asset_id"]
        r.buy_trade_ids = [buy["id"]]
        r.pm_cost = round(buy["usdc"], 6)

        # Exit leg
        if r.result in EXPIRY_RESULTS:
            # Market resolved on-chain; no SELL exists.
            payout = WIN_PAYOUT_PER_SHARE if r.result == "WIN_EXPIRY" else LOSS_PAYOUT_PER_SHARE
            r.pm_proceeds = round(buy["size"] * payout, 6)
            r.notes.append(f"BUY + on-chain resolution ({r.result})")
        else:
            sells = _find_sells(
                pm_trades, claimed,
                asset_id=buy["asset_id"],
                after_time=buy["match_time"],
                buy_size=buy["size"],
            )
            if not sells:
                r.notes.append("BUY matched but no SELL found — keeping local proceeds")
                r.pm_proceeds = r.local_proceeds
            else:
                for s in sells:
                    claimed.add(s["id"])
                r.sell_trade_ids = [s["id"] for s in sells]
                # Simply sum SELL USDC. If the bot's recorded `shares`
                # differs from the PM gross by a few %, that's the
                # fee/slippage we're trying to capture — don't try to
                # impute "residue" from those mismatches.
                r.pm_proceeds = round(sum(s["usdc"] for s in sells), 6)
                taker_n = sum(1 for s in sells if s["trader_side"] == "TAKER")
                maker_n = len(sells) - taker_n
                parts = [f"1 BUY", f"{len(sells)} SELL{'s' if len(sells) != 1 else ''}"]
                if maker_n:
                    parts.append(f"{maker_n} maker fill{'s' if maker_n != 1 else ''}")
                r.notes.append(", ".join(parts))

        r.pm_pnl = round(r.pm_proceeds - r.pm_cost, 6)

        # Surface the size drift between PM gross and trader-recorded shares.
        sz_err = abs(buy["size"] - r.shares)
        if sz_err > 0.001:
            r.notes.append(f"buy_size {buy['size']:.4f} vs local shares {r.shares:.4f} ({(sz_err/r.shares)*100:.2f}%)")

        results.append(r)

    return results


# ---------------------------------------------------------------------------
# Writing
# ---------------------------------------------------------------------------

def _atomic_write_json(path: Path, data: dict, backup_dir: Path) -> None:
    if path.exists():
        shutil.copy2(path, backup_dir / path.name)
    atomic_write_json(path, data)


def _atomic_write_csv(path: Path, rows: list[dict], backup_dir: Path) -> None:
    if path.exists():
        shutil.copy2(path, backup_dir / path.name)
    if not rows:
        return
    atomic_write_dicts_csv(path, rows)


def _load_csv(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def _apply(
    results: list[Recon],
    state: dict,
    trades_csv_rows: list[dict],
    equity_csv_rows: list[dict],
    backup_dir: Path,
) -> None:
    by_id = {r.position_id: r for r in results if r.matched}

    new_closed = []
    cumulative_pnl = 0.0
    for pos in state.get("closed_positions") or []:
        pid = str(pos.get("id", ""))
        r = by_id.get(pid)
        if r is None:
            new_closed.append(pos)
            cumulative_pnl += float(pos.get("pnl", 0.0) or 0.0)
            continue
        new = dict(pos)
        new["cost_basis"] = r.pm_cost
        new["proceeds"] = r.pm_proceeds
        new["pnl"] = r.pm_pnl
        new["pnl_pct"] = _pct(r.pm_pnl or 0.0, r.pm_cost or 1.0)
        new_closed.append(new)
        cumulative_pnl += float(new["pnl"] or 0.0)
    state["closed_positions"] = new_closed
    state["trade_pnls"] = [
        float(pos.get("pnl", 0.0) or 0.0)
        for pos in new_closed
        if isinstance(pos, dict)
    ]
    state["total_pnl"] = round(cumulative_pnl, 6)
    cap = state.setdefault("capital", {})
    cap["total_pnl"] = round(cumulative_pnl, 6)
    _atomic_write_json(STATE_PATH, state, backup_dir)

    if trades_csv_rows:
        for row in trades_csv_rows:
            pid = str(row.get("id", ""))
            r = by_id.get(pid)
            if r is None:
                continue
            row["cost_basis"] = f"{r.pm_cost:.6f}"
            row["proceeds"] = f"{r.pm_proceeds:.6f}"
            row["pnl"] = f"{r.pm_pnl:.6f}"
            row["pnl_pct"] = f"{_pct(r.pm_pnl, r.pm_cost):.2f}%"
        _atomic_write_csv(TRADES_CSV, trades_csv_rows, backup_dir)

    if equity_csv_rows:
        by_opened = {(r.direction, r.opened_at): r for r in results if r.matched}
        by_closed = {(r.direction, r.closed_at): r for r in results if r.matched}
        running = float(state.get("capital", {}).get("starting", 0.0) or 0.0)
        for row in equity_csv_rows:
            ts = row.get("timestamp", "")
            ev = row.get("event", "")
            direction = row.get("direction", "")
            if ev == "ENTRY":
                r = by_opened.get((direction, ts))
                if r is not None:
                    running -= (r.pm_cost or 0.0)
                    row["pnl"] = "0.00"
                    row["capital"] = f"{running:.2f}"
                    row["equity"] = f"{running + (r.pm_cost or 0.0):.2f}"
                    row["position_value"] = f"{(r.pm_cost or 0.0):.2f}"
            elif ev in RESOLVED_RESULTS:
                r = by_closed.get((direction, ts))
                if r is not None:
                    running += (r.pm_proceeds or 0.0)
                    row["pnl"] = f"{(r.pm_pnl or 0.0):.2f}"
                    row["capital"] = f"{running:.2f}"
                    row["equity"] = f"{running:.2f}"
                    row["position_value"] = "0.00"
        _atomic_write_csv(EQUITY_CSV, equity_csv_rows, backup_dir)


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def _print_table(results: list[Recon]) -> None:
    header = (
        f"{'#':>3} {'id':<12} {'dir':<4} {'result':<14} {'shares':>7} "
        f"{'local_pnl':>10} {'pm_pnl':>10} {'delta':>9}   notes"
    )
    print(header)
    print("-" * len(header))
    for i, r in enumerate(results, start=1):
        pm = "--" if r.pm_pnl is None else f"{r.pm_pnl:+.4f}"
        d = "--" if r.delta is None else f"{r.delta:+.4f}"
        note = "; ".join(r.notes) if r.notes else ""
        print(
            f"{i:>3} {r.position_id[:12]:<12} {r.direction:<4} {r.result:<14} "
            f"{r.shares:>7.3f} {r.local_pnl:>+10.4f} {pm:>10} {d:>9}   {note}"
        )


def _summary(results: list[Recon], state_total_pnl: float, state_capital: float, state_start: float) -> None:
    matched = [r for r in results if r.matched]
    unmatched = [r for r in results if not r.matched]
    sum_local = round(sum(r.local_pnl for r in results), 6)
    sum_pm = round(sum((r.pm_pnl if r.pm_pnl is not None else r.local_pnl) for r in results), 6)
    delta = round(sum_pm - sum_local, 6)
    gap = round((state_capital - state_start) - state_total_pnl, 6)
    remaining = round(gap - delta, 6)
    print("\nSUMMARY")
    print(f"  positions total / matched / unmatched  : {len(results)} / {len(matched)} / {len(unmatched)}")
    print(f"  sum local_pnl                          : {sum_local:+.4f}")
    print(f"  sum polymarket_pnl                     : {sum_pm:+.4f}")
    print(f"  delta (pm - local)                     : {delta:+.4f}")
    print(f"  USDC gap to explain                    : {gap:+.4f}   (current - starting - state.total_pnl)")
    print(f"  explained by reconciliation            : {delta:+.4f}")
    print(f"  remaining unexplained                  : {remaining:+.4f}")
    if unmatched:
        print(f"\n  unmatched positions (need manual review): {[r.position_id for r in unmatched]}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(argv: Optional[list[str]] = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    p.add_argument("--apply", action="store_true",
                   help="Rewrite state.json + the two CSVs. Dry-run without it.")
    args = p.parse_args(argv)

    if not STATE_PATH.exists():
        print(f"ERROR: {STATE_PATH} not found. Run from BTC_pricer_15m/.", file=sys.stderr)
        return 2

    print(f"reading local state: {STATE_PATH}")
    state = json.loads(STATE_PATH.read_text(encoding="utf-8"))
    closed = state.get("closed_positions") or []
    cap = state.get("capital") or {}
    starting = float(cap.get("starting", 0.0) or 0.0)
    current = float(cap.get("current", 0.0) or 0.0)
    total_pnl = float(cap.get("total_pnl", 0.0) or 0.0)
    print(f"  closed_positions: {len(closed)}")
    print(f"  capital: starting={starting:.4f} current={current:.4f} total_pnl={total_pnl:.4f}")
    print(f"  USDC gap vs book: ${(current - starting) - total_pnl:+.4f}")

    print("connecting to Polymarket ...")
    client, funder = _init_client()
    print("fetching trade history ...")
    trades, our_uuid = _fetch_all_trades(client, funder)
    print(f"  Polymarket trades fetched: {len(trades)}")
    print(f"  our account UUID (for maker-side identification): {our_uuid}")
    small = [t for t in trades if BOT_FILL_MIN_USDC <= t["usdc"] <= BOT_FILL_MAX_USDC]
    print(f"  in bot size range [${BOT_FILL_MIN_USDC:.0f}, ${BOT_FILL_MAX_USDC:.0f}]: {len(small)}")

    print("matching ...\n")
    results = _reconcile(closed, trades)

    _print_table(results)
    _summary(results, total_pnl, current, starting)

    if not args.apply:
        print("\n[dry-run] no files written. Re-run with --apply once the table above looks right.")
        return 0

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%SZ")
    backup_dir = RESULTS_DIR / f"pre_reconcile_{ts}"
    backup_dir.mkdir(parents=True, exist_ok=True)
    print(f"\napplying - backup dir: {backup_dir}")
    trades_rows = _load_csv(TRADES_CSV)
    equity_rows = _load_csv(EQUITY_CSV)
    _apply(results, state, trades_rows, equity_rows, backup_dir=backup_dir)
    print("done. Restart the dashboard backend so trades_tail re-seeds from the corrected CSV.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
