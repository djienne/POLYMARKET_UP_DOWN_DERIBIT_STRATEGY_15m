"""Periodic summaries and final leaderboard for the grid search."""

import csv
import math
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

from scripts.trader._io import atomic_write_csv_rows, atomic_write_text
from scripts.trader.trader import PaperInstance

LAST_CLOSE_LABELS = {
    "TP_FILLED": "TP win",
    "TP_SIGNAL_MARKET_EXIT": "TP signal exit",
    "STOP_LOSS": "SL loss",
    "WIN_EXPIRY": "EXP win",
    "LOSS_EXPIRY": "EXP loss",
    "TRAILING_STOP": "TS loss",
}


def print_market_summary(
    instances: list[PaperInstance],
    market_id: str,
    market_title: str,
    liquidity_mode: str = "",
):
    """Print summary after a market resolves."""
    traded = [i for i in instances if i.trades_count > 0 and i.trade_pnls]
    n_traded = len(traded)
    n_winning = sum(1 for i in traded if i.trade_pnls and i.trade_pnls[-1] > 0)
    mode_label = f" [{liquidity_mode}]" if liquidity_mode else ""

    print(f"\n{'='*70}")
    print(f"Market resolved{mode_label}: {market_title}")
    print(f"Instances traded: {n_traded}/{len(instances)}  |  Winners this round: {n_winning}")

    if traded:
        top = sorted(traded, key=lambda i: i.total_pnl, reverse=True)[:5]
        print("\nTop 5 by total P&L:")
        for rank, inst in enumerate(top, 1):
            p = inst.params
            wr = (inst.wins / inst.trades_count * 100) if inst.trades_count > 0 else 0
            print(
                f"  #{rank:2d}  PnL=${inst.total_pnl:+8.2f}  "
                f"W/L={inst.wins}/{inst.losses} ({wr:.0f}%)  "
                f"[{p.label()}]"
            )
    print(f"{'='*70}\n")


def print_status(instances: list[PaperInstance], market_title: str = ""):
    """Print current status of all instances."""
    active = sum(1 for i in instances if i.position is not None)
    traded = sum(1 for i in instances if i.trades_count > 0)
    total_pnl = sum(i.total_pnl for i in instances)

    print(f"\nStatus: {len(instances)} instances  |  {traded} have traded  |  {active} with open positions")
    if market_title:
        print(f"Current market: {market_title}")
    print(f"Aggregate P&L: ${total_pnl:+.2f}")
    print()


def pnl_mean_to_sd(inst) -> float | None:
    """Dimensionless mean/SD of closed-trade dollar P&Ls; not annualized Sharpe."""
    values = inst.trade_pnls
    count = int(getattr(inst, "pnl_count", len(values)))
    complete = getattr(inst, "pnl_moments_complete", count == len(values))
    if not complete or count < 2:
        return None
    total = float(inst.pnl_sum if hasattr(inst, "pnl_sum") else sum(values))
    sum_sq = float(inst.pnl_sum_sq if hasattr(inst, "pnl_sum_sq") else sum(value * value for value in values))
    variance = (sum_sq - total * total / count) / (count - 1)
    if variance <= 0 or not all(math.isfinite(value) for value in (total, sum_sq, variance)):
        return None
    return (total / count) / math.sqrt(variance)


def realized_drawdown(inst) -> tuple[float | None, float | None]:
    if hasattr(inst, "drawdown_history_complete"):
        if not inst.drawdown_history_complete:
            return None, None
        return inst.max_drawdown, inst.max_drawdown_pct
    if len(inst.trade_pnls) != inst.trades_count:
        return None, None
    return _max_drawdown(inst.trade_pnls, inst.shared_config.starting_capital)


def _metric_text(value: float | None, places: int = 2) -> str:
    return f"{value:.{places}f}" if value is not None else ""


def snapshot_report_instances(instances) -> tuple:
    """Copy bounded report inputs before a background writer sees them."""
    fields = (
        "id", "total_pnl", "wins", "losses", "trades_count", "pnl_sum", "pnl_sum_sq", "pnl_count",
        "pnl_moments_complete", "realized_peak", "max_drawdown", "max_drawdown_pct",
        "drawdown_history_complete", "last_close_at", "last_close_result",
    )
    return tuple(SimpleNamespace(
        **{name: getattr(inst, name) for name in fields if hasattr(inst, name)},
        params=inst.params,
        trade_pnls=tuple(inst.trade_pnls),
        shared_config=SimpleNamespace(
            starting_capital=inst.shared_config.starting_capital,
            experiment_id=getattr(inst.shared_config, "experiment_id", "legacy"),
        ),
    ) for inst in instances)


def _max_drawdown(trade_pnls: list[float], starting_capital: float) -> tuple[float, float]:
    """Return realized max drawdown in dollars and percent from closed trades."""
    peak = starting_capital
    equity = starting_capital
    max_dd_abs = 0.0
    max_dd_pct = 0.0

    for pnl in trade_pnls:
        equity += pnl
        if equity > peak:
            peak = equity
        if peak <= 0:
            continue
        dd_abs = peak - equity
        dd_pct = dd_abs / peak * 100
        if dd_abs > max_dd_abs:
            max_dd_abs = dd_abs
        if dd_pct > max_dd_pct:
            max_dd_pct = dd_pct

    return max_dd_abs, max_dd_pct


def _candidate_trade_logs(leaderboard_path: Path) -> list[Path]:
    """Return likely trade log paths for the available dry-run history."""
    leaderboard_path = Path(leaderboard_path)
    candidates = [
        leaderboard_path.with_name("trades.csv"),
        Path("results/trades.csv"),
    ]

    repo_results = leaderboard_path.parent.parent / "results" / "trades.csv"
    candidates.append(repo_results)

    unique: list[Path] = []
    seen = set()
    for candidate in candidates:
        key = str(candidate.resolve()) if candidate.exists() else str(candidate)
        if key not in seen:
            seen.add(key)
            unique.append(candidate)
    return unique


def _history_start_timestamp(leaderboard_path: Path) -> datetime | None:
    """Return the first available dry-run trade timestamp from history."""
    for trade_log in _candidate_trade_logs(leaderboard_path):
        if not trade_log.exists():
            continue
        try:
            with open(trade_log, newline="") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    ts = (row.get("timestamp") or "").strip()
                    if not ts:
                        continue
                    try:
                        return datetime.fromisoformat(ts)
                    except ValueError:
                        continue
        except OSError:
            continue
    return None


def _close_label(event: str, pnl: str | None = None) -> str:
    label = LAST_CLOSE_LABELS.get(event)
    if label is not None:
        return label
    try:
        pnl_value = float(pnl or "0")
    except ValueError:
        return event or "-"
    return "win" if pnl_value >= 0 else "loss"


def _last_close_per_instance(leaderboard_path: Path) -> dict[int, tuple[datetime, str]]:
    """Map instance_id -> timestamp/result label for its most recent closed trade."""
    out: dict[int, tuple[datetime, str]] = {}
    for trade_log in _candidate_trade_logs(leaderboard_path):
        if not trade_log.exists():
            continue
        try:
            with open(trade_log, newline="") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    event = (row.get("event") or "").strip()
                    if event in ("", "ENTRY", "UNRESOLVED_RESTART"):
                        continue
                    try:
                        inst_id = int(row.get("instance_id") or -1)
                        ts = datetime.fromisoformat((row.get("timestamp") or "").strip())
                    except (TypeError, ValueError):
                        continue
                    cur = out.get(inst_id)
                    if cur is None or ts > cur[0]:
                        out[inst_id] = (ts, _close_label(event, row.get("pnl")))
        except OSError:
            continue
        if out:
            break
    return out


def _fmt_idle(seconds: float) -> str:
    """Compact human-readable duration (e.g. '15m', '2h15m', '1d04h')."""
    if seconds < 0:
        seconds = 0
    if seconds < 60:
        return f"{int(seconds)}s"
    if seconds < 3600:
        return f"{int(seconds // 60)}m"
    if seconds < 86400:
        h = int(seconds // 3600)
        m = int((seconds % 3600) // 60)
        return f"{h}h{m:02d}m"
    d = int(seconds // 86400)
    h = int((seconds % 86400) // 3600)
    return f"{d}d{h:02d}h"


def print_leaderboard(instances: list[PaperInstance], top_n: int = 20, liquidity_mode: str = ""):
    """Print final leaderboard sorted by total P&L."""
    traded = [i for i in instances if i.trades_count > 0]
    if not traded:
        print("\nNo trades were executed by any instance.")
        return

    ranked = sorted(traded, key=lambda i: i.total_pnl, reverse=True)
    mode_label = f" [{liquidity_mode}]" if liquidity_mode else ""

    print(f"\n{'='*90}")
    print(f"LEADERBOARD{mode_label} - {len(traded)}/{len(instances)} instances traded")
    print(f"{'='*90}")
    print(f"{'Rank':>4}  {'PnL':>10}  {'Mean/SD':>7}  {'MDD%':>6}  {'W/L':>7}  {'WR%':>5}  {'Trades':>6}  Parameters")
    print(f"{'-'*90}")

    for rank, inst in enumerate(ranked[:top_n], 1):
        p = inst.params
        wr = (inst.wins / inst.trades_count * 100) if inst.trades_count > 0 else 0
        ratio = _metric_text(pnl_mean_to_sd(inst))
        _, max_dd_pct = realized_drawdown(inst)
        print(
            f"{rank:4d}  ${inst.total_pnl:+9.2f}  {ratio:>7}  {_metric_text(max_dd_pct, 1):>5}%  "
            f"{inst.wins:3d}/{inst.losses:<3d}  {wr:4.0f}%  "
            f"{inst.trades_count:5d}   {p.label()}"
        )

    if len(ranked) > top_n:
        print("  ...")
        for inst in ranked[-3:]:
            rank = ranked.index(inst) + 1
            p = inst.params
            wr = (inst.wins / inst.trades_count * 100) if inst.trades_count > 0 else 0
            ratio = _metric_text(pnl_mean_to_sd(inst))
            _, max_dd_pct = realized_drawdown(inst)
            print(
                f"{rank:4d}  ${inst.total_pnl:+9.2f}  {ratio:>7}  {_metric_text(max_dd_pct, 1):>5}%  "
                f"{inst.wins:3d}/{inst.losses:<3d}  {wr:4.0f}%  "
                f"{inst.trades_count:5d}   {p.label()}"
            )

    print(f"{'='*90}\n")


def write_leaderboard_csv(instances: list[PaperInstance], path: Path, liquidity_mode: str = "", *, as_of: datetime | None = None):
    """Write full leaderboard to CSV."""
    path.parent.mkdir(parents=True, exist_ok=True)
    traded = sorted(
        [i for i in instances if i.trades_count > 0],
        key=lambda i: i.total_pnl,
        reverse=True,
    )
    written_at = datetime.now(timezone.utc)
    as_of = as_of or written_at

    header = [
        "rank", "instance_id", "total_pnl", "pnl_mean_to_sd", "max_drawdown", "max_drawdown_pct", "wins", "losses",
        "win_rate", "trades", "alpha_up", "alpha_down", "floor_up",
        "floor_down", "tp_pct", "sl_pct", "liquidity_mode", "experiment_id", "as_of", "written_at",
    ]
    rows = []
    for rank, inst in enumerate(traded, 1):
        p = inst.params
        wr = (inst.wins / inst.trades_count * 100) if inst.trades_count > 0 else 0
        max_dd_abs, max_dd_pct = realized_drawdown(inst)
        rows.append([
            rank, inst.id, f"{inst.total_pnl:.2f}", _metric_text(pnl_mean_to_sd(inst), 4),
            _metric_text(max_dd_abs), _metric_text(max_dd_pct),
            inst.wins, inst.losses, f"{wr:.1f}", inst.trades_count,
            p.alpha_up, p.alpha_down, p.floor_up, p.floor_down,
            p.tp_percentage, p.stop_loss_pct, liquidity_mode,
            getattr(inst.shared_config, "experiment_id", "legacy"), as_of.isoformat(), written_at.isoformat(),
        ])
    atomic_write_csv_rows(path, header, rows)

    print(f"Leaderboard written to {path}")

    txt_path = path.with_suffix(".txt")
    n_profitable = sum(1 for i in traded if i.total_pnl > 0)
    now_utc = as_of
    now_str = now_utc.strftime("%Y-%m-%d %H:%M:%S UTC")
    history_start = _history_start_timestamp(path)
    # Runtime instances retain this incrementally and recover it once at startup.
    # The file adapter remains for older external callers, never the grid loop.
    needs_legacy_history = any(not hasattr(inst, "last_close_at") for inst in traded)
    last_close = _last_close_per_instance(path) if needs_legacy_history else {}
    for inst in traded:
        if getattr(inst, "last_close_at", None) is not None:
            last_close[inst.id] = (inst.last_close_at, _close_label(inst.last_close_result))
    parts: list[str] = []
    parts.append("BTC_pricer_15m Leaderboard")
    if liquidity_mode:
        parts.append(f" [{liquidity_mode}]")
    parts.append("\n")
    parts.append(f"State as of: {now_str} | Written: {written_at.isoformat()}\n")
    if history_start is not None:
        start_utc = history_start.astimezone(timezone.utc)
        days_since_start = (now_utc - start_utc).total_seconds() / 86400
        parts.append(
            "History start: "
            f"{start_utc.strftime('%Y-%m-%d %H:%M:%S UTC')}  |  "
            f"Days since start: {days_since_start:.2f}\n"
        )
    parts.append("Max drawdown is realized drawdown from starting capital using closed-trade P&L only.\n")
    parts.append("P&L mean/SD uses complete closed-trade dollar P&Ls; it is not an annualized Sharpe ratio.\n")
    parts.append("Blank statistics mean the complete history or nonzero sample variance is unavailable.\n")
    parts.append("Idle = time since the instance's most recent closed trade.\n")
    parts.append("Last = latest closed trade result/mechanism (TP/SL/EXP win/loss).\n")
    parts.append(f"{'='*114}\n")
    parts.append(f"{len(traded)}/{len(instances)} traded | {n_profitable} profitable | {len(traded) - n_profitable} losing\n\n")
    parts.append(f"{'Rank':>4}  {'PnL':>10}  {'Mean/SD':>7}  {'MDD%':>6}  {'W/L':>7}  {'WR%':>5}  {'Trades':>6}  {'Idle':>7}  {'Last':>8}  Parameters\n")
    parts.append(f"{'-'*114}\n")
    for rank, inst in enumerate(traded, 1):
        p = inst.params
        wr = (inst.wins / inst.trades_count * 100) if inst.trades_count > 0 else 0
        ratio = _metric_text(pnl_mean_to_sd(inst))
        _, max_dd_pct = realized_drawdown(inst)
        sl = f" sl={p.stop_loss_pct}" if p.stop_loss_pct > 0 else ""
        close_info = last_close.get(inst.id)
        if close_info is not None:
            ts_close, last_result = close_info
            idle = _fmt_idle((now_utc - ts_close.astimezone(timezone.utc)).total_seconds())
        else:
            last_result = "-"
            idle = "—"
        parts.append(
            f"{rank:4d}  ${inst.total_pnl:+9.2f}  {ratio:>7}  {_metric_text(max_dd_pct, 1):>5}%  "
            f"{inst.wins:3d}/{inst.losses:<3d}  {wr:4.0f}%  "
            f"{inst.trades_count:5d}  {idle:>7}  {last_result:>8}   "
            f"aU={p.alpha_up} aD={p.alpha_down} "
            f"fU={p.floor_up} fD={p.floor_down} "
            f"tp={p.tp_percentage}{sl}\n"
        )
    atomic_write_text(txt_path, "".join(parts))
