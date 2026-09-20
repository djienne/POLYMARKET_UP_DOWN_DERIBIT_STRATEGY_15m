from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path
from typing import Optional

from ..config import settings
from ..events.bus import bus
from ..models import InstanceStats, OpenPosition, PositionState, TradeEvent, ViewSource
from . import entry_registry

log = logging.getLogger(__name__)

STARTING_CAPITAL = 1000.0
LIVE_CLOSE_EPSILON = 1e-6


def _pnl_mean_to_sd(pnls: list[float]) -> Optional[float]:
    """Per-trade dollar P&L mean/sample SD, without time annualization."""
    if len(pnls) < 2:
        return None
    import math
    mean = sum(pnls) / len(pnls)
    var = sum((p - mean) ** 2 for p in pnls) / (len(pnls) - 1)
    sd = math.sqrt(var)
    if sd == 0:
        return None
    return mean / sd


def _compute_max_dd(pnls: list[float], starting: float) -> tuple[float, float]:
    peak = starting
    equity = starting
    max_dd = 0.0
    max_dd_pct = 0.0
    for p in pnls:
        equity += p
        peak = max(peak, equity)
        dd = peak - equity
        max_dd = max(max_dd, dd)
        max_dd_pct = max(max_dd_pct, (dd / peak) * 100.0 if peak > 0 else 0.0)
    return max_dd, max_dd_pct


def instance_from_raw(instance_id: int, raw: dict, starting_capital: float = STARTING_CAPITAL) -> InstanceStats:
    raw_starting_capital = raw.get("starting_capital")
    if raw_starting_capital is not None:
        try:
            starting_capital = float(raw_starting_capital)
        except (TypeError, ValueError):
            pass
    pnls = [float(x) for x in raw.get("trade_pnls", []) if isinstance(x, (int, float))]
    capital = float(raw.get("capital", starting_capital))
    total_pnl = float(raw.get("total_pnl", capital - starting_capital))
    wins = int(raw.get("wins", 0))
    losses = int(raw.get("losses", 0))
    trades = int(raw.get("trades_count", len(pnls)))
    wr = (wins / trades * 100.0) if trades else 0.0
    pnl_mean_to_sd = _pnl_mean_to_sd(pnls)
    mdd, mdd_pct = _compute_max_dd(pnls, starting_capital)
    if "pnl_moments_complete" in raw:
        pnl_mean_to_sd = None
        n = int(raw.get("pnl_count", 0))
        if raw.get("pnl_moments_complete") and n >= 2:
            mean = float(raw.get("pnl_sum", 0)) / n
            variance = (float(raw.get("pnl_sum_sq", 0)) - n * mean * mean) / (n - 1)
            if variance > 0:
                pnl_mean_to_sd = mean / variance ** 0.5
    if "drawdown_history_complete" in raw:
        mdd = raw.get("max_drawdown") if raw.get("drawdown_history_complete") else None
        mdd_pct = raw.get("max_drawdown_pct") if raw.get("drawdown_history_complete") else None
    return InstanceStats(
        instance_id=instance_id,
        capital=capital,
        starting_capital=starting_capital,
        total_pnl=total_pnl,
        total_pnl_pct=(total_pnl / starting_capital) * 100.0 if starting_capital else 0.0,
        pnl_mean_to_sd=pnl_mean_to_sd,
        max_drawdown=mdd,
        max_drawdown_pct=mdd_pct,
        wins=wins,
        losses=losses,
        win_rate=wr,
        trades_count=trades,
    )


def _edge_ratio_from_entry_ctx(pos: dict) -> Optional[float]:
    ctx = pos.get("entry_context")
    if not isinstance(ctx, dict):
        return None
    v = ctx.get("edge_ratio")
    if isinstance(v, (int, float)):
        return float(v)
    return None


def position_from_raw(raw: dict) -> PositionState:
    pos = raw.get("position")
    last_exit = raw.get("last_tp_sl_time")
    if not pos:
        return PositionState(open=None, last_exit_at=last_exit)
    try:
        open_pos = OpenPosition(
            direction=pos.get("direction"),
            entry_price=float(pos.get("entry_price", 0.0)),
            shares=float(pos.get("shares", 0.0)),
            tp_target=float(pos["tp_price"]) if pos.get("tp_price") is not None else None,
            sl_target=float(pos["stop_loss_price"]) if pos.get("stop_loss_price") is not None else None,
            entered_at=pos.get("opened_at"),
            market_id=pos.get("market_id"),
            notional=float(pos.get("cost_basis")) if pos.get("cost_basis") is not None else None,
            entry_edge_ratio=_edge_ratio_from_entry_ctx(pos),
        )
    except Exception:
        log.exception("position parse failed")
        open_pos = None
    return PositionState(open=open_pos, last_exit_at=last_exit)


def _live_trade_pnls(raw: dict) -> list[float]:
    pnls: list[float] = []
    for pos in raw.get("closed_positions") or []:
        if not _is_meaningful_live_close(pos):
            continue
        try:
            pnl = pos.get("pnl")
        except AttributeError:
            continue
        if isinstance(pnl, (int, float)):
            pnls.append(float(pnl))
    return pnls


def _is_meaningful_live_close_values(
    pnl: object,
    cost_basis: object,
    proceeds: object,
    *,
    epsilon: float = LIVE_CLOSE_EPSILON,
) -> bool:
    try:
        pnl_f = float(pnl)
    except (TypeError, ValueError):
        pnl_f = 0.0
    if abs(pnl_f) > epsilon:
        return True
    try:
        if cost_basis is None or proceeds is None:
            return False
        return abs(float(cost_basis) - float(proceeds)) > epsilon
    except (TypeError, ValueError):
        return False


def _is_meaningful_live_close(pos: object) -> bool:
    if not isinstance(pos, dict):
        return False
    try:
        pnl = pos.get("pnl")
        cost_basis = pos.get("cost_basis")
        proceeds = pos.get("proceeds")
    except AttributeError:
        return False
    return _is_meaningful_live_close_values(pnl, cost_basis, proceeds)


def instance_from_live_raw(
    instance_id: int,
    raw: dict,
    starting_capital: float = STARTING_CAPITAL,
) -> Optional[InstanceStats]:
    capital = raw.get("capital")
    if not isinstance(capital, dict):
        return None
    raw_starting = capital.get("starting")
    raw_current = capital.get("current")
    raw_total_pnl = capital.get("total_pnl")
    try:
        start = float(raw_starting) if raw_starting is not None else float(starting_capital)
    except (TypeError, ValueError):
        start = float(starting_capital)
    try:
        current = float(raw_current) if raw_current is not None else start
    except (TypeError, ValueError):
        current = start
    try:
        total_pnl = float(raw_total_pnl) if raw_total_pnl is not None else (current - start)
    except (TypeError, ValueError):
        total_pnl = current - start

    closed_positions = raw.get("closed_positions") or []
    wins = 0
    losses = 0
    trades = 0
    for pos in closed_positions:
        if not _is_meaningful_live_close(pos):
            continue
        result = pos.get("result")
        if result is None:
            continue
        trades += 1
        pnl = float(pos.get("pnl") or 0.0)
        if pnl > LIVE_CLOSE_EPSILON:
            wins += 1
        elif pnl < -LIVE_CLOSE_EPSILON:
            losses += 1

    pnls = _live_trade_pnls(raw)
    pnl_mean_to_sd = _pnl_mean_to_sd(pnls)
    mdd, mdd_pct = _compute_max_dd(pnls, start)
    win_rate = (wins / trades * 100.0) if trades else 0.0

    return InstanceStats(
        instance_id=instance_id,
        capital=current,
        starting_capital=start,
        total_pnl=total_pnl,
        total_pnl_pct=(total_pnl / start) * 100.0 if start else 0.0,
        pnl_mean_to_sd=pnl_mean_to_sd,
        max_drawdown=mdd,
        max_drawdown_pct=mdd_pct,
        wins=wins,
        losses=losses,
        win_rate=win_rate,
        trades_count=trades,
    )


def position_from_live_raw(raw: dict) -> PositionState:
    positions = raw.get("open_positions") or []
    if not positions:
        return PositionState(
            open=None,
            last_exit_at=raw.get("last_tp_fill_time"),
        )
    pos = positions[0]
    try:
        open_pos = OpenPosition(
            direction=pos.get("direction"),
            entry_price=float(pos.get("entry_price", 0.0)),
            shares=float(pos.get("shares", 0.0)),
            tp_target=float(pos["tp_price"]) if pos.get("tp_price") is not None else None,
            sl_target=float(pos["stop_loss_price"]) if pos.get("stop_loss_price") is not None else None,
            entered_at=pos.get("opened_at"),
            market_id=pos.get("market_id"),
            notional=float(pos.get("cost_basis")) if pos.get("cost_basis") is not None else None,
            entry_edge_ratio=_edge_ratio_from_entry_ctx(pos),
        )
    except Exception:
        log.exception("live position parse failed")
        open_pos = None
    return PositionState(
        open=open_pos,
        last_exit_at=raw.get("last_tp_fill_time"),
    )


class StateReader:
    def __init__(
        self,
        path_fn,
        instance_ids: Optional[list[int]] = None,
        run_source: ViewSource = "dry_run",
        single_trader_instance_id_fn=None,
    ) -> None:
        """path_fn: callable returning Path to state file (so we can switch files based on mode)."""
        self._path_fn = path_fn
        self.run_source: ViewSource = run_source
        self._single_trader_instance_id_fn = single_trader_instance_id_fn
        self._last_mtime: Optional[float] = None
        self._raw: dict = {}

    @property
    def path(self) -> Path:
        return self._path_fn()

    def read_if_changed(self) -> bool:
        try:
            st = self.path.stat()
        except FileNotFoundError:
            return False
        if self._last_mtime is not None and st.st_mtime == self._last_mtime:
            return False
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as e:
            log.warning("state read failed: %s", e)
            return False
        self._raw = raw
        self._last_mtime = st.st_mtime
        return True

    def instance(self, instance_id: int, starting_capital: float = STARTING_CAPITAL) -> Optional[InstanceStats]:
        instances = self._raw.get("instances")
        if isinstance(instances, dict):
            raw = instances.get(str(instance_id))
            if raw is None:
                return None
            return instance_from_raw(instance_id, raw, starting_capital=starting_capital)
        return instance_from_live_raw(instance_id, self._raw, starting_capital=starting_capital)

    def position(self, instance_id: int) -> PositionState:
        instances = self._raw.get("instances")
        if isinstance(instances, dict):
            raw = instances.get(str(instance_id))
            if raw is None:
                return PositionState()
            return position_from_raw(raw)
        return position_from_live_raw(self._raw)

    def trade_pnls(self, instance_id: int) -> list[float]:
        instances = self._raw.get("instances")
        if isinstance(instances, dict):
            raw = instances.get(str(instance_id))
            if raw is None:
                return []
            return [float(x) for x in raw.get("trade_pnls", []) if isinstance(x, (int, float))]
        return _live_trade_pnls(self._raw)

    @property
    def raw(self) -> dict:
        return self._raw

    def single_trader_instance_id(self) -> int:
        if self._single_trader_instance_id_fn is not None:
            try:
                return int(self._single_trader_instance_id_fn())
            except (TypeError, ValueError):
                pass
        return _single_trader_instance_id()


def _single_trader_instance_id() -> int:
    """Instance id used for live single-trader open-position events."""
    return int(settings.default_instance_id)


def _current_open_position(raw: dict) -> Optional[dict]:
    positions = raw.get("open_positions") or []
    if not positions:
        return None
    first = positions[0]
    return first if isinstance(first, dict) else None


def _open_key(raw: dict) -> Optional[str]:
    """Stable identifier for the currently-open position, or None if none.

    Uses ``opened_at`` — the same ISO timestamp the trader writes into
    ``15m_live_trades.csv`` at close, so state_reader and trades_tail agree
    on the dedup key.
    """
    pos = _current_open_position(raw)
    if pos is None:
        return None
    opened_at = pos.get("opened_at")
    return opened_at if isinstance(opened_at, str) and opened_at else None


def _build_entry_event(
    raw: dict,
    run_source: ViewSource = "live",
    instance_id: Optional[int] = None,
) -> Optional[TradeEvent]:
    pos = _current_open_position(raw)
    if pos is None:
        return None
    opened_at = pos.get("opened_at")
    if not isinstance(opened_at, str) or not opened_at:
        return None

    def _f(v: object) -> Optional[float]:
        if isinstance(v, (int, float)):
            return float(v)
        return None

    return TradeEvent(
        instance_id=instance_id if instance_id is not None else _single_trader_instance_id(),
        timestamp=opened_at,
        event="ENTRY",
        run_source=run_source,
        direction=pos.get("direction") if isinstance(pos.get("direction"), str) else None,
        market_id=pos.get("market_id") if isinstance(pos.get("market_id"), str) else None,
        entry_price=_f(pos.get("entry_price")),
        exit_price=None,
        shares=_f(pos.get("shares")),
        pnl=None,
        pnl_pct=None,
        capital=None,
        model_prob=_f(pos.get("model_prob")),
        poly_prob=_f(pos.get("poly_price") or pos.get("poly_prob")),
        spot_price=_f(pos.get("spot_price")),
        barrier=_f(pos.get("reference_price")),
    )


async def run_state_loop(reader: "StateReader", stop: asyncio.Event) -> None:
    # lifespan() already primed the reader (hub.state.read_if_changed()
    # before this task was created), so reader._raw is the baseline truth.
    # If a position is open at baseline we missed its opening — register
    # it in entry_registry so trades_tail doesn't retroactively emit an
    # ENTRY once the CSV close row lands. Every null?open transition the
    # loop sees from here on is genuine and fires unconditionally.
    last_open_key = _open_key(reader._raw)
    if last_open_key is not None:
        entry_registry.mark_emitted(reader.run_source, last_open_key)
        log.info(
            "state_loop baseline: position already open (opened_at=%s) — "
            "will suppress retroactive ENTRY on close",
            last_open_key,
        )
    else:
        log.info("state_loop baseline: no open position")

    while not stop.is_set():
        if reader.read_if_changed():
            await bus.publish(
                "state.update",
                {"source": reader.run_source, "mtime": reader._last_mtime},
            )

            current_key = _open_key(reader._raw)
            if current_key is not None and current_key != last_open_key:
                event = _build_entry_event(
                    reader._raw,
                    reader.run_source,
                    reader.single_trader_instance_id(),
                )
                if event is not None:
                    try:
                        await bus.publish("trade.append", event.model_dump())
                        entry_registry.mark_emitted(reader.run_source, current_key)
                        log.info(
                            "state_loop emitted ENTRY opened_at=%s "
                            "direction=%s entry_price=%s",
                            current_key,
                            event.direction,
                            event.entry_price,
                        )
                    except Exception:
                        log.exception("failed to publish ENTRY trade.append")
            last_open_key = current_key
        try:
            await asyncio.wait_for(stop.wait(), timeout=settings.state_poll_interval_seconds)
        except asyncio.TimeoutError:
            pass
