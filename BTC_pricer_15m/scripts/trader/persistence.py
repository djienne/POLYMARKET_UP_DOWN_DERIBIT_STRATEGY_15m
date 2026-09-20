"""SQLite persistence primitives for trader state and market lifecycle.

This module is intentionally small and dependency-free.  It gives the trader a
transactional store that can run alongside the legacy JSON/CSV files during the
migration.  The legacy files remain useful as exports, but SQLite becomes the
durable state boundary once enabled in config.
"""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, Optional

from .position import Trade


SCHEMA_VERSION = 2


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json_dumps(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


class SqliteStore:
    """Small SQLite repository for state snapshots and normalized read models."""

    def __init__(self, path: Path):
        self.path = Path(path)

    @contextmanager
    def connect(self, *, durable: bool = False) -> Iterator[sqlite3.Connection]:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=FULL" if durable else "PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA busy_timeout=5000")
            conn.execute("PRAGMA foreign_keys=ON")
            self.initialize(conn)
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def initialize(self, conn: Optional[sqlite3.Connection] = None) -> None:
        owns_conn = conn is None
        if conn is None:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            conn = sqlite3.connect(self.path)
        try:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS state_snapshots (
                    mode TEXT PRIMARY KEY,
                    version TEXT NOT NULL,
                    last_updated TEXT NOT NULL,
                    payload_json TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS positions (
                    position_id TEXT PRIMARY KEY,
                    mode TEXT NOT NULL,
                    market_id TEXT,
                    token_id TEXT,
                    direction TEXT NOT NULL,
                    shares REAL NOT NULL,
                    entry_price REAL NOT NULL,
                    cost_basis REAL NOT NULL,
                    tp_price REAL,
                    stop_loss_price REAL,
                    reference_price REAL,
                    status TEXT NOT NULL,
                    opened_at TEXT,
                    closed_at TEXT,
                    pending_settlement INTEGER NOT NULL DEFAULT 0,
                    pending_reason TEXT,
                    payload_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS trades (
                    trade_id TEXT PRIMARY KEY,
                    mode TEXT NOT NULL,
                    instance_id INTEGER,
                    market_id TEXT,
                    token_id TEXT,
                    direction TEXT NOT NULL,
                    shares REAL NOT NULL,
                    entry_price REAL NOT NULL,
                    exit_price REAL NOT NULL,
                    cost_basis REAL NOT NULL,
                    proceeds REAL NOT NULL,
                    pnl REAL NOT NULL,
                    pnl_pct REAL NOT NULL,
                    result TEXT NOT NULL,
                    proceeds_source TEXT,
                    settlement_source TEXT,
                    entry_order_id TEXT,
                    exit_order_id TEXT,
                    opened_at TEXT,
                    closed_at TEXT,
                    payload_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_trades_mode_closed
                    ON trades(mode, closed_at);
                CREATE INDEX IF NOT EXISTS idx_trades_market
                    ON trades(market_id);

                CREATE TABLE IF NOT EXISTS equity_points (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    mode TEXT NOT NULL,
                    instance_id INTEGER,
                    ts TEXT NOT NULL,
                    event TEXT NOT NULL,
                    direction TEXT,
                    entry_price REAL,
                    exit_price REAL,
                    shares REAL,
                    pnl REAL,
                    capital REAL NOT NULL,
                    equity REAL NOT NULL,
                    position_value REAL NOT NULL,
                    source TEXT NOT NULL DEFAULT 'state_manager'
                );

                CREATE INDEX IF NOT EXISTS idx_equity_mode_ts
                    ON equity_points(mode, ts);

                CREATE TABLE IF NOT EXISTS markets (
                    market_id TEXT PRIMARY KEY,
                    slug TEXT,
                    window_start_iso TEXT,
                    window_end_iso TEXT,
                    strike REAL,
                    strike_source TEXT,
                    strike_offset_s REAL,
                    strike_captured_at_iso TEXT,
                    settlement_status TEXT NOT NULL DEFAULT 'pending',
                    settlement_price REAL,
                    settlement_offset_s REAL,
                    settlement_source TEXT,
                    settlement_captured_at_iso TEXT,
                    settlement_error TEXT,
                    chain_status TEXT,
                    payload_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_markets_window_start
                    ON markets(window_start_iso);
                CREATE INDEX IF NOT EXISTS idx_markets_window_end
                    ON markets(window_end_iso);

                CREATE TABLE IF NOT EXISTS mirror_signal_statuses (
                    mode TEXT NOT NULL,
                    signal_id TEXT NOT NULL,
                    stream_id TEXT,
                    event TEXT,
                    market_id TEXT,
                    direction TEXT,
                    source_instance_id TEXT,
                    status TEXT NOT NULL,
                    reason TEXT,
                    attempts INTEGER,
                    first_seen TEXT NOT NULL,
                    last_attempt_at TEXT,
                    processed_at TEXT,
                    live_order_id TEXT,
                    live_position_id TEXT,
                    payload_json TEXT NOT NULL DEFAULT '{}',
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY(mode, signal_id)
                );

                CREATE INDEX IF NOT EXISTS idx_mirror_signal_status
                    ON mirror_signal_statuses(mode, status, updated_at);
                CREATE INDEX IF NOT EXISTS idx_mirror_signal_market
                    ON mirror_signal_statuses(market_id, event);
                """
            )
            conn.execute(
                "INSERT OR REPLACE INTO metadata(key, value) VALUES (?, ?)",
                ("schema_version", str(SCHEMA_VERSION)),
            )
            if owns_conn:
                conn.commit()
        finally:
            if owns_conn:
                conn.close()

    def save_state_snapshot(self, *, mode: str, version: str, payload: dict, durable: bool = False) -> None:
        updated = payload.get("last_updated") or _utc_now_iso()
        open_positions = list(payload.get("open_positions") or [])
        closed_positions = list(payload.get("closed_positions") or [])
        with self.connect(durable=durable) as conn:
            conn.execute(
                """
                INSERT INTO state_snapshots(mode, version, last_updated, payload_json)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(mode) DO UPDATE SET
                    version=excluded.version,
                    last_updated=excluded.last_updated,
                    payload_json=excluded.payload_json
                """,
                (mode, version, updated, _json_dumps(payload)),
            )
            for pos in open_positions:
                self._upsert_position_row(conn, mode=mode, pos=pos, status="open")
            open_ids = {str(pos.get("id") or "") for pos in open_positions}
            if open_ids:
                placeholders = ",".join("?" for _ in open_ids)
                conn.execute(
                    f"UPDATE positions SET status='closed' "
                    f"WHERE mode=? AND status='open' AND position_id NOT IN ({placeholders})",
                    [mode, *sorted(open_ids)],
                )
            else:
                conn.execute(
                    "UPDATE positions SET status='closed' WHERE mode=? AND status='open'",
                    (mode,),
                )
            for trade in closed_positions:
                self._upsert_trade_payload(conn, mode=mode, trade=trade)

    def load_state_snapshot(self, *, mode: str) -> Optional[dict]:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT payload_json FROM state_snapshots WHERE mode=?",
                (mode,),
            ).fetchone()
            if row is None:
                return None
            return json.loads(str(row["payload_json"]))

    def upsert_trade(self, *, mode: str, trade: Trade, instance_id: Optional[int] = None) -> None:
        with self.connect() as conn:
            self._upsert_trade_payload(
                conn,
                mode=mode,
                trade=trade.to_dict(),
                instance_id=instance_id,
            )

    def append_equity_point(
        self,
        *,
        mode: str,
        event: str,
        direction: str,
        entry_price: float,
        exit_price: float,
        shares: float,
        pnl: float,
        capital: float,
        equity: float,
        position_value: float,
        instance_id: Optional[int] = None,
        source: str = "state_manager",
    ) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO equity_points(
                    mode, instance_id, ts, event, direction, entry_price, exit_price,
                    shares, pnl, capital, equity, position_value, source
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    mode,
                    instance_id,
                    _utc_now_iso(),
                    event,
                    direction,
                    float(entry_price),
                    float(exit_price),
                    float(shares),
                    float(pnl),
                    float(capital),
                    float(equity),
                    float(position_value),
                    source,
                ),
            )

    def upsert_market(self, payload: dict) -> None:
        market_id = str(payload.get("market_id") or "")
        if not market_id:
            raise ValueError("market payload missing market_id")
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO markets(
                    market_id, slug, window_start_iso, window_end_iso,
                    strike, strike_source, strike_offset_s, strike_captured_at_iso,
                    settlement_status, settlement_price, settlement_offset_s,
                    settlement_source, settlement_captured_at_iso, settlement_error,
                    chain_status, payload_json, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(market_id) DO UPDATE SET
                    slug=excluded.slug,
                    window_start_iso=excluded.window_start_iso,
                    window_end_iso=excluded.window_end_iso,
                    strike=excluded.strike,
                    strike_source=excluded.strike_source,
                    strike_offset_s=excluded.strike_offset_s,
                    strike_captured_at_iso=excluded.strike_captured_at_iso,
                    settlement_status=excluded.settlement_status,
                    settlement_price=excluded.settlement_price,
                    settlement_offset_s=excluded.settlement_offset_s,
                    settlement_source=excluded.settlement_source,
                    settlement_captured_at_iso=excluded.settlement_captured_at_iso,
                    settlement_error=excluded.settlement_error,
                    chain_status=excluded.chain_status,
                    payload_json=excluded.payload_json,
                    updated_at=excluded.updated_at
                """,
                (
                    market_id,
                    payload.get("slug") or market_id,
                    payload.get("window_start_iso"),
                    payload.get("window_end_iso"),
                    _optional_float(payload.get("strike")),
                    payload.get("strike_source"),
                    _optional_float(payload.get("strike_offset_s")),
                    payload.get("strike_captured_at_iso"),
                    str(payload.get("settlement_status") or "pending"),
                    _optional_float(payload.get("settlement_price")),
                    _optional_float(payload.get("settlement_offset_s")),
                    payload.get("settlement_source"),
                    payload.get("settlement_captured_at_iso"),
                    payload.get("settlement_error"),
                    "chain_broken" if payload.get("strike_source") == "chain_broken" else "",
                    _json_dumps(payload),
                    _utc_now_iso(),
                ),
            )

    def get_market(self, market_id: str) -> Optional[dict]:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT payload_json FROM markets WHERE market_id=?",
                (market_id,),
            ).fetchone()
            if row is None:
                return None
            return json.loads(str(row["payload_json"]))

    def list_markets(self) -> list[dict]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT payload_json
                FROM markets
                ORDER BY COALESCE(window_start_iso, ''), market_id
                """
            ).fetchall()
            return [json.loads(str(row["payload_json"])) for row in rows]

    def find_market_by_time(self, *, key: str, iso: str) -> Optional[dict]:
        if key not in {"window_start_iso", "window_end_iso"}:
            raise ValueError(f"unsupported market time key: {key}")
        with self.connect() as conn:
            row = conn.execute(
                f"SELECT payload_json FROM markets WHERE {key}=? ORDER BY updated_at DESC LIMIT 1",
                (iso,),
            ).fetchone()
            if row is None:
                return None
            return json.loads(str(row["payload_json"]))

    def upsert_mirror_signal_status(
        self,
        *,
        mode: str,
        signal_id: str,
        status: str,
        reason: str = "",
        stream_id: str = "",
        event: str = "",
        market_id: str = "",
        direction: str = "",
        source_instance_id: str = "",
        attempts: Optional[int] = None,
        live_order_id: str = "",
        live_position_id: str = "",
        details: Optional[dict] = None,
        terminal: bool = False,
    ) -> None:
        signal_id = str(signal_id or "")
        if not signal_id:
            return
        now = _utc_now_iso()
        processed_at = now if terminal else None
        payload = dict(details or {})
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO mirror_signal_statuses(
                    mode, signal_id, stream_id, event, market_id, direction,
                    source_instance_id, status, reason, attempts, first_seen,
                    last_attempt_at, processed_at, live_order_id,
                    live_position_id, payload_json, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(mode, signal_id) DO UPDATE SET
                    stream_id=COALESCE(NULLIF(excluded.stream_id, ''), mirror_signal_statuses.stream_id),
                    event=COALESCE(NULLIF(excluded.event, ''), mirror_signal_statuses.event),
                    market_id=COALESCE(NULLIF(excluded.market_id, ''), mirror_signal_statuses.market_id),
                    direction=COALESCE(NULLIF(excluded.direction, ''), mirror_signal_statuses.direction),
                    source_instance_id=COALESCE(NULLIF(excluded.source_instance_id, ''), mirror_signal_statuses.source_instance_id),
                    status=excluded.status,
                    reason=excluded.reason,
                    attempts=COALESCE(excluded.attempts, mirror_signal_statuses.attempts),
                    last_attempt_at=COALESCE(excluded.last_attempt_at, mirror_signal_statuses.last_attempt_at),
                    processed_at=COALESCE(excluded.processed_at, mirror_signal_statuses.processed_at),
                    live_order_id=COALESCE(NULLIF(excluded.live_order_id, ''), mirror_signal_statuses.live_order_id),
                    live_position_id=COALESCE(NULLIF(excluded.live_position_id, ''), mirror_signal_statuses.live_position_id),
                    payload_json=excluded.payload_json,
                    updated_at=excluded.updated_at
                """,
                (
                    mode,
                    signal_id,
                    stream_id,
                    event,
                    market_id,
                    direction,
                    source_instance_id,
                    status,
                    reason,
                    attempts,
                    now,
                    now,
                    processed_at,
                    live_order_id,
                    live_position_id,
                    _json_dumps(payload),
                    now,
                ),
            )

    def get_mirror_signal_status(self, *, mode: str, signal_id: str) -> Optional[dict]:
        signal_id = str(signal_id or "")
        if not signal_id:
            return None
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT *
                FROM mirror_signal_statuses
                WHERE mode=? AND signal_id=?
                """,
                (mode, signal_id),
            ).fetchone()
            if row is None:
                return None
            out = dict(row)
            try:
                out["payload"] = json.loads(str(out.get("payload_json") or "{}"))
            except json.JSONDecodeError:
                out["payload"] = {}
            return out

    def delete_all_for_mode(self, *, mode: str) -> None:
        with self.connect() as conn:
            conn.execute("DELETE FROM state_snapshots WHERE mode=?", (mode,))
            conn.execute("DELETE FROM positions WHERE mode=?", (mode,))
            conn.execute("DELETE FROM trades WHERE mode=?", (mode,))
            conn.execute("DELETE FROM equity_points WHERE mode=?", (mode,))
            conn.execute("DELETE FROM mirror_signal_statuses WHERE mode=?", (mode,))

    @staticmethod
    def _upsert_position_row(
        conn: sqlite3.Connection,
        *,
        mode: str,
        pos: dict,
        status: str,
    ) -> None:
        position_id = str(pos.get("id") or "")
        if not position_id:
            return
        conn.execute(
            """
            INSERT INTO positions(
                position_id, mode, market_id, token_id, direction, shares,
                entry_price, cost_basis, tp_price, stop_loss_price,
                reference_price, status, opened_at, closed_at,
                pending_settlement, pending_reason, payload_json, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(position_id) DO UPDATE SET
                mode=excluded.mode,
                market_id=excluded.market_id,
                token_id=excluded.token_id,
                direction=excluded.direction,
                shares=excluded.shares,
                entry_price=excluded.entry_price,
                cost_basis=excluded.cost_basis,
                tp_price=excluded.tp_price,
                stop_loss_price=excluded.stop_loss_price,
                reference_price=excluded.reference_price,
                status=excluded.status,
                opened_at=excluded.opened_at,
                closed_at=excluded.closed_at,
                pending_settlement=excluded.pending_settlement,
                pending_reason=excluded.pending_reason,
                payload_json=excluded.payload_json,
                updated_at=excluded.updated_at
            """,
            (
                position_id,
                mode,
                pos.get("market_id"),
                pos.get("token_id"),
                str(pos.get("direction") or ""),
                float(pos.get("shares") or 0.0),
                float(pos.get("entry_price") or 0.0),
                float(pos.get("cost_basis") or 0.0),
                _optional_float(pos.get("tp_price")),
                _optional_float(pos.get("stop_loss_price")),
                _optional_float(pos.get("reference_price")),
                status,
                pos.get("opened_at"),
                pos.get("closed_at"),
                1 if pos.get("pending_settlement") else 0,
                pos.get("pending_settlement_reason"),
                _json_dumps(pos),
                _utc_now_iso(),
            ),
        )

    @staticmethod
    def _upsert_trade_payload(
        conn: sqlite3.Connection,
        *,
        mode: str,
        trade: dict,
        instance_id: Optional[int] = None,
    ) -> None:
        trade_id = str(trade.get("id") or "")
        if not trade_id:
            return
        entry_context = trade.get("entry_context") or {}
        market_id = (
            trade.get("market_id")
            or entry_context.get("market_id")
            or ""
        )
        settlement_source = (
            trade.get("settlement_source")
            or entry_context.get("settlement_source")
            or ""
        )
        conn.execute(
            """
            INSERT INTO trades(
                trade_id, mode, instance_id, market_id, token_id, direction,
                shares, entry_price, exit_price, cost_basis, proceeds, pnl,
                pnl_pct, result, proceeds_source, settlement_source,
                entry_order_id, exit_order_id, opened_at, closed_at,
                payload_json, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(trade_id) DO UPDATE SET
                mode=excluded.mode,
                instance_id=excluded.instance_id,
                market_id=excluded.market_id,
                token_id=excluded.token_id,
                direction=excluded.direction,
                shares=excluded.shares,
                entry_price=excluded.entry_price,
                exit_price=excluded.exit_price,
                cost_basis=excluded.cost_basis,
                proceeds=excluded.proceeds,
                pnl=excluded.pnl,
                pnl_pct=excluded.pnl_pct,
                result=excluded.result,
                proceeds_source=excluded.proceeds_source,
                settlement_source=excluded.settlement_source,
                entry_order_id=excluded.entry_order_id,
                exit_order_id=excluded.exit_order_id,
                opened_at=excluded.opened_at,
                closed_at=excluded.closed_at,
                payload_json=excluded.payload_json,
                updated_at=excluded.updated_at
            """,
            (
                trade_id,
                mode,
                instance_id,
                str(market_id),
                trade.get("token_id"),
                str(trade.get("direction") or ""),
                float(trade.get("shares") or 0.0),
                float(trade.get("entry_price") or 0.0),
                float(trade.get("exit_price") or 0.0),
                float(trade.get("cost_basis") or 0.0),
                float(trade.get("proceeds") or 0.0),
                float(trade.get("pnl") or 0.0),
                float(trade.get("pnl_pct") or 0.0),
                str(trade.get("result") or ""),
                trade.get("proceeds_source"),
                str(settlement_source),
                trade.get("entry_order_id"),
                trade.get("exit_order_id"),
                trade.get("opened_at"),
                trade.get("closed_at"),
                _json_dumps(trade),
                _utc_now_iso(),
            ),
        )


def _optional_float(value: object) -> Optional[float]:
    if value is None or value == "":
        return None
    return float(value)
