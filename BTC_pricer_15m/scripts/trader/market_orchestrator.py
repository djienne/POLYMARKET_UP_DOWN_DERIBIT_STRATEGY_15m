"""Market lifecycle persistence facade.

Calc still owns the live price-resolution loop today, but all durable market
lifecycle writes go through this facade.  That gives us one contract for
strike locks, pending settlements, finalized settlements, abandoned markets,
and chain-break flags while keeping the legacy settlement JSON files as
exports during the migration.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from ._io import atomic_write_json
from .persistence import SqliteStore


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True)
class PendingSettlement:
    market_id: str
    window_start: datetime
    window_end: datetime
    strike: float
    strike_offset_s: Optional[float]
    strike_source: str


class MarketOrchestrator:
    """Owns durable market lifecycle read/write operations."""

    def __init__(
        self,
        *,
        settlements_dir: Path,
        sqlite_store: Optional[SqliteStore] = None,
    ) -> None:
        self.settlements_dir = settlements_dir
        self.sqlite_store = sqlite_store
        self.settlements_dir.mkdir(parents=True, exist_ok=True)

    def settlement_path(self, market_id: str) -> Path:
        return self.settlements_dir / f"{market_id}.json"

    def read_market(self, market_id: str) -> Optional[dict]:
        if self.sqlite_store is not None:
            row = self.sqlite_store.get_market(market_id)
            if row is not None:
                return row
        path = self.settlement_path(market_id)
        if not path.exists():
            return None
        return json.loads(path.read_text(encoding="utf-8"))

    def iter_markets(self) -> list[dict]:
        markets_by_id: dict[str, dict] = {}
        if self.sqlite_store is not None:
            for payload in self.sqlite_store.list_markets():
                market_id = payload.get("market_id")
                if market_id:
                    markets_by_id[str(market_id)] = payload
        for path in self.settlements_dir.glob("*.json"):
            if path.stem in markets_by_id:
                continue  # canonical bulk row already loaded; do not open its JSON export
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            market_id = payload.get("market_id")
            if market_id:
                # Preserve JSON-only migration rows, never overwrite SQLite.
                markets_by_id.setdefault(str(market_id), payload)
        return list(markets_by_id.values())

    def attach_pending_contract(self, market_id: str, contract) -> bool:
        """Enrich only a still-pending record; finalized history is immutable here."""
        payload = self.read_market(market_id)
        if payload is None or str(payload.get("settlement_status") or "pending") != "pending":
            return False
        if payload.get("settlement_price") is not None or payload.get("settlement_payouts"):
            return False
        if payload.get("contract_metadata") == contract.to_dict():
            return True
        payload["contract_metadata"] = contract.to_dict()
        self._persist(payload)
        return True

    def write_strike(
        self,
        *,
        market_id: str,
        window_start: datetime,
        window_end: datetime,
        strike: float,
        strike_offset_s: Optional[float],
        strike_source: str,
        contract_metadata: Optional[dict] = None,
    ) -> bool:
        existing = self.read_market(market_id)
        if existing is not None:
            # Upgrade the currently pending market across a deployment; older
            # finalized research records keep their historical semantics.
            if (contract_metadata is not None and existing.get("contract_metadata") is None
                    and existing.get("settlement_status") == "pending"):
                existing["contract_metadata"] = contract_metadata
                self._persist(existing)
            return False
        payload = {
            "market_id": market_id,
            "slug": market_id,
            "window_start_iso": window_start.isoformat(),
            "window_end_iso": window_end.isoformat(),
            "strike": float(strike),
            "strike_offset_s": float(strike_offset_s) if strike_offset_s is not None else None,
            "strike_source": strike_source,
            "strike_captured_at_iso": _utc_now_iso(),
            "settlement_status": "pending",
            "settlement_price": None,
            "settlement_offset_s": None,
            "settlement_source": None,
            "settlement_captured_at_iso": None,
            "contract_metadata": contract_metadata,
        }
        self._persist(payload)
        return True

    def finalize_payout(self, market_id: str, resolution) -> dict:
        """Persist authoritative collateral payouts without inventing a BTC price."""
        payload = dict(self.read_market(market_id) or {"market_id": market_id, "slug": market_id})
        payload.update({
            "condition_id": resolution.condition_id,
            "settlement_status": "finalized",
            "settlement_payouts": dict(resolution.payout_by_token),
            "settlement_source": resolution.source,
            "settlement_captured_at_iso": resolution.observed_at,
            "settlement_price": None,
            "settlement_offset_s": None,
        })
        if getattr(resolution, "details", None):
            payload["settlement_details"] = dict(resolution.details)
        self._persist(payload)
        return payload

    def mark_chain_broken(self, market_id: str) -> None:
        payload = self.read_market(market_id)
        if payload is None:
            return
        payload["strike_source"] = "chain_broken"
        payload["chain_status"] = "chain_broken"
        self._persist(payload)

    def mark_abandoned(self, market_id: str, *, reason: str) -> None:
        payload = self.read_market(market_id) or {"market_id": market_id, "slug": market_id}
        payload["settlement_status"] = "abandoned"
        payload["settlement_error"] = reason
        payload["settlement_abandoned_at_iso"] = _utc_now_iso()
        payload.setdefault("settlement_price", None)
        payload.setdefault("settlement_offset_s", None)
        payload.setdefault("settlement_source", None)
        payload.setdefault("settlement_captured_at_iso", None)
        self._persist(payload)

    def finalize(
        self,
        *,
        market_id: str,
        window_start: datetime,
        window_end: datetime,
        strike: float,
        strike_offset_s: Optional[float],
        settlement_price: float,
        settlement_offset_s: float,
        settlement_source: str,
    ) -> dict:
        existing = self.read_market(market_id) or {}
        strike_source = existing.get("strike_source") or "chainlink"
        payload = {
            "market_id": market_id,
            "slug": existing.get("slug") or market_id,
            "window_start_iso": window_start.isoformat(),
            "window_end_iso": window_end.isoformat(),
            "strike": float(strike),
            "strike_offset_s": float(strike_offset_s) if strike_offset_s is not None else None,
            "strike_source": strike_source,
            "strike_captured_at_iso": existing.get("strike_captured_at_iso"),
            "settlement_status": "finalized",
            "settlement_price": float(settlement_price),
            "settlement_offset_s": float(settlement_offset_s),
            "settlement_source": settlement_source,
            "settlement_captured_at_iso": _utc_now_iso(),
        }
        if strike_source == "chain_broken":
            payload["chain_status"] = "chain_broken"
        self._persist(payload)
        return payload

    def find_market_at(self, *, key: str, iso: str) -> Optional[dict]:
        if self.sqlite_store is not None:
            row = self.sqlite_store.find_market_by_time(key=key, iso=iso)
            if row is not None:
                return row
        for market in self.iter_markets():
            if market.get(key) == iso:
                return market
        return None

    def restore_state(self) -> tuple[
        dict[str, tuple[float, str, Optional[float]]],
        dict[str, PendingSettlement],
        set[str],
    ]:
        barriers: dict[str, tuple[float, str, Optional[float]]] = {}
        pending: dict[str, PendingSettlement] = {}
        chain_broken: set[str] = set()
        for payload in self.iter_markets():
            try:
                market_id = str(payload["market_id"])
                strike = float(payload["strike"])
                strike_offset = payload.get("strike_offset_s")
                strike_offset_f = float(strike_offset) if strike_offset is not None else None
                window_start = datetime.fromisoformat(str(payload["window_start_iso"]))
                window_end = datetime.fromisoformat(str(payload["window_end_iso"]))
            except (KeyError, ValueError, TypeError):
                continue
            strike_source = str(payload.get("strike_source") or "chainlink")
            barriers[market_id] = (strike, strike_source, strike_offset_f)
            if strike_source == "chain_broken":
                chain_broken.add(market_id)
            settlement_price = payload.get("settlement_price")
            settlement_status = str(payload.get("settlement_status") or "pending")
            if settlement_price is None and not payload.get("settlement_payouts") and settlement_status == "pending":
                pending[market_id] = PendingSettlement(
                    market_id=market_id,
                    window_start=window_start,
                    window_end=window_end,
                    strike=strike,
                    strike_offset_s=strike_offset_f,
                    strike_source=strike_source,
                )
        return barriers, pending, chain_broken

    def _persist(self, payload: dict) -> None:
        path = self.settlement_path(str(payload["market_id"]))
        atomic_write_json(path, payload)
        if self.sqlite_store is not None:
            self.sqlite_store.upsert_market(payload)
