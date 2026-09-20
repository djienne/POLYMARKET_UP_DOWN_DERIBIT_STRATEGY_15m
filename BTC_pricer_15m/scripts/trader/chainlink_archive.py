"""On-chain Chainlink BTC/USD round archive (Polygon mainnet).

Used by `calc_daemon` as a fallback when the WebSocket buffer doesn't
have the chainlink emission needed (typically after downtime longer
than the 180 s buffer window). Queries the same on-chain aggregator
that Polymarket uses for resolution, so the values are guaranteed to
match Polymarket's settlement.

Public surface: ``ChainlinkArchive.get_round_at_or_after(target_ts_unix,
max_lookahead_s)`` returns ``(price_usd, round_ts_unix, round_id)`` for
the first round with ``updatedAt >= target_ts_unix``, or ``None`` when
no such round exists yet (or the gap exceeds the lookahead cap).

Implementation: hand-rolled JSON-RPC ``eth_call`` (mirrors the pattern
in ``executor_live.py:1463-1512``) + manual ABI decoding of the
aggregator return data. Avoids pulling in the full ``web3`` runtime
just for two read-only contract calls.
"""
from __future__ import annotations

import logging
import time
from typing import Optional

import requests

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Chainlink BTC/USD price feed proxy on Polygon mainnet.
# Verified via direct eth_call to publicnode.com on 2026-04-30:
# latestRoundData() decodes to a sane BTC/USD price with 8 decimals.
BTC_USD_AGGREGATOR_POLYGON: str = "0xDE31F8bFBD8c84b5360CFACCa3539B938dd78ae6"

# Function selectors (first 4 bytes of keccak256(signature)):
# - latestRoundData()  -> 0xfeaf968c
# - getRoundData(uint80) -> 0x9a6fc8f5
SELECTOR_LATEST_ROUND_DATA: bytes = bytes.fromhex("feaf968c")
SELECTOR_GET_ROUND_DATA: bytes = bytes.fromhex("9a6fc8f5")

# Chainlink BTC/USD on Polygon has 8 decimals.
DECIMALS: int = 8


def _decode_round_data(hex_payload: str) -> tuple[int, int, int, int, int]:
    """Decode the 5 × 32-byte ABI return tuple of {latest,get}RoundData.

    Returns ``(round_id, answer, started_at, updated_at, answered_in_round)``
    with ``answer`` and ``round_id`` interpreted as signed.
    """
    if hex_payload.startswith("0x"):
        hex_payload = hex_payload[2:]
    data = bytes.fromhex(hex_payload)
    if len(data) < 160:
        raise ValueError(f"unexpected return-data length {len(data)} (< 160)")

    def _slot(i: int, signed: bool) -> int:
        raw = int.from_bytes(data[i * 32:(i + 1) * 32], "big", signed=False)
        if signed and raw >= (1 << 255):
            raw -= 1 << 256
        return raw

    round_id = _slot(0, signed=True)
    answer = _slot(1, signed=True)
    started_at = _slot(2, signed=False)
    updated_at = _slot(3, signed=False)
    answered_in_round = _slot(4, signed=True)
    return (round_id, answer, started_at, updated_at, answered_in_round)


def _encode_get_round_data_call(round_id: int) -> str:
    """Pack getRoundData(uint80) calldata."""
    if round_id < 0:
        raise ValueError(f"round_id must be non-negative, got {round_id}")
    return "0x" + (
        SELECTOR_GET_ROUND_DATA + round_id.to_bytes(32, "big")
    ).hex()


# ---------------------------------------------------------------------------
# ChainlinkArchive
# ---------------------------------------------------------------------------


class ChainlinkArchive:
    """Polygon RPC fallback for chainlink BTC/USD historical rounds."""

    def __init__(
        self,
        rpc_urls: list[str],
        *,
        contract_address: str = BTC_USD_AGGREGATOR_POLYGON,
        timeout_s: float = 8.0,
        max_search_steps: int = 32,
    ) -> None:
        if not rpc_urls:
            raise ValueError("rpc_urls must not be empty")
        self._rpc_urls = list(rpc_urls)
        self._contract = contract_address
        self._timeout_s = timeout_s
        self._max_search_steps = max_search_steps
        # Cache: (target_ts_unix, max_lookahead_s) -> result.
        self._cache: dict[tuple[int, int], Optional[tuple[float, int, int]]] = {}
        self._cache_max_entries: int = 64

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_round_at_or_after(
        self,
        target_ts_unix: int,
        max_lookahead_s: int = 600,
    ) -> Optional[tuple[float, int, int]]:
        """Return ``(price_usd, round_ts_unix, round_id)`` for the first
        round with ``updatedAt >= target_ts_unix``.

        ``None`` when:
          * no round has been emitted with ``updatedAt >= target_ts``
            yet (latest is still before the target), OR
          * the closest such round is more than ``max_lookahead_s``
            seconds past the target (gap too long), OR
          * RPC failure across all configured endpoints.
        """
        cache_key = (int(target_ts_unix), int(max_lookahead_s))
        if cache_key in self._cache:
            return self._cache[cache_key]
        try:
            result = self._search(int(target_ts_unix), int(max_lookahead_s))
        except Exception as exc:  # noqa: BLE001
            log.warning("chainlink archive search failed: %s", exc)
            return None
        if result is not None:
            self._cache[cache_key] = result
            # Bound cache size.
            if len(self._cache) > self._cache_max_entries:
                # Drop the oldest insertion (Python 3.7+ dict preserves order).
                oldest = next(iter(self._cache))
                self._cache.pop(oldest, None)
        return result

    # ------------------------------------------------------------------
    # Search
    # ------------------------------------------------------------------

    def _search(
        self, target_ts_unix: int, max_lookahead_s: int,
    ) -> Optional[tuple[float, int, int]]:
        latest = self._fetch_latest()
        if latest is None:
            return None
        latest_id, latest_answer, _started, latest_updated, _aggr = latest
        if latest_updated < target_ts_unix:
            # Round at-or-after target hasn't been emitted yet.
            return None

        # Find a left bound: a round with updated_at < target_ts. Walk back
        # in geometric steps (1, 2, 4, 8, ...) until either we find one or
        # we've made max_search_steps calls.
        high_id = latest_id
        high_updated = latest_updated
        low_id: Optional[int] = None
        step = 1
        for _ in range(self._max_search_steps):
            candidate_id = high_id - step
            if candidate_id < 0:
                candidate_id = 0
            data = self._fetch_round(candidate_id)
            if data is None:
                # Either RPC error or call reverted (round before phase
                # start). Treat as "no earlier round available" and stop.
                low_id = candidate_id
                break
            _, _, _, candidate_updated, _ = data
            if candidate_updated < target_ts_unix:
                low_id = candidate_id
                break
            high_id = candidate_id
            high_updated = candidate_updated
            if candidate_id == 0:
                # Walked back to genesis — nothing earlier than target.
                low_id = -1
                break
            step *= 2
        if low_id is None:
            # Couldn't find a left bound within the search budget; the
            # target may be very far in the past. Give up — operator
            # should pick a smaller max_lookahead_s or use a faster RPC.
            log.warning(
                "chainlink archive: exceeded %d search steps without finding "
                "a left bound (target_ts=%d); aborting",
                self._max_search_steps, target_ts_unix,
            )
            return None

        # Binary search in (low_id, high_id]: the answer is the smallest
        # round_id in that range with updated_at >= target_ts. high_id
        # already satisfies; low_id does not (or doesn't exist).
        while low_id + 1 < high_id:
            mid = (low_id + high_id) // 2
            data = self._fetch_round(mid)
            if data is None:
                # Reverted / rpc error mid-search; treat the mid as
                # "below target" so the next iteration narrows toward
                # the higher end.
                low_id = mid
                continue
            _, _, _, mid_updated, _ = data
            if mid_updated >= target_ts_unix:
                high_id = mid
                high_updated = mid_updated
            else:
                low_id = mid

        # Re-fetch high to get the answer (we may not have its full row).
        final = self._fetch_round(high_id)
        if final is None:
            return None
        round_id, answer, _started, updated_at, _aggr = final
        offset_s = updated_at - target_ts_unix
        if offset_s > max_lookahead_s:
            return None

        price_usd = float(answer) / float(10 ** DECIMALS)
        return (price_usd, int(updated_at), int(round_id))

    # ------------------------------------------------------------------
    # RPC
    # ------------------------------------------------------------------

    def _fetch_latest(self) -> Optional[tuple[int, int, int, int, int]]:
        result = self._eth_call("0x" + SELECTOR_LATEST_ROUND_DATA.hex())
        if result is None:
            return None
        return _decode_round_data(result)

    def _fetch_round(self, round_id: int) -> Optional[tuple[int, int, int, int, int]]:
        result = self._eth_call(_encode_get_round_data_call(round_id))
        if result is None:
            return None
        try:
            return _decode_round_data(result)
        except ValueError:
            return None

    def _eth_call(self, calldata_hex: str) -> Optional[str]:
        """Send eth_call to each configured RPC URL until one returns a
        result. Returns the hex string from `result`, or None on total
        failure / revert."""
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_call",
            "params": [
                {"to": self._contract, "data": calldata_hex},
                "latest",
            ],
        }
        last_error: Optional[str] = None
        for url in self._rpc_urls:
            try:
                resp = requests.post(url, json=payload, timeout=self._timeout_s)
            except requests.RequestException as exc:
                last_error = f"{url}: {exc}"
                continue
            if resp.status_code != 200:
                last_error = f"{url}: HTTP {resp.status_code}"
                continue
            try:
                body = resp.json()
            except ValueError as exc:
                last_error = f"{url}: invalid JSON: {exc}"
                continue
            if "error" in body and body["error"]:
                # Likely a revert (e.g. round before phase boundary).
                # Don't try other URLs — this is a deterministic call result.
                log.debug("chainlink eth_call returned error from %s: %s",
                          url, body.get("error"))
                return None
            result = body.get("result")
            if isinstance(result, str) and result.startswith("0x"):
                return result
            last_error = f"{url}: missing or malformed result"
        if last_error is not None:
            log.warning("chainlink eth_call failed across all RPCs: %s",
                        last_error)
        return None
