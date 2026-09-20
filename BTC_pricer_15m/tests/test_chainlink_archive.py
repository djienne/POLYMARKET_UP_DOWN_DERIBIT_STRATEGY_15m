"""Tests for ChainlinkArchive — on-chain fallback for missed WS emissions."""
from __future__ import annotations

from typing import Optional
from unittest.mock import patch

import pytest

from scripts.trader.chainlink_archive import (
    BTC_USD_AGGREGATOR_POLYGON,
    DECIMALS,
    ChainlinkArchive,
    SELECTOR_GET_ROUND_DATA,
    SELECTOR_LATEST_ROUND_DATA,
    _decode_round_data,
    _encode_get_round_data_call,
)


# ---------------------------------------------------------------------------
# Encoding / decoding helpers
# ---------------------------------------------------------------------------


def _encode_round_payload(
    round_id: int, answer: int, started_at: int, updated_at: int,
    answered_in_round: int,
) -> str:
    """Pack a 5-uint256 ABI return blob the way the aggregator does."""

    def _enc(val: int, signed: bool) -> str:
        if signed and val < 0:
            val = (1 << 256) + val
        return val.to_bytes(32, "big", signed=False).hex()

    return "0x" + (
        _enc(round_id, signed=True)
        + _enc(answer, signed=True)
        + _enc(started_at, signed=False)
        + _enc(updated_at, signed=False)
        + _enc(answered_in_round, signed=True)
    )


def test_decode_round_data_unpacks_five_int256_slots():
    payload = _encode_round_payload(
        round_id=42,
        answer=76_113_810_000_000,  # 76,113.81 with 8 decimals → raw 7611381000000
        started_at=1700000000,
        updated_at=1700000010,
        answered_in_round=42,
    )
    round_id, answer, started, updated, aggr = _decode_round_data(payload)
    assert round_id == 42
    assert answer == 76_113_810_000_000
    assert started == 1700000000
    assert updated == 1700000010
    assert aggr == 42


def test_decode_round_data_handles_negative_int256():
    """Defensive: int256 can be negative. Decoder must treat the high bit
    as the sign bit on signed slots (round_id, answer, answeredInRound)."""
    payload = _encode_round_payload(
        round_id=-1, answer=-100, started_at=10, updated_at=20,
        answered_in_round=-1,
    )
    round_id, answer, started, updated, aggr = _decode_round_data(payload)
    assert round_id == -1
    assert answer == -100
    assert started == 10
    assert updated == 20
    assert aggr == -1


def test_encode_get_round_data_call_packs_selector_and_round_id():
    payload = _encode_get_round_data_call(0xDEAD)
    assert payload.startswith("0x")
    raw = bytes.fromhex(payload[2:])
    assert raw[:4] == SELECTOR_GET_ROUND_DATA
    # round_id occupies the rightmost 8 bytes of the 32-byte slot
    # (uint80 padded to 32 bytes), but our encoder right-aligns the value
    # in 32 bytes, so the last 8 bytes should hold the value.
    assert int.from_bytes(raw[4:36], "big") == 0xDEAD


# ---------------------------------------------------------------------------
# get_round_at_or_after — the public API
# ---------------------------------------------------------------------------


class _FakeRPC:
    """Pretend Polygon RPC: maintains a sorted list of (round_id, ts, price)
    rows and dispatches eth_calls to latestRoundData / getRoundData."""

    def __init__(self, rounds: list[tuple[int, int, float]]):
        # Each row: (round_id, updated_at_unix, price_usd).
        self._rounds: dict[int, tuple[int, int, float]] = {
            r[0]: r for r in rounds
        }
        self._latest_id: int = max(self._rounds.keys()) if rounds else 0
        self.calls: list[str] = []

    def post(self, _url, json=None, timeout=None):  # noqa: ANN001
        calldata = json["params"][0]["data"]
        self.calls.append(calldata)
        if calldata.startswith("0x" + SELECTOR_LATEST_ROUND_DATA.hex()):
            row = self._rounds.get(self._latest_id)
            if row is None:
                return _RpcResponse({"error": {"message": "no rounds"}})
            return _RpcResponse(
                {"result": _encode_round_payload(
                    round_id=row[0],
                    answer=int(row[2] * 10 ** DECIMALS),
                    started_at=row[1],
                    updated_at=row[1],
                    answered_in_round=row[0],
                )}
            )
        if calldata.startswith("0x" + SELECTOR_GET_ROUND_DATA.hex()):
            # round_id encoded as uint256 in the last 32 bytes of calldata.
            raw = bytes.fromhex(calldata[2:])
            round_id = int.from_bytes(raw[4:36], "big")
            row = self._rounds.get(round_id)
            if row is None:
                # Mimic an EVM revert (round before phase boundary).
                return _RpcResponse(
                    {"error": {"message": "execution reverted"}}
                )
            return _RpcResponse(
                {"result": _encode_round_payload(
                    round_id=row[0],
                    answer=int(row[2] * 10 ** DECIMALS),
                    started_at=row[1],
                    updated_at=row[1],
                    answered_in_round=row[0],
                )}
            )
        return _RpcResponse({"error": {"message": "unknown selector"}})


class _RpcResponse:
    def __init__(self, body: dict, status_code: int = 200):
        self._body = body
        self.status_code = status_code

    def json(self):
        return self._body


def test_get_round_at_or_after_finds_first_round_post_target():
    """30 rounds spanning ~15 minutes; target lands between them. Returns
    the first round with updated_at >= target."""
    target_ts = 1_700_000_120
    rounds = [
        (100 + i, 1_700_000_000 + i * 10, 76_000.0 + i)
        for i in range(30)
    ]
    rpc = _FakeRPC(rounds)

    arc = ChainlinkArchive(rpc_urls=["http://fake-rpc"])
    with patch("scripts.trader.chainlink_archive.requests.post",
               side_effect=rpc.post):
        result = arc.get_round_at_or_after(target_ts, max_lookahead_s=600)

    assert result is not None
    price, round_ts, round_id = result
    # Target = 1_700_000_120 → round_id 112 has updated_at = 1_700_000_120.
    assert round_id == 112
    assert round_ts == 1_700_000_120
    assert price == 76_012.0


def test_get_round_at_or_after_returns_none_when_target_in_future():
    """Latest emission timestamp < target_ts → no round at-or-after exists yet."""
    target_ts = 1_700_001_000
    rounds = [(100, 1_700_000_000, 76_000.0)]
    rpc = _FakeRPC(rounds)

    arc = ChainlinkArchive(rpc_urls=["http://fake-rpc"])
    with patch("scripts.trader.chainlink_archive.requests.post",
               side_effect=rpc.post):
        result = arc.get_round_at_or_after(target_ts, max_lookahead_s=600)

    assert result is None


def test_none_lookup_is_not_cached_permanently():
    target_ts = 1_700_001_000
    rpc = _FakeRPC([(100, 1_700_000_000, 76_000.0)])
    arc = ChainlinkArchive(rpc_urls=["http://fake-rpc"])

    with patch("scripts.trader.chainlink_archive.requests.post",
               side_effect=rpc.post):
        first = arc.get_round_at_or_after(target_ts, max_lookahead_s=600)
        rpc._rounds[101] = (101, target_ts, 76_101.0)
        rpc._latest_id = 101
        second = arc.get_round_at_or_after(target_ts, max_lookahead_s=600)

    assert first is None
    assert second is not None
    assert second[2] == 101


def test_get_round_at_or_after_returns_none_past_max_lookahead():
    """Closest round at-or-after target is more than max_lookahead_s away."""
    target_ts = 1_700_000_000
    # Big gap: only a single round 700 s after target.
    rounds = [(100, 1_700_000_700, 76_000.0)]
    rpc = _FakeRPC(rounds)

    arc = ChainlinkArchive(rpc_urls=["http://fake-rpc"])
    with patch("scripts.trader.chainlink_archive.requests.post",
               side_effect=rpc.post):
        result = arc.get_round_at_or_after(target_ts, max_lookahead_s=600)

    assert result is None


def test_rpc_failover_on_first_endpoint_error():
    """First URL returns a connection error; second succeeds. Result is
    returned from the second."""
    rounds = [(100 + i, 1_700_000_000 + i * 10, 76_000.0 + i) for i in range(5)]
    rpc = _FakeRPC(rounds)

    call_log: list[str] = []
    fail_first = {"count": 0}

    def _post(url, json=None, timeout=None):  # noqa: ANN001
        call_log.append(url)
        if url == "http://primary":
            fail_first["count"] += 1
            import requests
            raise requests.ConnectionError("primary down")
        return rpc.post(url, json=json, timeout=timeout)

    arc = ChainlinkArchive(rpc_urls=["http://primary", "http://fallback"])
    with patch("scripts.trader.chainlink_archive.requests.post",
               side_effect=_post):
        result = arc.get_round_at_or_after(1_700_000_010, max_lookahead_s=60)

    assert result is not None
    # Each eth_call attempt hit primary first, then failed over to fallback.
    assert "http://primary" in call_log
    assert "http://fallback" in call_log
    assert fail_first["count"] >= 1


def test_caches_repeated_lookups():
    """Repeated calls with the same (target_ts, max_lookahead_s) hit the
    cache and don't repeat the RPC sequence."""
    rounds = [(100 + i, 1_700_000_000 + i * 10, 76_000.0 + i) for i in range(10)]
    rpc = _FakeRPC(rounds)

    arc = ChainlinkArchive(rpc_urls=["http://fake-rpc"])
    with patch("scripts.trader.chainlink_archive.requests.post",
               side_effect=rpc.post):
        first = arc.get_round_at_or_after(1_700_000_050, max_lookahead_s=600)
        first_call_count = len(rpc.calls)
        second = arc.get_round_at_or_after(1_700_000_050, max_lookahead_s=600)
        second_call_count = len(rpc.calls)

    assert first == second
    assert second_call_count == first_call_count  # no new RPC traffic


def test_rejects_empty_rpc_url_list():
    with pytest.raises(ValueError):
        ChainlinkArchive(rpc_urls=[])


def test_address_constant_is_valid_polygon_format():
    """Sanity: the BTC/USD aggregator address is a 40-hex-char EVM address."""
    assert BTC_USD_AGGREGATOR_POLYGON.startswith("0x")
    assert len(BTC_USD_AGGREGATOR_POLYGON) == 42
    # All chars after 0x should be valid hex (case-insensitive).
    int(BTC_USD_AGGREGATOR_POLYGON[2:], 16)
