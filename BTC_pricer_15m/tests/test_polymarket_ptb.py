"""Lock the contract on ``scripts.trader.polymarket_ptb.PolymarketPTB`` —
the user's REPL-friendly module promoted into the trader package as the
new primary barrier source.

These tests stub external calls so they don't hit the network. They cover
slug round-tripping, the openPrice JSON decode path, the HTTP-error to None
contract, and the guard that keeps ``current_price`` off Polymarket openPrice.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest
import requests

from scripts.trader.polymarket_ptb import PolymarketPTB


def _make_response(status: int = 200, json_payload=None, raise_exc=None):
    resp = MagicMock()
    if raise_exc is not None:
        resp.raise_for_status.side_effect = raise_exc
    else:
        resp.raise_for_status.return_value = None
    resp.status_code = status
    resp.json.return_value = json_payload or {}
    return resp


def test_window_from_slug_round_trip():
    slug = "btc-updown-15m-1777617600"
    start, end = PolymarketPTB.window_from_slug(slug)
    assert start == datetime(2026, 5, 1, 6, 40, tzinfo=timezone.utc)
    assert end == datetime(2026, 5, 1, 6, 55, tzinfo=timezone.utc)
    assert PolymarketPTB.slug_from_start(start) == slug


def test_window_from_slug_rejects_unexpected_prefix():
    with pytest.raises(ValueError):
        PolymarketPTB.window_from_slug("btc-updown-1h-1777617600")


def test_ptb_for_window_decodes_open_price():
    session = MagicMock()
    session.headers = {}
    session.get.return_value = _make_response(json_payload={"openPrice": 77027.29})

    client = PolymarketPTB(session=session)
    start = datetime(2026, 5, 1, 6, 40, tzinfo=timezone.utc)
    end = start + timedelta(minutes=15)
    out = client.ptb_for_window(start, end)

    assert out == pytest.approx(77027.29)
    # Verify the params we sent.
    call = session.get.call_args
    assert call.kwargs["params"]["symbol"] == "BTC"
    assert call.kwargs["params"]["variant"] == "fiveminute"
    assert call.kwargs["params"]["eventStartTime"] == "2026-05-01T06:40:00Z"
    assert call.kwargs["params"]["endDate"] == "2026-05-01T06:55:00Z"


def test_ptb_for_window_returns_none_on_http_error():
    session = MagicMock()
    session.headers = {}
    session.get.side_effect = requests.RequestException("boom")
    client = PolymarketPTB(session=session)
    start = datetime(2026, 5, 1, 6, 40, tzinfo=timezone.utc)
    out = client.ptb_for_window(start, start + timedelta(minutes=15))
    assert out is None


def test_ptb_for_window_returns_none_on_missing_field():
    session = MagicMock()
    session.headers = {}
    session.get.return_value = _make_response(json_payload={"unrelated": 1})
    client = PolymarketPTB(session=session)
    start = datetime(2026, 5, 1, 6, 40, tzinfo=timezone.utc)
    out = client.ptb_for_window(start, start + timedelta(minutes=15))
    assert out is None


def test_ptb_for_window_does_not_gate_on_incomplete_flag():
    """The ``incomplete``/``completed`` flags track whether the
    *window itself* has closed, NOT whether ``openPrice`` is finalized.
    A live API probe on 2026-05-01 12:35 confirmed the active window
    reports ``incomplete=True`` even when its openPrice has been stable
    for minutes (matched the previous window's closePrice exactly).

    So ``ptb_for_window`` must NOT reject on ``incomplete=True``; the
    only correct freshness discriminator is the caller's time gate
    (see ``calc_daemon.POLYMARKET_FRESHNESS_MIN_S``).
    """
    session = MagicMock()
    session.headers = {}
    session.get.return_value = _make_response(json_payload={
        "openPrice": 77744.57,
        "incomplete": True,
        "completed": False,
        "cached": True,
    })
    client = PolymarketPTB(session=session)
    start = datetime(2026, 5, 1, 12, 25, tzinfo=timezone.utc)
    out = client.ptb_for_window(start, start + timedelta(minutes=15))
    assert out == pytest.approx(77744.57)


def test_ptb_for_window_accepts_completed_response():
    """``completed=True`` / ``incomplete=False`` payloads also pass
    through unchanged."""
    session = MagicMock()
    session.headers = {}
    session.get.return_value = _make_response(json_payload={
        "openPrice": 77744.57,
        "incomplete": False,
        "completed": True,
        "cached": False,
    })
    client = PolymarketPTB(session=session)
    start = datetime(2026, 5, 1, 12, 25, tzinfo=timezone.utc)
    out = client.ptb_for_window(start, start + timedelta(minutes=15))
    assert out == pytest.approx(77744.57)


def test_current_price_stays_independent_from_rtds_provider(monkeypatch):
    """``current_price`` must not query Polymarket openPrice or RTDS provider."""
    import scripts.polymarket_btc_15m as btc15m

    session = MagicMock()
    session.headers = {}
    session.get.side_effect = AssertionError("Polymarket openPrice is not spot")
    monkeypatch.setattr(btc15m, "_get_binance_rest_current_btc_price", lambda: 77000.0)
    monkeypatch.setattr(
        btc15m,
        "get_current_btc_price",
        lambda: (_ for _ in ()).throw(AssertionError("must not use RTDS spot provider")),
    )

    client = PolymarketPTB(session=session)
    out = client.current_price(lag_seconds=2.0)
    assert out == pytest.approx(77000.0)

    session.get.assert_not_called()


def test_ptb_for_slug_delegates_to_ptb_for_window():
    session = MagicMock()
    session.headers = {}
    session.get.return_value = _make_response(json_payload={"openPrice": 77000.0})
    client = PolymarketPTB(session=session)
    out = client.ptb_for_slug("btc-updown-15m-1777617600")
    assert out == pytest.approx(77000.0)
    sent = session.get.call_args.kwargs["params"]
    assert sent["eventStartTime"] == "2026-05-01T06:40:00Z"
    assert sent["endDate"] == "2026-05-01T06:55:00Z"
