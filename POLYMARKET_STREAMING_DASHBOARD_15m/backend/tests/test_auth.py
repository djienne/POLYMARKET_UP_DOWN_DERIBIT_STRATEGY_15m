from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from app.api.auth import require_api_token, websocket_authorized
from app.config import settings


@pytest.mark.asyncio
async def test_auth_allows_local_mode_without_token(monkeypatch) -> None:
    monkeypatch.setattr(settings, "dashboard_api_token", None)

    await require_api_token()


@pytest.mark.asyncio
async def test_auth_rejects_missing_token_when_configured(monkeypatch) -> None:
    monkeypatch.setattr(settings, "dashboard_api_token", "secret")

    with pytest.raises(HTTPException):
        await require_api_token()


@pytest.mark.asyncio
async def test_auth_accepts_bearer_token_when_configured(monkeypatch) -> None:
    monkeypatch.setattr(settings, "dashboard_api_token", "secret")

    await require_api_token(authorization="Bearer secret")


@pytest.mark.asyncio
async def test_websocket_auth_rejects_wrong_origin(monkeypatch) -> None:
    monkeypatch.setattr(settings, "dashboard_api_token", "secret")
    ws = SimpleNamespace(
        headers={"origin": "https://example.com"},
        query_params={"token": "secret"},
    )

    assert await websocket_authorized(ws) is False


@pytest.mark.asyncio
async def test_websocket_auth_accepts_local_token(monkeypatch) -> None:
    monkeypatch.setattr(settings, "dashboard_api_token", "secret")
    ws = SimpleNamespace(
        headers={"origin": "http://localhost:8798"},
        query_params={"token": "secret"},
    )

    assert await websocket_authorized(ws) is True


@pytest.mark.asyncio
async def test_websocket_auth_rejects_cross_origin_without_token(monkeypatch) -> None:
    monkeypatch.setattr(settings, "dashboard_api_token", None)
    ws = SimpleNamespace(
        headers={"origin": "http://evil.example", "host": "127.0.0.1:8798"},
        query_params={},
    )

    assert await websocket_authorized(ws) is False


@pytest.mark.asyncio
async def test_websocket_auth_allows_empty_origin_without_token(monkeypatch) -> None:
    monkeypatch.setattr(settings, "dashboard_api_token", None)
    ws = SimpleNamespace(headers={}, query_params={})

    assert await websocket_authorized(ws) is True


@pytest.mark.asyncio
async def test_websocket_auth_allows_same_origin_lan_host(monkeypatch) -> None:
    monkeypatch.setattr(settings, "dashboard_api_token", None)
    ws = SimpleNamespace(
        headers={
            "origin": "http://192.168.1.50:5175",
            "host": "192.168.1.50:5175",
        },
        query_params={},
    )

    assert await websocket_authorized(ws) is True


@pytest.mark.asyncio
async def test_websocket_auth_same_origin_still_requires_token(monkeypatch) -> None:
    monkeypatch.setattr(settings, "dashboard_api_token", "secret")
    ws = SimpleNamespace(
        headers={
            "origin": "http://192.168.1.50:5175",
            "host": "192.168.1.50:5175",
        },
        query_params={"token": "wrong"},
    )

    assert await websocket_authorized(ws) is False
