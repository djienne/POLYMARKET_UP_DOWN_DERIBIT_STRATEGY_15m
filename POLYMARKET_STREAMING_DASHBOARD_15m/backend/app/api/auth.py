from __future__ import annotations

from urllib.parse import urlsplit

from fastapi import Header, HTTPException, Query, WebSocket

from ..config import settings

_ALLOWED_ORIGINS = {
    "http://localhost:5175",
    "http://127.0.0.1:5175",
    "http://localhost:8798",
    "http://127.0.0.1:8798",
}


def _origin_allowed(ws: WebSocket) -> bool:
    origin = ws.headers.get("origin", "")
    if not origin:
        return True  # non-browser clients (curl, monitors) send no Origin
    if origin in _ALLOWED_ORIGINS:
        return True
    try:
        netloc = urlsplit(origin).netloc
    except ValueError:
        return False
    # Same-origin: the frontend connects to window.location.host, so a legit
    # browser Origin netloc always equals the WS request's Host header.
    return bool(netloc) and netloc == ws.headers.get("host", "")


def _token_matches(value: str | None) -> bool:
    expected = settings.dashboard_api_token
    if not expected:
        return True
    if not value or not isinstance(value, str):
        return False
    value = value.strip()
    if value.startswith("Bearer "):
        value = value.removeprefix("Bearer ").strip()
    return value == expected


async def require_api_token(
    authorization: str | None = Header(default=None),
    x_dashboard_token: str | None = Header(default=None),
    token: str | None = Query(default=None),
) -> None:
    if _token_matches(authorization) or _token_matches(x_dashboard_token) or _token_matches(token):
        return
    raise HTTPException(status_code=401, detail="dashboard API token required")


async def websocket_authorized(ws: WebSocket) -> bool:
    # Origin gate runs regardless of token config: CORSMiddleware does not
    # apply to WebSockets, so this is the only thing stopping a cross-site
    # browser page from streaming state when no token is set.
    if not _origin_allowed(ws):
        return False
    expected = settings.dashboard_api_token
    if not expected:
        return True
    token = (
        ws.query_params.get("token")
        or ws.headers.get("x-dashboard-token")
        or ws.headers.get("authorization")
    )
    return _token_matches(token)
