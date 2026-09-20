from http.server import ThreadingHTTPServer
from threading import Thread

import httpx
import pytest

from app import docker_logs_proxy as proxy


def test_logs_adapter_rejects_every_non_allowlisted_request():
    calls = []
    def fetch(since):
        calls.append(since)
        return b"2026-09-05T10:00:00Z Model: UP=60% DOWN=40%\n"
    server = ThreadingHTTPServer(("127.0.0.1", 0), proxy.handler_type(fetch))
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        with httpx.Client(base_url=f"http://127.0.0.1:{server.server_port}") as client:
            for target in ("/containers/json", "/logs?since_seconds=0", "/logs?since_seconds=1801",
                           "/logs?since_seconds=3&container=other", "/logs?since_seconds=3&since_seconds=4",
                           "/logs?since_seconds=..%2F..", "/logs%2F..%2Fcontainers/json"):
                assert client.get(target).status_code in {400, 404}
            assert client.post("/logs?since_seconds=3").status_code == 501
            assert client.get("/health").status_code == 200
            assert calls == []
            assert client.get("/logs?since_seconds=1800").status_code == 200
            assert calls == [1800]
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def test_adapter_fetch_is_fixed_container_get_and_strips_unrelated_fields(monkeypatch):
    request_log = []
    line = b"2026-09-05T10:00:00Z grid Model: UP=60% DOWN=40% token=private\n"
    payload = b"\x01\0\0\0" + len(line).to_bytes(4, "big") + line
    def transport(request):
        request_log.append(request)
        return httpx.Response(200, content=payload)
    monkeypatch.setattr(proxy.httpx, "HTTPTransport", lambda **kwargs: httpx.MockTransport(transport))
    monkeypatch.setenv("DOCKER_CONTAINER", "btc_pricer_15m_grid")
    assert proxy.fetch_logs(3) == b"2026-09-05T10:00:00Z Model: UP=60% DOWN=40%\n"
    assert len(request_log) == 1
    assert request_log[0].method == "GET"
    assert request_log[0].url.path == "/v1.45/containers/btc_pricer_15m_grid/logs"
    assert set(request_log[0].url.params) == {"stdout", "stderr", "timestamps", "since", "tail"}
    monkeypatch.setenv("DOCKER_CONTAINER", "../other")
    with pytest.raises(ValueError):
        proxy.fetch_logs(3)
    assert len(request_log) == 1


def test_adapter_enforces_actual_response_byte_limit(monkeypatch):
    monkeypatch.setattr(proxy, "MAX_LOG_BYTES", 16)
    monkeypatch.setattr(proxy.httpx, "HTTPTransport", lambda **kwargs: httpx.MockTransport(
        lambda request: httpx.Response(200, content=b"x" * 17)
    ))
    with pytest.raises(ValueError, match="exceeds limit"):
        proxy.fetch_logs(1)
