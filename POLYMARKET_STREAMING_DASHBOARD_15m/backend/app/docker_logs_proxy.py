"""Narrow Docker socket adapter: only fixed-container model-probability logs.

Run this in its own internal service. The dashboard receives neither the socket
nor an arbitrary Docker API proxy. Caller headers, paths and container names are
never forwarded to Docker. See Docker Engine API /containers/{id}/logs.
"""
from __future__ import annotations

import os
import re
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

import httpx

MAX_LOG_BYTES = 2 * 1024 * 1024
MODEL_LINE = re.compile(r"^(\S+) .*?(Model:\s*UP=[\d.]+%\s*DOWN=[\d.]+%)")


def model_lines(payload: bytes) -> bytes:
    # Non-TTY Docker logs use an 8-byte stream header; TTY logs are plain text.
    if len(payload) >= 8 and payload[0] in (1, 2) and payload[1:4] == b"\0\0\0":
        chunks = []
        offset = 0
        while offset + 8 <= len(payload):
            length = int.from_bytes(payload[offset + 4:offset + 8], "big")
            offset += 8
            if offset + length > len(payload):
                break
            chunks.append(payload[offset:offset + length])
            offset += length
        payload = b"".join(chunks)
    lines = []
    for line in payload.decode("utf-8", errors="replace").splitlines():
        match = MODEL_LINE.search(line)
        if match:
            # Discard all unrelated log fields and lines at this boundary.
            lines.append(f"{match[1]} {match[2]}")
    return ("\n".join(lines) + ("\n" if lines else "")).encode()


def fetch_logs(since_seconds: int) -> bytes:
    container = os.environ.get("DOCKER_CONTAINER", "btc_pricer_15m_grid")
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", container):
        raise ValueError("invalid configured log container")
    transport = httpx.HTTPTransport(uds="/var/run/docker.sock")
    with httpx.Client(transport=transport, timeout=5.0) as client:
        with client.stream(
            "GET", f"http://docker/v1.45/containers/{container}/logs",
            params={"stdout": "1", "stderr": "1", "timestamps": "1",
                    "since": str(int(time.time()) - since_seconds), "tail": "10000"},
        ) as response:
            response.raise_for_status()
            payload = bytearray()
            for chunk in response.iter_bytes():
                if len(payload) + len(chunk) > MAX_LOG_BYTES:
                    raise ValueError("Docker log response exceeds limit")
                payload.extend(chunk)
    return model_lines(bytes(payload))


def handler_type(fetch=fetch_logs):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            target = urlsplit(self.path)
            query = parse_qs(target.query, keep_blank_values=True)
            if target.path == "/health" and not query:
                self.respond(200, b"ok\n")
                return
            if target.path != "/logs":
                self.respond(404, b"not found\n")
                return
            values = query.get("since_seconds", [])
            if set(query) != {"since_seconds"} or len(values) != 1 or not re.fullmatch(r"[0-9]{1,4}", values[0]):
                self.respond(400, b"since_seconds must be an integer from 1 to 1800\n")
                return
            since = int(values[0])
            if not 1 <= since <= 1800:
                self.respond(400, b"since_seconds outside range\n")
                return
            try:
                payload = fetch(since)
            except Exception:
                self.respond(502, b"model logs unavailable\n")
                return
            self.respond(200, payload)

        def respond(self, status, payload):
            self.send_response(status)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, *_args):
            pass

    return Handler


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", 8799), handler_type()).serve_forever()
