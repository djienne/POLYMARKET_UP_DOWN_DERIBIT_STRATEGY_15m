"""Shared HTTP helpers for API clients."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from typing import Any

import requests


class HttpRequestError(RuntimeError):
    """Raised when an HTTP request fails after retry exhaustion."""


def get_json_with_retries(
    get: Callable[..., requests.Response],
    url: str,
    *,
    params: dict[str, Any] | None = None,
    timeout: float = 10.0,
    max_retries: int = 3,
    backoff_base_seconds: float = 1.0,
    logger: logging.Logger | None = None,
) -> Any:
    """GET JSON with bounded retries and exponential backoff.

    ``get`` is usually ``requests.get`` or ``requests.Session().get``. Keeping
    it injectable lets clients share retry behavior without sharing sessions.
    """
    attempts = max(1, int(max_retries))
    last_exc: BaseException | None = None
    for attempt in range(attempts):
        try:
            response = get(url, params=params, timeout=timeout)
            response.raise_for_status()
            return response.json()
        except (requests.RequestException, ValueError) as exc:
            last_exc = exc
            if attempt == attempts - 1:
                break
            delay = backoff_base_seconds * (2 ** attempt)
            if logger:
                logger.debug(
                    "HTTP GET failed for %s (attempt %s/%s): %s; retrying in %.1fs",
                    url,
                    attempt + 1,
                    attempts,
                    exc,
                    delay,
                )
            time.sleep(delay)
    raise HttpRequestError(f"GET {url} failed after {attempts} attempt(s): {last_exc}")
