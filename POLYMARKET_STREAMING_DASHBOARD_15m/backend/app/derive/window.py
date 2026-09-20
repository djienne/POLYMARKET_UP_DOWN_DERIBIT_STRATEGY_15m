from __future__ import annotations

import re
import time
from typing import Optional

from ..models import WindowState

_SLUG_RE = re.compile(r"btc-updown-15m-(\d+)")


def parse_slug_start_unix(slug: Optional[str]) -> Optional[int]:
    if not slug:
        return None
    m = _SLUG_RE.search(slug)
    return int(m.group(1)) if m else None


def compute_window(
    slug: Optional[str],
    now_unix: Optional[float] = None,
    no_trade_first_s: float = 300.0,
    no_trade_last_s: float = 180.0,
    total_s: float = 900.0,
) -> WindowState:
    if now_unix is None:
        now_unix = time.time()
    start = parse_slug_start_unix(slug)
    if start is None:
        return WindowState(
            elapsed_s=0.0,
            total_s=total_s,
            no_trade_first_s=no_trade_first_s,
            no_trade_last_s=no_trade_last_s,
            zone="unknown",
            slug=slug,
        )
    elapsed = max(0.0, now_unix - start)
    if elapsed >= total_s:
        zone = "expired"
    elif elapsed < no_trade_first_s:
        zone = "blocked_first"
    elif elapsed >= total_s - no_trade_last_s:
        zone = "blocked_last"
    else:
        zone = "tradeable"
    return WindowState(
        elapsed_s=elapsed,
        total_s=total_s,
        no_trade_first_s=no_trade_first_s,
        no_trade_last_s=no_trade_last_s,
        zone=zone,  # type: ignore[arg-type]
        slug=slug,
    )
