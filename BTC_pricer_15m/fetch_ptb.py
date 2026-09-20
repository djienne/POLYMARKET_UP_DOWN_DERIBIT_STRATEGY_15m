#!/usr/bin/env python3
"""Re-export shim for ``scripts/trader/polymarket_ptb.py``.

The PTB-fetch logic was promoted into the trader package
(``scripts.trader.polymarket_ptb``) once the live trader started using
it as a runtime dependency. This top-level module is preserved so the
REPL / script entrypoint:

    from fetch_ptb import PolymarketPTB

keeps working. New code should import directly from
``scripts.trader.polymarket_ptb``.
"""
from scripts.trader.polymarket_ptb import PolymarketPTB  # noqa: F401


if __name__ == "__main__":
    # Keep the original CLI behavior — print active slug, PTB, and
    # current Polymarket BTC price.
    from datetime import timezone

    with PolymarketPTB() as client:
        result = client.current_ptb()
        if result is None:
            print("No active 15-minute BTC market available.")
        else:
            slug, ptb = result
            start_dt, end_dt = PolymarketPTB.window_from_slug(slug)
            current = client.current_price()
            print(f"Active market : {slug}")
            print(f"Window        : {start_dt.isoformat()} -> {end_dt.isoformat()}")
            print(f"PTB           : {ptb}")
            print(f"Current price : {current}")
            if current is not None:
                delta = current - ptb
                pct = delta / ptb * 100
                print(f"Delta vs PTB  : {delta:+.2f} ({pct:+.4f}%)")
