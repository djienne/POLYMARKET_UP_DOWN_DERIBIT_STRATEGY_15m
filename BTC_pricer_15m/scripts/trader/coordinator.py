"""Trading coordinator compatibility entry point.

``SingleTrader`` still owns the public class name used throughout tests and
startup code. The current decomposition routes its tick loop through focused
services; this alias gives future imports a coordinator-shaped name without
forcing a risky migration today.
"""

from __future__ import annotations

from .single_trader import SingleTrader


class TradingCoordinator(SingleTrader):
    """Compatibility coordinator built on the service-backed SingleTrader."""
