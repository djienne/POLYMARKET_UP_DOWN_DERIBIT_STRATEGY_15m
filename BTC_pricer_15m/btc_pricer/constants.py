"""Centralized constants for BTC Pricer.

This module contains all magic numbers and hardcoded values that were
previously scattered across the codebase. Using named constants improves
readability and maintainability.
"""

from datetime import datetime

# =============================================================================
# Numerical Thresholds
# =============================================================================

# Tolerance for boundary check comparisons (e.g., parameter at bound detection)
BOUNDARY_CHECK_TOLERANCE: float = 0.001

# =============================================================================
# Calibration Settings
# =============================================================================

# Minimum points required for model fitting (relaxed for calibration)
RELAXED_MIN_POINTS: int = 5

# =============================================================================
# Trading Constants
# =============================================================================

MIN_FILL_RATIO: float = 0.99
MAX_CONTRACT_PRICE: float = 0.99
MIN_ORDER_USD: float = 2.0

# =============================================================================
# Time/Date Constants
# =============================================================================

DAYS_PER_YEAR: float = 365.25
SECONDS_PER_YEAR: float = DAYS_PER_YEAR * 24 * 3600

# Far future date used as fallback for unparseable expiries (sorts last)
FAR_FUTURE_DATE: datetime = datetime(2099, 12, 31)

# Options expiry time (Deribit expires at 08:00 UTC)
OPTION_EXPIRY_HOUR_UTC: int = 8
