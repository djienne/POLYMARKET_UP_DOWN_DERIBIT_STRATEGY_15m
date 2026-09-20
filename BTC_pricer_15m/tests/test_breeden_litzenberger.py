import numpy as np
import pytest

from btc_pricer.models.breeden_litzenberger import BreedenLitzenberger, RNDResult


def _simple_rnd() -> RNDResult:
    strikes = np.array([0.0, 1.0, 2.0])
    density = np.array([0.5, 0.5, 0.5])
    return RNDResult(
        strikes=strikes,
        density=density,
        forward=1.0,
        ttm=1.0,
        mean=1.0,
        mode=1.0,
        variance=1.0,
        std_dev=1.0,
        skewness=0.0,
        kurtosis=0.0,
        percentile_5=0.1,
        percentile_25=0.5,
        percentile_50=1.0,
        percentile_75=1.5,
        percentile_95=1.9,
        integral=1.0,
        is_valid=True,
        warnings=[],
    )


def test_probability_above_below_are_complementary_between_grid_points():
    bl = BreedenLitzenberger()
    rnd = _simple_rnd()

    below = bl.probability_below(rnd, 0.5)
    above = bl.probability_above(rnd, 0.5)

    assert below == pytest.approx(0.25)
    assert above == pytest.approx(0.75)
    assert above + below == pytest.approx(1.0)
