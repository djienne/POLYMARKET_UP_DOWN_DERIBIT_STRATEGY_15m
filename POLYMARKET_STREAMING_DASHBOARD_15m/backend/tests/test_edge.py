from app.derive.edge import required_model_prob, has_edge, compute_edge


def test_required_floor_applies():
    # Low market prob should be capped at the floor
    assert required_model_prob(0.05, alpha=1.0, floor=0.55) == 0.55


def test_required_uses_alpha_above_floor():
    # market=0.80, alpha=2 → 1 - 0.2^2 = 0.96
    r = required_model_prob(0.80, alpha=2.0, floor=0.5)
    assert abs(r - 0.96) < 1e-9


def test_has_edge_true_and_false():
    assert has_edge(model_prob=0.97, market_prob=0.80, alpha=2.0, floor=0.5) is True
    assert has_edge(model_prob=0.94, market_prob=0.80, alpha=2.0, floor=0.5) is False


def test_has_edge_market_below_floor_skips():
    # Trader gate 1: refuse the side when market_prob < floor, even if the
    # model would otherwise show edge. Mirrors btc_pricer/edge.py:has_edge.
    assert has_edge(model_prob=0.95, market_prob=0.30, alpha=2.0, floor=0.45) is False


def test_has_edge_model_below_market_skips():
    # Trader gate 2: refuse the side when the model isn't even above the
    # market — no informational advantage.
    assert has_edge(model_prob=0.40, market_prob=0.50, alpha=1.5, floor=0.30) is False


def test_compute_edge_fields():
    # market=0.6, alpha=1.0 → required = max(0.5, 0.6) = 0.6; model=0.85
    # passes all three trader gates.
    e = compute_edge("UP", model_prob=0.85, market_prob=0.6, alpha=1.0, floor=0.5)
    assert e.side == "UP"
    assert e.market_prob == 0.6
    assert e.model_prob == 0.85
    assert e.required_prob is not None
    assert e.current_ratio is not None
    assert e.required_ratio is not None
    assert e.margin is not None
    # has_edge mirrors the trader: passes all three gates.
    assert e.has_edge is True


def test_compute_edge_has_edge_false_when_market_below_floor():
    # The dashboard must agree with the trader: market 0.30 < floor 0.45 →
    # has_edge=False even though the ratio math (0.95/0.30 = 3.17) looks
    # "above required".
    e = compute_edge("DOWN", model_prob=0.95, market_prob=0.30, alpha=2.0, floor=0.45)
    assert e.has_edge is False
    # The ratios are still populated for the UI's chart, even though the
    # gate refuses the side.
    assert e.current_ratio is not None
    assert e.required_ratio is not None


def test_compute_edge_missing_market():
    e = compute_edge("DOWN", model_prob=0.7, market_prob=None, alpha=1.5, floor=0.55)
    assert e.required_prob is None
    assert e.has_edge is None
    assert e.current_ratio is None
