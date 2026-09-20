from app.derive.window import compute_window, parse_slug_start_unix


def test_parse_slug():
    assert parse_slug_start_unix("btc-updown-15m-1776696300") == 1776696300
    assert parse_slug_start_unix("nope") is None
    assert parse_slug_start_unix(None) is None


def test_window_zones():
    start = 1_000_000_000
    assert compute_window(f"btc-updown-15m-{start}", now_unix=start + 10).zone == "blocked_first"
    assert compute_window(f"btc-updown-15m-{start}", now_unix=start + 400).zone == "tradeable"
    assert compute_window(f"btc-updown-15m-{start}", now_unix=start + 750).zone == "blocked_last"
    assert compute_window(f"btc-updown-15m-{start}", now_unix=start + 900).zone == "expired"
