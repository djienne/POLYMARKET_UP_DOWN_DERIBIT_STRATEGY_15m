from types import SimpleNamespace

from app.derive.equity import equity_timeseries


def test_dry_run_equity_timeseries_prefers_pnl_over_stale_row_capital():
    events = [
        SimpleNamespace(
            event="TP_FILLED",
            timestamp="2026-04-29T10:00:00+00:00",
            pnl=10.0,
            capital=1010.0,
        ),
        SimpleNamespace(
            event="TP_FILLED",
            timestamp="2026-04-29T11:00:00+00:00",
            pnl=5.0,
            capital=1005.0,
        ),
    ]

    series = equity_timeseries(
        events,
        starting_capital=1000.0,
        prefer_event_capital=False,
    )

    assert [p["v"] for p in series] == [1000.0, 1010.0, 1015.0]


def test_live_equity_timeseries_keeps_event_capital_source_of_truth():
    events = [
        SimpleNamespace(
            event="TP_FILLED",
            timestamp="2026-04-29T10:00:00+00:00",
            pnl=10.0,
            capital=1010.0,
        ),
        SimpleNamespace(
            event="TP_FILLED",
            timestamp="2026-04-29T11:00:00+00:00",
            pnl=5.0,
            capital=1005.0,
        ),
    ]

    series = equity_timeseries(events, starting_capital=1000.0)

    assert [p["v"] for p in series] == [1000.0, 1010.0, 1005.0]
