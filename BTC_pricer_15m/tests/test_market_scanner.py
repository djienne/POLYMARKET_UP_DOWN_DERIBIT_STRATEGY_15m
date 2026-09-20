"""Tests for 15-minute market scanner."""

from datetime import datetime, timedelta, timezone

import scripts.polymarket_btc_15m as scanner
from scripts.polymarket_btc_15m import (
    parse_window_times_from_title,
    slug_for_timestamp,
    _quarter_hour_floor,
    _quarter_hour_ceil,
)


class TestTitleParsing:
    """Extract window start/end times from market titles."""

    def test_standard_title(self):
        title = "Bitcoin Up or Down - April 12, 10:30AM-10:45AM ET"
        start, end = parse_window_times_from_title(title)
        assert start is not None
        assert end is not None
        # 15-minute window
        diff = (end - start).total_seconds()
        assert diff == 900

    def test_pm_title(self):
        title = "Bitcoin Up or Down - April 12, 2:00PM-2:15PM ET"
        start, end = parse_window_times_from_title(title)
        assert start is not None
        assert end is not None
        diff = (end - start).total_seconds()
        assert diff == 900

    def test_no_match(self):
        title = "Something else entirely"
        start, end = parse_window_times_from_title(title)
        assert start is None
        assert end is None

    def test_title_fallback_uses_reference_year_near_new_year(self):
        reference_et = datetime(2026, 1, 1, 0, 0, tzinfo=scanner.ET)
        title = "Bitcoin Up or Down - December 31, 11:45PM-12:00AM ET"

        start, end = parse_window_times_from_title(title, reference_et=reference_et)

        assert start.astimezone(scanner.ET).year == 2025
        assert end.astimezone(scanner.ET).year == 2026
        assert (end - start).total_seconds() == 900

    def test_title_fallback_rolls_midnight_forward(self):
        reference_et = datetime(2026, 4, 13, 0, 0, tzinfo=scanner.ET)
        title = "Bitcoin Up or Down - April 12, 11:45PM-12:00AM ET"

        start, end = parse_window_times_from_title(title, reference_et=reference_et)

        assert (end - start).total_seconds() == 900
        assert end > start


class TestSlugGeneration:
    """Slug should match btc-updown-15m-{unix_ts} format."""

    def test_slug_format(self):
        dt = datetime(2026, 4, 12, 14, 30, 0, tzinfo=timezone.utc)
        slug = slug_for_timestamp(dt)
        assert slug.startswith("btc-updown-15m-")
        ts = int(slug.split("-")[-1])
        assert ts == int(dt.timestamp())


class TestFifteenMinute:
    """15-minute rounding utilities."""

    def test_floor_on_boundary(self):
        dt = datetime(2026, 4, 12, 14, 30, 0, tzinfo=timezone.utc)
        assert _quarter_hour_floor(dt) == dt

    def test_floor_between(self):
        dt = datetime(2026, 4, 12, 14, 37, 0, tzinfo=timezone.utc)
        result = _quarter_hour_floor(dt)
        assert result.minute == 30

    def test_ceil_on_boundary(self):
        dt = datetime(2026, 4, 12, 14, 30, 0, tzinfo=timezone.utc)
        assert _quarter_hour_ceil(dt) == dt

    def test_ceil_between(self):
        dt = datetime(2026, 4, 12, 14, 31, 0, tzinfo=timezone.utc)
        result = _quarter_hour_ceil(dt)
        assert result.minute == 45


class _FixedDateTime(datetime):
    _now = datetime(2026, 4, 12, 14, 32, 0, tzinfo=timezone.utc)

    @classmethod
    def now(cls, tz=None):
        if tz is None:
            return cls._now.replace(tzinfo=None)
        return cls._now.astimezone(tz)


def test_find_active_market_does_not_promote_future_window(monkeypatch):
    now = _FixedDateTime._now
    current_start = _quarter_hour_floor(now)
    next_start = current_start + timedelta(minutes=15)

    next_event = {
        "slug": slug_for_timestamp(next_start),
        "title": "Bitcoin Up or Down - April 12, 10:45AM-11:00AM ET",
        "startTime": next_start.isoformat().replace("+00:00", "Z"),
        "endDate": (next_start + timedelta(minutes=15)).isoformat().replace("+00:00", "Z"),
        "closed": False,
    }

    events = {
        slug_for_timestamp(current_start): None,
        slug_for_timestamp(next_start): next_event,
    }

    monkeypatch.setattr(scanner, "datetime", _FixedDateTime)
    monkeypatch.setattr(scanner, "fetch_event_by_slug", lambda slug: events.get(slug))

    assert scanner.find_active_15m_market() is None


def test_find_active_market_returns_started_window_from_start_time(monkeypatch):
    now = _FixedDateTime._now
    current_start = _quarter_hour_floor(now)
    current_event = {
        "slug": slug_for_timestamp(current_start),
        "title": "Bitcoin Up or Down - April 12, 10:30AM-10:45AM ET",
        "startTime": current_start.isoformat().replace("+00:00", "Z"),
        "endDate": (current_start + timedelta(minutes=15)).isoformat().replace("+00:00", "Z"),
        "closed": False,
    }

    monkeypatch.setattr(scanner, "datetime", _FixedDateTime)
    monkeypatch.setattr(scanner, "fetch_event_by_slug", lambda slug: current_event if slug == current_event["slug"] else None)

    result = scanner.find_active_15m_market()

    assert result == current_event


def test_find_active_market_uses_title_fallback_for_started_window(monkeypatch):
    now = _FixedDateTime._now
    current_start = _quarter_hour_floor(now)
    current_event = {
        "slug": slug_for_timestamp(current_start),
        "title": "Bitcoin Up or Down - April 12, 10:30AM-10:45AM ET",
        "endDate": (current_start + timedelta(minutes=15)).isoformat().replace("+00:00", "Z"),
        "closed": False,
    }

    monkeypatch.setattr(scanner, "datetime", _FixedDateTime)
    monkeypatch.setattr(scanner, "fetch_event_by_slug", lambda slug: current_event if slug == current_event["slug"] else None)

    result = scanner.find_active_15m_market()

    assert result == current_event


def test_extract_market_data_uses_end_date_for_year_boundary_fallback():
    event = {
        "slug": "btc-updown-15m-test",
        "title": "Bitcoin Up or Down - December 31, 11:45PM-12:00AM ET",
        "endDate": "2026-01-01T05:00:00Z",
        "closed": False,
    }

    data = scanner.extract_market_data(event)

    assert data["window_start_utc"] == datetime(2026, 1, 1, 4, 45, tzinfo=timezone.utc)
    assert data["window_end_utc"] == datetime(2026, 1, 1, 5, 0, tzinfo=timezone.utc)
