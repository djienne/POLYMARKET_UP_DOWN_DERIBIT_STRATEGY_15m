from datetime import timezone

from app.time_utils import iso_to_unix, parse_utc_iso


def test_parse_utc_iso_handles_z_suffix():
    dt = parse_utc_iso("2026-04-22T10:15:30Z")
    assert dt is not None
    assert dt.tzinfo == timezone.utc
    assert dt.hour == 10


def test_parse_utc_iso_handles_explicit_offset():
    dt = parse_utc_iso("2026-04-22T12:15:30+02:00")
    assert dt is not None
    assert dt.tzinfo == timezone.utc
    assert dt.hour == 10


def test_iso_to_unix_rejects_invalid_input():
    assert iso_to_unix("not-a-timestamp") is None
