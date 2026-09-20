from __future__ import annotations

from datetime import datetime, time, timezone
from typing import Optional
from zoneinfo import ZoneInfo


PARIS_TZ = ZoneInfo("Europe/Paris")


def parse_utc_iso(value: Optional[str]) -> Optional[datetime]:
    if value is None:
        return None
    text = value.strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def iso_to_unix(value: Optional[str]) -> Optional[float]:
    dt = parse_utc_iso(value)
    return dt.timestamp() if dt is not None else None


def paris_date_key(value: Optional[str] = None, *, dt: Optional[datetime] = None) -> str:
    parsed = dt if dt is not None else parse_utc_iso(value)
    if parsed is None:
        return ""
    return parsed.astimezone(PARIS_TZ).date().isoformat()


def paris_day_start_utc(day_key: str) -> Optional[datetime]:
    try:
        day = datetime.fromisoformat(day_key)
    except ValueError:
        return None
    local = datetime.combine(day.date(), time.min, tzinfo=PARIS_TZ)
    return local.astimezone(timezone.utc)
