from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


def parse_time(value: object) -> datetime | None:
    """Parse actor timestamps (ISO, Unix seconds, or Unix milliseconds)."""
    if value is None or value == "":
        return None
    try:
        if isinstance(value, (int, float)) or (isinstance(value, str) and value.strip().replace(".", "", 1).isdigit()):
            number = float(value)
            if number > 10_000_000_000:
                number /= 1000
            return datetime.fromtimestamp(number, tz=UTC)
        text = str(value).strip().replace("Z", "+00:00")
        parsed = datetime.fromisoformat(text)
        return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)
    except (OverflowError, OSError, ValueError):
        return None


def display_time(value: object, timezone: str = "Asia/Taipei") -> str:
    parsed = parse_time(value)
    if not parsed:
        return "時間未知"
    try:
        target = ZoneInfo(timezone)
    except ZoneInfoNotFoundError:
        target = timezone_module_fallback(timezone)
    return parsed.astimezone(target).strftime("%Y-%m-%d %H:%M")


def timezone_module_fallback(name: str):
    return timezone(timedelta(hours=8), name) if name == "Asia/Taipei" else UTC
