from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any


BEIJING_TZ = timezone(timedelta(hours=8), name="Asia/Shanghai")


def parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if not isinstance(value, str) or not value:
        return None
    normalized = value.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


def format_beijing_minute(value: Any) -> str:
    dt = parse_datetime(value)
    if dt is None:
        return str(value) if value not in (None, "") else "-"
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(BEIJING_TZ).strftime("%Y-%m-%d %H:%M")


def normalize_timestamp_fields(value: Any, *, key: str = "") -> Any:
    if isinstance(value, dict):
        return {sub_key: normalize_timestamp_fields(sub_value, key=sub_key) for sub_key, sub_value in value.items()}
    if isinstance(value, list):
        return [normalize_timestamp_fields(item, key=key) for item in value]
    if _looks_like_timestamp_field(key):
        parsed = parse_datetime(value)
        if parsed is not None:
            return format_beijing_minute(parsed)
    return value


def _looks_like_timestamp_field(key: str) -> bool:
    return key.endswith("_at") or key in {"timestamp", "since"}
