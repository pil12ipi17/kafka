from datetime import UTC, datetime


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


def unix_seconds_to_iso(value: int | float | str | None) -> str:
    if value is None:
        return utc_now()
    try:
        timestamp = float(value)
    except (TypeError, ValueError):
        return str(value)
    return datetime.fromtimestamp(timestamp, UTC).isoformat()
