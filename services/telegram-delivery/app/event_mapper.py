import hashlib
import json
from typing import Any

from app.models import TelegramEvent


def _get(payload: dict[str, Any], *names: str) -> Any:
    for name in names:
        if name in payload:
            return payload[name]
        upper = name.upper()
        if upper in payload:
            return payload[upper]
        lower = name.lower()
        if lower in payload:
            return payload[lower]
    return None


def _nested(payload: dict[str, Any], path: list[str]) -> Any:
    value: Any = payload
    for part in path:
        if not isinstance(value, dict):
            return None
        value = _get(value, part)
    return value


def _stable_event_id(payload: dict[str, Any]) -> str:
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def map_to_telegram_event(payload: dict[str, Any]) -> TelegramEvent:
    meta = _get(payload, "meta") or {}
    length = _get(payload, "length") or {}
    byte_change = _get(payload, "byte_change")
    if byte_change is None and isinstance(length, dict):
        new_length = _get(length, "new")
        old_length = _get(length, "old")
        if isinstance(new_length, int) and isinstance(old_length, int):
            byte_change = new_length - old_length

    event_id = (
        _get(payload, "event_id")
        or _get(payload, "id")
        or _nested(meta, ["id"])
        or _stable_event_id(payload)
    )

    return TelegramEvent(
        event_id=str(event_id),
        source=str(_get(payload, "source") or _get(meta, "domain") or _nested(meta, ["domain"]) or "Wikimedia"),
        action_type=str(_get(payload, "action_type") or _get(payload, "type") or "unknown"),
        entity_title=str(_get(payload, "entity_title") or _get(payload, "title") or "Unknown page"),
        user_name=str(_get(payload, "user_name") or _get(payload, "user") or "unknown"),
        project=_get(payload, "project") or _get(payload, "wiki"),
        summary=_get(payload, "summary") or _get(payload, "comment"),
        source_url=_get(payload, "source_url") or _get(meta, "uri") or _nested(meta, ["uri"]),
        event_time=_get(payload, "event_time") or _get(payload, "timestamp") or _get(meta, "dt"),
        byte_change=byte_change if isinstance(byte_change, int) else None,
        raw=payload,
    )
