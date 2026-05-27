from typing import Any

from app.models import Category, ClassifiedEvent, Priority, TelegramReadyEvent
from app.time import unix_seconds_to_iso

SECURITY_KEYWORDS = (
    "abuse",
    "block",
    "blocked",
    "delete",
    "deleted",
    "permission",
    "protect",
    "protected",
    "rights",
    "rollback",
    "spam",
    "vandal",
)


def field(payload: dict[str, Any], *names: str) -> Any:
    for name in names:
        if name in payload:
            return payload[name]
        upper_name = name.upper()
        if upper_name in payload:
            return payload[upper_name]
    return None


def normalize_event(payload: dict[str, Any]) -> TelegramReadyEvent:
    return TelegramReadyEvent(
        event_id=str(field(payload, "event_id") or field(payload, "id")),
        source=field(payload, "source"),
        action_type=field(payload, "action_type", "type"),
        entity_title=str(field(payload, "entity_title", "title") or "Unknown Wikimedia entity"),
        user_name=field(payload, "user_name", "user"),
        project=field(payload, "project", "wiki"),
        summary=field(payload, "summary", "comment"),
        source_url=str(field(payload, "source_url", "uri") or ""),
        event_time=field(payload, "event_time", "timestamp"),
        byte_change=field(payload, "byte_change"),
        raw=payload,
    )


def classify_category(event: TelegramReadyEvent) -> Category:
    text = f"{event.action_type or ''} {event.summary or ''} {event.entity_title}".lower()
    if any(keyword in text for keyword in SECURITY_KEYWORDS):
        return Category.security
    if event.action_type in {"edit", "new", "categorize"}:
        return Category.content
    if event.action_type == "log":
        return Category.ops
    return Category.other


def classify_priority(event: TelegramReadyEvent, category: Category) -> Priority:
    byte_change = abs(event.byte_change or 0)
    if category == Category.security:
        return Priority.high
    if byte_change >= 5000:
        return Priority.high
    if event.action_type in {"new", "log"} or byte_change >= 500:
        return Priority.medium
    return Priority.low


def to_classified_event(event: TelegramReadyEvent) -> ClassifiedEvent:
    category = classify_category(event)
    priority = classify_priority(event, category)
    action = event.action_type or "event"
    actor = event.user_name or "unknown user"
    summary = event.summary or f"{actor} performed {action} on {event.entity_title}"

    return ClassifiedEvent(
        event_id=event.event_id,
        event_time=unix_seconds_to_iso(event.event_time),
        category=category,
        priority=priority,
        title=event.entity_title,
        summary=summary,
        source_url=event.source_url,
        actor=event.user_name,
        project=event.project or event.source,
        raw=event.raw,
    )
