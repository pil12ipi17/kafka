from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field


class Category(StrEnum):
    ops = "ops"
    content = "content"
    security = "security"
    system = "system"
    other = "other"


class Priority(StrEnum):
    low = "low"
    medium = "medium"
    high = "high"
    critical = "critical"


class TelegramReadyEvent(BaseModel):
    event_id: str
    source: str | None = None
    action_type: str | None = None
    entity_title: str
    user_name: str | None = None
    project: str | None = None
    summary: str | None = None
    source_url: str
    event_time: int | str | None = None
    byte_change: int | None = None
    raw: dict[str, Any] = Field(default_factory=dict)


class ClassifiedEvent(BaseModel):
    event_id: str
    event_version: int = 1
    event_time: str
    category: Category
    priority: Priority
    title: str
    summary: str
    source_url: str
    actor: str | None = None
    project: str | None = None
    raw: dict[str, Any] = Field(default_factory=dict)
