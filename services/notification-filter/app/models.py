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


class DeliveryMode(StrEnum):
    realtime = "realtime"
    digest = "digest"


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


class UserPreference(BaseModel):
    chat_id: str
    categories: list[Category]
    min_priority: Priority
    mode: DeliveryMode
    timezone: str
    active: bool
    version: int
