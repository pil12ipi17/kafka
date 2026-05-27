from enum import StrEnum
from typing import Annotated

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


class QuietHours(BaseModel):
    start: str | None = Field(default=None, examples=["23:00"])
    end: str | None = Field(default=None, examples=["08:00"])


class PreferenceUpsert(BaseModel):
    categories: Annotated[list[Category], Field(min_length=1)] = [Category.security, Category.ops]
    min_priority: Priority = Priority.low
    mode: DeliveryMode = DeliveryMode.realtime
    quiet_hours: QuietHours = Field(default_factory=QuietHours)
    timezone: str = "Europe/Moscow"
    active: bool = True


class Preference(PreferenceUpsert):
    chat_id: str
    version: int = 1


class ModePatch(BaseModel):
    mode: DeliveryMode


class CategoriesPatch(BaseModel):
    categories: Annotated[list[Category], Field(min_length=1)]
