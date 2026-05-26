from enum import StrEnum

from pydantic import BaseModel


class DeliveryMode(StrEnum):
    realtime = "realtime"
    digest = "digest"


class FilteredNotification(BaseModel):
    event_id: str
    event_version: int = 1
    event_time: str
    category: str
    priority: str
    title: str
    summary: str
    source_url: str
    chat_id: str
    delivery_mode: DeliveryMode
    user_timezone: str
    preferences_version: int
    actor: str | None = None
    project: str | None = None
    filtered_at: str | None = None
