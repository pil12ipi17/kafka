from pydantic import BaseModel


class DigestNotification(BaseModel):
    event_id: str
    event_version: int = 1
    event_time: str
    category: str
    priority: str
    title: str
    summary: str
    source_url: str
    chat_id: str
    delivery_mode: str
    user_timezone: str
    preferences_version: int
    actor: str | None = None
    project: str | None = None
    filtered_at: str | None = None


class DigestReady(BaseModel):
    event_id: str
    event_version: int = 1
    event_time: str
    source: str = "Notification digest"
    action_type: str = "digest"
    entity_title: str
    user_name: str = "digest-service"
    project: str | None = None
    summary: str
    source_url: str | None = None
    chat_id: str
    digest_count: int
    top_priority: str
    digest_items: list[DigestNotification]
