from pydantic import BaseModel, Field


class Recipient(BaseModel):
    chat_id: str
    display_name: str
    enabled: bool = True


class TelegramEvent(BaseModel):
    event_id: str
    chat_id: str | None = None
    source: str = "Wikimedia"
    action_type: str = "unknown"
    entity_title: str = "Unknown page"
    user_name: str = "unknown"
    project: str | None = None
    summary: str | None = None
    source_url: str | None = None
    event_time: str | int | None = None
    byte_change: int | None = None
    raw: dict = Field(default_factory=dict)


class AuditEvent(BaseModel):
    event_id: str
    chat_id: str
    delivery_status: str
    attempt: int
    error_code: int | None = None
    error_message: str | None = None
    processed_at: str
    source_topic: str
    source_partition: int
    source_offset: int
