import json
from uuid import uuid4

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from app.config import settings
from app.models import Preference
from app.time import utc_now


def producer_config() -> dict:
    return {
        "bootstrap.servers": settings.bootstrap_servers,
        "client.id": settings.service_name,
        "enable.idempotence": True,
        "acks": "all",
    }


def ensure_topics() -> None:
    admin = AdminClient({"bootstrap.servers": settings.bootstrap_servers})
    metadata = admin.list_topics(timeout=10)
    if settings.preferences_changed_topic in metadata.topics:
        return
    topic = NewTopic(
        settings.preferences_changed_topic,
        num_partitions=settings.topic_partitions,
        replication_factor=1,
    )
    futures = admin.create_topics([topic])
    futures[settings.preferences_changed_topic].result(20)


class PreferencesEventPublisher:
    def __init__(self) -> None:
        self._producer = Producer(producer_config())

    def publish_changed(self, preference: Preference) -> None:
        payload = {
            "event_id": str(uuid4()),
            "event_version": 1,
            "chat_id": preference.chat_id,
            "categories": [category.value for category in preference.categories],
            "min_priority": preference.min_priority.value,
            "mode": preference.mode.value,
            "quiet_hours_start": preference.quiet_hours.start,
            "quiet_hours_end": preference.quiet_hours.end,
            "timezone": preference.timezone,
            "active": preference.active,
            "preferences_version": preference.version,
            "changed_at": utc_now(),
        }
        self._producer.produce(
            settings.preferences_changed_topic,
            key=preference.chat_id.encode("utf-8"),
            value=json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8"),
        )
        self._producer.flush(10)
