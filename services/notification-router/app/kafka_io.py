import logging
from typing import Iterable

from confluent_kafka.admin import AdminClient, NewTopic
from tenacity import retry, stop_after_attempt, wait_exponential

from app.config import settings

LOGGER = logging.getLogger(__name__)


def producer_config(client_id: str) -> dict:
    return {
        "bootstrap.servers": settings.bootstrap_servers,
        "client.id": client_id,
        "enable.idempotence": True,
        "acks": "all",
        "retries": 10,
    }


def consumer_config(client_id: str) -> dict:
    return {
        "bootstrap.servers": settings.bootstrap_servers,
        "client.id": client_id,
        "group.id": settings.consumer_group,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
        "isolation.level": "read_committed",
    }


@retry(stop=stop_after_attempt(8), wait=wait_exponential(multiplier=1, min=1, max=10))
def ensure_topics(topics: Iterable[str]) -> None:
    admin = AdminClient({"bootstrap.servers": settings.bootstrap_servers})
    existing = set(admin.list_topics(timeout=10).topics.keys())
    missing = [
        NewTopic(topic, num_partitions=settings.topic_partitions, replication_factor=1)
        for topic in topics
        if topic not in existing
    ]
    if not missing:
        return

    futures = admin.create_topics(missing, operation_timeout=30)
    for topic, future in futures.items():
        try:
            future.result()
            LOGGER.info("topic_created", extra={"_topic": topic})
        except Exception as exc:
            if "already exists" not in str(exc).lower():
                raise
