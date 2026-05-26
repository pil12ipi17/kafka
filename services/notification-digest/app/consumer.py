import json
import logging
import time
from collections import defaultdict
from threading import Event
from uuid import uuid4

from confluent_kafka import Consumer, KafkaException, Producer
from pydantic import ValidationError

from app.config import settings
from app.kafka_io import consumer_config, ensure_topics, producer_config
from app.models import DigestNotification, DigestReady
from app.time import utc_now

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
LOGGER = logging.getLogger(__name__)

PRIORITY_RANK = {
    "low": 0,
    "medium": 1,
    "high": 2,
    "critical": 3,
}


class DigestBuffer:
    def __init__(self) -> None:
        self._items: dict[str, list[DigestNotification]] = defaultdict(list)
        self._last_flush = time.monotonic()

    def add(self, event: DigestNotification) -> None:
        self._items[event.chat_id].append(event)

    def pop_ready(self, force: bool = False) -> list[tuple[str, list[DigestNotification]]]:
        now = time.monotonic()
        interval_elapsed = now - self._last_flush >= settings.flush_interval_seconds
        ready: list[tuple[str, list[DigestNotification]]] = []

        for chat_id, items in list(self._items.items()):
            if not items:
                continue
            if force or interval_elapsed or len(items) >= settings.max_events_per_digest:
                ready.append((chat_id, items[:]))
                self._items[chat_id].clear()
                del self._items[chat_id]

        if force or interval_elapsed:
            self._last_flush = now
        return ready


def publish_json(producer: Producer, topic: str, key: str, payload: dict) -> None:
    delivered = Event()
    delivery_error: list[Exception] = []

    def on_delivery(error, _message) -> None:
        if error is not None:
            delivery_error.append(KafkaException(error))
        delivered.set()

    producer.produce(
        topic,
        key=key.encode("utf-8"),
        value=json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8"),
        callback=on_delivery,
    )
    remaining = producer.flush(10)
    if remaining > 0 or not delivered.is_set():
        raise TimeoutError(f"Timed out delivering message to {topic}")
    if delivery_error:
        raise delivery_error[0]


def build_digest(chat_id: str, items: list[DigestNotification]) -> DigestReady:
    categories = sorted({item.category for item in items})
    top_priority = max((item.priority for item in items), key=lambda priority: PRIORITY_RANK.get(priority, -1), default="unknown")
    titles = [f"- {item.title}" for item in items[:5]]
    summary = "\n".join(titles)
    if len(items) > 5:
        summary += f"\n- ... and {len(items) - 5} more"

    return DigestReady(
        event_id=f"digest-{chat_id}-{uuid4()}",
        event_time=utc_now(),
        entity_title=f"{len(items)} notification(s): {', '.join(categories)}",
        project=items[0].project if items else None,
        summary=summary,
        source_url=items[0].source_url if items else None,
        chat_id=chat_id,
        digest_count=len(items),
        top_priority=top_priority,
        digest_items=items,
    )


def publish_digest(producer: Producer, chat_id: str, items: list[DigestNotification]) -> None:
    digest = build_digest(chat_id, items)
    publish_json(producer, settings.output_topic, f"{chat_id}:{digest.event_id}", digest.model_dump(mode="json"))
    LOGGER.info(
        "notification_digest_published",
        extra={"_chat_id": chat_id, "_digest_count": len(items), "_event_id": digest.event_id},
    )


def publish_dlq(producer: Producer, message, raw_value: str, reason: str) -> None:
    payload = {
        "reason": reason,
        "raw_value": raw_value,
        "source_topic": message.topic(),
        "source_partition": message.partition(),
        "source_offset": message.offset(),
        "sent_at": utc_now(),
    }
    publish_json(producer, settings.dlq_topic, f"{message.topic()}:{message.partition()}:{message.offset()}", payload)


def process_message(buffer: DigestBuffer, producer: Producer, message) -> None:
    raw_value = message.value().decode("utf-8") if message.value() else "{}"
    try:
        event = DigestNotification.model_validate_json(raw_value)
    except ValidationError as exc:
        publish_dlq(producer, message, raw_value, str(exc))
        LOGGER.warning("notification_digest_invalid_message")
        return

    buffer.add(event)
    LOGGER.info(
        "notification_digest_buffered",
        extra={"_chat_id": event.chat_id, "_event_id": event.event_id, "_category": event.category},
    )


def publish_ready_buffers(buffer: DigestBuffer, producer: Producer, force: bool = False) -> None:
    for chat_id, items in buffer.pop_ready(force=force):
        publish_digest(producer, chat_id, items)


def main() -> None:
    ensure_topics([settings.input_topic, settings.output_topic, settings.dlq_topic])
    buffer = DigestBuffer()
    consumer = Consumer(consumer_config(settings.service_name))
    producer = Producer(producer_config(settings.service_name))
    consumer.subscribe([settings.input_topic])
    LOGGER.info("notification_digest_started", extra={"_input_topic": settings.input_topic})
    try:
        while True:
            message = consumer.poll(1.0)
            if message is None:
                publish_ready_buffers(buffer, producer)
                continue
            if message.error():
                raise KafkaException(message.error())
            try:
                process_message(buffer, producer, message)
                publish_ready_buffers(buffer, producer)
            except Exception:
                LOGGER.exception("notification_digest_failed")
            finally:
                consumer.commit(message=message, asynchronous=False)
    finally:
        publish_ready_buffers(buffer, producer, force=True)
        consumer.close()
        producer.flush(10)


if __name__ == "__main__":
    main()
