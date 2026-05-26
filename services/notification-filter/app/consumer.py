import json
import logging
from threading import Event

from confluent_kafka import Consumer, KafkaException, Producer

from app.config import settings
from app.kafka_io import consumer_config, ensure_topics, producer_config
from app.models import ClassifiedEvent, Priority, UserPreference
from app.repository import PreferencesRepository
from app.time import utc_now

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
LOGGER = logging.getLogger(__name__)

PRIORITY_RANK = {
    Priority.low: 0,
    Priority.medium: 1,
    Priority.high: 2,
    Priority.critical: 3,
}


def should_deliver(event: ClassifiedEvent, preference: UserPreference) -> bool:
    if event.category not in preference.categories:
        return False
    return PRIORITY_RANK[event.priority] >= PRIORITY_RANK[preference.min_priority]


def filtered_payload(event: ClassifiedEvent, preference: UserPreference) -> dict:
    return {
        "event_id": event.event_id,
        "event_version": event.event_version,
        "event_time": event.event_time,
        "category": event.category.value,
        "priority": event.priority.value,
        "title": event.title,
        "summary": event.summary,
        "source_url": event.source_url,
        "actor": event.actor,
        "project": event.project,
        "chat_id": preference.chat_id,
        "delivery_mode": preference.mode.value,
        "user_timezone": preference.timezone,
        "preferences_version": preference.version,
        "filtered_at": utc_now(),
    }


def publish_json(producer: Producer, key: str, payload: dict) -> None:
    delivered = Event()
    delivery_error: list[Exception] = []

    def on_delivery(error, _message) -> None:
        if error is not None:
            delivery_error.append(KafkaException(error))
        delivered.set()

    producer.produce(
        settings.output_topic,
        key=key.encode("utf-8"),
        value=json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8"),
        callback=on_delivery,
    )
    remaining = producer.flush(10)
    if remaining > 0 or not delivered.is_set():
        raise TimeoutError(f"Timed out delivering message to {settings.output_topic}")
    if delivery_error:
        raise delivery_error[0]


def process_message(producer: Producer, repository: PreferencesRepository, message) -> None:
    raw_value = message.value().decode("utf-8") if message.value() else "{}"
    event = ClassifiedEvent.model_validate_json(raw_value)
    matched = 0
    for preference in repository.list_active():
        if not should_deliver(event, preference):
            continue
        publish_json(producer, f"{preference.chat_id}:{event.event_id}", filtered_payload(event, preference))
        matched += 1
    LOGGER.info(
        "notification_event_filtered",
        extra={"_event_id": event.event_id, "_category": event.category.value, "_matched": matched},
    )


def commit_message(consumer: Consumer, message) -> None:
    try:
        consumer.commit(message=message, asynchronous=False)
    except KafkaException:
        LOGGER.warning("notification_filter_commit_failed", exc_info=True)


def main() -> None:
    ensure_topics([settings.input_topic, settings.output_topic])
    repository = PreferencesRepository()
    consumer = Consumer(consumer_config(settings.service_name))
    producer = Producer(producer_config(settings.service_name))
    consumer.subscribe([settings.input_topic])
    LOGGER.info("notification_filter_started", extra={"_input_topic": settings.input_topic})
    try:
        while True:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                raise KafkaException(message.error())
            try:
                process_message(producer, repository, message)
            except Exception:
                LOGGER.exception("notification_filter_failed")
            finally:
                commit_message(consumer, message)
    finally:
        consumer.close()
        producer.flush(10)


if __name__ == "__main__":
    main()
