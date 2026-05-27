import json
import logging
from threading import Event

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer

from app.classifier import normalize_event, to_classified_event
from app.config import settings
from app.kafka_io import consumer_config, ensure_topics, producer_config
from app.time import utc_now

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
LOGGER = logging.getLogger(__name__)


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


def publish_dlq(producer: Producer, message, raw_value: str, reason: str) -> None:
    payload = {
        "reason": reason,
        "raw_value": raw_value,
        "source_topic": message.topic(),
        "source_partition": message.partition(),
        "source_offset": message.offset(),
        "sent_at": utc_now(),
    }
    key = f"{message.topic()}:{message.partition()}:{message.offset()}"
    publish_json(producer, settings.dlq_topic, key, payload)


def process_message(producer: Producer, message) -> None:
    raw_value = message.value().decode("utf-8") if message.value() else "{}"
    try:
        payload = json.loads(raw_value)
        ready_event = normalize_event(payload)
        classified_event = to_classified_event(ready_event)
    except Exception as exc:
        publish_dlq(producer, message, raw_value, str(exc))
        LOGGER.warning("notification_classifier_invalid_message")
        return

    publish_json(
        producer,
        settings.output_topic,
        classified_event.event_id,
        classified_event.model_dump(mode="json"),
    )
    LOGGER.info(
        "notification_event_classified",
        extra={
            "_event_id": classified_event.event_id,
            "_category": classified_event.category.value,
            "_priority": classified_event.priority.value,
        },
    )


def commit_message(consumer: Consumer, message) -> None:
    try:
        consumer.commit(message=message, asynchronous=False)
    except KafkaException:
        LOGGER.warning("notification_classifier_commit_failed", exc_info=True)


def main() -> None:
    ensure_topics([settings.output_topic, settings.dlq_topic])
    consumer = Consumer(consumer_config(settings.service_name))
    producer = Producer(producer_config(settings.service_name))
    consumer.subscribe([settings.input_topic])
    LOGGER.info("notification_classifier_started", extra={"_input_topic": settings.input_topic})
    try:
        while True:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                if message.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                    LOGGER.warning("notification_classifier_input_topic_not_ready")
                    continue
                raise KafkaException(message.error())
            try:
                process_message(producer, message)
            except Exception:
                LOGGER.exception("notification_classifier_failed")
            finally:
                commit_message(consumer, message)
    finally:
        consumer.close()
        producer.flush(10)


if __name__ == "__main__":
    main()
