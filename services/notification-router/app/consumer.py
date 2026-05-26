import json
import logging
from threading import Event

from confluent_kafka import Consumer, KafkaException, Producer
from pydantic import ValidationError

from app.config import settings
from app.kafka_io import consumer_config, ensure_topics, producer_config
from app.models import DeliveryMode, FilteredNotification

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


def route_topic(event: FilteredNotification) -> str:
    if event.delivery_mode == DeliveryMode.realtime:
        return settings.realtime_topic
    return settings.digest_topic


def process_message(producer: Producer, message) -> None:
    raw_value = message.value().decode("utf-8") if message.value() else "{}"
    try:
        event = FilteredNotification.model_validate_json(raw_value)
    except ValidationError as exc:
        payload = {
            "reason": "invalid_filtered_notification",
            "error": str(exc),
            "raw_value": raw_value,
            "source_topic": message.topic(),
            "source_partition": message.partition(),
            "source_offset": message.offset(),
        }
        publish_json(producer, settings.dlq_topic, f"{message.topic()}:{message.partition()}:{message.offset()}", payload)
        LOGGER.warning("notification_router_invalid_message")
        return

    payload = event.model_dump(mode="json")
    topic = route_topic(event)
    publish_json(producer, topic, f"{event.chat_id}:{event.event_id}", payload)
    LOGGER.info(
        "notification_routed",
        extra={
            "_event_id": event.event_id,
            "_chat_id": event.chat_id,
            "_delivery_mode": event.delivery_mode.value,
            "_target_topic": topic,
        },
    )


def main() -> None:
    ensure_topics([settings.input_topic, settings.realtime_topic, settings.digest_topic, settings.dlq_topic])
    consumer = Consumer(consumer_config(settings.service_name))
    producer = Producer(producer_config(settings.service_name))
    consumer.subscribe([settings.input_topic])
    LOGGER.info("notification_router_started", extra={"_input_topic": settings.input_topic})
    try:
        while True:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                raise KafkaException(message.error())
            try:
                process_message(producer, message)
            except Exception:
                LOGGER.exception("notification_router_failed")
            finally:
                consumer.commit(message=message, asynchronous=False)
    finally:
        consumer.close()
        producer.flush(10)


if __name__ == "__main__":
    main()
