import json
import logging
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer

from app.config import settings
from app.kafka_io import ensure_topics, producer_config
from app.logging_json import configure_logging
from app.metrics import produced_total, start_metrics_server

LOGGER = logging.getLogger(__name__)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def build_event() -> dict:
    user_id = f"user-{random.randint(1, 12):03d}"
    event = {
        "schema_version": settings.producer_schema_version,
        "event_id": str(uuid.uuid4()),
        "trace_id": str(uuid.uuid4()),
        "event_type": "order_created",
        "user_id": user_id,
        "order_id": f"order-{uuid.uuid4().hex[:12]}",
        "amount": round(random.uniform(10, 700), 2),
        "currency": "USD",
        "created_at": utc_now(),
    }
    if settings.producer_schema_version == "v2":
        event["coupon_code"] = random.choice([None, "WELCOME10", "SPRING15"])
    return event


def delivery_report(error, message) -> None:
    if error is not None:
        LOGGER.error("produce_failed", extra={"_error": str(error)})
        return
    LOGGER.info(
        "event_produced",
        extra={
            "_message_key": message.key().decode("utf-8") if message.key() else None,
            "_topic": message.topic(),
            "_partition": message.partition(),
            "_offset": message.offset(),
        },
    )


def main() -> None:
    configure_logging()
    start_metrics_server(settings.metrics_port)
    ensure_topics([settings.input_topic, settings.output_topic, settings.dlq_topic])

    producer = Producer(producer_config("python-producer-service"))
    LOGGER.info(
        "producer_started",
        extra={"_topic": settings.input_topic, "_schema_version": settings.producer_schema_version},
    )

    while True:
        for _ in range(settings.producer_batch_size):
            event = build_event()
            key = event["user_id"]
            producer.produce(
                settings.input_topic,
                key=key.encode("utf-8"),
                value=json.dumps(event, separators=(",", ":")).encode("utf-8"),
                on_delivery=delivery_report,
            )
            LOGGER.info(
                "event_enqueued",
                extra={
                    "_event_id": event["event_id"],
                    "_trace_id": event["trace_id"],
                    "_message_key": key,
                    "_topic": settings.input_topic,
                },
            )
            produced_total.inc()
        producer.poll(0)
        producer.flush(5)
        time.sleep(settings.producer_interval_seconds)


if __name__ == "__main__":
    main()
