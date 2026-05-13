import argparse
import json
import logging
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer

from app.config import settings
from app.kafka_io import ensure_topics, producer_config
from app.logging_json import configure_logging
from app.producer import build_event

LOGGER = logging.getLogger(__name__)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def produce(producer: Producer, event: dict) -> None:
    producer.produce(
        settings.input_topic,
        key=event.get("user_id", "broken").encode("utf-8"),
        value=json.dumps(event, separators=(",", ":")).encode("utf-8"),
    )
    producer.flush(10)
    LOGGER.info(
        "test_event_sent",
        extra={
            "_event_id": event.get("event_id"),
            "_trace_id": event.get("trace_id"),
            "_topic": settings.input_topic,
        },
    )


def invalid_event() -> dict:
    event = build_event()
    event.pop("user_id")
    return event


def temporary_failure_event() -> dict:
    event = build_event()
    event["amount"] = 503
    return event


def duplicate_events() -> list[dict]:
    event = build_event()
    event["event_id"] = f"duplicate-{uuid.uuid4()}"
    return [event, dict(event)]


def main() -> None:
    configure_logging()
    parser = argparse.ArgumentParser()
    parser.add_argument("scenario", choices=["valid", "invalid", "temporary-failure", "duplicate"])
    args = parser.parse_args()

    ensure_topics([settings.input_topic, settings.output_topic, settings.dlq_topic])
    producer = Producer(producer_config("python-test-event-sender"))

    if args.scenario == "valid":
        events = [build_event()]
    elif args.scenario == "invalid":
        events = [invalid_event()]
    elif args.scenario == "temporary-failure":
        events = [temporary_failure_event()]
    else:
        events = duplicate_events()

    for event in events:
        produce(producer, event)


if __name__ == "__main__":
    main()
