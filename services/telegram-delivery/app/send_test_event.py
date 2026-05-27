import argparse
import json
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer

from app.config import settings
from app.kafka_io import ensure_topics, producer_config


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def build_event(event_id: str | None = None) -> dict:
    return {
        "event_id": event_id or str(uuid.uuid4()),
        "source": "commons.wikimedia.org",
        "action_type": "edit",
        "entity_title": "File:Demo image from Kafka pipeline.jpg",
        "user_name": "DemoBot",
        "project": "commonswiki",
        "summary": "Updated structured metadata for a file from the local test scenario",
        "source_url": "https://commons.wikimedia.org/wiki/File:Demo_image_from_Kafka_pipeline.jpg",
        "event_time": utc_now(),
        "byte_change": 480,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("scenario", choices=["valid", "duplicate"])
    args = parser.parse_args()

    ensure_topics([settings.input_topic, settings.audit_topic, settings.dlq_topic])
    producer = Producer(producer_config("telegram-test-event-sender"))
    event_id = str(uuid.uuid4())
    events = [build_event(event_id), build_event(event_id)] if args.scenario == "duplicate" else [build_event()]
    for event in events:
        producer.produce(
            settings.input_topic,
            key=event["event_id"].encode("utf-8"),
            value=json.dumps(event, ensure_ascii=False, separators=(",", ":")).encode("utf-8"),
        )
    producer.flush(10)


if __name__ == "__main__":
    main()
