import json
import sys
from threading import Event
from uuid import uuid4

from confluent_kafka import KafkaException, Producer

from app.config import settings
from app.kafka_io import producer_config
from app.time import utc_now


def main() -> None:
    event_id = str(uuid4())
    payload = {
        "event_id": event_id,
        "event_version": 1,
        "event_time": utc_now(),
        "category": "security",
        "priority": "high",
        "title": "Suspicious permission change",
        "summary": "Security-related Wikimedia event for filter smoke test",
        "source_url": "https://example.com/security-event",
        "actor": "ExampleUser",
        "project": "commonswiki",
    }
    producer = Producer(producer_config("notification-filter-test-producer"))
    delivered = Event()
    delivery_error: list[Exception] = []

    def on_delivery(error, _message) -> None:
        if error is not None:
            delivery_error.append(KafkaException(error))
        delivered.set()

    producer.produce(
        settings.input_topic,
        key=event_id.encode("utf-8"),
        value=json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8"),
        callback=on_delivery,
    )
    remaining = producer.flush(10)
    if remaining > 0 or not delivered.is_set():
        print("Timed out delivering test event", file=sys.stderr)
        sys.exit(1)
    if delivery_error:
        print(str(delivery_error[0]), file=sys.stderr)
        sys.exit(1)
    print(json.dumps(payload, ensure_ascii=False))


if __name__ == "__main__":
    main()
