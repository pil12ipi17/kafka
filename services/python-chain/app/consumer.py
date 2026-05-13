import json
import logging
import sqlite3
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Consumer, KafkaException, Producer
from jsonschema import Draft7Validator, FormatChecker
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from app.config import settings
from app.kafka_io import consumer_config, ensure_topics, producer_config
from app.logging_json import configure_logging
from app.metrics import dlq_sent_total, failed_total, processed_total, start_metrics_server
from app.schema_registry import load_schema, register_contracts

LOGGER = logging.getLogger(__name__)


class TemporaryProcessingError(Exception):
    pass


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def open_state() -> sqlite3.Connection:
    Path(settings.state_dir).mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(Path(settings.state_dir) / "consumer.sqlite")
    connection.execute(
        """
        create table if not exists processed_events (
            event_id text primary key,
            processed_at text not null
        )
        """
    )
    connection.commit()
    return connection


def is_processed(connection: sqlite3.Connection, event_id: str) -> bool:
    row = connection.execute(
        "select 1 from processed_events where event_id = ?",
        (event_id,),
    ).fetchone()
    return row is not None


def mark_processed(connection: sqlite3.Connection, event_id: str) -> None:
    connection.execute(
        "insert or ignore into processed_events(event_id, processed_at) values(?, ?)",
        (event_id, utc_now()),
    )
    connection.commit()


def validators() -> dict[str, Draft7Validator]:
    return {
        "v1": Draft7Validator(load_schema("order_event_v1.json"), format_checker=FormatChecker()),
        "v2": Draft7Validator(load_schema("order_event_v2.json"), format_checker=FormatChecker()),
    }


def validate_event(event: dict, schema_validators: dict[str, Draft7Validator]) -> None:
    version = event.get("schema_version")
    if version not in schema_validators:
        raise ValueError(f"unsupported schema_version={version!r}")
    errors = sorted(schema_validators[version].iter_errors(event), key=lambda item: item.path)
    if errors:
        raise ValueError("; ".join(error.message for error in errors))


def build_processed_event(source: dict) -> dict:
    return {
        "schema_version": "v1",
        "event_id": str(uuid.uuid4()),
        "trace_id": source["trace_id"],
        "source_event_id": source["event_id"],
        "user_id": source["user_id"],
        "order_id": source["order_id"],
        "status": "accepted",
        "processed_at": utc_now(),
    }


@retry(
    retry=retry_if_exception_type(TemporaryProcessingError),
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=1, min=1, max=8),
)
def process_with_retry(event: dict, producer: Producer) -> None:
    if event.get("amount") == 503:
        raise TemporaryProcessingError("simulated temporary downstream failure")

    result = build_processed_event(event)
    producer.produce(
        settings.output_topic,
        key=event["user_id"].encode("utf-8"),
        value=json.dumps(result, separators=(",", ":")).encode("utf-8"),
    )
    producer.flush(10)


def send_to_dlq(producer: Producer, message, reason: str, raw_value: str | None) -> None:
    payload = {
        "event_id": str(uuid.uuid4()),
        "trace_id": None,
        "source_topic": message.topic(),
        "source_partition": message.partition(),
        "source_offset": message.offset(),
        "reason": reason,
        "raw_value": raw_value,
        "sent_at": utc_now(),
    }
    producer.produce(
        settings.dlq_topic,
        key=f"{message.topic()}:{message.partition()}:{message.offset()}".encode("utf-8"),
        value=json.dumps(payload, separators=(",", ":")).encode("utf-8"),
    )
    producer.flush(10)
    dlq_sent_total.inc()
    LOGGER.error(
        "sent_to_dlq",
        extra={
            "_reason": reason,
            "_topic": message.topic(),
            "_partition": message.partition(),
            "_offset": message.offset(),
        },
    )


def main() -> None:
    configure_logging()
    start_metrics_server(settings.metrics_port)
    ensure_topics([settings.input_topic, settings.output_topic, settings.dlq_topic])
    register_contracts()

    schema_validators = validators()
    state = open_state()
    consumer = Consumer(consumer_config("python-consumer-service"))
    producer = Producer(producer_config("python-consumer-service"))
    consumer.subscribe([settings.input_topic])
    LOGGER.info("consumer_started", extra={"_topic": settings.input_topic})

    try:
        while True:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                raise KafkaException(message.error())

            raw_value = message.value().decode("utf-8") if message.value() else None
            try:
                event = json.loads(raw_value or "{}")
                validate_event(event, schema_validators)
                log_context = {
                    "_event_id": event["event_id"],
                    "_trace_id": event["trace_id"],
                    "_topic": message.topic(),
                    "_partition": message.partition(),
                    "_offset": message.offset(),
                }
                if is_processed(state, event["event_id"]):
                    LOGGER.info("event_already_processed", extra=log_context)
                    consumer.commit(message=message, asynchronous=False)
                    continue

                process_with_retry(event, producer)
                mark_processed(state, event["event_id"])
                processed_total.inc()
                LOGGER.info("event_processed", extra=log_context)
                consumer.commit(message=message, asynchronous=False)
            except Exception as exc:
                failed_total.inc()
                send_to_dlq(producer, message, str(exc), raw_value)
                consumer.commit(message=message, asynchronous=False)
            time.sleep(0.01)
    finally:
        consumer.close()
        state.close()


if __name__ == "__main__":
    main()
