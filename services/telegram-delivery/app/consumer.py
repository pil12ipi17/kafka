import asyncio
import json
import logging
import signal
from contextlib import suppress

import aiohttp
from confluent_kafka import Consumer, KafkaException, Producer
from tenacity import AsyncRetrying, retry_if_exception_type, stop_after_attempt, wait_exponential

from app.config import settings
from app.event_mapper import map_to_telegram_event
from app.formatter import format_telegram_message
from app.health import start_health_server
from app.kafka_io import consumer_config, ensure_topics, producer_config
from app.logging_json import configure_logging
from app.metrics import deduplicated_total, dlq_total, failed_total, ready, retry_total, sent_total
from app.models import AuditEvent, Recipient, TelegramEvent
from app.rate_limit import PerChatRateLimiter
from app.recipients import RecipientsStore
from app.state import DeliveryState, utc_now
from app.telegram_client import PermanentTelegramError, TelegramClient, TemporaryTelegramError

LOGGER = logging.getLogger(__name__)


def publish_json(producer: Producer, topic: str, key: str, payload: dict) -> None:
    producer.produce(
        topic,
        key=key.encode("utf-8"),
        value=json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8"),
    )
    producer.flush(10)


def publish_audit(
    producer: Producer,
    message,
    event: TelegramEvent,
    recipient: Recipient,
    status: str,
    attempt: int,
    error_code: int | None = None,
    error_message: str | None = None,
) -> None:
    audit = AuditEvent(
        event_id=event.event_id,
        chat_id=recipient.chat_id,
        delivery_status=status,
        attempt=attempt,
        error_code=error_code,
        error_message=error_message,
        processed_at=utc_now(),
        source_topic=message.topic(),
        source_partition=message.partition(),
        source_offset=message.offset(),
    )
    publish_json(producer, settings.audit_topic, f"{event.event_id}:{recipient.chat_id}", audit.model_dump())


def publish_dlq(
    producer: Producer,
    message,
    event: TelegramEvent | None,
    raw_value: str | None,
    reason: str,
    recipient: Recipient | None = None,
    error_code: int | None = None,
) -> None:
    payload = {
        "event_id": event.event_id if event else None,
        "chat_id": recipient.chat_id if recipient else None,
        "source_topic": message.topic(),
        "source_partition": message.partition(),
        "source_offset": message.offset(),
        "reason": reason,
        "error_code": error_code,
        "raw_value": raw_value,
        "sent_at": utc_now(),
    }
    publish_json(
        producer,
        settings.dlq_topic,
        f"{message.topic()}:{message.partition()}:{message.offset()}:{recipient.chat_id if recipient else 'event'}",
        payload,
    )
    dlq_total.inc()


async def send_with_retry(
    telegram: TelegramClient,
    producer: Producer,
    message,
    event: TelegramEvent,
    recipient: Recipient,
    text: str,
    limiter: PerChatRateLimiter,
) -> None:
    attempt = 0
    async for retry_attempt in AsyncRetrying(
        retry=retry_if_exception_type(TemporaryTelegramError),
        wait=wait_exponential(multiplier=1, min=1, max=8),
        stop=stop_after_attempt(4),
        reraise=True,
    ):
        with retry_attempt:
            attempt = retry_attempt.retry_state.attempt_number
            if attempt > 1:
                retry_total.inc()
                publish_audit(producer, message, event, recipient, "retry", attempt)
            await limiter.wait(recipient.chat_id)
            await telegram.send_message(recipient.chat_id, text)
            sent_total.inc()
            publish_audit(producer, message, event, recipient, "sent", attempt)
            LOGGER.info(
                "telegram_message_sent",
                extra={
                    "_event_id": event.event_id,
                    "_chat_id": recipient.chat_id,
                    "_attempt": attempt,
                    "_topic": message.topic(),
                    "_partition": message.partition(),
                    "_offset": message.offset(),
                },
            )


async def process_message(
    consumer: Consumer,
    producer: Producer,
    telegram: TelegramClient,
    recipients: RecipientsStore,
    state: DeliveryState,
    limiter: PerChatRateLimiter,
    message,
) -> None:
    raw_value = message.value().decode("utf-8") if message.value() else None
    event: TelegramEvent | None = None
    try:
        payload = json.loads(raw_value or "{}")
        event = map_to_telegram_event(payload)
        text = format_telegram_message(event)
        enabled_recipients = recipients.enabled_recipients()
        if not enabled_recipients:
            LOGGER.warning("no_enabled_recipients", extra={"_event_id": event.event_id})

        for recipient in enabled_recipients:
            if state.is_sent(event.event_id, recipient.chat_id):
                deduplicated_total.inc()
                publish_audit(producer, message, event, recipient, "skipped", 0, error_message="duplicate")
                LOGGER.info(
                    "telegram_delivery_duplicate",
                    extra={"_event_id": event.event_id, "_chat_id": recipient.chat_id},
                )
                continue
            try:
                await send_with_retry(telegram, producer, message, event, recipient, text, limiter)
                state.mark_sent(event.event_id, recipient.chat_id)
            except PermanentTelegramError as exc:
                failed_total.inc()
                publish_audit(producer, message, event, recipient, "failed", 1, exc.error_code, str(exc))
                publish_dlq(producer, message, event, raw_value, str(exc), recipient, exc.error_code)
                LOGGER.error(
                    "telegram_delivery_permanent_failure",
                    extra={
                        "_event_id": event.event_id,
                        "_chat_id": recipient.chat_id,
                        "_error_code": exc.error_code,
                    },
                )
            except TemporaryTelegramError as exc:
                failed_total.inc()
                publish_audit(producer, message, event, recipient, "failed", 4, exc.error_code, str(exc))
                publish_dlq(producer, message, event, raw_value, str(exc), recipient, exc.error_code)
                LOGGER.error(
                    "telegram_delivery_retry_exhausted",
                    extra={
                        "_event_id": event.event_id,
                        "_chat_id": recipient.chat_id,
                        "_error_code": exc.error_code,
                    },
                )
        consumer.commit(message=message, asynchronous=False)
    except Exception as exc:
        failed_total.inc()
        publish_dlq(producer, message, event, raw_value, str(exc))
        consumer.commit(message=message, asynchronous=False)
        LOGGER.exception(
            "telegram_event_processing_failed",
            extra={
                "_topic": message.topic(),
                "_partition": message.partition(),
                "_offset": message.offset(),
            },
        )


async def consume(stop_event: asyncio.Event) -> None:
    ensure_topics([settings.input_topic, settings.audit_topic, settings.dlq_topic])
    recipients = RecipientsStore(settings.recipients_config, settings.reload_recipients_seconds)
    state = DeliveryState(settings.state_dir)
    consumer = Consumer(consumer_config("telegram-delivery-service"))
    producer = Producer(producer_config("telegram-delivery-service"))
    limiter = PerChatRateLimiter(settings.min_interval_per_chat_seconds)

    timeout = aiohttp.ClientTimeout(total=settings.telegram_timeout_seconds)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        telegram = TelegramClient(session)
        consumer.subscribe([settings.input_topic])
        ready.set(1)
        LOGGER.info(
            "telegram_delivery_started",
            extra={
                "_topic": settings.input_topic,
                "_audit_topic": settings.audit_topic,
                "_dlq_topic": settings.dlq_topic,
                "_dry_run": settings.telegram_dry_run,
            },
        )
        try:
            while not stop_event.is_set():
                message = await asyncio.to_thread(consumer.poll, 1.0)
                if message is None:
                    continue
                if message.error():
                    raise KafkaException(message.error())
                await process_message(consumer, producer, telegram, recipients, state, limiter, message)
        finally:
            ready.set(0)
            consumer.close()
            producer.flush(10)
            state.close()


async def main_async() -> None:
    configure_logging()
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signal_name in (signal.SIGINT, signal.SIGTERM):
        with suppress(NotImplementedError):
            loop.add_signal_handler(signal_name, stop_event.set)
    health_runner = await start_health_server(settings.metrics_port)
    try:
        await consume(stop_event)
    finally:
        await health_runner.cleanup()


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
