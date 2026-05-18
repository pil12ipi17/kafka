# Telegram delivery pipeline

This extension adds the final delivery step to the Kafka pipeline.

## Flow

```text
Wikimedia EventStreams
  -> wikipedia.parsed
  -> ksqlDB stream WIKIMEDIA_TELEGRAM_READY
  -> wikimedia.telegram.ready
  -> telegram-delivery-service
  -> Telegram Bot API
  -> telegram.delivery.audit
  -> telegram.delivery.dlq
```

`wikimedia.telegram.ready` contains a compact JSON payload prepared for a human notification channel.

## Service responsibility

`telegram-delivery-service`:

- consumes `wikimedia.telegram.ready`;
- loads recipients from JSON config;
- skips disabled recipients;
- renders an HTML Telegram message with emoji and escaped fields;
- sends the message through Telegram Bot API or dry-run mode;
- rate-limits delivery per `chat_id`;
- deduplicates already sent `(event_id, chat_id)` pairs in SQLite;
- publishes every delivery result to `telegram.delivery.audit`;
- publishes permanently failed or retry-exhausted deliveries to `telegram.delivery.dlq`.

## Recipients

Copy the example config before using a real bot:

```powershell
Copy-Item services\telegram-delivery\config\recipients.example.json services\telegram-delivery\config\recipients.json
```

`recipients.json` is ignored by Git because it may contain private chat IDs.

```json
[
  {
    "chat_id": "123456789",
    "display_name": "Timofey",
    "enabled": true
  }
]
```

## Local dry run

Dry run is enabled by default and does not call Telegram:

```powershell
.\scripts\create-telegram-ready-topic.ps1
.\scripts\start-telegram.ps1
docker exec telegram-delivery-service python -m app.send_test_event valid
docker logs telegram-delivery-service --tail 50
```

Metrics and health:

```text
http://localhost:18012/healthz
http://localhost:18012/readyz
http://localhost:18012/metrics
```

## Real Telegram delivery

Set a bot token and disable dry-run:

```powershell
$env:BOT_TOKEN = "<telegram-bot-token>"
$env:TELEGRAM_DRY_RUN = "false"
$env:TELEGRAM_RECIPIENTS_CONFIG = "D:\Projects\Fortech\kafka\services\telegram-delivery\config\recipients.json"
.\scripts\start-telegram.ps1
```

## Audit and DLQ

Audit events include:

- `event_id`
- `chat_id`
- `delivery_status`
- `attempt`
- `error_code`
- `error_message`
- `processed_at`
- source topic, partition, and offset

DLQ events preserve the original Kafka payload and delivery error context.
