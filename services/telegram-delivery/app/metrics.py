from prometheus_client import Counter, Gauge

sent_total = Counter("telegram_delivery_sent_total", "Messages sent to Telegram")
failed_total = Counter("telegram_delivery_failed_total", "Failed Telegram deliveries")
retry_total = Counter("telegram_delivery_retry_total", "Retried Telegram deliveries")
dlq_total = Counter("telegram_delivery_dlq_total", "Events sent to Telegram delivery DLQ")
deduplicated_total = Counter("telegram_delivery_deduplicated_total", "Duplicate deliveries skipped")
ready = Gauge("telegram_delivery_ready", "Service readiness state")
