# Что добавлено поверх Confluent cp-demo

## Контекст

За основу взят официальный стенд Confluent `cp-demo`. Он используется как готовая Kafka-инфраструктура: Kafka brokers, Schema Registry, Kafka Connect, ksqlDB, Control Center, Elasticsearch и Kibana.

Поверх этого стенда добавлена отдельная Python-цепочка, которая демонстрирует микросервисный event-driven сценарий:

```text
python-producer-service
  -> python.orders.raw
  -> python-consumer-service
  -> python.orders.processed

ошибочные сообщения:
  -> python.orders.dlq
```

## Основные добавленные артефакты

- `docker-compose.python.yml` - compose override для запуска Python producer и consumer рядом с `cp-demo`.
- `services/python-chain/` - исходный код Python-сервисов.
- `services/python-chain/app/producer.py` - producer, публикующий события заказов в Kafka.
- `services/python-chain/app/consumer.py` - consumer, валидирующий и обрабатывающий события.
- `services/python-chain/app/kafka_io.py` - конфигурация Kafka producer/consumer и создание topics.
- `services/python-chain/app/schema_registry.py` - регистрация JSON Schema и проверка совместимости `v2`.
- `services/python-chain/schemas/` - JSON Schema контракты событий.
- `services/python-chain/app/send_test_event.py` - ручные сценарии проверки `valid`, `invalid`, `temporary-failure`, `duplicate`.
- `services/python-chain/app/metrics.py` - Prometheus-метрики.
- `services/python-chain/app/logging_json.py` - структурированные JSON-логи.
- `scripts/start-python.ps1` - запуск Python-сервисов поверх поднятого `cp-demo`.
- `scripts/start-cpdemo-core.ps1` - Windows-friendly запуск core-части `cp-demo`.
- `scripts/elasticsearch-ksqldb.json` - конфигурация Elasticsearch sink connector для витрины `wikipediabot`.
- `docs/pipeline-map.md` - карта общего пайплайна.

## Что реализовано в Python-цепочке

Producer:

- генерирует события `order_created`;
- пишет в topic `python.orders.raw`;
- использует Kafka message key = `user_id`;
- включает `acks=all`, `enable.idempotence=True`, `retries=10`;
- логирует `event_id`, `trace_id`, topic, partition, offset;
- публикует метрику `python_chain_produced_total`.

Consumer:

- читает topic `python.orders.raw`;
- использует consumer group `python-order-consumer`;
- отключает auto commit и коммитит offset вручную;
- валидирует payload по JSON Schema;
- поддерживает версии события `v1` и `v2`;
- публикует успешный результат в `python.orders.processed`;
- отправляет невалидные или окончательно упавшие сообщения в `python.orders.dlq`;
- реализует retry с exponential backoff для временных ошибок;
- реализует идемпотентность по `event_id` через SQLite state store;
- логирует причину отправки в DLQ;
- публикует метрики `processed`, `failed`, `dlq_sent`.

## Контракты событий

Выбран формат JSON Schema.

Схемы:

- `order_event_v1.json` - базовый контракт входного события заказа.
- `order_event_v2.json` - совместимое изменение: добавлено optional/nullable поле `coupon_code` с default `null`.
- `processed_order_event.json` - контракт результата обработки.

При старте сервисы:

1. регистрируют `v1` в Schema Registry;
2. проверяют совместимость `v2` с последней версией subject;
3. регистрируют `v2`;
4. регистрируют схему processed event.

## Надежность обработки

Реализованы базовые механики надежности:

- manual commit после успешной обработки или публикации в DLQ;
- retry для временных ошибок через `tenacity`;
- DLQ topic для невалидных или неисправимых сообщений;
- явное логирование причины DLQ;
- идемпотентность по `event_id`;
- сохранение state consumer-а в Docker volume `python-consumer-state`.

## Наблюдаемость

Логи пишутся в JSON и содержат ключевые поля:

- `event_id`;
- `trace_id`;
- `topic`;
- `partition`;
- `offset`;
- `reason` для DLQ.

Метрики:

- producer: `http://localhost:18000/metrics`;
- consumer: `http://localhost:18001/metrics`.

Основные счетчики:

- `python_chain_produced_total`;
- `python_chain_processed_total`;
- `python_chain_failed_total`;
- `python_chain_dlq_sent_total`.

## Ручные проверки

Команды:

```powershell
docker exec python-producer-service python -m app.send_test_event valid
docker exec python-producer-service python -m app.send_test_event invalid
docker exec python-producer-service python -m app.send_test_event temporary-failure
docker exec python-producer-service python -m app.send_test_event duplicate
```

Ожидаемое поведение:

- `valid` попадает в `python.orders.processed`;
- `invalid` попадает в `python.orders.dlq`;
- `temporary-failure` ретраится с backoff, затем попадает в DLQ;
- `duplicate` создает два сообщения с одним `event_id`, но consumer обрабатывает только первое.

Уже наблюдавшийся лог для `invalid`:

```json
{
  "message": "sent_to_dlq",
  "reason": "'user_id' is a required property",
  "topic": "python.orders.raw",
  "partition": 1,
  "offset": 5958
}
```

Это подтверждает, что schema validation сработала, сообщение без `user_id` не обработалось как valid и было отправлено в DLQ.

## Связь с требованиями задания

Закрытые пункты:

- Python producer + consumer;
- key-based routing по `user_id`;
- controlled/manual commit;
- JSON Schema contracts;
- Schema Registry registration;
- schema evolution `v1 -> v2`;
- compatibility check для `v2`;
- retry с exponential backoff;
- DLQ;
- идемпотентность на consumer-стороне;
- structured logs;
- Prometheus metrics;
- compose override для запуска дополнительных сервисов;
- pipeline map.

## Текущие замечания

- `cp-demo` тяжелый для локального Docker Desktop, после перезапуска Docker контейнеры могут стартовать медленно.
- `scripts/start-cpdemo-core.ps1` доработан: теперь он ждет readiness/health ключевых контейнеров вместо фиксированного `Start-Sleep`.
- Для полной проверки Block A PowerShell-скрипт поднимает `elasticsearch` и `kibana`, а также регистрирует Elasticsearch sink connector `elasticsearch-ksqldb`.
