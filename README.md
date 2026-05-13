# Kafka cp-demo + Python Practice

Решение состоит из двух независимых частей:

- Block A: официальный Confluent `cp-demo`.
- Block B: Python producer/consumer поверх того же Kafka-кластера.

## Prerequisites

- Docker / Docker Compose
- Git

## Block A: cp-demo

Официальный стенд лежит в `cp-demo`.

На Windows удобнее запускать официальный сценарий через Git Bash, потому что `cp-demo` готовит сертификаты, RBAC и коннекторы через shell-скрипты:

```bash
cd cp-demo
./scripts/start.sh
docker compose ps
```

Если Git Bash/WSL недоступны, можно поднять core-часть стенда PowerShell-скриптом:

```powershell
.\scripts\start-cpdemo-core.ps1
```

Проверки:

- Control Center: http://localhost:9021
- Kibana: http://localhost:5601
- Schema Registry:

```powershell
curl.exe -k -u superUser:superUser https://localhost:8085/subjects
```

Поток данных `cp-demo`:

- Wikimedia EventStreams SSE является источником событий.
- Kafka Connect source connector `kafka-connect-sse` читает SSE.
- События попадают в Kafka.
- ksqlDB и Kafka Streams выполняют обработку.
- Kafka Connect Elasticsearch sink connector пишет результат в Elasticsearch.
- Kibana показывает витрины, Control Center показывает состояние Kafka-контура.

Карта пайплайна: `docs/pipeline-map.md`.

Локально проверено:

- `wikipedia-sse` connector в состоянии `RUNNING`;
- `elasticsearch-ksqldb` sink connector пишет `WIKIPEDIABOT` в индекс `wikipediabot`;
- `wikipedia.parsed` получает сообщения;
- ksqlDB создал `WIKIPEDIABOT`, `WIKIPEDIANOBOT`, `WIKIPEDIA_COUNT_GT_1`;
- Control Center доступен на http://localhost:9021.
- Kibana доступна на http://localhost:5601.

## Block B: Python services

Python-сервисы добавляются compose override-файлом `docker-compose.python.yml`.

Запуск после старта `cp-demo`:

```powershell
.\scripts\start-python.ps1
```

Скрипт сначала выполняет one-shot bootstrap схем:

```powershell
docker compose ... run --rm --build python-producer-service python -m app.schema_bootstrap
```

После этого он поднимает runtime-сервисы producer/consumer. Сами runtime-сервисы не регистрируют схемы при каждом старте.

Сервисы:

- `python-producer-service`: пишет события в `python.orders.raw`.
- `python-consumer-service`: читает `python.orders.raw`, валидирует, пишет в `python.orders.processed`.
- DLQ topic: `python.orders.dlq`.

Python-клиенты внутри compose-сети подключаются к Kafka через plaintext listeners:

```text
kafka1:12091,kafka2:12092
```

## Event Contracts

Выбран JSON Schema.

Схемы:

- `services/python-chain/schemas/order_event_v1.json`
- `services/python-chain/schemas/order_event_v2.json`
- `services/python-chain/schemas/processed_order_event.json`

Совместимое изменение `v2`: добавлено поле `coupon_code` с типом `string|null` и default `null`. Consumer поддерживает `v1` и `v2`.

Отдельная bootstrap-команда регистрирует `v1`, проверяет совместимость `v2` с последней версией subject в Schema Registry и только после этого регистрирует `v2`.

## Kafka concepts in this project

- Topic: `python.orders.raw`, `python.orders.processed`, `python.orders.dlq` разделяют входные, обработанные и ошибочные события.
- Partition: количество задается через `PYTHON_TOPIC_PARTITIONS`; оно влияет на параллелизм и порядок обработки.
- Producer: `python-producer-service` публикует события заказов в Kafka.
- Consumer: `python-consumer-service` читает входной topic, валидирует payload и публикует результат.
- Consumer group: `python-order-consumer` хранит прогресс чтения consumer-сервиса.
- Offset: consumer коммитит offset вручную только после успешной обработки или отправки в DLQ.
- Kafka как распределенный лог: события не вызывают сервис напрямую, а последовательно записываются в topic и читаются подписчиками.
- Key-based routing: ключом сообщения выбран `user_id`, поэтому события одного пользователя попадают в одну partition и сохраняют порядок внутри нее.
- Broker: Python-сервисы подключаются к `kafka1:12091,kafka2:12092`.
- Replication: в локальном Python-контуре topics создаются с `replication_factor=1`, потому что это учебный stand-alone сценарий поверх cp-demo.
- Acks: producer использует `acks=all` и `enable.idempotence=True`, чтобы Kafka подтверждала запись надежнее.

## Reliability

Consumer использует:

- manual commit: offset коммитится после успешной обработки или публикации в DLQ;
- retry with exponential backoff для временных ошибок;
- DLQ для невалидных или неисправимых сообщений;
- SQLite state store для идемпотентности по `event_id`.

Повторное чтение уже обработанного `event_id` не создает повторный processed event. Если процесс упадет после публикации в `python.orders.processed`, но до записи state/commit offset, повторная публикация будет иметь тот же deterministic `processed.event_id` и Kafka key = `source_event_id`, чтобы downstream мог дедуплицировать или использовать compacted topic по бизнес-ключу.

Ручные проверки:

```powershell
docker exec python-producer-service python -m app.send_test_event valid
docker exec python-producer-service python -m app.send_test_event invalid
docker exec python-producer-service python -m app.send_test_event temporary-failure
docker exec python-producer-service python -m app.send_test_event duplicate
```

Ожидаемый результат:

- `valid` попадает в `python.orders.processed`;
- `invalid` попадает в `python.orders.dlq`;
- `temporary-failure` ретраится с backoff, затем попадает в DLQ;
- `duplicate` публикует два события с одним `event_id`, consumer обрабатывает только первое.

## Observability

Structured logs содержат:

- `event_id`
- `trace_id`
- `topic`
- `partition`
- `offset`

Prometheus metrics:

- Producer: http://localhost:18000/metrics
- Consumer: http://localhost:18001/metrics

Основные метрики:

- `python_chain_processed_total`
- `python_chain_failed_total`
- `python_chain_dlq_sent_total`
- `python_chain_produced_total`

Пример проверенной выгрузки через `/metrics`:

```text
python_chain_processed_total 892
python_chain_failed_total 4
python_chain_dlq_sent_total 4
```

## Partition Experiment

Запуск с одной партицией:

```powershell
$env:PYTHON_TOPIC_PARTITIONS="1"
.\scripts\start-python.ps1
```

Запуск с несколькими партициями:

```powershell
$env:PYTHON_TOPIC_PARTITIONS="3"
.\scripts\start-python.ps1
```

Ожидаемое поведение:

- При `N=1` все ключи попадают в одну партицию.
- При `N>1` `user_id` распределяет сообщения по партициям.
- Одинаковый `user_id` сохраняет порядок внутри своей партиции.

Для повторного эксперимента с другим числом партиций проще удалить demo topics и создать их заново, так как Kafka не уменьшает количество партиций существующего topic.

## Local Checks

Проверить сборку compose-конфига:

```powershell
.\scripts\compose-config.ps1
```
