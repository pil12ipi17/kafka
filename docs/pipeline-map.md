# Kafka Pipeline Map

```mermaid
flowchart LR
    Wikimedia[Wikimedia EventStreams SSE] --> SourceConnector[kafka-connect-sse source connector]
    SourceConnector --> RawTopics[Kafka raw Wikipedia topics]
    RawTopics --> Ksql[ksqlDB processing]
    RawTopics --> Streams[Kafka Streams demo app]
    Ksql --> DerivedTopics[Derived Kafka topics]
    Streams --> DerivedTopics
    DerivedTopics --> SinkConnector[Elasticsearch sink connector]
    SinkConnector --> Elasticsearch[(Elasticsearch)]
    Elasticsearch --> Kibana[Kibana dashboards]
    RawTopics --> ControlCenter[Control Center monitoring]

    PythonProducer[python-producer-service] --> PythonRaw[python.orders.raw]
    PythonRaw --> PythonConsumer[python-consumer-service]
    PythonConsumer --> PythonProcessed[python.orders.processed]
    PythonConsumer --> PythonDlq[python.orders.dlq]
    PythonProducer --> SchemaRegistry[Schema Registry JSON schemas v1/v2]
    PythonConsumer --> SchemaRegistry
```

## cp-demo flow

- Source: Wikimedia EventStreams, real-time page edits.
- Ingest: Kafka Connect SSE source connector reads the SSE stream.
- Processing: ksqlDB and the bundled Kafka Streams application derive enriched/aggregated topics.
- Sink: Kafka Connect Elasticsearch sink connector materializes processed data.
- Visualization: Kibana reads the Elasticsearch indices; Control Center shows Kafka/Connect/ksqlDB health and topics.

## Python flow

- `python-producer-service` writes JSON events to `python.orders.raw`.
- Message key is `user_id`, so equal users are routed to the same partition.
- `python-consumer-service` manually commits offsets only after validation, processing, idempotency marking, or DLQ publication.
- Valid events become `python.orders.processed`.
- Invalid or permanently failed events go to `python.orders.dlq` with an explicit `reason`.
