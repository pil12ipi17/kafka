from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="", extra="ignore")

    bootstrap_servers: str = "kafka1:12091,kafka2:12092"
    schema_registry_url: str = "https://schemaregistry:8085"
    schema_registry_user_info: str = "superUser:superUser"

    input_topic: str = "python.orders.raw"
    output_topic: str = "python.orders.processed"
    dlq_topic: str = "python.orders.dlq"
    consumer_group: str = "python-order-consumer"

    topic_partitions: int = 3
    producer_interval_seconds: float = 1.0
    producer_schema_version: str = "v2"
    producer_batch_size: int = 1

    metrics_port: int = 8000
    state_dir: str = "/app/state"


settings = Settings()
