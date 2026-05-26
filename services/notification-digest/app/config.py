from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    service_name: str = "notification-digest-service"
    bootstrap_servers: str = "kafka1:12091,kafka2:12092"
    input_topic: str = "notifications.digest.pending"
    output_topic: str = "notifications.digest.ready"
    dlq_topic: str = "notifications.digest.dlq"
    consumer_group: str = "notification-digest-service"
    topic_partitions: int = 3
    flush_interval_seconds: int = 60
    max_events_per_digest: int = 5

    model_config = SettingsConfigDict(env_file=None, extra="ignore")


settings = Settings()
