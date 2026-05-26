from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    service_name: str = "notification-router-service"
    bootstrap_servers: str = "kafka1:12091,kafka2:12092"
    input_topic: str = "notifications.filtered"
    digest_ready_topic: str = "notifications.digest.ready"
    realtime_topic: str = "telegram.delivery.requested"
    digest_topic: str = "notifications.digest.pending"
    dlq_topic: str = "notifications.router.dlq"
    consumer_group: str = "notification-router-service"
    topic_partitions: int = 3

    model_config = SettingsConfigDict(env_file=None, extra="ignore")


settings = Settings()
