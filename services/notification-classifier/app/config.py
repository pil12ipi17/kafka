from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    service_name: str = "notification-classifier-service"
    bootstrap_servers: str = "kafka1:12091,kafka2:12092"
    input_topic: str = "wikimedia.telegram.ready"
    output_topic: str = "notifications.classified"
    dlq_topic: str = "notifications.classifier.dlq"
    consumer_group: str = "notification-classifier-service"
    topic_partitions: int = 3

    model_config = SettingsConfigDict(env_file=None, extra="ignore")


settings = Settings()
