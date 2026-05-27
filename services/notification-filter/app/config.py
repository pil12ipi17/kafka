from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    service_name: str = "notification-filter-service"
    bootstrap_servers: str = "kafka1:12091,kafka2:12092"
    database_url: str = "postgresql://preferences_user:preferences_pass@notification-preferences-db:5432/preferences_db"
    input_topic: str = "notifications.classified"
    output_topic: str = "notifications.filtered"
    consumer_group: str = "notification-filter-service"
    topic_partitions: int = 3

    model_config = SettingsConfigDict(env_file=None, extra="ignore")


settings = Settings()
