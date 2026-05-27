from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    service_name: str = "notification-preferences-service"
    metrics_port: int = 8000
    database_url: str = "postgresql://preferences_user:preferences_pass@notification-preferences-db:5432/preferences_db"
    bootstrap_servers: str = "kafka1:12091,kafka2:12092"
    preferences_changed_topic: str = "user.preferences.changed"
    topic_partitions: int = 3

    model_config = SettingsConfigDict(env_file=None, extra="ignore")


settings = Settings()
