from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="", extra="ignore")

    bootstrap_servers: str = "kafka1:12091,kafka2:12092"
    input_topic: str = "telegram.delivery.requested"
    audit_topic: str = "telegram.delivery.audit"
    dlq_topic: str = "telegram.delivery.dlq"
    consumer_group: str = "telegram-delivery-service"
    topic_partitions: int = 3

    bot_token: str = ""
    telegram_api_base_url: str = "https://api.telegram.org"
    telegram_parse_mode: str = "HTML"
    telegram_dry_run: bool = True
    telegram_timeout_seconds: float = 10.0
    preferences_api_url: str = ""

    recipients_config: str = "/app/config/recipients.json"
    reload_recipients_seconds: float = 5.0

    min_interval_per_chat_seconds: float = 1.0
    state_dir: str = "/app/state"
    metrics_port: int = 8000


settings = Settings()
