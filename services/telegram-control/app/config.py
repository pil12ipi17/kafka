from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="", extra="ignore")

    bot_token: str = ""
    preferences_api_url: str = "http://notification-preferences-service:8000"
    metrics_port: int = 8000


settings = Settings()
