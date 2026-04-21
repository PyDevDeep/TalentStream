"""
File: app/config.py
Task: 0.1.3 - Create app/config.py
Dependencies: pydantic, pydantic-settings
"""

from functools import lru_cache
from typing import List, Optional

from pydantic import PostgresDsn, RedisDsn, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    environment: str = "development"
    log_level: str = "INFO"

    # Database & Broker
    database_url: PostgresDsn
    redis_url: RedisDsn

    # External APIs
    serper_api_key: SecretStr
    openai_api_key: SecretStr
    gemini_api_key: SecretStr

    # Notifications
    slack_bot_token: SecretStr
    slack_channel_id: str

    # Scraper Logic
    scrape_query: str
    scrape_interval_minutes: int = 60
    dedup_ttl_seconds: int = 86400

    # Filtering
    filter_keywords: List[str] = []
    filter_location: Optional[str] = None
    filter_salary_min: Optional[int] = None

    # Monitoring
    sentry_dsn: Optional[str] = None

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


@lru_cache
def get_settings() -> Settings:
    return Settings.model_validate({})
