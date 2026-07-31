from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

ENV_FILE = Path(__file__).resolve().parents[2] / ".env"


class Settings(BaseSettings):
    # .env 파일을 읽어오기 위한 설정
    model_config = SettingsConfigDict(env_file=ENV_FILE, env_file_encoding="utf-8", extra="ignore")

    database_url: str = Field(
        default="postgresql+psycopg://etl:etl@localhost:15432/etl",
        validation_alias="DATABASE_URL",
    )

    kis_app_key: str = Field(default="", validation_alias="KIS_APP_KEY")
    kis_app_secret: str = Field(default="", validation_alias="KIS_APP_SECRET")
    kis_account_no: str = Field(default="", validation_alias="KIS_ACCOUNT_NO")
    kis_rate_limit_per_second: int = Field(default=15, validation_alias="KIS_RATE_LIMIT_PER_SECOND")

    stock_intraday_retention_days: int = Field(
        default=60,
        validation_alias="STOCK_INTRADAY_RETENTION_DAYS",
    )
    stock_intraday_target_delay_minutes: int = Field(
        default=5,
        validation_alias="STOCK_INTRADAY_TARGET_DELAY_MINUTES",
    )

    gemini_model: str = Field(default="", validation_alias="GEMINI_MODEL")
    google_api_key: str = Field(default="", validation_alias="GOOGLE_API_KEY")

    neo4j_uri: str = Field(default="", validation_alias="NEO4J_URI")
    neo4j_username: str = Field(default="", validation_alias="NEO4J_USERNAME")
    neo4j_password: str = Field(default="", validation_alias="NEO4J_PASSWORD")
    neo4j_database: str = Field(default="", validation_alias="NEO4J_DATABASE")


@lru_cache
def get_settings() -> Settings:
    return Settings()
