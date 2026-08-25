from __future__ import annotations

import logging
from functools import lru_cache
from pathlib import Path

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

ROOT_DIR = Path(__file__).resolve().parents[2]

ENV_FILES = (ROOT_DIR / ".env", ROOT_DIR / ".env.news")


class NewsSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=ENV_FILES, env_file_encoding="utf-8", extra="ignore")

    client_id: SecretStr = Field(default=SecretStr(""), validation_alias="CLIENT_ID")
    client_secret: SecretStr = Field(default=SecretStr(""), validation_alias="CLIENT_SECRET")
    api_base_url: str = Field(default="", validation_alias="API_BASE_URL")

    search_display: int = Field(default=100, validation_alias="SEARCH_DISPLAY")
    search_sort: str = Field(default="date", validation_alias="SEARCH_SORT")
    max_pages: int = Field(default=1, validation_alias="MAX_PAGES")

    search_queries: str = Field(default="특징주", validation_alias="NEWS_SEARCH_QUERIES")

    cluster_threshold: float = Field(default=0.35, validation_alias="NEWS_CLUSTER_THRESHOLD")
    cluster_description_weight: float = Field(
        default=0.4,
        validation_alias="NEWS_CLUSTER_DESCRIPTION_WEIGHT",
    )
    cluster_max_articles: int = Field(default=3, validation_alias="NEWS_CLUSTER_MAX_ARTICLES")

    anchor_host: str = Field(default="", validation_alias="ANCHOR_HOST")

    official_source_threshold: int = Field(default=0, validation_alias="OFFICIAL_SOURCE_THRESHOLD")

    request_delay: float = Field(default=1.0, validation_alias="REQUEST_DELAY")

    news_llm_body_limit: int = Field(default=12000, validation_alias="NEWS_LLM_BODY_LIMIT")
    news_llm_max_tokens: int = Field(default=512, validation_alias="NEWS_LLM_MAX_TOKENS")
    news_llm_max_concurrency: int = Field(default=4, validation_alias="NEWS_LLM_MAX_CONCURRENCY")
    news_llm_max_items_per_run: int = Field(
        default=100,
        validation_alias="NEWS_LLM_MAX_ITEMS_PER_RUN",
    )

    @field_validator("anchor_host", mode="after")
    @classmethod
    def _normalize_host(cls, value: str) -> str:
        return value.strip().lower()

    def search_query_list(self) -> list[str]:
        """콤마 구분 NEWS_SEARCH_QUERIES를 쿼리 목록으로 돌려준다."""
        return [query.strip() for query in self.search_queries.split(",") if query.strip()]


@lru_cache
def get_news_settings() -> NewsSettings:
    return NewsSettings()


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
