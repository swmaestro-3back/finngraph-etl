from __future__ import annotations

import json
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

    keyword_search_batch_size: int = Field(default=50, validation_alias="KEYWORD_SEARCH_BATCH_SIZE")

    search_queries: str = Field(default="특징주", validation_alias="NEWS_SEARCH_QUERIES")

    cluster_threshold: float = Field(default=0.35, validation_alias="NEWS_CLUSTER_THRESHOLD")
    cluster_description_weight: float = Field(
        default=0.4,
        validation_alias="NEWS_CLUSTER_DESCRIPTION_WEIGHT",
    )
    cluster_max_articles: int = Field(default=3, validation_alias="NEWS_CLUSTER_MAX_ARTICLES")

    anchor_host: str = Field(default="", validation_alias="ANCHOR_HOST")
    anchor_url_template: str = Field(default="", validation_alias="ANCHOR_URL_TEMPLATE")
    anchor_categories: dict[int, str] = Field(
        default_factory=dict,
        validation_alias="ANCHOR_CATEGORIES",
    )
    more_api_path_section: str = Field(default="", validation_alias="MORE_API_PATH_SECTION")
    more_api_path_latest: str = Field(default="", validation_alias="MORE_API_PATH_LATEST")
    parent_section_id: str = Field(default="", validation_alias="PARENT_SECTION_ID")
    headline_selector: str = Field(default="", validation_alias="HEADLINE_SELECTOR")

    max_total_collected_items: int = Field(
        default=100,
        validation_alias="MAX_TOTAL_COLLECTED_ITEMS",
    )

    official_source_threshold: int = Field(default=0, validation_alias="OFFICIAL_SOURCE_THRESHOLD")

    request_delay: float = Field(default=1.0, validation_alias="REQUEST_DELAY")

    headline_more_count: int = Field(default=3, validation_alias="HEADLINE_MORE_COUNT")

    material_event_filter_fail_open: bool = Field(
        default=False,
        validation_alias="MATERIAL_EVENT_FILTER_FAIL_OPEN",
    )

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

    @field_validator("anchor_url_template", mode="after")
    @classmethod
    def _strip_template(cls, value: str) -> str:
        return value.strip()

    @field_validator("anchor_categories", mode="before")
    @classmethod
    def _parse_categories(cls, value: object) -> dict[int, str]:

        if value is None or value == "":
            return {}

        if isinstance(value, dict):
            parsed: object = value
        elif isinstance(value, str):
            stripped = value.strip()

            if not stripped:
                return {}

            try:
                parsed = json.loads(stripped)
            except json.JSONDecodeError as e:
                raise ValueError("ANCHOR_CATEGORIES 환경 변수는 올바른 JSON이어야 합니다.") from e
        else:
            raise ValueError("ANCHOR_CATEGORIES는 JSON 문자열이어야 합니다.")

        if not isinstance(parsed, dict):
            raise ValueError("ANCHOR_CATEGORIES 환경 변수는 JSON 객체여야 합니다.")

        try:
            return {int(key): str(name) for key, name in parsed.items()}
        except (TypeError, ValueError) as e:
            raise ValueError("ANCHOR_CATEGORIES의 카테고리 ID는 정수여야 합니다.") from e

    def search_query_list(self) -> list[str]:
        """콤마 구분 NEWS_SEARCH_QUERIES를 쿼리 목록으로 돌려준다."""
        return [query.strip() for query in self.search_queries.split(",") if query.strip()]


@lru_cache
def get_news_settings() -> NewsSettings:
    return NewsSettings()


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
