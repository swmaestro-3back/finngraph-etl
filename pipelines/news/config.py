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
    # 최신순. 테마 last_searched_at 을 워터마크로 삼아 페이지를 멈추려면 날짜 순이어야 한다.
    search_sort: str = Field(default="date", validation_alias="SEARCH_SORT")
    # 종목명으로 실제 검색어를 만드는 서식 목록. 기업마다 서식 수만큼 검색한다.
    # "특징주,{name}" 은 개별 종목 시세 기사를, "{name}" 은 그 밖의 기업 기사를 고른다.
    # env 는 JSON 배열로 준다 (예: ["특징주,{name}","{name}"]).
    search_query_templates: list[str] = Field(
        default=["특징주,{name}", "{name}"], validation_alias="NEWS_SEARCH_QUERY_TEMPLATES"
    )
    # 종목당 페이지 상한. 워터마크보다 오래된 기사가 나오면 그 전에 멈춘다.
    search_max_pages: int = Field(default=3, validation_alias="NEWS_SEARCH_MAX_PAGES")
    # 워터마크가 없는 종목(첫 검색)은 이 일수까지만 거슬러 수집한다.
    search_lookback_days: int = Field(default=180, validation_alias="NEWS_SEARCH_LOOKBACK_DAYS")
    # search_history.last_searched_at 이 이 간격을 넘긴 기업만 이번 런의 검색 대상이다.
    search_interval_hours: int = Field(default=2, validation_alias="NEWS_SEARCH_INTERVAL_HOURS")
    # 런마다 검색할 급등락 테마 수. 상승 상위 절반 + 하락 상위 절반.
    theme_count: int = Field(default=30, validation_alias="NEWS_THEME_COUNT")
    hot_themes_redis_url: str = Field(
        default="redis://localhost:16379/0", validation_alias="HOT_THEMES_REDIS_URL"
    )

    cluster_threshold: float = Field(default=0.35, validation_alias="NEWS_CLUSTER_THRESHOLD")
    cluster_description_weight: float = Field(
        default=0.4,
        validation_alias="NEWS_CLUSTER_DESCRIPTION_WEIGHT",
    )
    cluster_max_articles: int = Field(default=3, validation_alias="NEWS_CLUSTER_MAX_ARTICLES")

    cluster_window_days: int = Field(default=7, validation_alias="NEWS_CLUSTER_WINDOW_DAYS")

    cluster_keyword_count: int = Field(default=6, validation_alias="NEWS_CLUSTER_KEYWORD_COUNT")
    # 판정 기사 수(original_size)가 이 값 이상이 되면 LLM 으로 클러스터 이름을 짓는다.
    cluster_title_min_size: int = Field(default=3, validation_alias="NEWS_CLUSTER_TITLE_MIN_SIZE")
    # 클러스터 이름 길이 상한. 시스템 프롬프트에 주입되고 검증에도 같은 값을 쓴다.
    cluster_title_max_chars: int = Field(
        default=25, validation_alias="NEWS_CLUSTER_TITLE_MAX_CHARS"
    )
    # 제목과 같은 호출에서 만드는 1~2문장 요약의 길이 상한. 넘치면 요약만 버리고 제목은 저장한다.
    cluster_summary_max_chars: int = Field(
        default=150, validation_alias="NEWS_CLUSTER_SUMMARY_MAX_CHARS"
    )

    # 이슈 타임라인 연결(jobs/link_issues.py). 임계값은 초기값이라 라벨 평가셋으로 다시 맞춘다.
    # 스케줄 연결 스위치. 기존 클러스터의 요약·연결 백필이 끝난 뒤에 켠다 — 먼저 돌면 요약 없는
    # 임베딩이 굳고, 백필 전의 옛 클러스터를 부모로 못 봐 루트로 굳는다. 백필 스크립트는 무시한다.
    issue_link_enabled: bool = Field(default=False, validation_alias="NEWS_ISSUE_LINK_ENABLED")
    # 기업이 겹치는 앞선 클러스터와의 코사인 하한
    issue_link_threshold: float = Field(default=0.6, validation_alias="NEWS_ISSUE_LINK_THRESHOLD")
    # 기업이 없는 클러스터끼리는 기업 겹침이라는 안전판이 없어 더 엄격하게 본다.
    issue_link_no_company_threshold: float = Field(
        default=0.75, validation_alias="NEWS_ISSUE_LINK_NO_COMPANY_THRESHOLD"
    )
    # 부모 후보는 대상의 first_published_at 이전 이 일수 안, 연결 대상은 지금부터 이 일수 안
    issue_link_lookback_days: int = Field(
        default=90, validation_alias="NEWS_ISSUE_LINK_LOOKBACK_DAYS"
    )
    issue_link_max_per_run: int = Field(default=200, validation_alias="NEWS_ISSUE_LINK_MAX_PER_RUN")
    # 테마용 BEDROCK_EMBEDDING_MODEL 과 따로 둔다 — 그쪽은 질의 측(ai-server)과 묶여 있다.
    issue_embedding_model: str = Field(
        default="amazon.titan-embed-text-v2:0", validation_alias="NEWS_ISSUE_EMBEDDING_MODEL"
    )

    anchor_host: str = Field(default="", validation_alias="ANCHOR_HOST")

    official_source_threshold: int = Field(default=0, validation_alias="OFFICIAL_SOURCE_THRESHOLD")

    request_delay: float = Field(default=1.0, validation_alias="REQUEST_DELAY")
    # 본문 크롤링 동시 요청 수. 대상이 여러 언론사 페이지라 API 제한은 없고,
    # 이 값이 유일한 상한이다.
    news_body_fetch_workers: int = Field(default=8, validation_alias="NEWS_BODY_FETCH_WORKERS")

    news_llm_body_limit: int = Field(default=12000, validation_alias="NEWS_LLM_BODY_LIMIT")
    news_llm_max_tokens: int = Field(default=1024, validation_alias="NEWS_LLM_MAX_TOKENS")
    news_llm_max_concurrency: int = Field(default=4, validation_alias="NEWS_LLM_MAX_CONCURRENCY")
    # 관련성 필터가 한 번의 LLM 호출에 넣는 기사 수. 시스템 프롬프트 반복과 요청 수를 줄인다.
    news_llm_batch_size: int = Field(default=10, validation_alias="NEWS_LLM_BATCH_SIZE")
    news_llm_max_items_per_run: int = Field(
        default=100,
        validation_alias="NEWS_LLM_MAX_ITEMS_PER_RUN",
    )

    @field_validator("anchor_host", mode="after")
    @classmethod
    def _normalize_host(cls, value: str) -> str:
        return value.strip().lower()


@lru_cache
def get_news_settings() -> NewsSettings:
    return NewsSettings()


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
