"""events 파이프라인 설정."""

from __future__ import annotations

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from pipelines.common.config import ENV_FILE


class EventSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=ENV_FILE, env_file_encoding="utf-8", extra="ignore")

    # 후보 조회 범위: updated_at 이 이 일수 안인 클러스터만 본다 (클러스터 창 7 + 여유).
    # 승격 기준은 news 설정의 NEWS_CLUSTER_PROMOTE_SIZE 를 그대로 쓴다.
    scan_days: int = Field(default=15, validation_alias="NEWS_EVENT_SCAN_DAYS")
    # HAS_EVENT 로 잇는 기업: 클러스터 후보 기사 중 이 수 이상에 연결된 기업만.
    # 사건의 당사자는 후보 대부분에 나오고, 지나가는 언급은 한두 건에만 나온다.
    company_min_articles: int = Field(default=2, validation_alias="NEWS_EVENT_COMPANY_MIN_ARTICLES")


@lru_cache
def get_event_settings() -> EventSettings:
    return EventSettings()
