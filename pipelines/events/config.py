"""events 파이프라인 설정. LLM 접속(BEDROCK_*)은 공용 Settings 가 담당한다."""

from __future__ import annotations

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from pipelines.common.config import ENV_FILE


class EventSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=ENV_FILE, env_file_encoding="utf-8", extra="ignore")

    # 승격 기준: news_clusters.original_size (버린 기사 포함 판정 수) 하한
    min_size: int = Field(default=5, validation_alias="NEWS_EVENT_MIN_SIZE")
    # 후보 조회 범위: updated_at 이 이 일수 안인 클러스터만 본다 (클러스터 윈도우 14 + 1)
    scan_days: int = Field(default=15, validation_alias="NEWS_EVENT_SCAN_DAYS")
    # 런당 LLM 생성 상한. 후보 추출 뒤에 적용한다
    max_items_per_run: int = Field(default=50, validation_alias="NEWS_EVENT_MAX_ITEMS_PER_RUN")
    llm_max_concurrency: int = Field(default=4, validation_alias="NEWS_EVENT_LLM_MAX_CONCURRENCY")
    # 요약이 없는 기사는 본문 앞 이 글자 수를 LLM 입력으로 쓴다
    lead_chars: int = Field(default=600, validation_alias="NEWS_EVENT_LEAD_CHARS")
    # 검증: 이보다 긴 제목은 실패 처리
    title_max_chars: int = Field(default=30, validation_alias="NEWS_EVENT_TITLE_MAX_CHARS")


@lru_cache
def get_event_settings() -> EventSettings:
    return EventSettings()
