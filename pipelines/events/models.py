"""events 파이프라인의 데이터 모델.

EventDraft 는 LLM 구조화 출력 스키마라 Field description 이 곧 프롬프트 규칙이다.
"""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field


class MemberArticle(BaseModel):
    """클러스터에 저장된 기사 한 건 (news 행)."""

    news_id: int
    title: str
    summary: str | None = None
    text: str
    published_at: datetime | None = None


class ClusterCandidate(BaseModel):
    """승격 후보 클러스터 (news_clusters 행 스냅샷)."""

    cluster_id: int
    representative_news_id: int | None
    keywords: list[str]
    original_size: int
    member_count: int
    first_published_at: datetime
    last_published_at: datetime


class EventDraft(BaseModel):
    """LLM 구조화 출력. companies 를 앞에 두어 당사자를 먼저 특정하고 제목을 쓰게 한다."""

    companies: list[str] = Field(description="후보 목록에서 글자 그대로 고른 사건 당사자.")
    title: str = Field(description="사건 이름을 붙인 짧은 한국어 명사구.")


class EventRefresh(BaseModel):
    """refresh_events 입력 = ClusterCandidate + 저장 멤버 news_ids."""

    cluster_id: int
    member_count: int
    original_size: int
    first_published_at: datetime
    last_published_at: datetime
    representative_news_id: int | None
    news_ids: list[int]
    keywords: list[str]


class EventRecord(EventRefresh):
    """create_event 입력 = EventRefresh + 검증된 draft."""

    title: str
    companies: list[str]
