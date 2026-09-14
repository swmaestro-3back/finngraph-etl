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
    # collect_articles 가 지은 사건 이름(news_clusters.title). 없으면 아직 승격하지 않는다.
    title: str | None = None


class EventDraft(BaseModel):
    """LLM 구조화 출력 — 사건 당사자만. 제목은 news_clusters.title 을 그대로 쓴다."""

    companies: list[str] = Field(description="후보 목록에서 글자 그대로 고른 사건 당사자.")


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
    """create_event 입력 = EventRefresh + 클러스터 제목 + 검증된 당사자."""

    title: str
    companies: list[str]
