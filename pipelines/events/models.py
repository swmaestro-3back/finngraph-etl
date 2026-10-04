"""events 파이프라인의 데이터 모델."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel


class ClusterCandidate(BaseModel):
    """Event 로 올릴 클러스터 (news_clusters 행 스냅샷). 대표와 제목이 있는 것만 후보다."""

    cluster_id: int
    # cluster_articles 잡이 후보 기사 제목들로 지은 사건 이름 (news_clusters.title)
    title: str
    first_published_at: datetime


class EventRecord(ClusterCandidate):
    """create_event 입력 = ClusterCandidate + 당사자 기업(companies.id)."""

    company_ids: list[int]
