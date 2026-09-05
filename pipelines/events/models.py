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

    companies: list[str] = Field(
        description=(
            "후보 목록에 있는 이름만, 글자 그대로 담는다. 이 사건의 주체 또는 상대방"
            "(계약·투자·인수·소송·제재 등의 당사자)만 담는다. 주가 동반 등락, 경쟁사 비교, "
            "업계 배경으로 언급된 기업은 제외한다. 당사자가 후보에 없으면 빈 목록."
        )
    )
    title: str = Field(
        description=(
            "사건 전체를 아우르는 한국어 한 줄. 주체 기업명으로 시작한다. 40자 안팎. "
            '기사에 없는 숫자·사실을 만들지 않는다. 따옴표, 말줄임표, "[속보]" 류 태그, '
            "감탄 표현을 쓰지 않는다. 최신 전개 하나가 아니라 기사 전체가 다루는 사건을 "
            "이름 붙인다."
        )
    )


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
