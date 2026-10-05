"""이슈 타임라인 연결 판정 — 이슈(클러스터)를 같은 이야기의 앞선 이슈에 잇는다.

이슈 하나의 타임라인은 같은 이야기를 다룬 앞선 이슈의 사슬이다(실적 프리뷰 → 실적 발표, 7월
건설지출 → 8월 건설지출). 대상마다 부모 하나만 고르고, 부모의 story_root_id 를 물려받아 이야기
전체가 한 루트로 묶인다.

부모 조건: 대상보다 먼저 시작했고(first_published_at, 같으면 작은 id — 대상 처리 순서와 같다),
주요 기업이 하나 이상 겹치고, 임베딩 코사인이 임계값 이상. 기업이 없는 이슈(거시 지표 등)는 기업이
없는 이슈에만, 더 높은 임계값으로 잇는다 — 기업 겹침이라는 안전판이 없어서다. 후보 중 코사인 최대,
같으면 더 늦게 시작한 쪽(바로 앞 노드), 그래도 같으면 큰 id 가 부모다.

같은 사건이 클러스터 둘로 갈리는 일이 잦아서(시간 창·문서 표현 차이), 연결마다 관계를 붙인다.
백엔드는 same_event 묶음을 한 노드로 접는다. DB·Bedrock·LLM 은 만지지 않는다.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Protocol

from pipelines.common.gazetteer import CompanyMatch
from pipelines.news.transformers.summarizer import PointKind

# 임베딩 텍스트에 넣는 멤버 기사 제목 수. 받은 순서(저장소의 멤버 순서 — 후보 기사 먼저 발행
# 시각순)의 앞에서부터
EMBEDDING_ARTICLE_TITLES = 3

SAME_EVENT = "same_event"
FOLLOW_UP = "follow_up"

# 요약 문단의 문장 경계. summarizer 가 문장 수를 셀 때와 같은 규칙이다 — 종결 부호 뒤의 공백만
# 경계로 보고, 소수점(7.0%)처럼 공백이 따르지 않는 마침표는 경계가 아니다.
_SENTENCE_BREAK = re.compile(r"(?<=[.!?])\s+")


class TitleMatcher(Protocol):
    """제목 × 기업 개체 사전 매처(common.gazetteer.CompanyMatcher). 테스트는 가짜를 넣는다."""

    def extract(self, text: str) -> list[CompanyMatch]: ...


@dataclass(frozen=True)
class ClusterMember:
    """클러스터에 속한 기사 한 건의 제목과 그 기사에 연결된 기업(news_companies)."""

    title: str
    company_ids: frozenset[int] = field(default_factory=frozenset)


@dataclass(frozen=True)
class LinkCandidate:
    """부모 후보 이슈. score 는 대상과의 코사인, company_ids 는 후보의 주요 기업이다."""

    cluster_id: int
    first_published_at: datetime
    score: float
    company_ids: frozenset[int] = field(default_factory=frozenset)


@dataclass(frozen=True)
class LinkDecision:
    """parent_id 가 None 이면 새 이야기의 루트다(score·relation 도 None)."""

    parent_id: int | None = None
    score: float | None = None
    relation: str | None = None


def _collapse(text: str | None) -> str:
    return " ".join((text or "").split())


def node_summary(summary_points: Any, summary: str | None) -> str | None:
    """타임라인 노드의 한 줄 요약. 대표 기사 핵심 포인트의 CHANGE(무엇이 바뀌나요) 항목이 먼저다.

    CHANGE 가 없으면(사건이 없는 기사는 포인트가 빈 배열이다) 요약 문단의 첫 문장, 둘 다 없으면
    None 이다. summary_points 는 news.summary_points JSONB 를 읽은 값이라 형식을 믿지 않는다.
    """

    if isinstance(summary_points, list):
        for point in summary_points:
            if isinstance(point, dict) and point.get("kind") == PointKind.CHANGE.value:
                text = _collapse(point.get("text"))
                if text:
                    return text

    paragraph = _collapse(summary)
    if not paragraph:
        return None
    return _SENTENCE_BREAK.split(paragraph, maxsplit=1)[0]


def build_embedding_text(
    title: str,
    summary: str | None,
    article_titles: Sequence[str],
    max_titles: int = EMBEDDING_ARTICLE_TITLES,
) -> str:
    """제목, 노드 요약(있으면), 멤버 기사 제목(주어진 순서의 앞 max_titles 개)을 줄바꿈으로 잇는다.

    제목은 금액·수치를 뺀 사건 라벨이라 회차가 다른 같은 이야기를 잘 묶고, 요약과 기사 제목이
    당사자·지표 같은 맥락을 보탠다. 전재 기사의 같은 제목도 그대로 둔다 — 임계값을 고른
    시뮬레이션과 같은 텍스트여야 한다. 그래서 기사 제목은 승격 뒤에 바뀌지 않는 후보 기사에서
    고르도록 저장소가 순서를 정한다(SELECT_CLUSTER_MEMBERS_SQL).
    """

    lines = [_collapse(title)]
    if _collapse(summary):
        lines.append(_collapse(summary))
    titles = [_collapse(article_title) for article_title in article_titles]
    lines.extend([article_title for article_title in titles if article_title][:max_titles])
    return "\n".join(lines)


def to_vector_literal(values: Iterable[float]) -> str:
    """pgvector 텍스트 표현 '[x,y,...]'. 파이썬 패키지 없이 CAST(... AS vector) 로 넘긴다."""

    return "[" + ",".join(repr(float(value)) for value in values) + "]"


def primary_company_ids(members: Iterable[ClusterMember], matcher: TitleMatcher) -> frozenset[int]:
    """이슈의 주요 기업 — news_companies 기업 중 멤버 기사 제목 하나 이상에 나온 기업.

    news_companies 에는 본문에만 스친 거래처·경쟁사도 들어 있어, 그 합집합으로 겹침을 보면 남의
    사건끼리 이어진다. 제목에 나온 기업은 대개 사건의 주인공이다. 제목 매치는 수집 잡과 같은
    개체 사전 매처라 약칭도 같은 기업으로 잡는다. 제목에 아무 기업도 안 걸리면(표기가 사전에
    없는 경우 등) 주인공을 모르는 것이므로 전체 기업으로 돌아간다. 기업이 없는 이슈는 빈 집합이다.
    """

    members = list(members)
    linked = frozenset(company_id for member in members for company_id in member.company_ids)
    if not linked:
        return frozenset()

    mentioned = {
        match.entry.company_id for member in members for match in matcher.extract(member.title)
    }
    primary = linked & mentioned
    return frozenset(primary) if primary else linked


def is_eligible(
    candidate: LinkCandidate,
    *,
    cluster_id: int,
    first_published_at: datetime,
    company_ids: frozenset[int],
    threshold: float,
    no_company_threshold: float,
) -> bool:
    """후보가 대상의 부모가 될 수 있는가. 먼저 시작했고, 기업 규칙과 임계값을 넘어야 한다.

    "먼저"는 (first_published_at, id) 순서다 — 같은 시각에 시작한 두 클러스터(같은 사건이 갈린
    경우가 많다)도 한쪽이 다른 쪽을 부모로 볼 수 있다. company_ids 와 후보의 company_ids 는 둘 다
    주요 기업이다.
    """

    if (candidate.first_published_at, candidate.cluster_id) >= (first_published_at, cluster_id):
        return False
    if company_ids:
        return bool(candidate.company_ids & company_ids) and candidate.score >= threshold
    return not candidate.company_ids and candidate.score >= no_company_threshold


def choose_parent(
    candidates: Iterable[LinkCandidate],
    *,
    cluster_id: int,
    first_published_at: datetime,
    company_ids: Iterable[int],
    threshold: float,
    no_company_threshold: float,
) -> LinkCandidate | None:
    """조건을 넘는 후보 중 코사인 최대. 같으면 더 늦게 시작한 쪽(바로 앞 노드), 그다음 큰 id."""

    targets = frozenset(company_ids)
    eligible = [
        candidate
        for candidate in candidates
        if is_eligible(
            candidate,
            cluster_id=cluster_id,
            first_published_at=first_published_at,
            company_ids=targets,
            threshold=threshold,
            no_company_threshold=no_company_threshold,
        )
    ]
    if not eligible:
        return None

    return max(eligible, key=lambda c: (c.score, c.first_published_at, c.cluster_id))


def classify_relation(
    gap: timedelta,
    score: float,
    *,
    same_event_max_gap: timedelta,
    same_event_score: float,
    same_event_score_max_gap: timedelta,
) -> str:
    """부모와의 관계. gap 은 대상 first_published_at − 부모 first_published_at 이다.

    첫 기사가 가까우면(같은 날 같은 발표를 다른 표현으로 묶은 클러스터) 점수와 무관하게 같은 사건이
    갈린 것으로 본다. 내용이 아주 가까워도 같은 사건이지만, 간격이 same_event_score_max_gap 이내일
    때만이다. 한 사건이 클러스터 둘로 갈리는 일은 클러스터 시간 창 안에서 일어나는 반면, 시리즈
    지표의 다음 회차(7월 건설지출 → 8월 건설지출)는 제목·요약이 거의 같아 코사인이 높아도 몇 주
    떨어져 있다. 이 상한이 없으면 하한부터 점수 기준을 넘는 기업 없는 연결이 전부 한 노드로 접힌다.
    둘 다 아니면 시간을 두고 이어진 후속 사건이다.
    """

    if gap <= same_event_max_gap:
        return SAME_EVENT
    if score >= same_event_score and gap <= same_event_score_max_gap:
        return SAME_EVENT
    return FOLLOW_UP


def decide_link(
    candidates: Iterable[LinkCandidate],
    *,
    cluster_id: int,
    first_published_at: datetime,
    company_ids: Iterable[int],
    threshold: float,
    no_company_threshold: float,
    same_event_max_gap: timedelta,
    same_event_score: float,
    same_event_score_max_gap: timedelta,
) -> LinkDecision:
    """부모를 고르고 관계를 붙인다. 부모가 없으면 루트."""

    parent = choose_parent(
        candidates,
        cluster_id=cluster_id,
        first_published_at=first_published_at,
        company_ids=company_ids,
        threshold=threshold,
        no_company_threshold=no_company_threshold,
    )
    if parent is None:
        return LinkDecision()

    relation = classify_relation(
        first_published_at - parent.first_published_at,
        parent.score,
        same_event_max_gap=same_event_max_gap,
        same_event_score=same_event_score,
        same_event_score_max_gap=same_event_score_max_gap,
    )
    return LinkDecision(parent_id=parent.cluster_id, score=parent.score, relation=relation)
