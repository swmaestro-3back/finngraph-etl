"""이슈 타임라인 연결 판정: 이슈(클러스터)를 같은 타임라인의 앞선 이슈에 잇는다.

타임라인은 서로 이어지는 이슈들을 시작 순서대로 이은 것이다(실적 프리뷰 → 실적 발표, 7월
건설지출 → 8월 건설지출). 대상마다 부모 하나만 고르고 부모의 story_root_id 를 물려받으므로,
타임라인 전체가 루트(부모가 없는 타임라인 첫 이슈) 하나로 묶인다.

부모가 되려면 대상보다 먼저 시작했고(first_published_at 순서이며, 같으면 id 가 작은 쪽이 먼저라
대상 처리 순서와 같다), 주요 기업이 하나 이상 겹치고, 임베딩 코사인이 임계값 이상이어야 한다.
기업이 없는 이슈(거시 지표 등)는 기업 겹침 조건으로 거를 수 없으므로, 기업이 없는 이슈끼리만 더
높은 임계값으로 잇는다. 후보 중 코사인이 가장 큰 쪽이 부모이고, 같으면 더 늦게 시작한 쪽(바로
앞 노드), 그래도 같으면 id 가 큰 쪽이 부모다.

같은 사건이 클러스터 둘로 나뉘는 일이 잦아서(클러스터 기간·기사 표현 차이), 연결마다 같은
사건(same_event)인지 후속 사건(follow_up)인지 관계를 붙인다. 백엔드는 same_event 묶음을 한 노드로
합친다. 이 모듈은 DB·Bedrock·LLM 을 부르지 않는다.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Protocol

from pipelines.common.gazetteer import CompanyMatch
from pipelines.news.transformers.summarizer import PointKind

# 임베딩 텍스트에 넣는 멤버 기사 제목 수다. 저장소가 준 순서대로 앞에서부터 넣는다. 저장소는 후보
# 기사(승격 전에 들어온 기사)를 먼저 주고, 그 안에서는 발행 시각순으로 준다.
EMBEDDING_ARTICLE_TITLES = 3

SAME_EVENT = "same_event"
FOLLOW_UP = "follow_up"

# 요약 문단의 문장 경계이며, summarizer 가 문장 수를 셀 때와 같은 규칙이다. 종결 부호 뒤에 공백이
# 올 때만 경계로 보므로, 소수점(7.0%)처럼 공백이 따르지 않는 마침표는 경계가 아니다.
_SENTENCE_BREAK = re.compile(r"(?<=[.!?])\s+")


class TitleMatcher(Protocol):
    """제목에서 기업을 찾는 개체 사전 매처(common.gazetteer.CompanyMatcher)의 인터페이스다.
    테스트에서는 가짜 구현을 넣는다."""

    def extract(self, text: str) -> list[CompanyMatch]: ...


@dataclass(frozen=True)
class ClusterMember:
    """클러스터에 속한 기사 한 건의 제목과 그 기사에 연결된 기업(news_companies)을 담는다."""

    title: str
    company_ids: frozenset[int] = field(default_factory=frozenset)


@dataclass(frozen=True)
class LinkCandidate:
    """부모 후보 이슈를 담는다. score 는 대상과의 코사인, company_ids 는 후보의 주요 기업이다."""

    cluster_id: int
    first_published_at: datetime
    score: float
    company_ids: frozenset[int] = field(default_factory=frozenset)


@dataclass(frozen=True)
class LinkDecision:
    """연결 판정 결과를 담는다. parent_id 가 None 이면 새 타임라인의 루트다(score·relation 도
    None)."""

    parent_id: int | None = None
    score: float | None = None
    relation: str | None = None


def _collapse(text: str | None) -> str:
    return " ".join((text or "").split())


def node_summary(summary_points: Any, summary: str | None) -> str | None:
    """타임라인 노드에 쓸 한 줄 요약을 고른다. 대표 기사 핵심 포인트의 CHANGE(무엇이 바뀌나요)
    항목을 먼저 쓴다.

    CHANGE 가 없으면(사건이 없는 기사는 포인트가 빈 배열이다) 요약 문단의 첫 문장을, 둘 다 없으면
    None 을 돌려준다. summary_points 는 news.summary_points JSONB 를 읽은 값이라 형식을 하나씩
    확인한다.
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

    제목은 금액·수치를 뺀 사건 이름이라 회차만 다른 이슈끼리 잘 묶고, 요약과 기사 제목은
    당사자·지표 같은 맥락을 보탠다. 연결 임계값은 이 텍스트로 계산한 코사인에 맞춰 정했으므로,
    구성을 바꾸면 임계값도 다시 맞춰야 한다. 전재 기사의 중복 제목을 빼지 않는 것도 같은 이유다.
    언제 만들어도 같은 텍스트가 나오도록, 저장소는 승격 뒤에 바뀌지 않는 후보 기사를 앞에 둔다
    (SELECT_CLUSTER_MEMBERS_SQL).
    """

    lines = [_collapse(title)]
    if _collapse(summary):
        lines.append(_collapse(summary))
    titles = [_collapse(article_title) for article_title in article_titles]
    lines.extend([article_title for article_title in titles if article_title][:max_titles])
    return "\n".join(lines)


def to_vector_literal(values: Iterable[float]) -> str:
    """pgvector 텍스트 표현 '[x,y,...]' 로 바꾼다. pgvector 파이썬 패키지 없이 SQL 에서
    CAST(... AS vector) 로 넘기기 위해서다."""

    return "[" + ",".join(repr(float(value)) for value in values) + "]"


def primary_company_ids(members: Iterable[ClusterMember], matcher: TitleMatcher) -> frozenset[int]:
    """이슈의 주요 기업을 고른다. news_companies 에 연결된 기업 중 멤버 기사 제목 하나 이상에
    나온 기업이 주요 기업이다.

    news_companies 에는 본문에만 언급된 거래처·경쟁사도 들어 있어서, 그 합집합으로 겹침을 보면 서로
    다른 사건이 이어진다. 제목에 나온 기업은 대개 사건의 당사자다. 제목 매치는 기사 수집 작업과 같은
    개체 사전 매처를 쓰므로 약칭도 같은 기업으로 잡는다. 제목에서 기업을 하나도 찾지 못하면(표기가
    사전에 없는 경우 등) 당사자를 판단할 수 없으므로 연결된 기업 전체를 쓴다. 기업이 없는 이슈는
    빈 집합이다.
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
    """후보가 대상의 부모가 될 수 있는지 판단한다. 대상보다 먼저 시작했고, 기업 규칙과 임계값을
    넘어야 한다.

    "먼저"는 (first_published_at, id) 순서로 비교하므로, 같은 시각에 시작한 두 클러스터(같은 사건이
    나뉜 경우가 많다)도 한쪽이 다른 쪽을 부모로 볼 수 있다. company_ids 와 후보의 company_ids 는
    둘 다 주요 기업이다.
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
    """조건을 넘는 후보 중 코사인이 가장 큰 후보를 고른다. 코사인이 같으면 더 늦게 시작한 쪽(바로
    앞 노드)을, 그래도 같으면 id 가 큰 쪽을 고른다."""

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
    """부모와의 관계를 정한다. gap 은 대상 first_published_at 에서 부모 first_published_at 을 뺀
    값이다.

    첫 기사 시각이 가까우면(같은 날 같은 발표를 다른 표현으로 묶은 클러스터) 점수와 무관하게 같은
    사건이 나뉜 것으로 본다. 코사인이 same_event_score 이상이어도 같은 사건으로 보지만, 간격이
    same_event_score_max_gap 이내일 때만 그렇다. 한 사건이 클러스터 둘로 나뉘는 일은 클러스터 기간
    안에서 일어나는 반면, 시리즈 지표의 다음 회차(7월 건설지출 → 8월 건설지출)는 제목·요약이 거의
    같아 코사인이 높아도 몇 주 떨어져 있기 때문이다. 이 상한이 없으면 기업 없는 연결은 하한이 이미
    점수 기준 이상이라 전부 한 노드로 합쳐진다. 둘 다 아니면 시간을 두고 이어진 후속 사건이다.
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
    """부모를 고르고 관계를 붙인다. 부모가 없으면 루트로 판정한다."""

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
