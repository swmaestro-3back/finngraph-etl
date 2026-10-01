"""이슈 타임라인 연결 판정 — 클러스터를 같은 이야기의 앞선 클러스터에 잇는다.

이슈 하나(이름이 붙은 클러스터)의 타임라인은 같은 이야기를 다룬 앞선 클러스터의 사슬이다
(실적 프리뷰 → 실적 발표, 7월 건설지출 → 8월 건설지출). 대상마다 부모 하나만 고르고, 부모의
story_root_id 를 물려받아 이야기 전체가 한 루트로 묶인다.

부모 조건: 대상보다 먼저 시작했고(first_published_at), 기업이 하나 이상 겹치고, 임베딩 코사인이
임계값 이상. 기업이 없는 클러스터(거시 지표 등)는 기업이 없는 클러스터에만, 더 높은 임계값으로
잇는다 — 기업 겹침이라는 안전판이 없어서다. 후보 중 코사인 최대, 같으면 더 늦게 시작한 쪽(바로
앞 노드)이 부모다. DB·Bedrock 은 만지지 않는다.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from datetime import datetime

# 임베딩 텍스트에 넣는 멤버 기사 제목 수. 최신 기사부터
EMBEDDING_ARTICLE_TITLES = 3


@dataclass(frozen=True)
class LinkCandidate:
    """부모 후보 클러스터. score 는 대상과의 코사인 유사도."""

    cluster_id: int
    first_published_at: datetime
    score: float
    company_ids: frozenset[int] = field(default_factory=frozenset)


@dataclass(frozen=True)
class LinkDecision:
    """parent_id 가 None 이면 새 이야기의 루트다. score 는 부모와의 코사인."""

    parent_id: int | None = None
    score: float | None = None


def _collapse(text: str | None) -> str:
    return " ".join((text or "").split())


def build_embedding_text(
    title: str,
    summary: str | None,
    article_titles: Sequence[str],
    max_titles: int = EMBEDDING_ARTICLE_TITLES,
) -> str:
    """제목, 요약(있으면), 멤버 기사 제목(최신순 앞 max_titles 개)을 줄바꿈으로 잇는다.

    제목은 금액·수치를 뺀 사건 라벨이라 회차가 다른 같은 이야기를 잘 묶고, 요약과 기사 제목이
    당사자·지표 같은 맥락을 보탠다. 임계값을 맞추는 평가 스크립트(scripts/eval/issue_links)와
    같은 텍스트여야 해서 전재 기사 중복도 그대로 둔다.
    """

    lines = [_collapse(title)]
    if _collapse(summary):
        lines.append(_collapse(summary))
    lines.extend(_collapse(t) for t in article_titles[:max_titles] if _collapse(t))
    return "\n".join(lines)


def to_vector_literal(values: Iterable[float]) -> str:
    """pgvector 텍스트 표현 '[x,y,...]'. 파이썬 패키지 없이 CAST(... AS vector) 로 넘긴다."""

    return "[" + ",".join(repr(float(value)) for value in values) + "]"


def is_eligible(
    candidate: LinkCandidate,
    *,
    first_published_at: datetime,
    company_ids: frozenset[int],
    threshold: float,
    no_company_threshold: float,
) -> bool:
    """후보가 대상의 부모가 될 수 있는가. 먼저 시작했고, 기업 규칙과 임계값을 넘어야 한다."""

    if candidate.first_published_at >= first_published_at:
        return False
    if company_ids:
        return bool(candidate.company_ids & company_ids) and candidate.score >= threshold
    return not candidate.company_ids and candidate.score >= no_company_threshold


def choose_parent(
    candidates: Iterable[LinkCandidate],
    *,
    first_published_at: datetime,
    company_ids: Iterable[int],
    threshold: float,
    no_company_threshold: float,
) -> LinkDecision:
    """조건을 넘는 후보 중 코사인 최대. 같으면 더 늦게 시작한 쪽, 그래도 같으면 큰 id."""

    targets = frozenset(company_ids)
    eligible = [
        candidate
        for candidate in candidates
        if is_eligible(
            candidate,
            first_published_at=first_published_at,
            company_ids=targets,
            threshold=threshold,
            no_company_threshold=no_company_threshold,
        )
    ]
    if not eligible:
        return LinkDecision()

    best = max(eligible, key=lambda c: (c.score, c.first_published_at, c.cluster_id))
    return LinkDecision(parent_id=best.cluster_id, score=best.score)
