"""부모 후보가 될 수 있는 이슈를 정하는 후보 범위 규칙이다. pair 와 rank 투표자가 같은 규칙을 쓴다.

대상 이슈 B 보다 먼저 시작했고((first_published_at, id) 순서) lookback 기간(기본 90일, job 은
NEWS_ISSUE_LINK_LOOKBACK_DAYS 를 넘긴다) 안인 이슈 A 중 아래 조건을 만족하는 이슈가 후보다:
  - 연결 기업이 하나라도 겹치면 코사인 0.10 이상,
  - 둘 다 기업이 없으면 0.50 이상,
  - 그 밖(한쪽만 기업이 있거나 겹치는 기업이 없으면)은 0.45 이상.
코사인은 이슈 임베딩(news_clusters.embedding) 사이의 코사인이다. 주가 반응 이슈는 job 이 미리 뺀다.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from pipelines.news.transformers.issue_link_vote.issue import Issue

WINDOW = timedelta(days=90)
SHARED_MIN_COSINE = 0.10
NO_SHARE_MIN_COSINE = 0.45
NO_COMPANY_MIN_COSINE = 0.50


@dataclass(frozen=True)
class Candidate:
    """B 의 부모 후보 A 와, 두 이슈 임베딩 사이의 코사인이다."""

    issue: Issue
    cosine: float

    @property
    def id(self) -> int:
        return self.issue.id


def is_earlier(parent: Issue, child: Issue) -> bool:
    return (parent.first_published_at, parent.id) < (child.first_published_at, child.id)


def eligible(parent: Issue, child: Issue, cosine: float, window: timedelta = WINDOW) -> bool:
    """A 가 B 의 부모 후보가 될 수 있는지 모듈 설명의 규칙으로 판단한다."""

    if not is_earlier(parent, child):
        return False
    if child.first_published_at - parent.first_published_at > window:
        return False
    shares = bool(parent.linked & child.linked)
    both_none = not parent.linked and not child.linked
    if shares:
        return cosine >= SHARED_MIN_COSINE
    if both_none:
        return cosine >= NO_COMPANY_MIN_COSINE
    return cosine >= NO_SHARE_MIN_COSINE
