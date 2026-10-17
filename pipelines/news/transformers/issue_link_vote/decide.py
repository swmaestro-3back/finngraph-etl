"""투표를 모아 부모와 관계를 정하는 규칙이다.

투표자 넷이 (A, B) 쌍마다 통과 여부를 낸다. 제안자는 pair·rank, 확인자는 judge(scope 판정,
통과하지 못하면 계획 이행 확인)·check 이며, 확인자는 제안자가 받아들인 쌍에만 묻는다.

  accepted  judge AND check AND (pair OR rank). 넷 모두 통과했거나, 제안자 둘 중 하나만
            통과하지 못하고 나머지 셋이 통과한 경우다.
  parent    받아들인 쌍 중 표가 가장 많은 쌍, 그다음 순위 점수(사건 유사도와 내용 유사도 중 큰 값)가
            큰 쌍, 그다음 늦게 시작한 쌍, 그다음 큰 id 를 고른다.
  relation  간격 규칙(기본값: 첫 기사 24시간 이내, 또는 내용 유사도 0.75 이상이면서 168시간
            이내)이 같은 사건으로 보거나, judge·pair·rank 중 둘 이상이 SAME_EVENT 라고 하면
            same_event 이고, 아니면 follow_up 이다.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from pipelines.news.transformers.issue_link_vote.confirm import Confirmation

SAME_EVENT = "same_event"
FOLLOW_UP = "follow_up"


@dataclass
class CandidateVotes:
    """후보 A 하나가 받은 표다. 묻지 않은 투표자의 표는 None 이다."""

    candidate_id: int
    first_published_at: datetime
    cosine: float
    gap_hours: float
    pair: dict[str, Any] | None = None
    rank: dict[str, Any] | None = None
    confirmation: Confirmation | None = None
    event: float | None = None
    evidence: dict[str, Any] = field(default_factory=dict)
    chosen: bool = False

    @property
    def pair_pass(self) -> bool | None:
        return None if self.pair is None else bool(self.pair["pass"])

    @property
    def rank_pass(self) -> bool | None:
        return None if self.rank is None else bool(self.rank["pass"])

    @property
    def judge_pass(self) -> bool | None:
        return None if self.confirmation is None else self.confirmation.judge_pass

    @property
    def check_pass(self) -> bool | None:
        return None if self.confirmation is None else self.confirmation.check_pass

    @property
    def proposed(self) -> bool:
        return bool(self.pair_pass or self.rank_pass)

    @property
    def vote_count(self) -> int:
        return sum(
            bool(v) for v in (self.judge_pass, self.check_pass, self.pair_pass, self.rank_pass)
        )

    @property
    def accepted(self) -> bool:
        return bool(self.judge_pass and self.check_pass and (self.pair_pass or self.rank_pass))

    @property
    def score(self) -> float:
        """순위 점수다. 사건 유사도(event)와 내용 유사도(코사인) 중 큰 값이고, 사건 유사도를 구하지
        않았으면 코사인을 쓴다."""

        return self.cosine if self.event is None else max(self.event, self.cosine)

    def same_event_labels(self) -> int:
        labels = [
            self.confirmation.judge_label if self.confirmation else None,
            (self.pair or {}).get("label"),
            (self.rank or {}).get("label"),
        ]
        return sum(1 for label in labels if label == "SAME_EVENT")


@dataclass(frozen=True)
class Decision:
    parent_id: int | None = None
    score: float | None = None
    relation: str | None = None


def relation(
    votes: CandidateVotes,
    *,
    same_event_max_gap_hours: float,
    same_event_score: float,
    same_event_score_max_gap_hours: float,
) -> str:
    gap = votes.gap_hours
    same = (
        gap <= same_event_max_gap_hours
        or (votes.cosine >= same_event_score and gap <= same_event_score_max_gap_hours)
        or votes.same_event_labels() >= 2
    )
    return SAME_EVENT if same else FOLLOW_UP


def decide(
    rows: list[CandidateVotes],
    *,
    same_event_max_gap_hours: float,
    same_event_score: float,
    same_event_score_max_gap_hours: float,
) -> Decision:
    """받아들인 쌍 중 부모를 고르고 chosen 을 표시한다. 받아들인 쌍이 없으면 루트(부모가 없는
    타임라인 첫 이슈)로 판정한다."""

    accepted = [row for row in rows if row.accepted]
    if not accepted:
        return Decision()
    best = max(
        accepted,
        key=lambda r: (r.vote_count, r.score, r.first_published_at, r.candidate_id),
    )
    best.chosen = True
    return Decision(
        parent_id=best.candidate_id,
        score=best.score,
        relation=relation(
            best,
            same_event_max_gap_hours=same_event_max_gap_hours,
            same_event_score=same_event_score,
            same_event_score_max_gap_hours=same_event_score_max_gap_hours,
        ),
    )
