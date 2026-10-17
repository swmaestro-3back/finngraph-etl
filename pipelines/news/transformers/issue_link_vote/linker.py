"""대상 이슈 B 하나를 투표로 판정한다. 제안자 둘(pair, rank), 확인자 둘(judge, check), 결정
순서로 진행한다.

후보 목록(universe)은 job 이 DB 에서 채운다. B 보다 먼저 시작했고 판정이 끝났으며 성격이 event 인
이슈 중 후보 범위 규칙(universe.py)을 통과한 이슈와, 그 이슈의 코사인을 담는다. 이 모듈은 DB 에
접근하지 않는다.
"""

from __future__ import annotations

from dataclasses import dataclass

from pipelines.news.transformers.issue_link_vote import pair, rank
from pipelines.news.transformers.issue_link_vote.confirm import confirm
from pipelines.news.transformers.issue_link_vote.decide import CandidateVotes, Decision, decide
from pipelines.news.transformers.issue_link_vote.issue import CompanyName, Issue, gap_hours
from pipelines.news.transformers.issue_link_vote.llm import LlmClient, parallel_map
from pipelines.news.transformers.issue_link_vote.universe import Candidate
from pipelines.news.transformers.issue_link_vote.views import EventScorer


@dataclass(frozen=True)
class VoteResult:
    decision: Decision
    votes: list[CandidateVotes]

    @property
    def proposed(self) -> int:
        return sum(1 for v in self.votes if v.proposed)

    @property
    def accepted(self) -> int:
        return sum(1 for v in self.votes if v.accepted)


@dataclass(frozen=True)
class RelationRule:
    same_event_max_gap_hours: float
    same_event_score: float
    same_event_score_max_gap_hours: float


class VoteLinker:
    def __init__(
        self,
        client: LlmClient,
        names: dict[int, CompanyName],
        scorer: EventScorer,
        rule: RelationRule,
        workers: int,
    ):
        self.client = client
        self.names = names
        self.scorer = scorer
        self.rule = rule
        self.workers = max(1, workers)

    def judge(self, child: Issue, universe: list[Candidate]) -> VoteResult:
        pair_cands = pair.select_candidates(universe)
        rank_cands = rank.select_candidates(child, universe)

        stages = pair.classify(child, pair_cands, self.client, self.names, self.workers)
        pair_votes = stages.verdicts()
        rank_votes = rank.verdicts(child, rank_cands, self.client, self.names)

        by_id = {c.id: c for c in universe}
        rows: dict[int, CandidateVotes] = {}
        for cid in [c.id for c in pair_cands] + [c.id for c in rank_cands]:
            if cid in rows:
                continue
            cand = by_id[cid]
            rows[cid] = CandidateVotes(
                candidate_id=cid,
                first_published_at=cand.issue.first_published_at,
                cosine=cand.cosine,
                gap_hours=gap_hours(cand.issue, child),
                pair=pair_votes.get(cid),
                rank=rank_votes.get(cid),
            )
            if cid in pair_votes:
                rows[cid].evidence["pair"] = stages.evidence(cid)
            if cid in rank_votes:
                rows[cid].evidence["rank"] = {"why": rank_votes[cid].get("why")}

        proposed = sorted(cid for cid, row in rows.items() if row.proposed)

        def confirm_one(cid: int):
            return confirm(by_id[cid].issue, child, self.client, self.names)

        for cid, confirmation in zip(
            proposed, parallel_map(confirm_one, proposed, self.workers), strict=True
        ):
            row = rows[cid]
            row.confirmation = confirmation
            row.event = self.scorer.score(by_id[cid].issue, child)
            if confirmation.plan_why is not None:
                row.evidence["plan_reader"] = confirmation.plan_why

        votes = list(rows.values())
        decision = decide(
            votes,
            same_event_max_gap_hours=self.rule.same_event_max_gap_hours,
            same_event_score=self.rule.same_event_score,
            same_event_score_max_gap_hours=self.rule.same_event_score_max_gap_hours,
        )
        return VoteResult(decision=decision, votes=votes)
