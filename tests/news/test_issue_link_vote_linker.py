"""VoteLinker 흐름 테스트다. 가짜 Bedrock 으로 제안자, 확인자, 결정까지 돌린다."""

from __future__ import annotations

from datetime import datetime, timedelta

from issue_link_vote_fakes import FakeBedrock, MemoryStore, PairScript

from pipelines.common.utils.time import KST
from pipelines.news.transformers.issue_link_vote.issue import CompanyName, Issue, Member
from pipelines.news.transformers.issue_link_vote.linker import RelationRule, VoteLinker
from pipelines.news.transformers.issue_link_vote.llm import STAGES, LlmClient
from pipelines.news.transformers.issue_link_vote.universe import Candidate
from pipelines.news.transformers.issue_link_vote.views import EventScorer

T0 = datetime(2026, 9, 1, 9, tzinfo=KST)
GABIA = 3
NAMES = {GABIA: CompanyName("가비아", "079940")}

PROPOSAL = "맥쿼리 가비아 공개매수 추진"
RESULTS = "가비아 2분기 실적 발표"
FAILURE = "맥쿼리 가비아 공개매수 실패"


def _issue(cid: int, title: str, days: float) -> Issue:
    first = T0 + timedelta(days=days)
    return Issue(
        id=cid,
        title=title,
        first_published_at=first,
        last_published_at=first,
        members=(Member(title, first, frozenset({GABIA})),),
        companies={GABIA: "가비아"},
        primary_company_ids=(GABIA,),
    )


def _linker(bedrock: FakeBedrock, store: MemoryStore | None = None) -> VoteLinker:
    client = LlmClient(
        dict.fromkeys(STAGES, "moonshotai.kimi-k2.5"),
        store or MemoryStore(),
        bedrock,
        run_cap=500,
        issue_cap=500,
    )
    scorer = EventScorer(lambda texts: [[1.0, float(len(t) % 3)] for t in texts], NAMES)
    rule = RelationRule(24, 0.75, 168)
    return VoteLinker(client, NAMES, scorer, rule, workers=4)


def _universe():
    proposal, results = _issue(1, PROPOSAL, 0), _issue(2, RESULTS, 1)
    child = _issue(3, FAILURE, 20)
    return child, [Candidate(proposal, 0.6), Candidate(results, 0.55)]


def test_follow_up_is_linked_by_all_four_voters():
    child, universe = _universe()
    bedrock = FakeBedrock(pairs={(PROPOSAL, FAILURE): PairScript(rank_key="공개매수")})
    result = _linker(bedrock).judge(child, universe)

    assert result.decision.parent_id == 1
    assert result.decision.relation == "follow_up"
    chosen = next(v for v in result.votes if v.chosen)
    assert (chosen.vote_count, chosen.accepted) == (4, True)
    assert chosen.confirmation.judge_by == "scope_judge"
    assert result.decision.score == max(chosen.event, 0.6)
    # 제안자가 거절한 쌍에는 확인자를 묻지 않는다.
    other = next(v for v in result.votes if v.candidate_id == 2)
    assert (other.pair_pass, other.rank_pass, other.judge_pass, other.check_pass) == (
        False,
        False,
        None,
        None,
    )
    assert other.event is None
    assert bedrock.calls["judge"] == 1 and bedrock.calls["check"] == 1
    # route 1 이 부모를 줬으므로 계획 경로는 돌지 않는다.
    assert bedrock.calls["plan_extract"] == 0
    assert chosen.evidence["pair"]["screen"] == "SAME_STORY"


def test_check_veto_leaves_root():
    child, universe = _universe()
    script = PairScript(rank_key="공개매수", check=("COMMENTARY", True, True))
    result = _linker(FakeBedrock(pairs={(PROPOSAL, FAILURE): script})).judge(child, universe)

    assert result.decision.parent_id is None
    assert result.proposed == 1 and result.accepted == 0


def test_plan_reader_completes_judge_with_grounded_quotes():
    child, universe = _universe()
    script = PairScript(
        rank=None,
        judge=("DIFFERENT", "SPECIFIC", "SPECIFIC"),
        plan=("THE_ITEM", PROPOSAL, FAILURE, "맥쿼리", "맥쿼리", "공개매수"),
    )
    result = _linker(FakeBedrock(pairs={(PROPOSAL, FAILURE): script})).judge(child, universe)

    assert result.decision.parent_id == 1
    chosen = next(v for v in result.votes if v.chosen)
    assert chosen.confirmation.judge_by == "plan_reader"
    assert chosen.confirmation.judge_label == "SAME_STORY"
    assert chosen.vote_count == 3


def test_plan_reader_quote_not_in_text_fails():
    child, universe = _universe()
    script = PairScript(
        rank=None,
        judge=("SAME_STORY", "THEME", "SPECIFIC"),
        plan=("THE_ITEM", "맥쿼리 가비아 상장폐지 확정", FAILURE, "맥쿼리", "맥쿼리", "공개매수"),
    )
    result = _linker(FakeBedrock(pairs={(PROPOSAL, FAILURE): script})).judge(child, universe)

    assert result.decision.parent_id is None
    votes = next(v for v in result.votes if v.candidate_id == 1)
    assert votes.evidence["plan_reader"] == "a_quote not in A"


def test_rank_alone_can_propose_and_garbled_rank_proposes_nothing():
    child, universe = _universe()
    rank_only = PairScript(screen="DIFFERENT", rank_key="공개매수")
    result = _linker(FakeBedrock(pairs={(PROPOSAL, FAILURE): rank_only})).judge(child, universe)
    assert result.decision.parent_id == 1

    garbled = FakeBedrock(pairs={(PROPOSAL, FAILURE): rank_only}, garbled_rank=True)
    result = _linker(garbled).judge(child, universe)
    assert result.decision.parent_id is None
    assert all(v.rank["label"] == "UNPARSED" for v in result.votes if v.rank)
    # 읽지 못한 응답은 형식 재요청 한 번 뒤에 포기한다.
    assert garbled.calls["rank"] == 2


def test_same_event_when_two_voters_say_so():
    child, universe = _universe()
    script = PairScript(
        verify="SAME_EVENT",
        rank="SAME_EVENT",
        rank_key="공개매수",
        judge=("SAME_EVENT", "SPECIFIC", "SPECIFIC"),
    )
    child = _issue(3, FAILURE, 3)
    result = _linker(FakeBedrock(pairs={(PROPOSAL, FAILURE): script})).judge(child, universe)
    # 3일 떨어져 간격 규칙으로는 후속이지만, judge·pair·rank 가 SAME_EVENT 라고 한다.
    assert (result.decision.parent_id, result.decision.relation) == (1, "same_event")


def test_plan_route_runs_only_when_route_one_leaves_no_parent():
    child, universe = _universe()
    # screen 은 통과하지만 matter 관점 프롬프트가 거절해 route 1 이 실패하고, 계획이 없어 계획
    # 경로도 실패한다.
    script = PairScript(matter=None, rank=None)
    bedrock = FakeBedrock(pairs={(PROPOSAL, FAILURE): script})
    result = _linker(bedrock).judge(child, universe)

    assert result.decision.parent_id is None
    assert bedrock.calls["plan_extract"] == 1
    assert bedrock.calls["plan_step.planstep"] == 0


def test_second_run_reuses_cached_answers():
    child, universe = _universe()
    store = MemoryStore()
    script = {(PROPOSAL, FAILURE): PairScript(rank_key="공개매수")}
    first = _linker(FakeBedrock(pairs=script), store).judge(child, universe)
    again = FakeBedrock(pairs=script)
    second = _linker(again, store).judge(child, universe)

    assert sum(again.calls.values()) == 0
    assert second.decision == first.decision
