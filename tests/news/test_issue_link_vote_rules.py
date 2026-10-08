"""투표 판정 규칙 단위 테스트다. 투표·결정 규칙, 제안자 경로, 코드 점검(check_affirm, plan step,
계획 이행 확인 인용 근거), 판정 예시 제외, 후보 범위, 이슈 성격 분류를 확인한다."""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest
from issue_link_vote_fakes import MemoryStore

from pipelines.common.utils.time import KST
from pipelines.news.transformers.issue_link_vote import confirm, pair, rank
from pipelines.news.transformers.issue_link_vote.confirm import Confirmation
from pipelines.news.transformers.issue_link_vote.decide import CandidateVotes, decide
from pipelines.news.transformers.issue_link_vote.issue import CompanyName, Issue, Member
from pipelines.news.transformers.issue_link_vote.kind import (
    EVENT,
    MARKET_REACTION,
    UNPARSED_REASON,
    classify_kind,
    kind_input,
)
from pipelines.news.transformers.issue_link_vote.llm import STAGES, LlmClient
from pipelines.news.transformers.issue_link_vote.universe import Candidate, eligible

T0 = datetime(2026, 9, 1, 9, tzinfo=KST)
GABIA, SAMSUNG = 3, 1
NAMES = {
    GABIA: CompanyName("가비아", "079940"),
    SAMSUNG: CompanyName("삼성전자", "005930", ("삼전",)),
}
RULE = {
    "same_event_max_gap_hours": 24,
    "same_event_score": 0.75,
    "same_event_score_max_gap_hours": 168,
}


def _issue(
    cid: int,
    title: str,
    hours: float,
    *,
    titles: tuple[str, ...] = (),
    companies: tuple[int, ...] = (),
    primary: tuple[int, ...] | None = None,
    node: str | None = None,
    summary: str | None = None,
) -> Issue:
    first = T0 + timedelta(hours=hours)
    members = tuple(
        Member(t, first + timedelta(hours=k), frozenset(companies))
        for k, t in enumerate(titles or (title,))
    )
    return Issue(
        id=cid,
        title=title,
        first_published_at=first,
        last_published_at=members[-1].published_at,
        members=members,
        companies={c: NAMES[c].name for c in companies},
        primary_company_ids=tuple(companies if primary is None else primary),
        node_summary=node,
        summary=summary,
    )


def _confirmation(judge=True, check=True, label="SAME_STORY") -> Confirmation:
    return Confirmation(
        judge_pass=judge,
        judge_label=label,
        judge_by="scope_judge",
        a_scope="SPECIFIC",
        b_scope="SPECIFIC",
        plan_why=None,
        check_pass=check,
        check_role="NEW_STEP" if check else "OTHER_MATTER",
    )


def _votes(cid, *, pair=None, rank=None, conf=None, cosine=0.5, gap=48.0, event=None, hours=0):
    return CandidateVotes(
        candidate_id=cid,
        first_published_at=T0 + timedelta(hours=hours),
        cosine=cosine,
        gap_hours=gap,
        pair=None if pair is None else {"pass": pair[0], "label": pair[1], "route": 1},
        rank=None if rank is None else {"pass": rank[0], "label": rank[1]},
        confirmation=conf,
        event=event,
    )


# ── 결정 ──


def test_accept_needs_both_confirmers_and_one_proposer():
    ok = _votes(1, pair=(True, "SAME_STORY"), rank=(False, "DIFFERENT"), conf=_confirmation())
    no_check = _votes(2, pair=(True, "SAME_STORY"), conf=_confirmation(check=False))
    no_judge = _votes(3, rank=(True, "SAME_STORY"), conf=_confirmation(judge=False))
    no_proposer = _votes(4, pair=(False, "DIFFERENT"), conf=_confirmation())

    assert (ok.accepted, ok.vote_count) == (True, 3)
    assert not no_check.accepted
    assert not no_judge.accepted
    assert not no_proposer.accepted
    assert decide([no_check, no_judge, no_proposer], **RULE).parent_id is None


def test_parent_by_votes_then_score_then_later_start_then_id():
    three = _votes(1, pair=(True, "SAME_STORY"), conf=_confirmation(), cosine=0.9)
    four = _votes(
        2, pair=(True, "SAME_STORY"), rank=(True, "SAME_STORY"), conf=_confirmation(), cosine=0.3
    )
    decision = decide([three, four], **RULE)
    assert decision.parent_id == 2
    assert four.chosen and not three.chosen

    # 표가 같으면 s = max(event, content) 가 큰 쪽이 부모다.
    low = _votes(3, pair=(True, "SAME_STORY"), conf=_confirmation(), cosine=0.4, event=0.8)
    high = _votes(4, pair=(True, "SAME_STORY"), conf=_confirmation(), cosine=0.7, event=0.2)
    decision = decide([low, high], **RULE)
    assert (decision.parent_id, decision.score) == (3, pytest.approx(0.8))

    # s 도 같으면 늦게 시작한 쪽, 그다음 큰 id 가 부모다.
    early = _votes(5, pair=(True, "SAME_STORY"), conf=_confirmation(), hours=0)
    late = _votes(6, pair=(True, "SAME_STORY"), conf=_confirmation(), hours=5)
    assert decide([late, early], **RULE).parent_id == 6
    a = _votes(7, pair=(True, "SAME_STORY"), conf=_confirmation())
    b = _votes(8, pair=(True, "SAME_STORY"), conf=_confirmation())
    assert decide([a, b], **RULE).parent_id == 8


def test_relation_gap_rule_or_two_same_event_labels():
    near = _votes(1, pair=(True, "SAME_STORY"), conf=_confirmation(), gap=20)
    assert decide([near], **RULE).relation == "same_event"

    close_content = _votes(1, pair=(True, "SAME_STORY"), conf=_confirmation(), gap=100, cosine=0.8)
    assert decide([close_content], **RULE).relation == "same_event"
    far_content = _votes(1, pair=(True, "SAME_STORY"), conf=_confirmation(), gap=200, cosine=0.8)
    assert decide([far_content], **RULE).relation == "follow_up"

    one_label = _votes(
        1, pair=(True, "SAME_EVENT"), conf=_confirmation(label="SAME_STORY"), gap=100
    )
    assert decide([one_label], **RULE).relation == "follow_up"
    two_labels = _votes(
        1, pair=(True, "SAME_EVENT"), rank=(False, "SAME_EVENT"), conf=_confirmation(), gap=100
    )
    assert decide([two_labels], **RULE).relation == "same_event"


# ── 후보 범위 ──


def test_universe_rule():
    child = _issue(10, "가비아 공개매수 실패", 100, companies=(GABIA,))
    shared = _issue(1, "가비아 공개매수 추진", 0, companies=(GABIA,))
    other = _issue(2, "삼성전자 실적", 0, companies=(SAMSUNG,))
    none_a = _issue(3, "건설지출", 0)
    none_b = _issue(11, "건설지출 8월", 100)

    assert eligible(shared, child, 0.10)
    assert not eligible(shared, child, 0.09)
    assert eligible(other, child, 0.45) and not eligible(other, child, 0.44)
    assert eligible(none_a, none_b, 0.50) and not eligible(none_a, none_b, 0.49)
    # 한쪽만 기업이 있으면 하한은 0.45 다.
    assert eligible(none_a, child, 0.45) and not eligible(none_a, child, 0.44)
    # 90일 기간 밖의 이슈와 대상보다 늦게 시작한 이슈는 후보가 아니다.
    old = _issue(4, "가비아 옛 사건", -24 * 91, companies=(GABIA,))
    assert not eligible(old, child, 0.9)
    assert not eligible(child, shared, 0.9)


def test_candidate_selection_orders():
    child = _issue(10, "B", 100, companies=(GABIA,))
    cands = [
        Candidate(_issue(1, "공유 낮음", 0, companies=(GABIA,)), 0.2),
        Candidate(_issue(2, "비공유 높음", 1, companies=(SAMSUNG,)), 0.9),
        Candidate(_issue(3, "공유 높음", 2, companies=(GABIA,)), 0.6),
        Candidate(_issue(4, "본문만 공유", 3, companies=(GABIA,), primary=()), 0.95),
    ]
    # pair 는 코사인 순으로 고른다.
    assert [c.id for c in pair.select_candidates(cands)] == [4, 2, 3, 1]
    # rank 는 주요 기업 공유, 연결 기업 공유, 나머지 순으로 묶고 그 안에서 코사인 순으로 고른다.
    assert [c.id for c in rank.select_candidates(child, cands)] == [3, 1, 4, 2]


# ── pair 경로 ──


def _samples(*accepted, label="SAME_STORY"):
    return [{"accepted": ok, "label": label if ok else "DIFFERENT"} for ok in accepted]


def test_pair_routes():
    c = Candidate(_issue(1, "A", 0), 0.5)
    stages = pair.PairStages(candidates=[c])
    stages.screen[1] = {"label": "SAME_STORY"}
    stages.verify[1] = _samples(True, True, False, label="SAME_EVENT")
    stages.matter[1] = _samples(True, False, True)
    assert stages.route1(1)
    assert stages.verdicts()[1] == {"pass": True, "label": "SAME_EVENT", "route": 1}

    stages.matter[1] = _samples(True, False, False)
    assert not stages.route1(1)
    # route 2 는 셋 모두 통과해야 한다.
    stages.planstep[1] = _samples(True, True, True)
    stages.evidence3[1] = _samples(True, True, False)
    assert not stages.route2(1)
    stages.evidence3[1] = _samples(True, True, True)
    assert stages.verdicts()[1] == {"pass": True, "label": "SAME_STORY", "route": 2}
    # screen 이 DIFFERENT 면 어느 경로도 없다.
    stages.screen[1] = {"label": "DIFFERENT"}
    assert stages.verdicts()[1]["pass"] is False


def test_planstep_code_checks():
    child = _issue(10, "SG 인도네시아 합작법인 출자", 100, companies=(GABIA,))
    plans = [{"who": "가비아", "plan": "현지 생산", "named_object": "에코스틸아스콘"}]
    good = {
        "relation": "carries_out",
        "label": "SAME_STORY",
        "plan_index": 1,
        "b_words": "인도네시아 합작법인",
    }
    assert pair.planstep_accepts(good, plans, child)
    # 인용한 단어가 B 자신의 소식에 없다.
    assert not pair.planstep_accepts({**good, "b_words": "에코스틸아스콘 공장"}, plans, child)
    # 계획 번호가 범위 밖이다.
    assert not pair.planstep_accepts({**good, "plan_index": 2}, plans, child)
    assert not pair.planstep_accepts({**good, "relation": "market_outcome"}, plans, child)
    # 계획의 who 가 B 의 주요 기업이어야 한다.
    assert pair.plan_who_matches(plans[0], child, NAMES)
    assert not pair.plan_who_matches({"who": "포스코인터내셔널"}, child, NAMES)


def test_screen_parser_falls_back_to_different():
    assert pair.parse_verdict('{"label": "same_event"}')["label"] == "SAME_EVENT"
    assert pair.parse_verdict("")["label"] == "DIFFERENT"
    tool = pair.parse_tool_verdict(
        {"tool_input": {"label": "SAME_STORY", "basis": "instance"}, "stop_reason": "tool_use"},
        pair.MATTER_ACCEPTED,
    )
    assert not tool["accepted"]
    truncated = pair.parse_tool_verdict(
        {"tool_input": {"label": "SAME_STORY"}, "stop_reason": "max_tokens"}
    )
    assert truncated == {"label": "DIFFERENT", "invalid": True, "accepted": False}


# ── rank 점검 ──


def _rank_cand(parent: Issue, child: Issue, cosine: float) -> rank.RankCandidate:
    return rank.select_candidates(child, [Candidate(parent, cosine)])[0]


def test_check_affirm_gates():
    a = _issue(1, "맥쿼리 가비아 공개매수 추진", 0, companies=(GABIA,))
    b = _issue(2, "맥쿼리 가비아 공개매수 실패", 24 * 20, companies=(GABIA,))
    cand = _rank_cand(a, b, 0.6)
    row = {
        "label": "SAME_STORY",
        "new_matter": "가비아 공개매수 실패",
        "old_matter": "가비아 공개매수 추진",
        "key": "공개매수",
        "stage": "실패",
        "step_by": "party",
    }
    assert rank.check_affirm(b, cand, row, NAMES)["ok"]
    # 공통 주요 기업 이름만 겹치면 같은 사안이 아니다.
    only_company = {**row, "new_matter": "가비아 실패", "old_matter": "가비아 추진"}
    assert not rank.check_affirm(b, cand, only_company, NAMES)["ok"]
    # 논평·주가 흐름(other)은 다음 단계가 아니다.
    assert not rank.check_affirm(b, cand, {**row, "step_by": "other"}, NAMES)["ok"]
    assert rank.check_affirm(b, cand, {**row, "step_by": "schedule"}, NAMES)["ok"]
    # key 가 두 이슈의 제목 텍스트에 모두 있어야 한다.
    assert not rank.check_affirm(b, cand, {**row, "key": "상장폐지"}, NAMES)["ok"]
    # SAME_EVENT 는 168시간 안이어야 한다.
    event = {**row, "label": "SAME_EVENT"}
    assert not rank.check_affirm(b, cand, event, NAMES)["ok"]


def test_rank_unparsed_reply_proposes_nothing():
    a = _issue(1, "A", 0, companies=(GABIA,))
    b = _issue(2, "B", 10, companies=(GABIA,))
    garbled = {"output": {"message": {"content": [{"text": "…"}]}}, "stopReason": "end_turn"}
    client = LlmClient(
        dict.fromkeys(STAGES, "m"),
        MemoryStore(),
        lambda req: garbled,
        run_cap=10,
        issue_cap=10,
    )
    votes = rank.verdicts(b, [_rank_cand(a, b, 0.9)], client, NAMES)
    assert votes[1]["pass"] is False
    assert votes[1]["label"] == "UNPARSED"


# ── 확인자 ──


def test_judge_and_check_pass_rules():
    assert confirm.judge_passes({"label": "SAME_EVENT", "a_scope": "THEME", "b_scope": "THEME"})
    assert confirm.judge_passes(
        {"label": "SAME_STORY", "a_scope": "SPECIFIC", "b_scope": "SPECIFIC"}
    )
    assert not confirm.judge_passes(
        {"label": "SAME_STORY", "a_scope": "THEME", "b_scope": "SPECIFIC"}
    )
    assert confirm.check_passes({"role": "SAME_REPORT", "a_specific": False, "b_specific": False})
    assert confirm.check_passes({"role": "NEW_STEP", "a_specific": True, "b_specific": True})
    assert not confirm.check_passes({"role": "NEW_STEP", "a_specific": True, "b_specific": False})
    assert not confirm.check_passes({"role": "COMMENTARY", "a_specific": True, "b_specific": True})
    # 읽지 못한 답은 가장 보수적인 값으로 읽는다.
    assert confirm.parse_judge("")["label"] == "DIFFERENT"
    assert confirm.parse_check("")["role"] == "OTHER_MATTER"
    assert confirm.parse_plan("")["fit"] == "NONE"


def test_plan_reader_code_checks():
    a = _issue(
        1,
        "가비아 공개매수 추진",
        0,
        titles=("맥쿼리, 가비아 공개매수 추진",),
        companies=(GABIA,),
    )
    b = _issue(
        2, "가비아 공개매수 실패", 100, titles=("맥쿼리 가비아 공개매수 실패",), companies=(GABIA,)
    )
    block_a, block_b = confirm.blocks(a, b, NAMES)
    aliases = ["가비아"]
    v = {
        "fit": "THE_ITEM",
        "a_quote": "맥쿼리, 가비아 공개매수 추진",
        "b_quote": "맥쿼리 가비아 공개매수 실패",
        "a_actor": "맥쿼리자산운용",
        "b_actor": "맥쿼리",
        "shared": "공개매수",
    }
    assert confirm.plan_passes(v, block_a, block_b, aliases) == {
        "pass": True,
        "why": "THE_ITEM grounded",
    }
    assert (
        confirm.plan_passes({**v, "fit": "ONE_OF_MANY"}, block_a, block_b, aliases)["pass"] is False
    )
    # 인용문이 텍스트에 없으면 통과 못 한다.
    fake_quote = {**v, "a_quote": "맥쿼리, 가비아 상장폐지 결정"}
    assert confirm.plan_passes(fake_quote, block_a, block_b, aliases)["why"] == "a_quote not in A"
    # 말줄임표로 줄인 인용은 조각마다 확인한다.
    assert confirm.grounded("맥쿼리 … 공개매수 실패", block_b)
    assert not confirm.grounded("맥쿼리 … 공개매수 성공", block_b)
    # 공통 대상이 기업 이름뿐이면 통과 못 한다.
    assert confirm.plan_passes({**v, "shared": "가비아"}, block_a, block_b, aliases)["why"] == (
        "no shared object"
    )
    assert confirm.plan_passes({**v, "b_actor": "금융위원회"}, block_a, block_b, aliases)[
        "why"
    ] == ("other actor")


def test_examples_left_out_by_static_company_names():
    examples = [(("삼성전자",), "삼성 예시"), (("가비아", "HLB"), "가비아 예시"), ((), "일반 예시")]
    assert confirm.select_examples(examples, {"삼성전자"}) == ["가비아 예시", "일반 예시"]
    assert confirm.select_examples(examples, set()) == ["삼성 예시", "가비아 예시", "일반 예시"]

    a = _issue(1, "가비아 공개매수 추진", 0, companies=(GABIA,))
    b = _issue(2, "가비아 공개매수 실패", 100, companies=(GABIA,))
    judge = confirm.judge_prompt(a, b, NAMES)
    plan = confirm.plan_prompt(a, b, NAMES)
    # 가비아 예시(공개매수)는 빠지고 다른 예시와 일반 예시는 남는다.
    assert "맥쿼리 가비아 공개매수 실패 → SAME_STORY" not in judge
    assert "(일반 예시)" in judge
    assert "맥쿼리 가비아 공개매수 실패 → THE_ITEM" not in plan
    assert "삼성전자 110조원 규모 주주환원 계획 발표 → THE_ITEM" in plan
    assert "{examples}" not in judge and "{a}" not in judge


def test_blocks_cut_a_at_b_last_time():
    a = _issue(
        1, "가비아 공개매수", 0, titles=("첫 기사", "둘째 기사", "늦은 기사"), companies=(GABIA,)
    )
    a = Issue(
        **{
            **a.__dict__,
            "members": a.members[:2] + (Member("늦은 기사", T0 + timedelta(hours=500)),),
            "last_published_at": T0 + timedelta(hours=500),
        }
    )
    b = _issue(2, "가비아 결과", 100, companies=(GABIA,))
    block_a, block_b = confirm.blocks(a, b, NAMES)
    assert "늦은 기사" not in block_a
    assert "기간: 2026-09-01 09:00 ~ 2026-09-05 13:00" in block_a
    assert "주요 기업: 가비아" in block_b


# ── 이슈 성격 ──


def _kind_client(payload):
    def invoke(req):
        content = [{"toolUse": {"name": "record_issue_kind", "input": payload}}] if payload else []
        return {"output": {"message": {"content": content}}, "stopReason": "end_turn", "usage": {}}

    return LlmClient(
        dict.fromkeys(STAGES, "moonshotai.kimi-k2.5"),
        MemoryStore(),
        invoke,
        run_cap=10,
        issue_cap=10,
    )


def test_issue_kind_classification():
    issue = _issue(1, "에쓰오일·GS 국제유가 상승 수혜 강세", 0, companies=(SAMSUNG,))
    result = classify_kind(
        issue,
        _kind_client(
            {"main_news": "유가 상승 수혜", "reason": "외부 요인", "kind": "market_reaction"}
        ),
    )
    assert (result.kind, result.reason, result.model, result.prompt_version) == (
        MARKET_REACTION,
        "외부 요인",
        "moonshotai.kimi-k2.5",
        "issue-kind-v4",
    )
    event = classify_kind(
        issue, _kind_client({"main_news": "실적", "reason": "자기 사건", "kind": "EVENT"})
    )
    assert event.kind == "event"
    # 읽지 못하면 연결에서 빼는 쪽(가장 보수적인 답)으로 분류한다.
    unparsed = classify_kind(issue, _kind_client(None))
    assert (unparsed.kind, unparsed.reason) == (MARKET_REACTION, UNPARSED_REASON)


def test_kind_input_puts_article_titles_before_summaries():
    issue = _issue(
        1,
        "가람전자 3분기 실적 전망",
        0,
        titles=("목표가 상향", "실적 기대에 강세"),
        node="앞선 공급계약을 다시 적은 한 줄 요약",
        summary="대표 기사 하나의 요약 문단",
    )
    lines = kind_input(issue).split("\n")
    assert lines[:4] == [
        "제목: 가람전자 3분기 실적 전망",
        "기사 제목:",
        "- 목표가 상향",
        "- 실적 기대에 강세",
    ]
    assert lines[4:] == [
        "한 줄 요약: 앞선 공급계약을 다시 적은 한 줄 요약",
        "요약: 대표 기사 하나의 요약 문단",
    ]


# ── 이슈 성격: 1차 모델과 성격 분류 모델 ──

KIMI, SONNET = "moonshotai.kimi-k2.5", "us.anthropic.claude-sonnet-4-6"


class KindByModel:
    """모델마다 정해 둔 성격으로 답하는 Converse 가짜다. 부른 모델을 순서대로 남긴다."""

    def __init__(self, **kinds: str):
        self.kinds = {KIMI: kinds.get("kimi"), SONNET: kinds.get("sonnet")}
        self.models: list[str] = []

    def __call__(self, req):
        model = req["modelId"]
        self.models.append(model)
        payload = {"main_news": "n", "reason": f"{model} 판단", "kind": self.kinds[model]}
        return {
            "output": {"message": {"content": [{"toolUse": {"name": "t", "input": payload}}]}},
            "stopReason": "tool_use",
            "usage": {"inputTokens": 1000, "outputTokens": 100},
        }


def _hybrid_client(invoke, store=None) -> LlmClient:
    return LlmClient(
        dict.fromkeys(STAGES, SONNET), store or MemoryStore(), invoke, run_cap=10, issue_cap=10
    )


def test_screen_market_reaction_is_confirmed_by_the_kind_model():
    issue = _issue(1, "가비아 데이터센터 증설 발표", 0, companies=(GABIA,))
    invoke = KindByModel(kimi=MARKET_REACTION, sonnet=EVENT)

    result = classify_kind(issue, _hybrid_client(invoke), KIMI)

    # 이슈를 타임라인에서 빼는 답은 성격 분류 모델이 다시 보고, 그 답이 최종 답이다.
    assert invoke.models == [KIMI, SONNET]
    assert (result.kind, result.model, result.reason, result.escalated) == (
        EVENT,
        SONNET,
        f"{SONNET} 판단",
        True,
    )


def test_screen_event_with_report_cue_is_confirmed():
    issue = _issue(
        1,
        "가비아 3분기 실적",
        0,
        titles=("가비아 실적 개선", "가비아 목표가 상향"),
        companies=(GABIA,),
    )
    invoke = KindByModel(kimi=EVENT, sonnet=MARKET_REACTION)

    result = classify_kind(issue, _hybrid_client(invoke), KIMI)

    assert invoke.models == [KIMI, SONNET]
    assert (result.kind, result.model, result.escalated) == (MARKET_REACTION, SONNET, True)


def test_screen_event_without_cue_is_final():
    issue = _issue(1, "가비아 공개매수 추진", 0, titles=("맥쿼리 가비아 공개매수",))
    invoke = KindByModel(kimi=EVENT, sonnet=MARKET_REACTION)

    result = classify_kind(issue, _hybrid_client(invoke), KIMI)

    assert invoke.models == [KIMI]
    assert (result.kind, result.model, result.escalated) == (EVENT, KIMI, False)


@pytest.mark.parametrize("screen", ["", SONNET])
def test_kind_model_alone_without_a_distinct_screen_model(screen):
    issue = _issue(1, "가비아 목표가 상향", 0)
    invoke = KindByModel(kimi=EVENT, sonnet=MARKET_REACTION)

    result = classify_kind(issue, _hybrid_client(invoke), screen)

    assert invoke.models == [SONNET]
    assert (result.kind, result.model, result.escalated) == (MARKET_REACTION, SONNET, False)


def test_both_kind_answers_are_cached_per_model():
    issue = _issue(1, "가비아 실적 전망", 0)
    store = MemoryStore()
    first = classify_kind(issue, _hybrid_client(KindByModel(kimi=EVENT, sonnet=EVENT), store), KIMI)
    rows = sorted((r.stage, r.model, r.prompt_version) for r in store.rows.values())
    assert rows == [("issue_kind", KIMI, "issue-kind-v4"), ("issue_kind", SONNET, "issue-kind-v4")]

    def down(req):
        raise AssertionError("cached answers must be reused")

    client = _hybrid_client(down, store)
    again = classify_kind(issue, client, KIMI)

    assert again == first and again.escalated
    assert (client.stats.calls, client.stats.cache_hits) == (0, 2)
