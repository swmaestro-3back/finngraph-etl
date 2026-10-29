"""jev 엔티티 필터 단위 테스트. jev 는 부르지 않는다."""

from __future__ import annotations

from pipelines.news.transformers.filters import entity_filter
from pipelines.news.transformers.filters.entity_filter import (
    KEPT_ROLES,
    THRESHOLDS,
    EntityJudgement,
    EntityJudgementList,
    build_state,
    decide,
    filter_candidates,
    merge_linked,
    resolve_judgements,
    token_is_name,
    tokens_with,
)
from pipelines.news.transformers.prompts import entity as questions


def _company(company_id: int, name: str, surface: str | None = None) -> dict:
    return {"company_id": company_id, "name": name, "surface": surface or name}


def _judgement(entity: str, role: str) -> EntityJudgement:
    return EntityJudgement(entity=entity, role=role)


def _verdict(*pairs: tuple[str, str]) -> EntityJudgementList:
    return EntityJudgementList(judgements=[_judgement(entity, role) for entity, role in pairs])


def _item(candidates: list[dict], linked: list[dict] | None = None, text: str = "본문") -> dict:
    return {
        "title": "엘앤에프, 양극재 공급",
        "_text": text,
        "_candidates": candidates,
        "_linked_companies": linked
        if linked is not None
        else [{"company_id": 100, "name": "엘앤에프"}],
    }


# ── resolve_judgements ───────────────────────────────────────────────────────


def test_only_party_is_linked():
    assert KEPT_ROLES == {"party"}


def test_resolve_keeps_only_companies_with_a_party_surface():
    companies = [
        _company(300, "삼성SDI"),
        _company(400, "대상"),
        _company(500, "삼성전자"),
        _company(600, "포스코퓨처엠"),
        _company(700, "키움증권"),
    ]

    passed = resolve_judgements(
        companies,
        [
            _judgement("삼성SDI", "party"),
            _judgement("대상", "not_company"),
            _judgement("삼성전자", "background"),
            _judgement("포스코퓨처엠", "listed"),
            _judgement("키움증권", "source"),
        ],
    )

    assert passed == [{"company_id": 300, "name": "삼성SDI"}]


def test_resolve_drops_surfaces_the_judge_skipped():
    companies = [_company(300, "삼성SDI"), _company(400, "포스코퓨처엠")]

    # 포스코퓨처엠의 판정이 빠졌다 — 애매하면 버린다
    assert resolve_judgements(companies, [_judgement("삼성SDI", "party")]) == [
        {"company_id": 300, "name": "삼성SDI"}
    ]


def test_resolve_ignores_names_outside_input():
    companies = [_company(300, "삼성SDI")]

    passed = resolve_judgements(
        companies, [_judgement("삼성SDI", "party"), _judgement("테슬라", "party")]
    )

    assert passed == [{"company_id": 300, "name": "삼성SDI"}]


def test_resolve_passes_company_once_when_any_spelling_is_party():
    companies = [
        _company(300, "LG에너지솔루션", "LG엔솔"),
        _company(300, "LG에너지솔루션", "LG에너지솔루션"),
    ]

    passed = resolve_judgements(
        companies, [_judgement("LG엔솔", "listed"), _judgement("LG에너지솔루션", "party")]
    )

    assert passed == [{"company_id": 300, "name": "LG에너지솔루션"}]


def test_resolve_uses_first_judgement_and_strips_echo():
    companies = [_company(300, "삼성SDI")]

    passed = resolve_judgements(
        companies, [_judgement(" 삼성SDI ", "background"), _judgement("삼성SDI", "party")]
    )

    assert passed == []


def test_resolve_passes_company_once_when_both_spellings_are_party():
    companies = [
        _company(300, "LG에너지솔루션", "LG엔솔"),
        _company(300, "LG에너지솔루션", "LG에너지솔루션"),
    ]

    passed = resolve_judgements(
        companies, [_judgement("LG엔솔", "party"), _judgement("LG에너지솔루션", "party")]
    )

    assert passed == [{"company_id": 300, "name": "LG에너지솔루션"}]


def test_merge_linked_keeps_search_companies_first_without_duplicates():
    query = [{"company_id": 100, "name": "엘앤에프"}]
    passed = [{"company_id": 300, "name": "삼성SDI"}, {"company_id": 100, "name": "엘앤에프"}]

    assert merge_linked(query, passed) == [
        {"company_id": 100, "name": "엘앤에프"},
        {"company_id": 300, "name": "삼성SDI"},
    ]


# ── filter_candidates ─────────────────────────────────────────────────────


def test_filter_gives_judge_headline_and_adds_party_companies_to_linked():
    seen: list[tuple] = []

    async def judge(title, query_companies, text, candidates):
        seen.append((title, query_companies, text, candidates))
        return _verdict(("삼성SDI", "party"), ("대상", "not_company"))

    item = _item(
        [_company(300, "삼성SDI"), _company(400, "대상")], text="엘앤에프가 삼성SDI에 공급한다."
    )

    result = filter_candidates([item], judge=judge, max_concurrency=2)

    assert item["_linked_companies"] == [
        {"company_id": 100, "name": "엘앤에프"},
        {"company_id": 300, "name": "삼성SDI"},
    ]
    assert (result.judged, result.failed, result.kept) == (1, 0, 1)
    # 제목과 관련성을 통과한 검색 종목 이름이 가고, 후보는 사전 표준명·표기로 등장 순서 그대로 간다
    assert seen == [
        (
            "엘앤에프, 양극재 공급",
            ["엘앤에프"],
            "엘앤에프가 삼성SDI에 공급한다.",
            [{"name": "삼성SDI", "surface": "삼성SDI"}, {"name": "대상", "surface": "대상"}],
        )
    ]


def test_filter_drops_background_company_cited_to_explain_the_headline():
    """삼성SDS 급등 기사에서 수혜의 근거로 인용된 삼성전자는 연결하지 않는다."""

    async def judge(title, query_companies, text, candidates):
        return _verdict(("삼성전자", "background"))

    item = _item(
        [_company(500, "삼성전자")], linked=[{"company_id": 200, "name": "삼성에스디에스"}]
    )

    result = filter_candidates([item], judge=judge, max_concurrency=1)

    assert item["_linked_companies"] == [{"company_id": 200, "name": "삼성에스디에스"}]
    assert result.kept == 0


def test_filter_counts_roles_of_judged_candidates_only():
    async def judge(title, query_companies, text, candidates):
        return _verdict(
            ("삼성SDI", "party"),
            ("삼성전자", "background"),
            ("키움증권", "source"),
            ("테슬라", "party"),  # 입력에 없는 이름은 세지 않는다
        )

    item = _item([_company(300, "삼성SDI"), _company(500, "삼성전자"), _company(700, "키움증권")])

    result = filter_candidates([item], judge=judge, max_concurrency=1)

    assert dict(result.roles) == {"party": 1, "background": 1, "source": 1}


def test_filter_skips_jev_for_articles_without_candidates():
    async def judge(title, query_companies, text, candidates):  # pragma: no cover
        raise AssertionError("호출되면 안 된다")

    item = _item([])

    result = filter_candidates([item], judge=judge, max_concurrency=2)

    assert item["_linked_companies"] == [{"company_id": 100, "name": "엘앤에프"}]
    assert (result.judged, result.failed, result.kept) == (0, 0, 0)


def test_filter_isolates_failures_and_counts_them():
    async def judge(title, query_companies, text, candidates):
        if "실패" in text:
            raise RuntimeError("jev down")
        return _verdict(("삼성SDI", "party"))

    failed = _item([_company(300, "삼성SDI")], text="실패하는 본문")
    passed = _item([_company(300, "삼성SDI")], text="정상 본문")

    result = filter_candidates([failed, passed], judge=judge, max_concurrency=2)

    # 실패한 기사는 검색 종목만 남는다
    assert failed["_linked_companies"] == [{"company_id": 100, "name": "엘앤에프"}]
    assert [c["company_id"] for c in passed["_linked_companies"]] == [100, 300]
    assert (result.judged, result.failed, result.kept) == (2, 1, 1)


def test_filter_without_targets_does_not_build_a_judge(monkeypatch):
    def fail():
        raise AssertionError("jev judge 를 만듦")

    monkeypatch.setattr(entity_filter, "get_entity_judge", fail)

    result = filter_candidates([_item([])])

    assert (result.judged, result.failed, result.kept) == (0, 0, 0)


def test_latency_is_recorded_per_judge_call_including_failures():
    async def judge(title, query_companies, text, candidates):
        if title.startswith("실패"):
            raise RuntimeError("jev down")
        return _verdict(("삼성SDI", "party"))

    ok = _item([_company(300, "삼성SDI")], text="엘앤에프가 삼성SDI에 공급한다.")
    bad = _item([_company(300, "삼성SDI")], text="엘앤에프가 삼성SDI에 공급한다.")
    bad["title"] = "실패 기사"

    result = filter_candidates([ok, bad], judge=judge, max_concurrency=2)

    assert (result.judged, result.failed) == (2, 1)
    assert len(result.latencies_ms) == 2
    assert all(ms >= 0 for ms in result.latencies_ms)


# ── 코드 단계: 어절 규칙 ──────────────────────────────────────────────────────


def test_token_rule_passes_name_alone_or_with_particles():
    sentences = [
        "엘앤에프는 삼성SDI와 계약했다.",
        "SK그룹에서 SK에코플랜트가 매각한다.",
        "강남제비스코와는 협력한다.",
    ]
    assert token_is_name("삼성SDI", tokens_with("삼성SDI", sentences))
    assert token_is_name("SK", tokens_with("SK", sentences))  # 'SK그룹에서' = 그룹 + 에서
    assert token_is_name("강남제비스코", tokens_with("강남제비스코", sentences))  # 겹조사 와 + 는


def test_token_rule_drops_fragments_of_longer_words():
    cases = {
        "SK": "SK하이닉스는 이날 SK실트론과 함께 올랐다.",
        "디오": "디오션자산운용의 펀드가 참여했다.",
        "레이": "인플레이션 감축법을 고려한다.",
        "비자": "소비자가 늘었다.",  # 표기 앞에 글자가 붙은 어절
        "에스엠": "에스엠엔터테인먼트 등 콘텐츠 계열사",
    }
    for surface, sentence in cases.items():
        assert not token_is_name(surface, tokens_with(surface, [sentence])), surface


def test_token_rule_handles_surfaces_with_spaces_and_list_separators():
    assert tokens_with("YG PLUS", ["YG PLUS는 이날 공시했다. (YG PLUS) 관계자"]) == [
        "YG PLUS는",
        "YG PLUS",
    ]
    assert tokens_with("LG이노텍", ["엔비디아 본사를 방문한 LG전자·LG씨엔에스·LG이노텍 3사"]) == [
        "LG이노텍"
    ]
    assert tokens_with("포스코퓨처엠", ["포스코퓨처엠(2.1%) 등이 올랐다."]) == ["포스코퓨처엠"]


def test_build_state_sends_lead_and_candidate_sentences_and_drops_fragments():
    text = (
        "엘앤에프는 삼성SDI와 하이니켈 양극재 공급 계약을 체결했다고 5일 공시했다.\n"
        "계약 규모는 7억9000만 달러(한화 약 1조2200억원)다.\n"
        "이날 SK하이닉스는 2% 올랐다.\n"
        "넷째 문단은 리드에 들어가지 않는다."
    )
    state, dropped = build_state(
        "엘앤에프, 공급 계약",
        ["엘앤에프"],
        text,
        [{"name": n, "surface": n} for n in ("삼성SDI", "한화", "SK")],
    )

    assert dropped == ["SK"]  # 'SK하이닉스'의 일부 — 모델에 묻지 않는다
    assert state["headline"] == "엘앤에프, 공급 계약"
    assert state["query_stocks"] == ["엘앤에프"]
    assert state["lead"].endswith("2% 올랐다.") and "넷째 문단" not in state["lead"]
    assert [(c["name"], c["surface"]) for c in state["candidates"]] == [
        ("삼성SDI", "삼성SDI"),
        ("한화", "한화"),
    ]
    assert state["candidates"][0]["sentences"] == [
        "엘앤에프는 삼성SDI와 하이니켈 양극재 공급 계약을 체결했다고 5일 공시했다."
    ]
    # 원화 표기 '한화' 는 어절로는 깨끗해서 모델(is_company)에게 간다
    assert state["candidates"][1]["sentences"] == [
        "계약 규모는 7억9000만 달러(한화 약 1조2200억원)다."
    ]


def test_build_state_reads_the_headline_as_the_first_sentence():
    # 제목에만 나온 기업은 제목 문장으로 토큰 규칙과 모델을 통과한다. 'SK하이닉스' 의 SK 는 제목에서도 조각이다
    state, dropped = build_state(
        "두산밥캣·현대차, SK하이닉스와 협약",
        ["두산밥캣"],
        "두산밥캣이 협약을 맺었다.\n본문에는 그 이름이 없다.",
        [{"name": n, "surface": n} for n in ("현대차", "SK")],
    )

    assert dropped == ["SK"]
    assert state["candidates"] == [
        {"name": "현대차", "surface": "현대차", "sentences": ["두산밥캣·현대차, SK하이닉스와 협약"]}
    ]
    assert state["lead"] == "두산밥캣이 협약을 맺었다.\n본문에는 그 이름이 없다."  # 리드는 본문만


# ── 코드 단계: 임계값 ──────────────────────────────────────────────────────────


class _Noul:
    def __init__(self, p):
        self.noul = p


class _Choice:
    def __init__(self, probabilities):
        self.probabilities = probabilities
        self.choice = max(probabilities, key=probabilities.__getitem__)
        self.confidence = 1.0


def _answers(is_company, **probabilities):
    return {"c0_is_company": _Noul(is_company), "c0_role": _Choice(probabilities)}


def test_decide_links_party_only_above_both_thresholds():
    role, scores = decide(_answers(0.98, party=0.9, background=0.1, listed=0.0, source=0.0), 0)
    assert role == "party"
    assert (
        scores["is_company"] == 0.98 and scores["p_party"] == 0.9 and scores["p_background"] == 0.1
    )


def test_decide_reports_not_company_below_company_threshold():
    role, _ = decide(_answers(THRESHOLDS["company_min"] - 0.01, party=0.9, background=0.1), 0)
    assert role == "not_company"


def test_decide_falls_back_to_best_non_party_role_when_party_is_below_threshold():
    # party 가 최다 옵션이어도 임계값에 못 미치면 연결하지 않고, 차순위 역할을 보고한다
    role, _ = decide(_answers(0.9, party=0.45, background=0.4, listed=0.15), 0)
    assert role == "background"
    role, _ = decide(_answers(0.9, party=0.1, background=0.2, listed=0.7), 0)
    assert role == "listed"


# ── 질문 ──────────────────────────────────────────────────────────────────────


def test_questions_cover_every_candidate_with_two_questions_each():
    built = questions.build_questions(3, "lead")
    assert list(built) == [f"c{i}_{q}" for i in range(3) for q in ("is_company", "role")]
    role = built["c2_role"].model_dump(mode="json")
    assert "`candidates[2].sentences`" in role["instructions"]["question"]
    assert "`lead`" in role["instructions"]["question"]
    assert "`query_stocks`" in role["instructions"]["question"]  # 주인공 = 검색 종목


def test_role_options_are_the_schema_roles_minus_not_company_with_party_last():
    schema_roles = set(EntityJudgement.model_json_schema()["properties"]["role"]["enum"])
    options = list(questions.ROLE_CRITERIA)
    assert set(options) == schema_roles - {"not_company"}  # not_company 는 is_company 질문이 맡는다
    assert options[-1] == "party"  # 첫 옵션 편향 때문에 맨 뒤
    for option in options:
        assert (
            questions.ROLE_CRITERIA[option]["what"] and questions.ROLE_CRITERIA[option]["examples"]
        )


def test_role_criteria_keep_the_boundary_rules_and_the_bias():
    party, background, listed = (
        questions.ROLE_CRITERIA[k] for k in ("party", "background", "listed")
    )
    assert "customer as much as the supplier" in party["what"]  # 공급계약의 상대방
    assert "nearest listed parent" in party["what"]  # 비상장 계열사 → 모회사
    assert "A wrong party is worse than a miss" in party["not_for"]  # 애매하면 party 아님
    assert "financial service provider" in background["what"]  # 주관사·수탁사
    assert "existing customer, supplier, or partner" in background["what"]  # 고객 설명
    assert "counterparties of one contract" in listed["not_for"]  # 고객사 열거는 나열이 아니다
    assert "won currency notation" in questions.IS_COMPANY_FALSE
