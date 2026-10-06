"""LLM 엔티티 필터 단위 테스트. Bedrock 은 부르지 않는다."""

from __future__ import annotations

import json

from pipelines.news.transformers.filters import entity_filter
from pipelines.news.transformers.filters.entity_filter import (
    KEPT_ROLES,
    EntityJudgement,
    EntityJudgementList,
    filter_body_entities,
    merge_linked,
    resolve_judgements,
)
from pipelines.news.transformers.prompts import entity


def _company(company_id: int, name: str, surface: str | None = None) -> dict:
    return {"company_id": company_id, "name": name, "surface": surface or name}


def _judgement(entity: str, role: str, mention: str | None = None) -> EntityJudgement:
    # 기본 mention 은 그 표기를 담은 문장이다 — 표기가 없는 mention 은 통과 판정을 무효로 만든다
    if mention is None:
        mention = f"{entity.strip()} 이 나오는 원문 문장"
    return EntityJudgement(entity=entity, mention=mention, reason="근거", role=role)


def _verdict(*pairs: tuple[str, str]) -> EntityJudgementList:
    return EntityJudgementList(judgements=[_judgement(entity, role) for entity, role in pairs])


def _item(body_companies: list[dict], linked: list[dict] | None = None, text: str = "본문") -> dict:
    return {
        "title": "엘앤에프, 양극재 공급",
        "_text": text,
        "_body_companies": body_companies,
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


def test_resolve_drops_surfaces_the_llm_skipped():
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


def test_resolve_drops_party_surface_missing_from_its_mention():
    companies = [_company(300, "삼성SDI"), _company(400, "레이")]

    # 모델이 '오퍼레이터' 속 '레이'의 위치를 못 찾고 다른 문장을 인용했다 — 근거 없는 통과는 버린다
    passed = resolve_judgements(
        companies,
        [
            _judgement("삼성SDI", "party", mention="엘앤에프가 삼성SDI에 양극재를 공급한다."),
            _judgement("레이", "party", mention="엘앤에프가 삼성SDI에 양극재를 공급한다."),
        ],
    )

    assert passed == [{"company_id": 300, "name": "삼성SDI"}]


def test_resolve_passes_company_when_another_spelling_has_a_grounded_party():
    companies = [
        _company(300, "LG에너지솔루션", "LG엔솔"),
        _company(300, "LG에너지솔루션", "LG에너지솔루션"),
    ]

    passed = resolve_judgements(
        companies,
        [
            _judgement("LG엔솔", "party", mention="배터리 업계가 주목했다."),
            _judgement("LG에너지솔루션", "party", mention="LG에너지솔루션이 ESS 를 공급한다."),
        ],
    )

    assert passed == [{"company_id": 300, "name": "LG에너지솔루션"}]


def test_merge_linked_keeps_title_companies_first_without_duplicates():
    title = [{"company_id": 100, "name": "엘앤에프"}]
    body = [{"company_id": 300, "name": "삼성SDI"}, {"company_id": 100, "name": "엘앤에프"}]

    assert merge_linked(title, body) == [
        {"company_id": 100, "name": "엘앤에프"},
        {"company_id": 300, "name": "삼성SDI"},
    ]


# ── filter_body_entities ─────────────────────────────────────────────────────


def test_filter_gives_judge_headline_and_adds_party_companies_to_linked():
    seen: list[tuple] = []

    async def judge(title, headline_companies, text, surfaces):
        seen.append((title, headline_companies, text, surfaces))
        return _verdict(("삼성SDI", "party"), ("대상", "not_company"))

    item = _item(
        [_company(300, "삼성SDI"), _company(400, "대상")], text="엘앤에프가 삼성SDI에 공급한다."
    )

    result = filter_body_entities([item], judge=judge, max_concurrency=2, body_limit=12000)

    assert item["_linked_companies"] == [
        {"company_id": 100, "name": "엘앤에프"},
        {"company_id": 300, "name": "삼성SDI"},
    ]
    assert (result.judged, result.failed, result.kept) == (1, 0, 1)
    # 제목과 제목 통과 기업의 이름이 가고, 후보 표기는 본문 등장 순서 그대로 간다
    assert seen == [
        (
            "엘앤에프, 양극재 공급",
            ["엘앤에프"],
            "엘앤에프가 삼성SDI에 공급한다.",
            ["삼성SDI", "대상"],
        )
    ]


def test_filter_drops_background_company_cited_to_explain_the_headline():
    """삼성SDS 급등 기사에서 수혜의 근거로 인용된 삼성전자는 연결하지 않는다."""

    async def judge(title, headline_companies, text, surfaces):
        return _verdict(("삼성전자", "background"))

    item = _item(
        [_company(500, "삼성전자")], linked=[{"company_id": 200, "name": "삼성에스디에스"}]
    )

    result = filter_body_entities([item], judge=judge, max_concurrency=1, body_limit=12000)

    assert item["_linked_companies"] == [{"company_id": 200, "name": "삼성에스디에스"}]
    assert result.kept == 0


def test_filter_counts_roles_of_judged_candidates_only():
    async def judge(title, headline_companies, text, surfaces):
        return _verdict(
            ("삼성SDI", "party"),
            ("삼성전자", "background"),
            ("키움증권", "source"),
            ("테슬라", "party"),  # 입력에 없는 이름은 세지 않는다
        )

    item = _item([_company(300, "삼성SDI"), _company(500, "삼성전자"), _company(700, "키움증권")])

    result = filter_body_entities([item], judge=judge, max_concurrency=1, body_limit=12000)

    assert dict(result.roles) == {"party": 1, "background": 1, "source": 1}


def test_filter_skips_llm_for_articles_without_body_companies():
    async def judge(title, headline_companies, text, surfaces):  # pragma: no cover
        raise AssertionError("호출되면 안 된다")

    item = _item([])

    result = filter_body_entities([item], judge=judge, max_concurrency=2, body_limit=12000)

    assert item["_linked_companies"] == [{"company_id": 100, "name": "엘앤에프"}]
    assert (result.judged, result.failed, result.kept) == (0, 0, 0)


def test_filter_truncates_body_to_limit():
    seen: list[str] = []

    async def judge(title, headline_companies, text, surfaces):
        seen.append(text)
        return _verdict(("삼성SDI", "party"))

    item = _item([_company(300, "삼성SDI")], text="가" * 50)

    filter_body_entities([item], judge=judge, max_concurrency=1, body_limit=10)

    assert seen == ["가" * 10]


def test_filter_isolates_failures_and_counts_them():
    async def judge(title, headline_companies, text, surfaces):
        if "실패" in text:
            raise RuntimeError("bedrock down")
        return _verdict(("삼성SDI", "party"))

    failed = _item([_company(300, "삼성SDI")], text="실패하는 본문")
    passed = _item([_company(300, "삼성SDI")], text="정상 본문")

    result = filter_body_entities(
        [failed, passed], judge=judge, max_concurrency=2, body_limit=12000
    )

    # 실패한 기사는 제목 기업만 남는다
    assert failed["_linked_companies"] == [{"company_id": 100, "name": "엘앤에프"}]
    assert [c["company_id"] for c in passed["_linked_companies"]] == [100, 300]
    assert (result.judged, result.failed, result.kept) == (2, 1, 1)


def test_filter_without_targets_does_not_build_a_judge(monkeypatch):
    def fail():
        raise AssertionError("Bedrock 체인을 만듦")

    monkeypatch.setattr(entity_filter, "get_entity_judge", fail)

    result = filter_body_entities([_item([])])

    assert (result.judged, result.failed, result.kept) == (0, 0, 0)


# ── 프롬프트 ─────────────────────────────────────────────────────────────────


def _example_judgements() -> list[tuple[dict, list[dict]]]:
    return [(example, json.loads(example["output"])["judgements"]) for example in entity._EXAMPLES]


def test_prompt_judges_role_against_the_headline_and_drops_when_unsure():
    assert "a wrong party is worse than a miss" in entity._SYSTEM
    assert 'the role is not "party"' in entity._SYSTEM
    # 제목과 제목 기업을 받아 주된 뉴스를 먼저 정한다
    assert "First name the main news" in entity._SYSTEM
    for variable in ("{title}", "{headline_companies}", "{text}", "{entities}"):
        assert variable in entity._HUMAN
    assert set(entity.PROMPT.input_variables) == {"title", "headline_companies", "text", "entities"}


def test_prompt_roles_match_the_schema():
    """프롬프트가 설명하는 역할 이름과 구조화 출력의 Literal 이 같다."""
    schema_roles = set(EntityJudgement.model_json_schema()["properties"]["role"]["enum"])
    for role in schema_roles:
        assert f'"{role}"' in entity._SYSTEM
    assert {j["role"] for _, js in _example_judgements() for j in js} == schema_roles


def test_prompt_examples_follow_the_hard_rules():
    """예시가 프롬프트의 Hard Rules 를 스스로 지킨다 — 어기면 모델도 그렇게 배운다."""
    for example, judgements in _example_judgements():
        candidates = [line[2:] for line in example["entities"].splitlines()]

        # 후보마다 정확히 한 번, 같은 순서로, 표기 그대로
        assert [judgement["entity"] for judgement in judgements] == candidates
        for judgement in judgements:
            # mention 은 본문에서 그대로 옮긴 문장이고 그 표기를 담는다
            assert judgement["mention"] in example["text"]
            assert judgement["entity"] in judgement["mention"]
            assert judgement["reason"]
        # 제목 기업은 후보에 없다
        for headline_company in example["headline_companies"].split(", "):
            assert headline_company not in candidates


def test_prompt_examples_cover_the_common_false_hits():
    """후보는 본문에만 나온 기업이다(제목 기업은 관련성 필터가 판정했다). 예시가 실제 본문에서 가장
    흔한 오탐(공유 버튼·저작권 문구, 원화 표기, 긴 단어의 일부, 증권사·주가 나열)과 주인공 기업의
    뉴스를 설명하려고 인용된 다른 기업(그룹사 계획·다른 기업 실적)을 보여 준다."""
    assert "only in the body" in entity._SYSTEM
    assert "share buttons" in entity._SYSTEM
    assert "parent or group company's plan" in entity._SYSTEM
    assert "Affiliate or group label" in entity._SYSTEM

    roles: dict[str, set[str]] = {}
    for _, judgements in _example_judgements():
        for judgement in judgements:
            roles.setdefault(judgement["entity"], set()).add(judgement["role"])
    roles = {entity: role for entity, (role,) in roles.items()}  # 같은 표기는 예시마다 같은 역할
    assert roles["카카오"] == "not_company"  # 공유 버튼("카카오톡"), "카카오 계열사" 소속 표기
    assert roles["한화"] == "not_company"  # 원화 표기
    assert roles["레이"] == "not_company"  # 긴 단어의 일부("인플레이션")
    assert roles["키움증권"] == "source"  # 목표주가를 낸 증권사
    assert roles["포스코퓨처엠"] == "listed"  # 함께 오른 종목
    # 삼성SDS 수혜 기사의 삼성전자(그룹사 계획)와 SK텔레콤(다른 기업 실적)은 배경이다
    assert roles["삼성전자"] == "background"
    assert roles["SK텔레콤"] == "background"
    # 남기는 것: 계약 상대방, 수주의 발주처, 주가 급등 뒤 사건의 공동 취득자
    for party in ("삼성SDI", "삼성바이오로직스", "삼성카드"):
        assert roles[party] == "party"
