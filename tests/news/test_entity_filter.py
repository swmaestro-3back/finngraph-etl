"""LLM 엔티티 필터 단위 테스트. Bedrock 은 부르지 않는다."""

from __future__ import annotations

from pipelines.news.transformers.filters import entity_filter
from pipelines.news.transformers.filters.entity_filter import (
    EntityJudgement,
    EntityJudgementList,
    filter_body_entities,
    merge_linked,
    resolve_judgements,
)


def _company(company_id: int, name: str, surface: str | None = None) -> dict:
    return {"company_id": company_id, "name": name, "surface": surface or name}


def _judgement(entity: str, keep: bool, mention: str | None = None) -> EntityJudgement:
    # 기본 mention 은 그 표기를 담은 문장이다 — 표기가 없는 mention 은 유지 판정을 무효로 만든다
    if mention is None:
        mention = f"{entity.strip()} 이 나오는 원문 문장"
    return EntityJudgement(entity=entity, mention=mention, reason="근거", keep=keep)


def _verdict(*pairs: tuple[str, bool]) -> EntityJudgementList:
    return EntityJudgementList(judgements=[_judgement(entity, keep) for entity, keep in pairs])


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


def test_resolve_keeps_only_companies_with_a_kept_surface():
    companies = [_company(300, "삼성SDI"), _company(400, "대상")]

    passed = resolve_judgements(companies, [_judgement("삼성SDI", True), _judgement("대상", False)])

    assert passed == [{"company_id": 300, "name": "삼성SDI"}]


def test_resolve_drops_surfaces_the_llm_skipped():
    companies = [_company(300, "삼성SDI"), _company(400, "포스코퓨처엠")]

    # 포스코퓨처엠의 판정이 빠졌다 — 애매하면 버린다
    assert resolve_judgements(companies, [_judgement("삼성SDI", True)]) == [
        {"company_id": 300, "name": "삼성SDI"}
    ]


def test_resolve_ignores_names_outside_input():
    companies = [_company(300, "삼성SDI")]

    passed = resolve_judgements(
        companies, [_judgement("삼성SDI", True), _judgement("테슬라", True)]
    )

    assert passed == [{"company_id": 300, "name": "삼성SDI"}]


def test_resolve_passes_company_once_when_any_spelling_is_kept():
    companies = [
        _company(300, "LG에너지솔루션", "LG엔솔"),
        _company(300, "LG에너지솔루션", "LG에너지솔루션"),
    ]

    passed = resolve_judgements(
        companies, [_judgement("LG엔솔", False), _judgement("LG에너지솔루션", True)]
    )

    assert passed == [{"company_id": 300, "name": "LG에너지솔루션"}]


def test_resolve_uses_first_judgement_and_strips_echo():
    companies = [_company(300, "삼성SDI")]

    passed = resolve_judgements(
        companies, [_judgement(" 삼성SDI ", False), _judgement("삼성SDI", True)]
    )

    assert passed == []


def test_resolve_drops_kept_surface_missing_from_its_mention():
    companies = [_company(300, "삼성SDI"), _company(400, "레이")]

    # 모델이 '오퍼레이터' 속 '레이'의 위치를 못 찾고 다른 문장을 인용했다 — 근거 없는 유지는 버린다
    passed = resolve_judgements(
        companies,
        [
            _judgement("삼성SDI", True, mention="엘앤에프가 삼성SDI에 양극재를 공급한다."),
            _judgement("레이", True, mention="엘앤에프가 삼성SDI에 양극재를 공급한다."),
        ],
    )

    assert passed == [{"company_id": 300, "name": "삼성SDI"}]


def test_resolve_passes_company_when_another_spelling_has_a_grounded_keep():
    companies = [
        _company(300, "LG에너지솔루션", "LG엔솔"),
        _company(300, "LG에너지솔루션", "LG에너지솔루션"),
    ]

    passed = resolve_judgements(
        companies,
        [
            _judgement("LG엔솔", True, mention="배터리 업계가 주목했다."),
            _judgement("LG에너지솔루션", True, mention="LG에너지솔루션이 ESS 를 공급한다."),
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


def test_filter_adds_kept_body_companies_to_linked_companies():
    seen: list[tuple[str, list[str]]] = []

    async def judge(text, surfaces):
        seen.append((text, surfaces))
        return _verdict(("삼성SDI", True), ("대상", False))

    item = _item(
        [_company(300, "삼성SDI"), _company(400, "대상")], text="엘앤에프가 삼성SDI에 공급한다."
    )

    result = filter_body_entities([item], judge=judge, max_concurrency=2, body_limit=12000)

    assert item["_linked_companies"] == [
        {"company_id": 100, "name": "엘앤에프"},
        {"company_id": 300, "name": "삼성SDI"},
    ]
    assert (result.judged, result.failed, result.kept) == (1, 0, 1)
    # 후보 표기가 본문 등장 순서 그대로 간다
    assert seen == [("엘앤에프가 삼성SDI에 공급한다.", ["삼성SDI", "대상"])]


def test_filter_skips_llm_for_articles_without_body_companies():
    async def judge(text, surfaces):  # pragma: no cover
        raise AssertionError("호출되면 안 된다")

    item = _item([])

    result = filter_body_entities([item], judge=judge, max_concurrency=2, body_limit=12000)

    assert item["_linked_companies"] == [{"company_id": 100, "name": "엘앤에프"}]
    assert (result.judged, result.failed, result.kept) == (0, 0, 0)


def test_filter_truncates_body_to_limit():
    seen: list[str] = []

    async def judge(text, surfaces):
        seen.append(text)
        return _verdict(("삼성SDI", True))

    item = _item([_company(300, "삼성SDI")], text="가" * 50)

    filter_body_entities([item], judge=judge, max_concurrency=1, body_limit=10)

    assert seen == ["가" * 10]


def test_filter_isolates_failures_and_counts_them():
    async def judge(text, surfaces):
        if "실패" in text:
            raise RuntimeError("bedrock down")
        return _verdict(("삼성SDI", True))

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


def test_prompt_drops_candidates_when_unsure():
    from pipelines.news.transformers.prompts import entity

    assert "a wrong keep is worse than a miss" in entity._SYSTEM
    # triples 용 프롬프트의 "애매하면 keep" 문장이 남아 있으면 안 된다
    assert "only its weight is in doubt, keep it" not in entity._SYSTEM


def _example_judgements() -> list[tuple[dict, list[dict]]]:
    import json

    from pipelines.news.transformers.prompts import entity

    return [(example, json.loads(example["output"])["judgements"]) for example in entity._EXAMPLES]


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
        # 남기는 경우와 버리는 경우를 둘 다 보여 준다
        assert {judgement["keep"] for judgement in judgements} == {True, False}


def test_prompt_is_written_for_body_only_candidates():
    """후보는 본문에만 나온 기업이다(제목 기업은 관련성 필터가 판정했다). 프롬프트와 예시가 그 상황을
    다루고, 실제 본문에서 가장 흔한 오탐(공유 버튼·저작권 문구, 원화 표기, 긴 단어의 일부,
    증권사·주가 나열)을 보여 준다."""
    from pipelines.news.transformers.prompts import entity

    assert "only in the body" in entity._SYSTEM
    assert "share buttons" in entity._SYSTEM
    assert "securities firm" in entity._SYSTEM
    assert "FULL article text" not in entity._SYSTEM

    verdicts = {
        judgement["entity"]: judgement["keep"]
        for _, judgements in _example_judgements()
        for judgement in judgements
    }
    # 버리는 것: 공유 버튼("카카오톡"), 원화 표기("한화 약 …원"), 긴 단어의 일부("하이브리드"),
    # 목표주가를 낸 증권사, 주가가 함께 오른 종목
    for dropped in ("카카오", "한화", "하이브", "키움증권", "포스코퓨처엠"):
        assert verdicts[dropped] is False
    # 남기는 것: 본문에만 나온 계약 상대방과 고객사
    for kept in ("삼성SDI", "LG에너지솔루션"):
        assert verdicts[kept] is True
