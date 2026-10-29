"""jev 관련성 필터 단위 테스트. jev 는 부르지 않고 judge 를 페이크로 준다.

판정 대상은 이 기사를 가져온 검색 종목뿐이다 — 다른 제목 기업은 엔티티 필터가 본문과 함께 판정한다."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

from pipelines.news.transformers.filters.relevance_filter import (
    GATES,
    ArticleInput,
    ArticleVerdict,
    CompanyVerdict,
    build_state,
    decide,
    filter_relevant_news,
)
from pipelines.news.transformers.prompts import relevance as questions


def _company(company_id: int, name: str, surface: str | None = None) -> dict:
    return {"company_id": company_id, "name": name, "surface": surface or name}


def _item(title: str, company: str, company_id: int = 100, others: tuple = ()) -> dict:
    """검색 종목이 첫 번째인 제목 기업 목록을 단 기사. others 는 검색하지 않은 제목 기업이다."""
    return {
        "title": title,
        "description": "",
        "link": f"https://x/{hash(title)}",
        "_query_company": {"company_id": company_id, "name": company},
        "_title_companies": [_company(company_id, company), *others],
    }


def _verdict(
    article: ArticleInput, valid: bool = True, per_company: dict | None = None
) -> ArticleVerdict:
    """판정 기업 전부를 valid 로 판정한다. per_company 로 표기별 값을 바꾼다."""
    per_company = per_company or {}
    return ArticleVerdict(
        companies=[
            CompanyVerdict(
                name=c["surface"], is_company=True, valid=per_company.get(c["surface"], valid)
            )
            for c in article.companies
        ]
    )


def _judge_with(**valid_by_surface):
    """표기별 valid 를 정한 judge. 목록에 없는 표기는 판정에서 빠진다(위치 순서 유지)."""

    async def judge(article):
        companies = []
        for c in article.companies:
            if c["surface"] not in valid_by_surface:
                break  # 판정이 짧게 끝난다 — 뒤쪽 기업은 판정 없음
            companies.append(
                CompanyVerdict(
                    name=c["surface"], is_company=True, valid=valid_by_surface[c["surface"]]
                )
            )
        return ArticleVerdict(companies=companies)

    return judge


# ── 기사 단위 흐름 ───────────────────────────────────────────────────────────


def test_each_article_is_judged_once_and_items_without_target_are_invalid():
    good = _item("엘앤에프, 양극재 공급 계약", "엘앤에프")
    market = _item("코스피 7000선 사수… 엘앤에프 강세", "엘앤에프")
    no_target = {"title": "대상 없음", "description": "", "link": "https://x/none"}
    sent: list[str] = []

    async def judge(article):
        sent.append(article.title)
        return _verdict(article, valid="코스피" not in article.title)

    result = filter_relevant_news([good, market, no_target], judge, 2)

    assert sorted(sent) == sorted(
        [good["title"], market["title"]]
    )  # 검색 종목 없는 기사는 안 보낸다
    assert result.passed == [good]
    assert result.invalid == [market, no_target]
    assert result.failed == []
    assert good["_linked_companies"] == [{"company_id": 100, "name": "엘앤에프"}]
    assert "_linked_companies" not in market


def test_judge_exception_fails_only_that_article():
    a = _item("A 기사", "엘앤에프")
    b = _item("B 기사", "에코프로", company_id=200)
    c = _item("C 기사", "삼성SDI", company_id=300)

    async def judge(article):
        if article.title == "B 기사":
            raise RuntimeError("jev down")
        return _verdict(article)

    result = filter_relevant_news([a, b, c], judge, 2)

    assert result.passed == [a, c]  # 입력 순서를 지킨다
    assert result.failed == [b]
    assert result.invalid == []


def test_concurrency_is_bounded_by_max_concurrency():
    items = [_item(f"기사 {i}", "엘앤에프") for i in range(9)]
    running = {"now": 0, "peak": 0}

    async def judge(article):
        running["now"] += 1
        running["peak"] = max(running["peak"], running["now"])
        await asyncio.sleep(0)
        running["now"] -= 1
        return _verdict(article)

    result = filter_relevant_news(items, judge, 3)

    assert result.passed == items
    assert running["peak"] <= 3


def test_empty_input_skips_jev():
    async def judge(article):  # pragma: no cover - 호출되면 안 된다
        raise AssertionError("jev 호출됨")

    result = filter_relevant_news([], judge, 2)

    assert result.passed == [] and result.invalid == [] and result.failed == []


def test_item_without_title_companies_is_judged_by_query_company_alone():
    item = {
        "title": "엘앤에프 CB 발행",
        "description": "",
        "link": "https://x/1",
        "_query_company": {"company_id": 100, "name": "엘앤에프"},
    }
    sent: list[tuple[dict, ...]] = []

    async def judge(article):
        sent.append(article.companies)
        return _verdict(article)

    result = filter_relevant_news([item], judge, 1)

    assert sent == [({"surface": "엘앤에프", "name": "엘앤에프"},)]
    assert result.passed == [item]
    assert item["_linked_companies"] == [{"company_id": 100, "name": "엘앤에프"}]


def test_raw_scores_are_kept_on_the_item_for_evaluation():
    item = _item("엘앤에프, 양극재 공급 계약", "엘앤에프")

    async def judge(article):
        return ArticleVerdict(
            companies=[
                CompanyVerdict(
                    name="엘앤에프", is_company=True, valid=True, scores={"has_event": 0.9}
                )
            ]
        )

    filter_relevant_news([item], judge, 1)

    assert item["_relevance_judgements"] == [
        {"name": "엘앤에프", "is_company": True, "valid": True, "scores": {"has_event": 0.9}}
    ]


# ── 판정을 기업으로 되돌리기 ─────────────────────────────────────────────────


def test_only_the_search_company_is_sent_and_linked_other_title_companies_go_to_the_entity_filter():
    item = _item(
        "두산밥캣·LG엔솔·현대차·SK하이닉스 협약",
        "두산밥캣",
        others=(
            _company(300, "LG에너지솔루션", "LG엔솔"),
            _company(400, "현대차"),
            _company(500, "SK하이닉스"),
        ),
    )
    sent: list[tuple[dict, ...]] = []

    async def judge(article):
        sent.append(article.companies)
        return _verdict(article)

    result = filter_relevant_news([item], judge, 1)

    # jev 에는 검색 종목의 제목 표기와 표준명만 간다 — 나머지 제목 기업은 묻지 않는다
    assert sent == [({"surface": "두산밥캣", "name": "두산밥캣"},)]
    assert result.passed == [item]
    assert item["_linked_companies"] == [{"company_id": 100, "name": "두산밥캣"}]


def test_search_company_invalid_drops_article_regardless_of_other_title_companies():
    item = _item(
        "LG엔솔, 두산밥캣코리아와 협약",
        "두산밥캣",
        others=(_company(300, "LG에너지솔루션", "LG엔솔"),),
    )

    async def judge(article):
        return _verdict(article, valid=False)

    result = filter_relevant_news([item], judge, 1)

    assert result.invalid == [item]
    assert "_linked_companies" not in item


def test_missing_target_verdict_is_failed():
    a = _item("A 기사", "엘앤에프", others=(_company(300, "삼성SDI"),))
    b = _item("B 기사", "에코프로", company_id=200)

    async def judge(article):
        if article.title == "A 기사":
            # 검색 종목(엘앤에프) 판정이 빠졌다 — 다른 기업 판정만으로는 통과시키지 않는다
            return ArticleVerdict(companies=[])
        return _verdict(article)

    result = filter_relevant_news([a, b], judge, 2)

    assert result.passed == [b]
    assert result.failed == [a]


def _fetched_by_both(title: str) -> dict:
    """삼성전자(100)·SK하이닉스(200) 두 검색에 걸려 중복 제거로 합쳐진 기사. 엔비디아(300)는 검색 안 함."""
    item = _item(
        title,
        "삼성전자",
        others=(_company(200, "SK하이닉스"), _company(300, "엔비디아")),
    )
    item["_query_companies"] = [
        {"company_id": 100, "name": "삼성전자"},
        {"company_id": 200, "name": "SK하이닉스"},
    ]
    return item


def test_article_passes_when_any_fetching_search_company_is_valid():
    item = _fetched_by_both("삼성전자·SK하이닉스, 엔비디아에 HBM 공급")
    sent: list[tuple[dict, ...]] = []

    async def judge(article):
        sent.append(article.companies)
        return await _judge_with(**{"삼성전자": False, "SK하이닉스": True})(article)

    result = filter_relevant_news([item], judge, 1)

    # 검색에 걸린 두 종목만 판정한다. 엔비디아는 검색하지 않았으니 엔티티 필터의 몫이다
    assert sent == [
        (
            {"surface": "삼성전자", "name": "삼성전자"},
            {"surface": "SK하이닉스", "name": "SK하이닉스"},
        )
    ]
    assert result.passed == [item]
    assert item["_linked_companies"] == [{"company_id": 200, "name": "SK하이닉스"}]


def test_article_is_invalid_when_every_fetching_search_company_is_invalid():
    item = _fetched_by_both("삼성전자·SK하이닉스, 엔비디아에 HBM 공급")
    judge = _judge_with(**{"삼성전자": False, "SK하이닉스": False})

    result = filter_relevant_news([item], judge, 1)

    assert result.invalid == [item]
    assert "_linked_companies" not in item


def test_missing_verdict_of_a_fetching_company_is_failed_when_none_is_valid():
    item = _fetched_by_both("삼성전자·SK하이닉스, 엔비디아에 HBM 공급")
    # SK하이닉스 판정이 빠졌고 삼성전자는 invalid — SK하이닉스가 valid 일 수 있으니 확정하지 않는다
    judge = _judge_with(**{"삼성전자": False})

    result = filter_relevant_news([item], judge, 1)

    assert result.failed == [item]


def test_one_valid_fetching_company_is_enough_even_if_another_is_missing():
    item = _fetched_by_both("삼성전자·SK하이닉스, 엔비디아에 HBM 공급")
    judge = _judge_with(**{"삼성전자": True})

    result = filter_relevant_news([item], judge, 1)

    assert result.passed == [item]
    assert item["_linked_companies"] == [{"company_id": 100, "name": "삼성전자"}]


def test_search_company_that_is_not_referred_to_does_not_store_article():
    # 제목의 '하이브' 는 폴로하이브머티리얼즈의 일부다 — valid 여도 통과가 아니다
    item = _item("폴로하이브머티리얼즈, 임직원 대상 성과급", "하이브")

    async def judge(article):
        return ArticleVerdict(
            companies=[CompanyVerdict(name="하이브", is_company=False, valid=True)]
        )

    result = filter_relevant_news([item], judge, 1)

    assert result.invalid == [item]
    assert "_linked_companies" not in item


# ── jev state·질문·임계값 ────────────────────────────────────────────────────


def test_build_state_sends_headline_and_companies_only():
    article = ArticleInput(
        title="엘앤에프, 삼성SDI에 양극재 공급",
        companies=(
            {"surface": "엘앤에프", "name": "엘앤에프"},
            {"surface": "삼성SDI", "name": "삼성SDI"},
        ),
    )

    assert build_state(article) == {
        "headline": "엘앤에프, 삼성SDI에 양극재 공급",
        "companies": [
            {"surface": "엘앤에프", "name": "엘앤에프"},
            {"surface": "삼성SDI", "name": "삼성SDI"},
        ],
    }


def _answers(is_company: float, **gates: float) -> dict:
    answers = {"c0_is_company": SimpleNamespace(noul=is_company)}
    for gate in GATES:
        answers[f"c0_{gate}"] = SimpleNamespace(noul=gates.get(gate, 0.0))
    return answers


def test_decide_is_valid_when_any_gate_clears_the_threshold():
    is_company, valid, scores = decide(_answers(0.95, has_event=0.8), 0)

    assert is_company and valid
    assert scores == {"is_company": 0.95, "has_event": 0.8, "has_driver": 0.0}


def test_decide_is_invalid_when_every_gate_is_below_threshold():
    _, valid, _ = decide(_answers(0.95, has_event=0.49, has_driver=0.3), 0)

    assert not valid


def test_decide_separates_is_company_from_valid():
    is_company, valid, _ = decide(_answers(0.49, has_event=0.9), 0)

    assert not is_company and valid  # 연결은 둘 다 true 여야 한다 — resolve_verdict 가 본다


def test_questions_cover_every_company_with_three_questions_each():
    built = questions.build_questions(3)

    assert len(built) == 9
    assert {k for k in built if k.startswith("c2_")} == {
        "c2_is_company",
        "c2_has_event",
        "c2_has_driver",
    }
    # 질문은 state 의 기업을 경로로 가리킨다
    assert "`companies[2].surface`" in built["c2_is_company"].instructions["question"]
    assert "`companies[2].name`" in built["c2_has_event"].instructions["question"]


def test_questions_keep_the_gate_boundaries_and_the_namesake_rule():
    built = questions.build_questions(1)

    # SDK 는 criteria 를 {true, false} 딕셔너리로 둔다
    event = built["c0_has_event"].criteria
    # 실행된 관계(계약·수주)와 기업 사건(실적·CB)이 한 질문이고, 제목에 명시된 상대도 통과한다
    assert "counterparty passes as much as the actor" in event["true"]["what"]
    assert "earnings" in event["true"]["what"] and "supply" in event["true"]["what"]
    assert "'관련주'" in event["false"]["what"]
    assert "market-wide" in built["c0_has_driver"].criteria["false"]["what"]
    # 동반 움직임은 별도 질문이 아니라 각 질문의 false 기준이다
    assert "only moves with it" in event["false"]["what"]
    assert "another company" in built["c0_has_driver"].criteria["false"]["what"]
    # 피인수·지분 매각의 대상도 참여자다
    assert "acquired or merged company" in event["true"]["what"]
    assert "whose stake is sold" in event["true"]["what"]
    # 국제정세·거시 사건은 제목이 이 기업의 사업과의 연결을 말할 때만 동인이다
    driver = built["c0_has_driver"].criteria
    assert "geopolitical or macro event" in driver["true"]["what"]
    assert "reaches this company's own products, markets, or projects" in driver["true"]["what"]
    assert "only to the index" in driver["false"]["what"]
    # 매처가 못 가르는 동명(폴로하이브머티리얼즈의 하이브, 태양광의 태양)은 is_company 가 거른다
    assert "태양광" in built["c0_is_company"].criteria["false"]
    assert "폴로하이브머티리얼즈" in built["c0_is_company"].criteria["false"]


def test_latency_is_recorded_per_judge_call_including_failures():
    a = _item("A 기사", "엘앤에프")
    b = _item("B 기사", "에코프로", company_id=200)
    c = {"title": "검색 종목 없음", "link": "https://x/none", "_title_companies": []}

    async def judge(article):
        if article.title == "B 기사":
            raise RuntimeError("jev down")
        return _verdict(article)

    result = filter_relevant_news([a, b, c], judge, 2)

    # 호출한 기사(성공·실패)마다 하나. jev 를 부르지 않은 기사는 세지 않는다
    assert len(result.latencies_ms) == 2
    assert all(ms >= 0 for ms in result.latencies_ms)
