"""LLM 관련성 필터(묶음 호출) 단위 테스트. Bedrock 은 부르지 않고 judge 를 페이크로 준다."""

from __future__ import annotations

import logging

from pipelines.news.transformers.filters.relevance_filter import (
    ArticleInput,
    ArticleVerdict,
    BatchVerdict,
    CompanyVerdict,
    build_relevance_input,
    chunked,
    filter_relevant_news,
)


def _company(company_id: int, name: str, surface: str | None = None) -> dict:
    return {"company_id": company_id, "name": name, "surface": surface or name}


def _item(title: str, company: str, company_id: int = 100, others: tuple = ()) -> dict:
    """검색 종목이 첫 번째인 판정 기업 목록을 단 기사."""
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
    """판정 기업 전부를 valid 로 판정한다. per_company 로 기업별 값을 바꾼다."""
    per_company = per_company or {}
    return ArticleVerdict(
        id=article.id,
        companies=[
            CompanyVerdict(name=name, valid=per_company.get(name, valid))
            for name in article.companies
        ],
    )


def test_build_relevance_input_numbers_articles_and_lists_companies():
    text = build_relevance_input(
        [
            ArticleInput(0, "엘앤에프, 삼성SDI에 양극재 공급", "LFP", ("엘앤에프", "삼성SDI")),
            ArticleInput(3, "코스피 마감", "", ("삼성전자",)),
        ]
    )

    assert (
        "[기사 0]\n제목: 엘앤에프, 삼성SDI에 양극재 공급\n요약: LFP\n판정 기업: 엘앤에프, 삼성SDI"
        in text
    )
    assert "[기사 3]\n제목: 코스피 마감\n요약: \n판정 기업: 삼성전자" in text


def test_chunked():
    assert chunked([1, 2, 3, 4, 5], 2) == [[1, 2], [3, 4], [5]]
    assert chunked([], 3) == []


def test_batch_matches_ids_out_of_order_and_skips_items_without_target():
    good = _item("엘앤에프, 양극재 공급 계약", "엘앤에프")
    market = _item("코스피 7000선 사수… 엘앤에프 강세", "엘앤에프")
    no_target = {"title": "대상 없음", "description": "", "link": "https://x/none"}
    calls: list[list[int]] = []

    async def judge(articles):
        calls.append([a.id for a in articles])
        by_id = {a.id: a for a in articles}
        # 순서를 뒤집어 돌려준다
        return BatchVerdict(verdicts=[_verdict(by_id[1], valid=False), _verdict(by_id[0])])

    result = filter_relevant_news([good, market, no_target], judge, 2, 10)

    assert calls == [[0, 1]]  # 검색 종목 없는 2번은 보내지 않는다
    assert result.passed == [good]
    assert result.invalid == [market, no_target]
    assert result.failed == []
    assert good["_linked_companies"] == [{"company_id": 100, "name": "엘앤에프"}]


def test_missing_ids_are_rejudged_individually_and_individual_failure_is_failed():
    a = _item("A 기사", "엘앤에프")
    b = _item("B 기사", "에코프로", company_id=200)
    c = _item("C 기사", "삼성SDI", company_id=300)
    sizes: list[int] = []

    async def judge(articles):
        sizes.append(len(articles))
        if len(articles) == 3:
            return BatchVerdict(verdicts=[_verdict(articles[0])])  # 1·2번 누락
        if articles[0].id == 2:
            raise RuntimeError("bedrock down")
        return BatchVerdict(verdicts=[_verdict(articles[0])])

    result = filter_relevant_news([a, b, c], judge, 2, 10)

    assert sizes == [3, 1, 1]
    assert result.passed == [a, b]
    assert result.failed == [c]


def test_batch_exception_falls_back_to_individual_calls():
    a = _item("A 기사", "엘앤에프")
    b = _item("B 기사", "에코프로", company_id=200)
    sizes: list[int] = []

    async def judge(articles):
        sizes.append(len(articles))
        if len(articles) > 1:
            raise RuntimeError("bad json")
        return BatchVerdict(verdicts=[_verdict(articles[0])])

    result = filter_relevant_news([a, b], judge, 2, 10)

    assert sizes == [2, 1, 1]
    assert result.passed == [a, b]
    assert result.failed == []


def test_items_are_split_into_batches_of_batch_size(caplog):
    caplog.set_level(logging.DEBUG)
    items = [_item(f"기사 {i}", "엘앤에프") for i in range(25)]
    sizes: list[int] = []

    async def judge(articles):
        sizes.append(len(articles))
        return BatchVerdict(verdicts=[_verdict(a) for a in articles])

    result = filter_relevant_news(items, judge, 2, 10)

    assert sorted(sizes, reverse=True) == [10, 10, 5]
    assert result.passed == items
    # 묶음이 끝날 때마다 진행률을 남기고 마지막 줄은 전체 수와 같다
    assert "[LLM] 판정 3/3 묶음 (기사 25/25건)" in caplog.text


def test_empty_input_skips_llm():
    async def judge(articles):  # pragma: no cover - 호출되면 안 된다
        raise AssertionError("LLM 호출됨")

    result = filter_relevant_news([], judge, 2, 10)

    assert result.passed == [] and result.invalid == [] and result.failed == []


def test_duplicate_and_unknown_ids_are_ignored_without_fallback():
    a = _item("A 기사", "엘앤에프")
    b = _item("B 기사", "에코프로", company_id=200)
    sizes: list[int] = []

    async def judge(articles):
        sizes.append(len(articles))
        first, second = articles
        return BatchVerdict(
            verdicts=[
                _verdict(first),
                _verdict(first, valid=False),  # 중복 — 첫 판정이 이긴다
                ArticleVerdict(id=99, companies=[]),  # 미지 번호 — 무시
                _verdict(second),
            ]
        )

    result = filter_relevant_news([a, b], judge, 2, 10)

    assert sizes == [2]  # 누락이 없으니 개별 재판정도 없다
    assert result.passed == [a, b]


def test_item_without_title_companies_is_judged_by_query_company_alone():
    item = {
        "title": "엘앤에프 CB 발행",
        "description": "",
        "link": "https://x/1",
        "_query_company": {"company_id": 100, "name": "엘앤에프"},
    }
    sent: list[tuple[str, ...]] = []

    async def judge(articles):
        sent.extend(a.companies for a in articles)
        return BatchVerdict(verdicts=[_verdict(a) for a in articles])

    result = filter_relevant_news([item], judge, 1, 10)

    assert sent == [("엘앤에프",)]
    assert result.passed == [item]
    assert item["_linked_companies"] == [{"company_id": 100, "name": "엘앤에프"}]


def test_linked_companies_are_valid_title_companies_resolved_by_surface_or_name():
    item = _item(
        "두산밥캣·LG엔솔·현대차·SK하이닉스 협약",
        "두산밥캣",
        others=(
            _company(300, "LG에너지솔루션", "LG엔솔"),
            _company(400, "현대차"),
            _company(500, "SK하이닉스"),
        ),
    )
    sent: list[tuple[str, ...]] = []

    async def judge(articles):
        sent.extend(a.companies for a in articles)
        return BatchVerdict(
            verdicts=[
                ArticleVerdict(
                    id=articles[0].id,
                    companies=[
                        CompanyVerdict(name="두산밥캣", valid=True),
                        CompanyVerdict(name="LG에너지솔루션", valid=True),  # 표기 대신 정식명
                        CompanyVerdict(name=" 현대차 ", valid=False),
                        CompanyVerdict(name="삼성전자", valid=True),  # 목록에 없음 — 무시
                    ],  # SK하이닉스 판정 누락 — 연결하지 않는다
                )
            ]
        )

    result = filter_relevant_news([item], judge, 1, 10)

    # LLM 에는 제목 표기가 간다
    assert sent == [("두산밥캣", "LG엔솔", "현대차", "SK하이닉스")]
    assert result.passed == [item]
    assert item["_linked_companies"] == [
        {"company_id": 100, "name": "두산밥캣"},
        {"company_id": 300, "name": "LG에너지솔루션"},
    ]


def test_target_invalid_drops_article_even_if_others_are_valid():
    item = _item(
        "LG엔솔, 두산밥캣코리아와 협약",
        "두산밥캣",
        others=(_company(300, "LG에너지솔루션", "LG엔솔"),),
    )

    async def judge(articles):
        return BatchVerdict(verdicts=[_verdict(articles[0], per_company={"두산밥캣": False})])

    result = filter_relevant_news([item], judge, 1, 10)

    assert result.invalid == [item]
    assert "_linked_companies" not in item


def test_missing_target_verdict_is_rejudged_then_failed():
    a = _item("A 기사", "엘앤에프", others=(_company(300, "삼성SDI"),))
    b = _item("B 기사", "에코프로", company_id=200)
    sizes: list[int] = []
    other_only = [CompanyVerdict(name="삼성SDI", valid=True)]

    async def judge(articles):
        sizes.append(len(articles))
        if len(articles) == 2:
            # a 는 검색 종목 판정이 빠졌다 — 다른 기업 판정만으로는 통과시키지 않는다
            return BatchVerdict(
                verdicts=[
                    ArticleVerdict(id=articles[0].id, companies=other_only),
                    _verdict(articles[1]),
                ]
            )
        return BatchVerdict(verdicts=[ArticleVerdict(id=articles[0].id, companies=other_only)])

    result = filter_relevant_news([a, b], judge, 2, 10)

    assert sizes == [2, 1]  # a 만 개별 재판정
    assert result.passed == [b]
    assert result.failed == [a]


def test_verdict_names_match_ignoring_inner_whitespace():
    # 제목 표기 'LG 엔솔' 을 LLM 이 'LG엔솔' 로 붙여 답해도 같은 기업이다 — 검색 종목이면 기사가 날아간다
    item = _item("LG 엔솔, 美 공장 증설", "LG에너지솔루션", company_id=300)
    item["_title_companies"] = [_company(300, "LG에너지솔루션", "LG 엔솔")]

    async def judge(articles):
        return BatchVerdict(
            verdicts=[
                ArticleVerdict(
                    id=articles[0].id, companies=[CompanyVerdict(name="LG엔솔", valid=True)]
                )
            ]
        )

    result = filter_relevant_news([item], judge, 1, 10)

    assert result.passed == [item]
    assert item["_linked_companies"] == [{"company_id": 300, "name": "LG에너지솔루션"}]


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


def _judge_with(**valid_by_name):
    async def judge(articles):
        return BatchVerdict(
            verdicts=[
                ArticleVerdict(
                    id=article.id,
                    companies=[
                        CompanyVerdict(name=name, valid=valid)
                        for name, valid in valid_by_name.items()
                    ],
                )
                for article in articles
            ]
        )

    return judge


def test_article_passes_when_any_fetching_search_company_is_valid():
    item = _fetched_by_both("삼성전자·SK하이닉스, 엔비디아에 HBM 공급")
    judge = _judge_with(**{"삼성전자": False, "SK하이닉스": True, "엔비디아": True})

    result = filter_relevant_news([item], judge, 1, 10)

    assert result.passed == [item]
    assert item["_linked_companies"] == [
        {"company_id": 200, "name": "SK하이닉스"},
        {"company_id": 300, "name": "엔비디아"},
    ]


def test_valid_title_company_that_did_not_fetch_the_article_does_not_store_it():
    item = _fetched_by_both("삼성전자·SK하이닉스, 엔비디아에 HBM 공급")
    judge = _judge_with(**{"삼성전자": False, "SK하이닉스": False, "엔비디아": True})

    result = filter_relevant_news([item], judge, 1, 10)

    assert result.invalid == [item]


def test_missing_verdict_of_a_fetching_company_is_rejudged_when_none_is_valid():
    item = _fetched_by_both("삼성전자·SK하이닉스, 엔비디아에 HBM 공급")
    # SK하이닉스 판정이 빠졌고 삼성전자는 invalid — SK하이닉스가 valid 일 수 있으니 확정하지 않는다
    judge = _judge_with(**{"삼성전자": False, "엔비디아": True})

    result = filter_relevant_news([item], judge, 1, 10)

    assert result.failed == [item]


def test_one_valid_fetching_company_is_enough_even_if_another_is_missing():
    item = _fetched_by_both("삼성전자·SK하이닉스, 엔비디아에 HBM 공급")
    judge = _judge_with(**{"SK하이닉스": True})

    result = filter_relevant_news([item], judge, 1, 10)

    assert result.passed == [item]
    assert item["_linked_companies"] == [{"company_id": 200, "name": "SK하이닉스"}]
