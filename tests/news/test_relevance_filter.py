"""LLM 관련성 필터(묶음 호출) 단위 테스트. Bedrock 은 부르지 않고 judge 를 페이크로 준다."""

from __future__ import annotations

import logging

from pipelines.news.transformers.filters.relevance_filter import (
    ArticleInput,
    ArticleVerdict,
    BatchVerdict,
    filter_relevant_news,
)


def _item(title: str, company: str, description: str = "") -> dict:
    return {
        "title": title,
        "description": description,
        "link": f"https://x/{hash(title)}",
        "_query_company": {"company_id": 100, "name": company},
    }


def _ok(article: ArticleInput) -> ArticleVerdict:
    return ArticleVerdict(id=article.id, valid=True)


def test_build_relevance_input_numbers_articles_and_names_target():
    from pipelines.news.transformers.filters.relevance_filter import build_relevance_input

    text = build_relevance_input(
        [
            ArticleInput(0, "엘앤에프, CB 발행 검토", "LFP 투자 재원", "엘앤에프"),
            ArticleInput(3, "코스피 마감", "", "삼성전자"),
        ]
    )

    assert (
        "[기사 0]\n제목: 엘앤에프, CB 발행 검토\n요약: LFP 투자 재원\n대상 종목: 엘앤에프" in text
    )
    assert "[기사 3]\n제목: 코스피 마감\n요약: \n대상 종목: 삼성전자" in text


def test_chunked():
    from pipelines.news.transformers.filters.relevance_filter import chunked

    assert chunked([1, 2, 3, 4, 5], 2) == [[1, 2], [3, 4], [5]]
    assert chunked([], 3) == []


def test_batch_matches_ids_out_of_order_and_skips_items_without_target():
    good = _item("엘앤에프, 양극재 공급 계약", "엘앤에프")
    market = _item("코스피 7000선 사수… 엘앤에프 강세", "엘앤에프")
    no_target = {"title": "대상 없음", "description": "", "link": "https://x/none"}
    calls: list[list[int]] = []

    async def judge(articles):
        calls.append([a.id for a in articles])
        # 순서를 뒤집어 돌려준다
        return BatchVerdict(
            verdicts=[ArticleVerdict(id=1, valid=False), ArticleVerdict(id=0, valid=True)]
        )

    result = filter_relevant_news([good, market, no_target], judge, 2, 10)

    assert calls == [[0, 1]]  # 대상 종목 없는 2번은 보내지 않는다
    assert result.passed == [good]
    assert result.invalid == [market, no_target]
    assert result.failed == []


def test_missing_ids_are_rejudged_individually_and_individual_failure_is_failed():
    a = _item("A 기사", "엘앤에프")
    b = _item("B 기사", "에코프로")
    c = _item("C 기사", "삼성SDI")
    sizes: list[int] = []

    async def judge(articles):
        sizes.append(len(articles))
        if len(articles) == 3:
            return BatchVerdict(verdicts=[_ok(articles[0])])  # 1·2번 누락
        if articles[0].id == 2:
            raise RuntimeError("bedrock down")
        return BatchVerdict(verdicts=[_ok(articles[0])])

    result = filter_relevant_news([a, b, c], judge, 2, 10)

    assert sizes == [3, 1, 1]
    assert result.passed == [a, b]
    assert result.failed == [c]


def test_batch_exception_falls_back_to_individual_calls():
    a = _item("A 기사", "엘앤에프")
    b = _item("B 기사", "에코프로")
    sizes: list[int] = []

    async def judge(articles):
        sizes.append(len(articles))
        if len(articles) > 1:
            raise RuntimeError("bad json")
        return BatchVerdict(verdicts=[_ok(articles[0])])

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
        return BatchVerdict(verdicts=[_ok(a) for a in articles])

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
    b = _item("B 기사", "에코프로")
    sizes: list[int] = []

    async def judge(articles):
        sizes.append(len(articles))
        return BatchVerdict(
            verdicts=[
                ArticleVerdict(id=0, valid=True),
                ArticleVerdict(id=0, valid=False),  # 중복 — 첫 판정이 이긴다
                ArticleVerdict(id=99, valid=True),  # 미지 번호 — 무시
                ArticleVerdict(id=1, valid=True),
            ]
        )

    result = filter_relevant_news([a, b], judge, 2, 10)

    assert sizes == [2]  # 누락이 없으니 개별 재판정도 없다
    assert result.passed == [a, b]
