"""LLM 관련성 필터(묶음 호출) 단위 테스트. Bedrock 은 부르지 않고 judge 를 페이크로 준다."""

from __future__ import annotations

import logging

from pipelines.news.transformers.relevance_filter import (
    ArticleInput,
    ArticleVerdict,
    BatchVerdict,
    filter_relevant_news,
)


def _item(title: str, candidates: list[str], description: str = "") -> dict:
    return {
        "title": title,
        "description": description,
        "link": f"https://x/{hash(title)}",
        "_candidate_companies": list(candidates),
    }


def _ok(article: ArticleInput) -> ArticleVerdict:
    return ArticleVerdict(id=article.id, valid=True, companies=list(article.candidates))


def test_build_relevance_input_numbers_articles_and_lists_candidates():
    from pipelines.news.transformers.relevance_filter import build_relevance_input

    text = build_relevance_input(
        [
            ArticleInput(0, "엘앤에프, CB 발행 검토", "LFP 투자 재원", ("엘앤에프", "에코프로")),
            ArticleInput(3, "코스피 마감", "", ()),
        ]
    )

    assert (
        "[기사 0]\n제목: 엘앤에프, CB 발행 검토\n요약: LFP 투자 재원\n후보 종목:\n- 엘앤에프\n- 에코프로"
        in text
    )
    assert "[기사 3]\n제목: 코스피 마감\n요약: \n후보 종목:\n- (없음)" in text


def test_validate_subjects_keeps_candidates_only_in_order(caplog):
    from pipelines.news.transformers.relevance_filter import validate_subjects

    kept = validate_subjects(
        ["에코프로", "삼성전자", "엘앤에프", "에코프로"], ["엘앤에프", "에코프로"], "양극재 급등"
    )

    assert kept == ["에코프로", "엘앤에프"]
    # 드랍 경고에 후보와 제목이 같이 남아 gazetteer 누락인지 추적할 수 있다
    assert "삼성전자" in caplog.text and "양극재 급등" in caplog.text


def test_chunked():
    from pipelines.news.transformers.relevance_filter import chunked

    assert chunked([1, 2, 3, 4, 5], 2) == [[1, 2], [3, 4], [5]]
    assert chunked([], 3) == []


def test_batch_matches_ids_out_of_order_and_drops_foreign_companies():
    good = _item("엘앤에프·에코프로 동반 급등", ["엘앤에프", "에코프로"])
    market = _item("코스피 7000선 사수", ["엘앤에프"])
    off = _item("BH(청와대) 로비 의혹", ["비에이치"])
    no_candidates = {"title": "후보 없음", "description": "", "link": "https://x/none"}
    calls: list[list[int]] = []

    async def judge(articles):
        calls.append([a.id for a in articles])
        # 순서를 뒤집어 돌려주고, 0번에 다른 기사(2번)의 후보를 섞어 넣는다
        return BatchVerdict(
            verdicts=[
                ArticleVerdict(id=2, valid=True, companies=[]),
                ArticleVerdict(id=1, valid=False, companies=[]),
                ArticleVerdict(id=0, valid=True, companies=["에코프로", "비에이치"]),
            ]
        )

    result = filter_relevant_news([good, market, off, no_candidates], judge, 2, 10)

    assert calls == [[0, 1, 2]]  # 후보 없는 3번은 보내지 않는다
    assert result.passed == [good]
    assert good["_subject_names"] == ["에코프로"]
    assert result.invalid == [market]
    assert result.irrelevant == [off, no_candidates]
    assert result.failed == []


def test_missing_ids_are_rejudged_individually_and_individual_failure_is_failed():
    a = _item("A 기사", ["엘앤에프"])
    b = _item("B 기사", ["에코프로"])
    c = _item("C 기사", ["삼성SDI"])
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
    a = _item("A 기사", ["엘앤에프"])
    b = _item("B 기사", ["에코프로"])
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
    caplog.set_level(logging.INFO)
    items = [_item(f"기사 {i}", ["엘앤에프"]) for i in range(25)]
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

    assert result.passed == [] and result.invalid == [] and result.irrelevant == []


def test_post_verdict_failure_is_isolated_to_the_item():
    class BrokenVerdict:
        def __init__(self, article_id: int):
            self.id = article_id
            self.valid = True

        @property
        def companies(self):
            raise RuntimeError("bad payload")

    class BrokenBatch:
        def __init__(self, verdicts):
            self.verdicts = verdicts

    good = _item("엘앤에프, CB 발행 검토", ["엘앤에프"])
    bad = _item("깨진 응답", ["엘앤에프"])

    async def judge(articles):
        return BrokenBatch([_ok(articles[0]), BrokenVerdict(articles[1].id)])

    result = filter_relevant_news([good, bad], judge, 2, 10)

    assert result.passed == [good]
    assert result.failed == [bad]


def test_duplicate_and_unknown_ids_are_ignored_without_fallback():
    a = _item("A 기사", ["엘앤에프"])
    b = _item("B 기사", ["에코프로"])
    sizes: list[int] = []

    async def judge(articles):
        sizes.append(len(articles))
        return BatchVerdict(
            verdicts=[
                ArticleVerdict(id=0, valid=True, companies=["엘앤에프"]),
                ArticleVerdict(id=0, valid=False, companies=[]),  # 중복 — 첫 판정이 이긴다
                ArticleVerdict(id=99, valid=True, companies=["에코프로"]),  # 미지 번호 — 무시
                ArticleVerdict(id=1, valid=True, companies=["에코프로"]),
            ]
        )

    result = filter_relevant_news([a, b], judge, 2, 10)

    assert sizes == [2]  # 누락이 없으니 개별 재판정도 없다
    assert result.passed == [a, b]
    assert a["_subject_names"] == ["엘앤에프"]
