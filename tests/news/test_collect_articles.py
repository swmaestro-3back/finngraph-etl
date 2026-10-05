"""collect_articles job 단위 테스트. I/O 는 전부 갈아끼우고 단계 순서와 부수효과만 본다."""

from __future__ import annotations

import pytest

from pipelines.common.gazetteer import CompanyMatcher, GazetteerEntry
from pipelines.news.repositories.postgres import news_companies
from pipelines.news.repositories.postgres.search_history import CompanyQuery, CompanyQueryBatch
from pipelines.news.transformers import company_matches
from pipelines.news.transformers.filters import entity_filter, relevance_filter
from pipelines.news.transformers.filters.entity_filter import EntityJudgement, EntityJudgementList
from pipelines.news.transformers.filters.relevance_filter import (
    ArticleVerdict,
    BatchVerdict,
    CompanyVerdict,
)

# 개체 사전 대역: 검색 종목 엘앤에프(100), 제목에 함께 나오는 삼성SDI(300), 본문에만 나오는
# 포스코퓨처엠(400)
MATCHER = CompanyMatcher(
    {
        "엘앤에프": GazetteerEntry(
            company_id=100, stock_id=1100, ticker="066970", canonical="엘앤에프"
        ),
        "삼성SDI": GazetteerEntry(
            company_id=300, stock_id=1300, ticker="006400", canonical="삼성SDI"
        ),
        "포스코퓨처엠": GazetteerEntry(
            company_id=400, stock_id=1400, ticker="003670", canonical="포스코퓨처엠"
        ),
    }
)

BODY = "엘앤에프가 삼성SDI에 양극재를 공급한다. 원료는 포스코퓨처엠이 댄다. " * 10
BODY_WITHOUT_OTHERS = "엘앤에프가 삼성SDI에 양극재를 공급한다. " * 10
FRESH_TITLE = "엘앤에프, 삼성SDI에 양극재 공급 계약"


def _item(title: str, link: str, description: str = "") -> dict:
    return {
        "title": title,
        "description": description,
        "link": link,
        "originallink": link,
        "pubDate": "Wed, 09 Sep 2026 10:00:00 +0900",
        "_query_company": {"company_id": 100, "name": "엘앤에프"},
    }


def _keep_all(surfaces: list[str]) -> EntityJudgementList:
    return EntityJudgementList(
        judgements=[
            EntityJudgement(
                entity=surface, mention=f"{surface} 원문 문장", reason="근거", keep=True
            )
            for surface in surfaces
        ]
    )


@pytest.fixture
def wired(monkeypatch):
    """run() 의 I/O 를 전부 갈아끼우고 호출 기록을 남긴다. 개체 사전은 인라인 매처다."""
    from pipelines.news import config
    from pipelines.news.jobs import collect_articles as job

    # run() 이 읽는 설정을 고정한다 — 로컬 .env 값에 좌우되지 않게.
    monkeypatch.setenv("NEWS_SEARCH_INTERVAL_HOURS", "2")
    monkeypatch.setenv("NEWS_SEARCH_LOOKBACK_DAYS", "180")
    monkeypatch.setenv("NEWS_LLM_MAX_CONCURRENCY", "2")
    monkeypatch.setenv("NEWS_LLM_BODY_LIMIT", "12000")
    monkeypatch.setenv("NEWS_BODY_CANDIDATE_MAX", "15")
    config.get_news_settings.cache_clear()

    calls: dict[str, list] = {
        "news_companies": [],
        "marked": [],
        "crawled": [],
        "saved": [],
        "entity_judged": [],
        "collect_window": [],
        "query_templates": [],
    }
    bodies: dict[str, str] = {}

    batch = CompanyQueryBatch(
        theme_ids=[10, 11],
        queries=[CompanyQuery(company_id=100, name="엘앤에프"), CompanyQuery(200, "에코프로")],
        skipped_no_company=0,
        skipped_not_due=1,
    )
    fresh = _item(FRESH_TITLE, "https://a/1", "LFP 양극재")
    market = _item("코스피 7000선 사수", "https://a/2")
    existing = _item("엘앤에프 2분기 흑자 전환", "https://a/old")

    monkeypatch.setattr(company_matches, "get_company_matcher", lambda: MATCHER)
    monkeypatch.setattr(job, "validate_search_settings", lambda: None)
    monkeypatch.setattr(job, "fetch_due_company_queries", lambda ids, hours, now: batch)
    monkeypatch.setattr(job, "fetch_due_krx100_queries", lambda hours, now, company_ids: batch)

    # 200 번 기업의 검색은 실패한 것으로 본다
    def fake_collect(queries, run_started_at, lookback_days, max_pages, query_templates):
        calls["collect_window"].append((lookback_days, max_pages))
        calls["query_templates"].append(query_templates)
        return [fresh, market, existing], [200]

    monkeypatch.setattr(job, "collect_company_news", fake_collect)
    monkeypatch.setattr(
        job, "remove_stored_by_url", lambda items: [i for i in items if i is not existing]
    )

    # 본문: 기본은 BODY, 테스트가 bodies[link] 로 기사별 본문을 정한다
    def fake_fetch_body(items):
        calls["crawled"].append([item["link"] for item in items])
        for item in items:
            item["_text"] = bodies.get(item["link"], BODY)
        return items

    monkeypatch.setattr(job, "fetch_article_body", fake_fetch_body)

    # 저장소 대역: 기사와 그 기업 연결(_linked_companies)을 함께 저장한다
    def fake_save(items):
        calls["saved"].append([item["link"] for item in items])
        linked = 0
        for index, item in enumerate(items, start=900):
            item["_news_id"] = index
            item["_save_action"] = "inserted"
            company_ids = news_companies.linked_company_ids(item)
            calls["news_companies"].append((index, company_ids))
            linked += len(company_ids)
        return {
            "inserted_count": len(items),
            "skipped_existing_count": 0,
            "failed_count": 0,
            "linked_count": linked,
        }

    monkeypatch.setattr(job, "save_news", fake_save)
    monkeypatch.setattr(
        job,
        "mark_companies_searched",
        lambda ids, searched_at: calls["marked"].append(list(ids)) or len(ids),
    )

    async def relevance_judge(articles):
        # 시황 기사는 무효, 나머지는 판정 기업 모두 통과로 본다
        return BatchVerdict(
            verdicts=[
                ArticleVerdict(
                    id=article.id,
                    companies=[
                        CompanyVerdict(
                            is_company=True, name=name, valid=not article.title.startswith("코스피")
                        )
                        for name in article.companies
                    ],
                )
                for article in articles
            ]
        )

    async def entity_judge(text, surfaces):
        calls["entity_judged"].append(list(surfaces))
        return _keep_all(surfaces)

    # Bedrock 싱글톤 대신 가짜 judge
    monkeypatch.setattr(relevance_filter, "get_relevance_judge", lambda: relevance_judge)
    monkeypatch.setattr(entity_filter, "get_entity_judge", lambda: entity_judge)

    yield job, calls, bodies
    config.get_news_settings.cache_clear()


def test_run_crawls_relevant_articles_judges_body_companies_and_saves(wired):
    job, calls, _ = wired

    result = job.run(theme_ids=[10, 11])

    # 시장 일반 기사는 제목에 검색 종목이 없어서, 기존 기사는 DB 대조로 버려진다. 관련성을 통과한
    # fresh 하나만 크롤링하고 저장한다.
    assert calls["crawled"] == [["https://a/1"]]
    assert calls["saved"] == [["https://a/1"]]
    # 본문 판정 후보는 본문에만 나온 포스코퓨처엠뿐이다 — 제목 기업은 다시 판정하지 않는다
    assert calls["entity_judged"] == [["포스코퓨처엠"]]
    # 제목 통과 기업(엘앤에프·삼성SDI) 뒤에 본문 통과 기업이 붙는다
    assert calls["news_companies"] == [(900, [100, 300, 400])]
    # 검색에 성공한 기업만 search_history 에 기록된다 — 실패한 200 은 다음 런에 다시 읽는다
    assert calls["marked"] == [[100]]
    assert result == {"saved": 1, "linked": 3}


def test_run_drops_articles_whose_body_could_not_be_fetched(wired):
    job, calls, bodies = wired
    bodies["https://a/1"] = ""

    result = job.run(theme_ids=[10, 11])

    # 본문이 없으면 저장도 연결도 엔티티 판정도 하지 않는다
    assert calls["saved"] == [[]]
    assert calls["news_companies"] == []
    assert calls["entity_judged"] == []
    assert result == {"saved": 0, "linked": 0}
    # 검색은 끝났으므로 마킹은 한다
    assert calls["marked"] == [[100]]


def test_article_without_body_only_companies_skips_entity_llm(wired, monkeypatch):
    job, calls, bodies = wired
    bodies["https://a/1"] = BODY_WITHOUT_OTHERS

    def fail():
        raise AssertionError("엔티티 judge 를 만듦")

    monkeypatch.setattr(entity_filter, "get_entity_judge", fail)

    result = job.run(theme_ids=[10, 11])

    assert calls["news_companies"] == [(900, [100, 300])]
    assert result == {"saved": 1, "linked": 2}


def test_article_with_too_many_body_candidates_is_dropped_without_entity_llm(wired, monkeypatch):
    from pipelines.news import config

    job, calls, _ = wired
    # 본문 후보(포스코퓨처엠 1개)가 상한(0)을 넘는다 — 여러 종목을 모은 기사로 보고 저장하지 않는다
    monkeypatch.setenv("NEWS_BODY_CANDIDATE_MAX", "0")
    config.get_news_settings.cache_clear()

    result = job.run(theme_ids=[10, 11])

    assert calls["entity_judged"] == []
    assert calls["saved"] == [[]]
    assert calls["news_companies"] == []
    assert result == {"saved": 0, "linked": 0}
    # 검색은 끝났으므로 마킹은 한다
    assert calls["marked"] == [[100]]


def test_article_at_the_body_candidate_limit_is_judged_and_saved(wired, monkeypatch):
    from pipelines.news import config

    job, calls, _ = wired
    monkeypatch.setenv("NEWS_BODY_CANDIDATE_MAX", "1")
    config.get_news_settings.cache_clear()

    result = job.run(theme_ids=[10, 11])

    assert calls["entity_judged"] == [["포스코퓨처엠"]]
    assert result == {"saved": 1, "linked": 3}


def test_dropping_every_article_for_too_many_candidates_is_not_an_llm_outage(wired, monkeypatch):
    from pipelines.news import config

    job, calls, _ = wired
    monkeypatch.setenv("NEWS_BODY_CANDIDATE_MAX", "0")
    config.get_news_settings.cache_clear()

    async def boom(text, surfaces):
        raise RuntimeError("bedrock down")

    monkeypatch.setattr(entity_filter, "get_entity_judge", lambda: boom)

    # 판정할 기사가 없으니 전건 실패 예외도 없다
    assert job.run(theme_ids=[10, 11]) == {"saved": 0, "linked": 0}


def test_listing_title_is_dropped_before_relevance_and_crawl(wired, monkeypatch):
    job, calls, _ = wired
    listing = _item("엘앤에프·삼성SDI·포스코퓨처엠 등", "https://a/3")
    monkeypatch.setattr(
        job,
        "collect_company_news",
        lambda queries, now, lookback, pages, templates: ([listing], []),
    )

    def fail():
        raise AssertionError("관련성 judge 를 만듦")

    monkeypatch.setattr(relevance_filter, "get_relevance_judge", fail)

    result = job.run(theme_ids=[10, 11])

    assert calls["crawled"] == [[]]
    assert result == {"saved": 0, "linked": 0}


def test_body_company_rejected_by_entity_filter_is_not_linked(wired, monkeypatch):
    job, calls, _ = wired

    async def reject(text, surfaces):
        return EntityJudgementList(
            judgements=[
                EntityJudgement(entity=surface, mention="문장", reason="배경 언급", keep=False)
                for surface in surfaces
            ]
        )

    monkeypatch.setattr(entity_filter, "get_entity_judge", lambda: reject)

    job.run(theme_ids=[10, 11])

    assert calls["news_companies"] == [(900, [100, 300])]


def test_title_company_rejected_by_relevance_is_not_rejudged_from_body(wired, monkeypatch):
    job, calls, _ = wired
    sent: list[tuple[str, ...]] = []

    async def judge(articles):
        sent.extend(article.companies for article in articles)
        return BatchVerdict(
            verdicts=[
                ArticleVerdict(
                    id=article.id,
                    companies=[
                        CompanyVerdict(is_company=True, name="엘앤에프", valid=True),
                        CompanyVerdict(is_company=True, name="삼성SDI", valid=False),
                    ],
                )
                for article in articles
            ]
        )

    monkeypatch.setattr(relevance_filter, "get_relevance_judge", lambda: judge)

    job.run(theme_ids=[10, 11])

    # 검색 종목이 첫 번째, 제목 표기 그대로
    assert sent == [("엘앤에프", "삼성SDI")]
    # 삼성SDI 는 본문에도 나오지만 제목 판정에서 탈락했으므로 본문 후보에 들지 않는다
    assert calls["entity_judged"] == [["포스코퓨처엠"]]
    assert calls["news_companies"] == [(900, [100, 400])]


def test_entity_failure_on_one_article_links_only_its_title_companies(wired, monkeypatch):
    job, calls, bodies = wired
    fresh = _item(FRESH_TITLE, "https://a/1")
    second = _item("엘앤에프, 미국 공장 증설", "https://a/3")
    bodies["https://a/3"] = "엘앤에프가 미국 공장을 증설한다. 포스코퓨처엠이 원료를 댄다. " * 10
    monkeypatch.setattr(
        job,
        "collect_company_news",
        lambda queries, now, lookback, pages, templates: ([fresh, second], []),
    )

    async def flaky(text, surfaces):
        if "미국" in text:
            raise RuntimeError("bedrock down")
        return _keep_all(surfaces)

    monkeypatch.setattr(entity_filter, "get_entity_judge", lambda: flaky)

    result = job.run(theme_ids=[10, 11])

    # 실패한 기사도 저장하고 제목 기업만 연결한다. 런은 계속된다.
    assert calls["news_companies"] == [(900, [100, 300, 400]), (901, [100])]
    assert result == {"saved": 2, "linked": 4}


def test_run_raises_before_saving_when_all_entity_judgments_fail(wired, monkeypatch):
    job, calls, _ = wired

    async def always_failing(text, surfaces):
        raise RuntimeError("bedrock down")

    monkeypatch.setattr(entity_filter, "get_entity_judge", lambda: always_failing)

    with pytest.raises(RuntimeError):
        job.run(theme_ids=[10, 11])

    # 저장·마킹 전에 올라가야 Airflow 재시도가 같은 창을 다시 읽는다
    assert calls["saved"] == []
    assert calls["marked"] == []


def test_run_raises_before_marking_when_all_relevance_judgments_fail(wired, monkeypatch):
    job, calls, _ = wired

    async def always_failing_judge(articles):
        raise RuntimeError("bedrock down")

    monkeypatch.setattr(relevance_filter, "get_relevance_judge", lambda: always_failing_judge)

    with pytest.raises(RuntimeError):
        job.run(theme_ids=[10, 11])

    assert calls["crawled"] == []
    assert calls["marked"] == []


def test_run_does_not_link_articles_another_run_already_stored(wired, monkeypatch):
    job, calls, _ = wired

    def fake_save(items):
        for item in items:
            item["_news_id"] = 900
            item["_save_action"] = "skipped_existing"  # URL 대조와 저장 사이에 다른 런이 넣었다
        return {
            "inserted_count": 0,
            "skipped_existing_count": len(items),
            "failed_count": 0,
            "linked_count": 0,
        }

    monkeypatch.setattr(job, "save_news", fake_save)

    result = job.run(theme_ids=[10, 11])

    assert result == {"saved": 0, "linked": 0}


def test_run_with_tickers_searches_backend_hot_theme_stocks(wired, monkeypatch):
    job, calls, _ = wired
    seen: list[tuple] = []

    def fake_ticker_queries(ids, tickers, hours, now):
        seen.append((ids, tickers, hours))
        return CompanyQueryBatch(theme_ids=ids, queries=[], skipped_no_company=0, skipped_not_due=0)

    monkeypatch.setattr(job, "fetch_due_ticker_queries", fake_ticker_queries)
    monkeypatch.setattr(
        job,
        "fetch_due_company_queries",
        lambda ids, hours, now: (_ for _ in ()).throw(AssertionError("편입 종목 전체를 조회함")),
    )

    assert job.run(theme_ids=[10], tickers=["066970"]) == {"saved": 0, "linked": 0}
    assert seen == [([10], ["066970"], 2)]


def test_run_with_no_due_companies_does_nothing(wired, monkeypatch):
    job, calls, _ = wired
    monkeypatch.setattr(
        job,
        "fetch_due_company_queries",
        lambda ids, hours, now: CompanyQueryBatch(
            theme_ids=[10], queries=[], skipped_no_company=0, skipped_not_due=3
        ),
    )

    result = job.run(theme_ids=[10])

    assert result == {"saved": 0, "linked": 0}
    assert calls["marked"] == []
    assert calls["crawled"] == []
    assert calls["saved"] == []


def test_run_krx100_passes_collection_window_and_marks_searched(wired):
    job, calls, _ = wired

    result = job.run_krx100([100, 200], lookback_days=180, max_pages=8)

    assert result == {"saved": 1, "linked": 3}
    # 백필이 넓힌 수집 창이 수집기까지 내려간다 — 스케줄 런(run)은 None 으로 설정값을 쓴다
    assert calls["collect_window"] == [(180, 8)]
    assert calls["marked"] == [[100]]


def test_run_uses_settings_collection_window(wired):
    job, calls, _ = wired

    job.run(theme_ids=[10, 11])

    assert calls["collect_window"] == [(None, None)]
    assert calls["query_templates"] == [None]


def test_run_krx100_passes_query_templates(wired):
    job, calls, _ = wired

    job.run_krx100([100, 200], query_templates=["{name},수혜주"])

    # 백필이 고른 검색어 서식만 수집기로 내려간다 — 안 주면 None 으로 설정값(스케줄 런과 같다)
    assert calls["query_templates"] == [["{name},수혜주"]]


@pytest.mark.parametrize("templates", [[], ["수혜주"], ["{name},수혜", "특징주"]])
def test_run_krx100_rejects_templates_without_name(wired, templates):
    job, calls, _ = wired

    # {name} 이 없으면 모든 기업이 같은 검색어로 검색된다
    with pytest.raises(ValueError):
        job.run_krx100([100, 200], query_templates=templates)

    assert calls["collect_window"] == []


def test_run_filters_in_memory_first_then_db_then_llm_then_crawl(wired, monkeypatch):
    job, calls, _ = wired
    order: list[tuple[str, list[str]]] = []
    crawl = job.fetch_article_body

    def fake_title(items):
        order.append(("title", [item["link"] for item in items]))
        return items, []

    def fake_stored(items):
        order.append(("stored", [item["link"] for item in items]))
        return [item for item in items if item["link"] != "https://a/old"]

    def fake_crawl(items):
        order.append(("crawl", [item["link"] for item in items]))
        return crawl(items)

    monkeypatch.setattr(job, "filter_titles", fake_title)
    monkeypatch.setattr(job, "remove_stored_by_url", fake_stored)
    monkeypatch.setattr(job, "fetch_article_body", fake_crawl)

    job.run(theme_ids=[10, 11])

    # 제목 매치가 시황 기사(https://a/2)를 먼저 버리고, DB 대조는 메모리 필터를 다 통과한 기사에만
    # 돌고, 크롤링은 관련성까지 통과한 기사에만 돈다
    assert order == [
        ("title", ["https://a/1", "https://a/old"]),
        ("stored", ["https://a/1", "https://a/old"]),
        ("crawl", ["https://a/1"]),
    ]


def test_run_keeps_the_copy_whose_query_company_is_in_the_title(wired, monkeypatch):
    job, calls, _ = wired
    # 같은 기사가 에코프로 검색에 먼저 걸렸다. 제목에 에코프로가 없으므로 그 사본은 버리고, 제목의
    # 주인공 엘앤에프로 검색한 사본이 중복 제거에서 살아남아야 한다.
    fresh = _item(FRESH_TITLE, "https://a/1", "LFP 양극재")
    first_copy = dict(fresh, _query_company={"company_id": 200, "name": "에코프로"})
    monkeypatch.setattr(
        job,
        "collect_company_news",
        lambda queries, now, lookback, pages, templates: ([first_copy, fresh], []),
    )

    job.run(theme_ids=[10, 11])

    assert calls["news_companies"] == [(900, [100, 300, 400])]


def test_run_stores_article_when_another_fetching_search_company_is_valid(wired, monkeypatch):
    job, calls, _ = wired
    # 같은 기사가 엘앤에프(100)·삼성SDI(300) 두 검색에 걸렸다. 중복 제거는 먼저 걸린 엘앤에프 사본을
    # 남기지만, 엘앤에프가 invalid 여도 함께 가져온 삼성SDI 가 valid 면 저장·연결돼야 한다.
    fresh = _item(FRESH_TITLE, "https://a/1", "LFP 양극재")
    sdi_copy = dict(fresh, _query_company={"company_id": 300, "name": "삼성SDI"})
    monkeypatch.setattr(
        job,
        "collect_company_news",
        lambda queries, now, lookback, pages, templates: ([fresh, sdi_copy], []),
    )

    async def judge(articles):
        return BatchVerdict(
            verdicts=[
                ArticleVerdict(
                    id=article.id,
                    companies=[
                        CompanyVerdict(is_company=True, name="엘앤에프", valid=False),
                        CompanyVerdict(is_company=True, name="삼성SDI", valid=True),
                    ],
                )
                for article in articles
            ]
        )

    monkeypatch.setattr(relevance_filter, "get_relevance_judge", lambda: judge)

    job.run(theme_ids=[10, 11])

    # 저장된 기사는 통과한 검색 종목 하나 이상에 반드시 연결된다
    assert calls["news_companies"] == [(900, [300, 400])]
