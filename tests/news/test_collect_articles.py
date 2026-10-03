"""collect_articles job 단위 테스트. I/O 는 전부 갈아끼우고 단계 순서와 부수효과만 본다."""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from pipelines.common.gazetteer import CompanyMatcher, GazetteerEntry
from pipelines.news.repositories.postgres import news_companies
from pipelines.news.repositories.postgres.search_history import CompanyQuery, CompanyQueryBatch
from pipelines.news.transformers import company_matches
from pipelines.news.transformers.filters import relevance_filter
from pipelines.news.transformers.filters.relevance_filter import (
    ArticleVerdict,
    BatchVerdict,
    CompanyVerdict,
)
from pipelines.news.utils.date_utils import SEOUL_TIMEZONE

# 개체 사전 대역: 검색 종목 엘앤에프(100)와 제목에 함께 나오는 삼성SDI(300)
MATCHER = CompanyMatcher(
    {
        "엘앤에프": GazetteerEntry(
            company_id=100, stock_id=1100, ticker="066970", canonical="엘앤에프"
        ),
        "삼성SDI": GazetteerEntry(
            company_id=300, stock_id=1300, ticker="006400", canonical="삼성SDI"
        ),
    }
)


def _item(title: str, link: str, description: str = "") -> dict:
    return {
        "title": title,
        "description": description,
        "link": link,
        "originallink": link,
        "pubDate": "Wed, 09 Sep 2026 10:00:00 +0900",
        "_query_company": {"company_id": 100, "name": "엘앤에프"},
    }


@pytest.fixture
def wired(monkeypatch):
    """run() 의 I/O 를 전부 갈아끼우고 호출 기록을 남긴다. 개체 사전은 인라인 매처다."""
    from pipelines.news import config
    from pipelines.news.jobs import collect_articles as job

    # run() 이 읽는 설정을 고정한다 — 로컬 .env 값에 좌우되지 않게.
    monkeypatch.setenv("NEWS_CLUSTER_WINDOW_DAYS", "7")
    monkeypatch.setenv("NEWS_CLUSTER_THRESHOLD", "0.35")
    monkeypatch.setenv("NEWS_CLUSTER_DESCRIPTION_WEIGHT", "0.4")
    monkeypatch.setenv("NEWS_CLUSTER_MAX_ARTICLES", "3")
    monkeypatch.setenv("NEWS_SEARCH_INTERVAL_HOURS", "2")
    monkeypatch.setenv("NEWS_SEARCH_LOOKBACK_DAYS", "180")
    monkeypatch.setenv("NEWS_LLM_MAX_CONCURRENCY", "2")
    config.get_news_settings.cache_clear()

    calls: dict[str, list] = {
        "news_companies": [],
        "marked": [],
        "seed_window": [],
        "titles": [],
    }

    batch = CompanyQueryBatch(
        theme_ids=[10, 11],
        queries=[CompanyQuery(company_id=100, name="엘앤에프"), CompanyQuery(200, "에코프로")],
        skipped_no_company=0,
        skipped_not_due=1,
    )
    fresh = _item("엘앤에프, 삼성SDI에 양극재 공급 계약", "https://a/1", "LFP 양극재")
    market = _item("코스피 7000선 사수", "https://a/2")
    existing = _item("엘앤에프 2분기 흑자 전환", "https://a/old")

    monkeypatch.setattr(company_matches, "get_company_matcher", lambda: MATCHER)
    monkeypatch.setattr(job, "validate_search_settings", lambda: None)
    monkeypatch.setattr(job, "fetch_due_company_queries", lambda ids, hours, now: batch)
    # 200 번 기업의 검색은 실패한 것으로 본다
    calls["collect_window"] = []

    def fake_collect(queries, run_started_at, lookback_days, max_pages):
        calls["collect_window"].append((lookback_days, max_pages))
        return [fresh, market, existing], [200]

    monkeypatch.setattr(job, "collect_company_news", fake_collect)
    monkeypatch.setattr(job, "fetch_due_krx100_queries", lambda hours, now, company_ids: batch)
    monkeypatch.setattr(
        job, "remove_stored_by_url", lambda items: [i for i in items if i is not existing]
    )

    def fake_seeds(window_start, window_end):
        calls["seed_window"].append((window_start, window_end))
        return []

    monkeypatch.setattr(job, "fetch_active_cluster_seeds", fake_seeds)

    def fake_fetch_body(items):
        for item in items:
            item["_text"] = "본문 " * 50
        return items

    monkeypatch.setattr(job, "fetch_article_body", fake_fetch_body)
    monkeypatch.setattr(job, "has_article_body", lambda item: bool(item.get("_text")))

    def fake_save(items, save_summary, skip_existing):
        for index, item in enumerate(items, start=900):
            item["_news_id"] = index
            item["_save_action"] = "inserted"
        return {
            "inserted_count": len(items),
            "updated_count": 0,
            "skipped_existing_count": 0,
            "skipped_no_body_count": 0,
            "failed_count": 0,
        }

    monkeypatch.setattr(job, "save_news_items", fake_save)
    monkeypatch.setattr(
        job,
        "record_cluster_assignments",
        lambda assignments, items, documents, keyword_count: {
            "created": 1,
            "updated": 0,
            "failed": 0,
        },
    )

    # 이번 런에 기준을 넘은 클러스터 77 에 이름을 짓는다
    monkeypatch.setattr(job, "fetch_untitled_cluster_ids", lambda min_size, since: [77])
    monkeypatch.setattr(job, "fetch_cluster_articles", lambda ids: {77: ["article"]})
    monkeypatch.setattr(
        job, "title_clusters", lambda clusters, max_concurrency, max_chars: {77: "양극재 공급계약"}
    )
    monkeypatch.setattr(
        job, "update_cluster_title", lambda cid, title: calls["titles"].append((cid, title))
    )

    monkeypatch.setattr(
        news_companies,
        "insert_news_companies",
        lambda news_id, ids: calls["news_companies"].append((news_id, list(ids))) or len(ids),
    )
    monkeypatch.setattr(
        job,
        "mark_companies_searched",
        lambda ids, searched_at: calls["marked"].append(list(ids)) or len(ids),
    )

    async def judge(articles):
        # 시황 기사는 무효, 나머지는 판정 기업 모두 통과로 본다
        return BatchVerdict(
            verdicts=[
                ArticleVerdict(
                    id=article.id,
                    companies=[
                        CompanyVerdict(name=name, valid=not article.title.startswith("코스피"))
                        for name in article.companies
                    ],
                )
                for article in articles
            ]
        )

    # Bedrock 싱글톤 대신 가짜 judge
    monkeypatch.setattr(relevance_filter, "get_relevance_judge", lambda: judge)

    yield job, calls
    config.get_news_settings.cache_clear()


def test_run_filters_then_clusters_then_links_and_marks_only_searched(wired):
    job, calls = wired

    result = job.run(theme_ids=[10, 11])

    assert result == {"created": 1, "updated": 0, "failed": 0}
    # 시장 일반 기사는 제목에 검색 종목이 없어서, 기존 기사는 DB 대조로 버려져 fresh 하나만
    # 저장·연결된다. 제목에 함께 나와 판정을 통과한 삼성SDI 도 같이 연결된다.
    assert calls["news_companies"] == [(900, [100, 300])]
    # 기준을 넘은 클러스터에 이름이 붙는다
    assert calls["titles"] == [(77, "양극재 공급계약")]
    # 검색에 성공한 기업만 search_history 에 기록된다 — 실패한 200 은 다음 런에 다시 읽는다
    assert calls["marked"] == [[100]]
    # 시드 창은 배치 기사 발행일 기준이다: [가장 이른 발행 − 7일, 가장 늦은 발행]
    published = datetime(2026, 9, 9, 10, tzinfo=SEOUL_TIMEZONE)
    assert calls["seed_window"] == [(published - timedelta(days=7), published)]


def test_run_with_tickers_searches_backend_hot_theme_stocks(wired, monkeypatch):
    job, calls = wired
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

    assert job.run(theme_ids=[10], tickers=["066970"]) == {"created": 0, "updated": 0, "failed": 0}
    assert seen == [([10], ["066970"], 2)]


def test_run_with_no_due_companies_marks_nothing_and_skips(wired, monkeypatch):
    job, calls = wired
    monkeypatch.setattr(
        job,
        "fetch_due_company_queries",
        lambda ids, hours, now: CompanyQueryBatch(
            theme_ids=[10], queries=[], skipped_no_company=0, skipped_not_due=3
        ),
    )

    result = job.run(theme_ids=[10])

    assert result == {"created": 0, "updated": 0, "failed": 0}
    assert calls["marked"] == []
    assert calls["news_companies"] == []


def test_run_raises_before_marking_when_all_relevance_judgments_fail(wired, monkeypatch):
    job, calls = wired

    async def always_failing_judge(articles):
        raise RuntimeError("bedrock down")

    monkeypatch.setattr(relevance_filter, "get_relevance_judge", lambda: always_failing_judge)

    with pytest.raises(RuntimeError):
        job.run(theme_ids=[10, 11])

    # 마킹 전에 올라가야 Airflow 재시도가 같은 창을 다시 읽는다
    assert calls["marked"] == []


def test_run_krx100_passes_collection_window_and_marks_searched(wired):
    job, calls = wired

    result = job.run_krx100([100, 200], lookback_days=180, max_pages=8)

    assert result == {"created": 1, "updated": 0, "failed": 0}
    # 백필이 넓힌 수집 창이 수집기까지 내려간다 — 스케줄 런(run)은 None 으로 설정값을 쓴다
    assert calls["collect_window"] == [(180, 8)]
    assert calls["marked"] == [[100]]


def test_run_uses_settings_collection_window(wired):
    job, calls = wired

    job.run(theme_ids=[10, 11])

    assert calls["collect_window"] == [(None, None)]


def test_run_sends_title_companies_and_links_only_valid_ones(wired, monkeypatch):
    job, calls = wired
    sent: list[tuple[str, ...]] = []

    async def judge(articles):
        sent.extend(article.companies for article in articles)
        return BatchVerdict(
            verdicts=[
                ArticleVerdict(
                    id=article.id,
                    companies=[
                        CompanyVerdict(name="엘앤에프", valid=True),
                        CompanyVerdict(name="삼성SDI", valid=False),
                    ],
                )
                for article in articles
            ]
        )

    monkeypatch.setattr(relevance_filter, "get_relevance_judge", lambda: judge)

    job.run(theme_ids=[10, 11])

    # 검색 종목이 첫 번째, 제목 표기 그대로
    assert sent == [("엘앤에프", "삼성SDI")]
    assert calls["news_companies"] == [(900, [100])]


def test_run_matches_titles_first_and_queries_stored_urls_last(wired, monkeypatch):
    job, calls = wired
    order: list[tuple[str, list[str]]] = []

    def fake_title(items):
        order.append(("title", [item["link"] for item in items]))
        return items, []

    def fake_stored(items):
        order.append(("stored", [item["link"] for item in items]))
        return [item for item in items if item["link"] != "https://a/old"]

    monkeypatch.setattr(job, "filter_titles", fake_title)
    monkeypatch.setattr(job, "remove_stored_by_url", fake_stored)

    job.run(theme_ids=[10, 11])

    # 제목 매치가 시황 기사(https://a/2)를 먼저 버리고, DB 대조는 메모리 필터를 다 통과한 기사에만
    # 맨 마지막에 돈다
    assert order == [
        ("title", ["https://a/1", "https://a/old"]),
        ("stored", ["https://a/1", "https://a/old"]),
    ]


def test_run_keeps_the_copy_whose_query_company_is_in_the_title(wired, monkeypatch):
    job, calls = wired
    # 같은 기사가 에코프로 검색에 먼저 걸렸다. 제목에 에코프로가 없으므로 그 사본은 버리고, 제목의
    # 주인공 엘앤에프로 검색한 사본이 중복 제거에서 살아남아야 한다.
    fresh = _item("엘앤에프, 삼성SDI에 양극재 공급 계약", "https://a/1", "LFP 양극재")
    first_copy = dict(fresh, _query_company={"company_id": 200, "name": "에코프로"})
    monkeypatch.setattr(
        job, "collect_company_news", lambda queries, now, lookback, pages: ([first_copy, fresh], [])
    )

    job.run(theme_ids=[10, 11])

    assert calls["news_companies"] == [(900, [100, 300])]


def test_run_stores_article_when_another_fetching_search_company_is_valid(wired, monkeypatch):
    job, calls = wired
    # 같은 기사가 엘앤에프(100)·삼성SDI(300) 두 검색에 걸렸다. 중복 제거는 먼저 걸린 엘앤에프 사본을
    # 남기지만, 엘앤에프가 invalid 여도 함께 가져온 삼성SDI 가 valid 면 저장·연결돼야 한다.
    fresh = _item("엘앤에프, 삼성SDI에 양극재 공급 계약", "https://a/1", "LFP 양극재")
    sdi_copy = dict(fresh, _query_company={"company_id": 300, "name": "삼성SDI"})
    monkeypatch.setattr(
        job, "collect_company_news", lambda queries, now, lookback, pages: ([fresh, sdi_copy], [])
    )

    async def judge(articles):
        return BatchVerdict(
            verdicts=[
                ArticleVerdict(
                    id=article.id,
                    companies=[
                        CompanyVerdict(name="엘앤에프", valid=False),
                        CompanyVerdict(name="삼성SDI", valid=True),
                    ],
                )
                for article in articles
            ]
        )

    monkeypatch.setattr(relevance_filter, "get_relevance_judge", lambda: judge)

    job.run(theme_ids=[10, 11])

    assert calls["news_companies"] == [(900, [300])]
