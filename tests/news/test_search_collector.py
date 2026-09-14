"""검색 수집기 단위 테스트. 네이버 API 호출은 fetch_search_news_page 를 갈아끼워 흉내낸다."""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from pipelines.news.repositories.search_history import CompanyQuery
from pipelines.news.utils.date_utils import SEOUL_TIMEZONE

NOW = datetime(2026, 9, 10, 12, 0, tzinfo=SEOUL_TIMEZONE)
QUERY = CompanyQuery(company_id=100, name="엘앤에프")
MARKED = CompanyQuery(company_id=100, name="엘앤에프", watermark=NOW - timedelta(hours=2))


def _raw(link: str, hours_ago: float | None = 0) -> dict:
    pub = ""
    if hours_ago is not None:
        pub = (NOW - timedelta(hours=hours_ago)).strftime("%a, %d %b %Y %H:%M:%S +0900")
    return {"title": f"기사 {link}", "description": "", "link": link, "pubDate": pub}


@pytest.fixture
def settings(monkeypatch):
    """env 로 설정을 고정한다. conftest 가 SEARCH_DISPLAY=1 을 주입하므로 100 으로 덮는다."""
    from pipelines.news import config

    monkeypatch.setenv("SEARCH_DISPLAY", "100")
    monkeypatch.setenv("SEARCH_SORT", "date")
    monkeypatch.setenv("NEWS_SEARCH_QUERY_TEMPLATE", "특징주,{name}")
    monkeypatch.setenv("NEWS_SEARCH_MAX_PAGES", "3")
    monkeypatch.setenv("NEWS_SEARCH_LOOKBACK_DAYS", "180")
    monkeypatch.setenv("REQUEST_DELAY", "0")
    config.get_news_settings.cache_clear()
    yield config.get_news_settings()
    config.get_news_settings.cache_clear()


@pytest.fixture
def fake_pages(monkeypatch):
    """start 번호 → raw 목록. 요청 로그를 남긴다."""
    from pipelines.news.extractors import search_collector

    calls: list[tuple[str, int]] = []
    pages: dict[int, list[dict]] = {}

    def fake_fetch(session, keyword, display, start, sort, timeout=10):
        calls.append((keyword, start))
        return pages.get(start, [])

    monkeypatch.setattr(search_collector, "fetch_search_news_page", fake_fetch)
    return pages, calls


def test_build_search_query_uses_template(settings):
    from pipelines.news.extractors.search_collector import build_search_query

    assert build_search_query(QUERY) == "특징주,엘앤에프"


def test_collection_cutoff(settings):
    from pipelines.news.extractors.search_collector import collection_cutoff

    lookback = NOW - timedelta(days=180)
    assert collection_cutoff(QUERY, NOW) == lookback
    assert collection_cutoff(MARKED, NOW) == MARKED.watermark

    stale = CompanyQuery(100, "엘앤에프", watermark=NOW - timedelta(days=400))
    assert collection_cutoff(stale, NOW) == lookback  # 워터마크가 lookback 보다 오래되면 lookback


def test_tag_query_companies_sets_origin_and_keyword():
    from pipelines.news.extractors.search_collector import tag_query_companies

    items = [{"title": "a"}, {"title": "b"}]
    tag_query_companies(items, QUERY, "특징주,엘앤에프")

    assert items[0]["_query_companies"] == [{"company_id": 100, "name": "엘앤에프"}]
    assert items[1]["_search_keyword"] == "특징주,엘앤에프"


def test_drop_older_than_keeps_unparseable():
    from pipelines.news.extractors.search_collector import drop_older_than

    fresh = {"pubDate": _raw("x", 3)["pubDate"]}
    old = {"pubDate": _raw("y", 24 * 400)["pubDate"]}
    unknown = {"pubDate": "not-a-date"}

    kept, removed = drop_older_than([fresh, old, unknown], NOW - timedelta(days=180))

    assert kept == [fresh, unknown]
    assert removed == [old]


def test_collect_stock_news_stops_at_first_page_older_than_cutoff(settings, fake_pages):
    from pipelines.news.extractors.search_collector import collect_stock_news

    pages, calls = fake_pages
    pages[1] = [_raw(f"https://a/{i}", hours_ago=0.5) for i in range(100)]
    # 두 번째 페이지 절반은 워터마크(2시간 전) 보다 오래됨
    pages[101] = [_raw(f"https://b/{i}", hours_ago=1.5) for i in range(50)] + [
        _raw(f"https://c/{i}", hours_ago=5) for i in range(50)
    ]
    pages[201] = [_raw(f"https://d/{i}", hours_ago=10) for i in range(100)]

    items = collect_stock_news(MARKED, NOW)

    assert [(keyword, start) for keyword, start in calls] == [
        ("특징주,엘앤에프", 1),
        ("특징주,엘앤에프", 101),
    ]
    assert len(items) == 150  # 세 번째 페이지는 읽지 않고, 두 번째 페이지의 오래된 절반은 버린다
    assert all(i["_search_keyword"] == "특징주,엘앤에프" for i in items)


def test_collect_stock_news_reads_up_to_max_pages_when_all_fresh(settings, fake_pages):
    from pipelines.news.extractors.search_collector import collect_stock_news

    pages, calls = fake_pages
    pages[1] = [_raw(f"https://a/{i}") for i in range(100)]
    pages[101] = [_raw(f"https://b/{i}") for i in range(100)]
    pages[201] = [_raw(f"https://c/{i}") for i in range(100)]
    pages[301] = [_raw(f"https://d/{i}") for i in range(100)]

    items = collect_stock_news(QUERY, NOW)

    assert [start for _, start in calls] == [1, 101, 201]  # 상한 3 페이지
    assert len(items) == 300


def test_collect_stock_news_short_page_ends_pagination(settings, fake_pages):
    from pipelines.news.extractors.search_collector import collect_stock_news

    pages, calls = fake_pages
    pages[1] = [_raw(f"https://a/{i}") for i in range(40)]  # display 미만 → 마지막 페이지

    items = collect_stock_news(QUERY, NOW)

    assert len(items) == 40
    assert [start for _, start in calls] == [1]


def test_collect_company_news_isolates_stock_failure(settings, monkeypatch):
    from pipelines.news.extractors import search_collector
    from pipelines.news.extractors.search_collector import NewsSearchError, collect_company_news

    ok = CompanyQuery(company_id=100, name="엘앤에프")
    bad = CompanyQuery(company_id=200, name="에코프로")

    def fake_collect(query, run_started_at):
        if query is bad:
            raise NewsSearchError("boom")
        return [{"title": "t", "_query_companies": []}]

    monkeypatch.setattr(search_collector, "collect_stock_news", fake_collect)

    items, failed = collect_company_news([ok, bad], NOW)

    assert len(items) == 1
    assert failed == [200]


def test_collect_company_news_raises_when_all_fail(settings, monkeypatch):
    from pipelines.news.extractors import search_collector
    from pipelines.news.extractors.search_collector import NewsSearchError, collect_company_news

    def fake_collect(query, run_started_at):
        raise NewsSearchError("boom")

    monkeypatch.setattr(search_collector, "collect_stock_news", fake_collect)

    with pytest.raises(NewsSearchError):
        collect_company_news([QUERY], NOW)


def test_fetch_search_news_page_raises_on_request_error(monkeypatch):
    import requests

    from pipelines.news.extractors.search_collector import NewsSearchError, fetch_search_news_page

    class FailingSession:
        def get(self, *args, **kwargs):
            raise requests.ConnectionError("down")

    with pytest.raises(NewsSearchError):
        fetch_search_news_page(FailingSession(), "특징주,엘앤에프", 100, 1, "date")


def test_validate_search_settings_rejects_non_date_sort(monkeypatch):
    from pipelines.news import config
    from pipelines.news.extractors.search_collector import validate_search_settings

    monkeypatch.setenv("CLIENT_ID", "id")
    monkeypatch.setenv("CLIENT_SECRET", "secret")
    monkeypatch.setenv("API_BASE_URL", "https://openapi.naver.com/v1/search/news.json")
    monkeypatch.setenv("SEARCH_SORT", "sim")
    config.get_news_settings.cache_clear()
    try:
        with pytest.raises(RuntimeError, match="SEARCH_SORT"):
            validate_search_settings()
        monkeypatch.setenv("SEARCH_SORT", "date")
        config.get_news_settings.cache_clear()
        validate_search_settings()
    finally:
        config.get_news_settings.cache_clear()
