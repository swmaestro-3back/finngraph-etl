"""collect_articles job 단위 테스트. I/O 는 전부 갈아끼우고 단계 순서와 부수효과만 본다."""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from pipelines.news.repositories import news_companies
from pipelines.news.repositories.search_history import CompanyQuery, CompanyQueryBatch
from pipelines.news.transformers.cluster_titler import ClusterLabel
from pipelines.news.transformers.filters import relevance_filter
from pipelines.news.transformers.filters.relevance_filter import ArticleVerdict, BatchVerdict
from pipelines.news.utils.date_utils import SEOUL_TIMEZONE


def _item(title: str, link: str, description: str = "") -> dict:
    return {
        "title": title,
        "description": description,
        "link": link,
        "originallink": link,
        "pubDate": "Wed, 09 Sep 2026 10:00:00 +0900",
        "_query_companies": [{"company_id": 100, "name": "엘앤에프"}],
    }


@pytest.fixture
def wired(monkeypatch):
    """run() 의 I/O 를 전부 갈아끼우고 호출 기록을 남긴다. gazetteer 후보 추출은 실제 코드다."""
    from pipelines.news import config
    from pipelines.news.jobs import collect_articles as job

    # run() 이 읽는 설정을 고정한다 — 로컬 .env 값(OFFICIAL_SOURCE_THRESHOLD 등)에 좌우되지 않게.
    monkeypatch.setenv("OFFICIAL_SOURCE_THRESHOLD", "0")
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
        "resolved_names": [],
        "titles": [],
        "judgments": [],
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

    monkeypatch.setattr(job, "validate_search_settings", lambda: None)
    monkeypatch.setattr(job, "fetch_due_company_queries", lambda ids, hours, now: batch)
    # 200 번 기업의 검색은 실패한 것으로 본다
    monkeypatch.setattr(
        job,
        "collect_company_news",
        lambda queries, run_started_at: ([fresh, market, existing], [200]),
    )
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
    # 판정마다 새 클러스터 501, 502, ... 가 만들어진 것으로 본다
    monkeypatch.setattr(
        job,
        "record_cluster_assignments",
        lambda assignments, items, documents, keyword_count: (
            {"created": 1, "updated": 0, "failed": 0},
            [501 + i for i in range(len(assignments))],
        ),
    )
    monkeypatch.setattr(
        job,
        "insert_cluster_judgments",
        lambda judgments: calls["judgments"].extend(judgments) or len(judgments),
    )

    # 이번 런에 기준을 넘은 클러스터 77 에 이름·요약을, 78 에는 이름만(요약 버림) 짓는다
    monkeypatch.setattr(job, "fetch_untitled_cluster_ids", lambda min_size, since: [77, 78])
    monkeypatch.setattr(
        job, "fetch_cluster_articles", lambda ids: {77: ["article"], 78: ["article"]}
    )
    monkeypatch.setattr(
        job,
        "title_clusters",
        lambda clusters, max_concurrency, max_chars, summary_max_chars: {
            77: ClusterLabel("양극재 공급계약", "엘앤에프가 삼성SDI에 양극재를 공급해요."),
            78: ClusterLabel("2분기 실적", None),
        },
    )
    monkeypatch.setattr(
        job,
        "update_cluster_label",
        lambda cid, title, summary: calls["titles"].append((cid, title, summary)),
    )

    def fake_resolve(names):
        calls["resolved_names"].append(sorted(names))
        return {"삼성SDI": 300}  # 엘앤에프는 일부러 빼서 출처 종목 fallback 을 검증한다

    monkeypatch.setattr(news_companies, "fetch_company_ids_by_stock_names", fake_resolve)
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
        return BatchVerdict(
            verdicts=[
                ArticleVerdict(
                    id=article.id,
                    valid=not article.title.startswith("코스피"),
                    companies=[]
                    if article.title.startswith("코스피")
                    else list(article.candidates),
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
    # 시장 일반 기사는 invalid 로, 기존 기사는 DB 대조로 버려져 fresh 하나만 저장·연결된다.
    # gazetteer 가 찾은 삼성SDI 와 출처 종목 엘앤에프가 모두 연결된다 (엘앤에프는 해석 실패 → 출처 fallback)
    assert calls["resolved_names"] == [["삼성SDI", "엘앤에프"]]
    assert calls["news_companies"] == [(900, [100, 300])]
    # 기준을 넘은 클러스터에 이름이 붙는다. 요약이 버려진 클러스터는 요약 NULL 로 이름만 저장
    assert calls["titles"] == [
        (77, "양극재 공급계약", "엘앤에프가 삼성SDI에 양극재를 공급해요."),
        (78, "2분기 실적", None),
    ]
    # 검색에 성공한 기업만 search_history 에 기록된다 — 실패한 200 은 다음 런에 다시 읽는다
    assert calls["marked"] == [[100]]
    # 시드 창은 배치 기사 발행일 기준이다: [가장 이른 발행 − 7일, 가장 늦은 발행]
    published = datetime(2026, 9, 9, 10, tzinfo=SEOUL_TIMEZONE)
    assert calls["seed_window"] == [(published - timedelta(days=7), published)]
    # 판정된 기사(LLM 통과분)마다 판정 기록이 한 행 남는다
    [judgment] = calls["judgments"]
    assert (judgment.link, judgment.title, judgment.description) == (
        "https://a/1",
        "엘앤에프, 삼성SDI에 양극재 공급 계약",
        "LFP 양극재",
    )
    assert judgment.published_at == published
    assert (judgment.cluster_id, judgment.is_new_cluster, judgment.kept) == (501, True, True)
    assert judgment.seed_similarity is None
    assert judgment.run_at.tzinfo is not None


def test_run_records_body_failed_article_as_non_member(wired, monkeypatch):
    job, calls = wired
    # 본문 크롤링이 실패해 저장되지 않으면 새 클러스터도 만들어지지 않는다
    monkeypatch.setattr(job, "fetch_article_body", lambda items: items)
    monkeypatch.setattr(
        job,
        "record_cluster_assignments",
        lambda assignments, items, documents, keyword_count: (
            {"created": 0, "updated": 0, "failed": 0},
            [None] * len(assignments),
        ),
    )

    job.run(theme_ids=[10, 11])

    [judgment] = calls["judgments"]
    assert (judgment.link, judgment.cluster_id, judgment.is_new_cluster, judgment.kept) == (
        "https://a/1",
        None,
        True,
        False,
    )


def test_run_continues_when_judgment_logging_fails(wired, monkeypatch, caplog):
    job, calls = wired

    def failing_insert(judgments):
        raise RuntimeError("relation news_cluster_judgments does not exist")

    monkeypatch.setattr(job, "insert_cluster_judgments", failing_insert)

    result = job.run(theme_ids=[10, 11])

    # 판정 기록은 평가용이라 실패해도 이름 짓기·기업 연결·검색 기록까지 끝까지 간다
    assert result == {"created": 1, "updated": 0, "failed": 0}
    assert [cid for cid, _, _ in calls["titles"]] == [77, 78]
    assert calls["news_companies"] == [(900, [100, 300])]
    assert calls["marked"] == [[100]]
    assert "[판정 기록] 실패(건너뜀) 기사 1건 / 클러스터 1개: RuntimeError" in caplog.text


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
