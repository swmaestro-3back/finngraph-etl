from __future__ import annotations

from datetime import datetime

from pipelines.common.logging import get_logger
from pipelines.news.config import get_news_settings
from pipelines.news.extractors.search_collector import (
    collect_company_news,
    validate_search_settings,
)
from pipelines.news.extractors.text_fetcher import fetch_article_body
from pipelines.news.repositories.postgres.news import (
    has_article_body,
    remove_stored_by_url,
    save_news_items,
)
from pipelines.news.repositories.postgres.news_clusters import (
    fetch_active_cluster_seeds,
    fetch_cluster_articles,
    fetch_untitled_cluster_ids,
    record_cluster_assignments,
    update_cluster_title,
)
from pipelines.news.repositories.postgres.news_companies import link_saved_items
from pipelines.news.repositories.postgres.search_history import (
    CompanyQuery,
    fetch_due_company_queries,
    fetch_due_krx100_queries,
    fetch_due_ticker_queries,
    mark_companies_searched,
)
from pipelines.news.transformers.cluster_titler import title_clusters
from pipelines.news.transformers.clustering import (
    assign_batch,
    batch_documents,
    seed_window,
)
from pipelines.news.transformers.company_matches import match_title_companies
from pipelines.news.transformers.filters.duplicate_filter import remove_duplicate_by_url
from pipelines.news.transformers.filters.relevance_filter import filter_relevant_news
from pipelines.news.transformers.filters.title_filter import filter_titles
from pipelines.news.utils.date_utils import SEOUL_TIMEZONE

logger = get_logger(__name__)


def run(theme_ids: list[int], tickers: list[str] | None = None) -> dict[str, int]:
    """theme_ids 의 편입 기업 중 재검색 시점이 된 기업의 뉴스 수집"""

    validate_search_settings()
    settings = get_news_settings()
    run_started_at = datetime.now(SEOUL_TIMEZONE)

    # 1. 테마 편입 기업 중 search_history 기준 재검색 시점이 된 기업 조회
    if tickers:
        batch = fetch_due_ticker_queries(
            theme_ids, tickers, settings.search_interval_hours, run_started_at
        )
    else:
        batch = fetch_due_company_queries(theme_ids, settings.search_interval_hours, run_started_at)
    logger.info(
        "[collect_articles] 대상: 테마 %d개(%s) → 검색 기업 %d개 "
        "(간격 미도래 %d, company_id 없음 %d, 첫 검색 %d)",
        len(batch.theme_ids),
        f"핫테마 종목 {len(tickers)}개" if tickers else "편입 종목 전체",
        len(batch.queries),
        batch.skipped_not_due,
        batch.skipped_no_company,
        sum(1 for q in batch.queries if q.watermark is None),
    )

    return collect(batch.queries, run_started_at)


def run_krx100(
    company_ids: list[int],
    lookback_days: int | None = None,
    max_pages: int | None = None,
) -> dict[str, int]:
    """KRX100 기업 중 company_ids 에 든, 재검색 시점이 된 기업의 뉴스 수집 (news_backfill_krx100).

    lookback_days·max_pages 로 수집 창을 넓힌다. None 이면 스케줄 런과 같은 설정값이다.
    """

    validate_search_settings()
    settings = get_news_settings()
    run_started_at = datetime.now(SEOUL_TIMEZONE)

    # 1. KRX100 기업 중 search_history 기준 재검색 시점이 된 기업 조회
    batch = fetch_due_krx100_queries(settings.search_interval_hours, run_started_at, company_ids)
    logger.info(
        "[collect_articles] 대상: KRX100 기업 %d개 → 검색 기업 %d개 (간격 미도래 %d, 첫 검색 %d)",
        len(company_ids),
        len(batch.queries),
        batch.skipped_not_due,
        sum(1 for q in batch.queries if q.watermark is None),
    )

    return collect(batch.queries, run_started_at, lookback_days, max_pages)


def collect(
    queries: list[CompanyQuery],
    run_started_at: datetime,
    lookback_days: int | None = None,
    max_pages: int | None = None,
) -> dict[str, int]:
    """검색 대상 기업의 기사를 수집해 필터·클러스터·저장하고 search_history 를 마킹한다."""

    settings = get_news_settings()

    if not queries:
        return {"created": 0, "updated": 0, "failed": 0}

    # 2. 네이버 기사 수집
    collected, failed_company_ids = collect_company_news(
        queries, run_started_at, lookback_days, max_pages
    )
    logger.info(
        "[collect_articles] 수집: 기사 %d건 (기업 %d개 중 실패 %d개)",
        len(collected),
        len(queries),
        len(failed_company_ids),
    )
    if failed_company_ids:
        logger.warning(
            "[collect_articles] 수집 실패 기업 %d개: company_id=%s",
            len(failed_company_ids),
            failed_company_ids,
        )

    # 3. 제목 기업 매치 — 제목에 검색 종목(별칭 포함)이 없는 기사 제거, 제목의 상장사는 판정
    # 대상으로 붙인다. URL 중복 제거보다 먼저다 — 같은 기사가 여러 종목 검색에 걸리면 중복 제거는
    # 먼저 걸린 사본만 남기는데, 그 검색 종목이 제목에 없으면 제목의 주인공 기업 사본까지 함께
    # 사라진다. 매치가 먼저 거르면 살아남는 사본은 검색 종목이 제목에 있는 것뿐이다.
    matched = match_title_companies(collected)

    # 4. 배치 내 URL 중복 제거
    unique_items, _ = remove_duplicate_by_url(matched.kept)

    # 5. 제목 필터 — 제외 패턴이 걸린 기사는 탈락, 통과 기사는 제목 선두 브라켓 제거
    titled_items, _ = filter_titles(unique_items)

    # 6. 이미 저장된 URL 제거 — DB 조회라 메모리 필터를 다 거친 뒤 한 번만 한다
    new_items = remove_stored_by_url(titled_items)
    logger.info(
        "[collect_articles] 필터: 제목에 검색 종목 %d (판정 기업 %d개, 기사당 최대 %d) "
        "→ URL 중복 제거 %d → 제목 필터 %d → DB 기존 제거 %d",
        len(matched.kept),
        matched.companies,
        matched.max_companies,
        len(unique_items),
        len(titled_items),
        len(new_items),
    )

    # 7. LLM 관련성 필터
    relevance = filter_relevant_news(new_items, max_concurrency=settings.news_llm_max_concurrency)
    if new_items and len(relevance.failed) == len(new_items):
        raise RuntimeError(
            f"관련성 판정 전건 실패 ({len(new_items)}건) — LLM 장애로 보고 재시도한다"
        )
    passed = relevance.passed
    logger.info(
        "[collect_articles] 관련성: 통과 %d (연결 기업 %d개) / 무효 %d / 실패 %d",
        len(passed),
        sum(len(item.get("_linked_companies") or []) for item in passed),
        len(relevance.invalid),
        len(relevance.failed),
    )

    # 8. 배치 간 클러스터 판정. 시드 창은 배치 기사의 발행일 범위 기준이다
    documents, published_ats = batch_documents(
        passed, settings.cluster_description_weight, run_started_at
    )

    seeds = []
    if passed:
        window_start, window_end = seed_window(published_ats, settings.cluster_window_days)
        seeds = fetch_active_cluster_seeds(window_start, window_end)

    assignments = assign_batch(
        documents,
        published_ats,
        seeds,
        threshold=settings.cluster_threshold,
        cap=settings.cluster_max_articles,
        window_days=settings.cluster_window_days,
    )

    selected = [passed[index] for assignment in assignments for index in assignment.kept]
    joined = sum(1 for a in assignments if a.seed is not None)
    logger.info(
        "[collect_articles] 클러스터: 시드 %d개 중 합류 %d / 신규 %d → 선별 %d건 (cap 제외 %d)",
        len(seeds),
        joined,
        len(assignments) - joined,
        len(selected),
        sum(len(a.dropped) for a in assignments),
    )

    # 9. 선별된 기사만 본문 크롤링
    fetched = fetch_article_body(selected)
    storable = [item for item in fetched if has_article_body(item)]

    # 10. DB에 뉴스 저장
    save_result = save_news_items(items=storable, save_summary=False, skip_existing=True)
    logger.info(
        "[collect_articles] 저장: 본문 성공 %d / 실패 %d → 신규 %d / 기존 스킵 %d / 실패 %d",
        len(storable),
        len(fetched) - len(storable),
        save_result["inserted_count"],
        save_result["skipped_existing_count"],
        save_result["failed_count"],
    )

    # 11. 클러스터 기록: 본문 실패로 저장 안 된 기사는 멤버에서 빠진다
    cluster_result = record_cluster_assignments(
        assignments, passed, documents, settings.cluster_keyword_count
    )

    # 12. 클러스터 이름 — 이번 런에 판정 기사 수가 기준을 넘었는데 이름이 없는 클러스터만
    untitled = fetch_untitled_cluster_ids(settings.cluster_title_min_size, run_started_at)
    titles = title_clusters(
        fetch_cluster_articles(untitled),
        max_concurrency=settings.news_llm_max_concurrency,
        max_chars=settings.cluster_title_max_chars,
    )
    for cluster_id, title in titles.items():
        update_cluster_title(cluster_id, title)
    untitled_failed = [cluster_id for cluster_id in untitled if cluster_id not in titles]
    logger.info(
        "[collect_articles] 제목: 대상 %d → 생성 %d / 실패 %d",
        len(untitled),
        len(titles),
        len(untitled_failed),
    )
    if untitled_failed:
        logger.warning(
            "[collect_articles] 제목 생성 실패 %d건: cluster_id=%s",
            len(untitled_failed),
            untitled_failed,
        )

    # 13. 기업 연결 — 제목에 나와 판정을 통과한 기업 전부를 news_companies 에
    linked = link_saved_items(storable)

    # 14. 기업 최신 검색 기록 갱신
    searched = [q.company_id for q in queries if q.company_id not in failed_company_ids]
    marked = mark_companies_searched(searched, run_started_at)
    logger.info(
        "[collect_articles] 완료: 클러스터 생성 %d / 갱신 %d / 실패 %d, "
        "기업 연결 %d행 (실패 %d), 검색 기록 %d개 기업",
        cluster_result["created"],
        cluster_result["updated"],
        cluster_result["failed"],
        linked["rows"],
        linked["failed"],
        marked,
    )

    return cluster_result
