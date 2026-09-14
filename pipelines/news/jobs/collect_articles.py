from __future__ import annotations

from datetime import datetime

from pipelines.common.logging import get_logger
from pipelines.news.config import get_news_settings
from pipelines.news.extractors.search_collector import (
    collect_company_news,
    validate_search_settings,
)
from pipelines.news.extractors.text_fetcher import fetch_article_body
from pipelines.news.repositories.news import (
    has_article_body,
    remove_stored_by_url,
    save_news_items,
)
from pipelines.news.repositories.news_clusters import (
    fetch_active_cluster_seeds,
    fetch_cluster_articles,
    fetch_untitled_cluster_ids,
    record_cluster_assignments,
    update_cluster_title,
)
from pipelines.news.repositories.news_companies import link_saved_items
from pipelines.news.repositories.search_history import (
    fetch_due_company_queries,
    mark_companies_searched,
)
from pipelines.news.transformers.cluster_titler import title_clusters
from pipelines.news.transformers.clustering import (
    assign_batch,
    batch_documents,
    seed_window,
)
from pipelines.news.transformers.company_candidates import attach_candidate_companies
from pipelines.news.transformers.duplicate_filter import remove_duplicate_by_url
from pipelines.news.transformers.news_type_filter import filter_official_source_news
from pipelines.news.transformers.relevance_filter import filter_relevant_news
from pipelines.news.utils.date_utils import SEOUL_TIMEZONE
from pipelines.news.utils.text_utils import remove_leading_title_brackets

logger = get_logger(__name__)


def run(theme_ids: list[int]) -> dict[str, int]:
    """theme_ids 의 편입 기업 중 재검색 시점이 된 기업의 뉴스 수집"""

    validate_search_settings()
    settings = get_news_settings()
    run_started_at = datetime.now(SEOUL_TIMEZONE)

    # 1. 테마 편입 기업 중 search_history 기준 재검색 시점이 된 기업 조회
    batch = fetch_due_company_queries(theme_ids, settings.search_interval_hours, run_started_at)
    logger.info(
        "[대상] 테마 %d개 → 검색 기업 %d개 (간격 미도래 %d, company_id 없음 %d, 첫 검색 %d)",
        len(batch.theme_ids),
        len(batch.queries),
        batch.skipped_not_due,
        batch.skipped_no_company,
        sum(1 for q in batch.queries if q.watermark is None),
    )

    if not batch.queries:
        return {"created": 0, "updated": 0, "failed": 0}

    # 2. 네이버 기사 수집
    collected, failed_company_ids = collect_company_news(batch.queries, run_started_at)
    logger.info(
        "[수집] 기사 %d건 (기업 %d개 중 실패 %d개%s)",
        len(collected),
        len(batch.queries),
        len(failed_company_ids),
        f": {failed_company_ids}" if failed_company_ids else "",
    )

    # 3. 배치 내 URL 중복 제거
    unique_items, _ = remove_duplicate_by_url(collected)

    # 4. 기사 유형 필터로 제거
    typed_items, _ = filter_official_source_news(
        unique_items,
        pipeline_input={},
        official_source_threshold=settings.official_source_threshold,
    )

    # 5. 제목 폴리싱 (선두 브라켓 제거)
    for item in typed_items:
        item["title"] = remove_leading_title_brackets(item.get("title", ""))

    # 6. 이미 저장된 URL 제거
    new_items = remove_stored_by_url(typed_items)
    logger.info(
        "[필터] 수집 %d → URL 중복 제거 %d → 유형 필터 %d → DB 기존 제거 %d (신규)",
        len(collected),
        len(unique_items),
        len(typed_items),
        len(new_items),
    )

    # 7. 후보 상장사 부착 (gazetteer 활용)
    attach_candidate_companies(new_items)

    # 8. LLM 관련성 필터
    relevance = filter_relevant_news(new_items, max_concurrency=settings.news_llm_max_concurrency)
    if new_items and len(relevance.failed) == len(new_items):
        raise RuntimeError(
            f"관련성 판정 전건 실패 ({len(new_items)}건) — LLM 장애로 보고 재시도한다"
        )
    passed = relevance.passed
    logger.info(
        "[LLM] 통과 %d / 무효 %d / 무관 %d / 실패 %d",
        len(passed),
        len(relevance.invalid),
        len(relevance.irrelevant),
        len(relevance.failed),
    )

    # 9. 배치 간 클러스터 판정. 시드 창은 배치 기사의 발행일 범위 기준이다
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
        "[클러스터] 시드 %d개 중 합류 %d, 신규 %d → 선별 %d건 (cap 제외 %d)",
        len(seeds),
        joined,
        len(assignments) - joined,
        len(selected),
        sum(len(a.dropped) for a in assignments),
    )

    # 10. 선별된 기사만 본문 크롤링
    fetched = fetch_article_body(selected)
    storable = [item for item in fetched if has_article_body(item)]

    # 11. DB에 뉴스 저장
    save_result = save_news_items(items=storable, save_summary=False, skip_existing=True)
    logger.info(
        "[저장] 본문 성공 %d / 실패 %d → 신규 %d, 기존 스킵 %d, 실패 %d",
        len(storable),
        len(fetched) - len(storable),
        save_result["inserted_count"],
        save_result["skipped_existing_count"],
        save_result["failed_count"],
    )

    # 12. 클러스터 기록: 본문 실패로 저장 안 된 기사는 멤버에서 빠진다
    cluster_result = record_cluster_assignments(
        assignments, passed, documents, settings.cluster_keyword_count
    )

    # 13. 클러스터 이름 — 이번 런에 판정 기사 수가 기준을 넘었는데 이름이 없는 클러스터만
    untitled = fetch_untitled_cluster_ids(settings.cluster_title_min_size, run_started_at)
    titles = title_clusters(
        fetch_cluster_articles(untitled),
        max_concurrency=settings.news_llm_max_concurrency,
        max_chars=settings.cluster_title_max_chars,
    )
    for cluster_id, title in titles.items():
        update_cluster_title(cluster_id, title)
    logger.info(
        "[제목] 대상 %d → 생성 %d / 실패 %d",
        len(untitled),
        len(titles),
        len(untitled) - len(titles),
    )

    # 14. 기업 연결 — 주체 종목명을 company_id 로 해석해 news_companies 에
    linked = link_saved_items(storable)

    # 15. 기업 최신 검색 기록 갱신
    searched = [q.company_id for q in batch.queries if q.company_id not in failed_company_ids]
    marked = mark_companies_searched(searched, run_started_at)
    logger.info(
        "[기록] 클러스터 생성 %d / 갱신 %d / 실패 %d, 기업 연결 %d행 (해석 실패 %d), "
        "검색 기록 %d개 기업",
        cluster_result["created"],
        cluster_result["updated"],
        cluster_result["failed"],
        linked["rows"],
        linked["unresolved_names"],
        marked,
    )

    return cluster_result
