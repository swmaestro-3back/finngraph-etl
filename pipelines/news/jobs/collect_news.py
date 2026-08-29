"""뉴스 통합 수집 job (news_pipeline DAG의 collect_news task).

헤드라인·검색 수집을 동시에 진행하고, 병합 배치에 통합 필터 → 전체
클러스터링(클러스터당 최대 N개 선별) → 본문 크롤링 → 저장 → 클러스터 대표
기록 순으로 진행한다. 클러스터링을 본문 수집보다 앞에 두어, 버려질 기사의
본문은 크롤링하지 않는다.

한 소스 수집이 실패하면 경고만 남기고 나머지 소스로 계속 진행한다(부분 진행).
두 소스가 모두 실패했을 때만 예외를 올려 Airflow retry에 맡긴다.
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from pipelines.news.config import get_news_settings
from pipelines.news.extractors.headline_collector import (
    validate_anchor_headline_settings,
)
from pipelines.news.extractors.search_collector import validate_search_settings
from pipelines.news.extractors.text_fetcher import enrich_items_with_article_body
from pipelines.news.jobs.collect_headline import collect_category_new_headlines
from pipelines.news.jobs.collect_search import collect_search_news
from pipelines.news.loaders.postgres import (
    assign_cluster_representatives,
    fetch_search_keywords,
    filter_new_news_by_db,
    has_article_body,
    mark_keywords_searched,
    mark_news_source_type,
    save_news_items,
)
from pipelines.news.transformers.clustering import select_articles_by_cluster
from pipelines.news.transformers.duplicate_filter import (
    remove_duplicate_by_title,
    remove_duplicate_by_url,
)
from pipelines.news.transformers.news_type_filter import filter_official_source_news


def collect_headline_items() -> list[dict[str, Any]]:
    """전 카테고리 헤드라인을 수집하고 _source_type을 태깅한다.

    페이지별 필터(DB 워터마크 중단·공식출처·목표 개수)는 수집기 내부의 크롤링
    제어 장치로 그대로 동작하며, 병합 배치에서 같은 필터가 한 번 더 적용된다.
    """

    validate_anchor_headline_settings()
    settings = get_news_settings()

    items: list[dict[str, Any]] = []

    for category_id in settings.anchor_categories:
        category_items, category_stats = collect_category_new_headlines(
            category_id=category_id,
            target_count=settings.max_total_collected_items,
            max_more_calls=settings.headline_more_count,
        )
        items.extend(category_items)
        logging.info(
            "헤드라인 수집 %s: 선정 %d개 (raw %d)",
            category_stats.get("category_name", category_id),
            category_stats.get("selected", len(category_items)),
            category_stats.get("raw", 0),
        )

    for item in items:
        item["_source_type"] = "headline"

    return items


def collect_search_items() -> tuple[list[dict[str, Any]], list[int]]:
    """search_keywords 기반 검색 수집. (기사 목록, 키워드 id 목록)을 반환한다."""

    validate_search_settings()
    keywords = fetch_search_keywords()

    if not keywords:
        logging.info("search_keywords 테이블에 검색 쿼리가 없습니다.")
        return [], []

    items, _stats = collect_search_news([keyword["keyword"] for keyword in keywords])

    for item in items:
        item["_source_type"] = "search"

    return items, [keyword["id"] for keyword in keywords]


def collect_all_sources() -> tuple[list[dict[str, Any]], list[int], dict[str, Any]]:
    """두 소스를 동시에 수집한다. 한쪽 실패는 건너뛰고, 둘 다 실패하면 예외."""

    with ThreadPoolExecutor(max_workers=2) as executor:
        headline_future = executor.submit(collect_headline_items)
        search_future = executor.submit(collect_search_items)

    errors: dict[str, Exception] = {}
    headline_items: list[dict[str, Any]] = []
    search_items: list[dict[str, Any]] = []
    keyword_ids: list[int] = []

    try:
        headline_items = headline_future.result()
    except Exception as e:  # noqa: BLE001 - 소스 단위 격리가 목적
        errors["headline"] = e
        logging.warning("헤드라인 수집 실패, 다른 소스로 진행: %s", e)

    try:
        search_items, keyword_ids = search_future.result()
    except Exception as e:  # noqa: BLE001 - 소스 단위 격리가 목적
        errors["search"] = e
        logging.warning("검색 수집 실패, 다른 소스로 진행: %s", e)

    if len(errors) == 2:
        raise RuntimeError(f"헤드라인·검색 수집이 모두 실패했습니다: {errors}")

    stats = {
        "headline_collected": len(headline_items),
        "search_collected": len(search_items),
        "failed_sources": sorted(errors),
    }

    return headline_items + search_items, keyword_ids, stats


def apply_batch_filters(
    items: list[dict[str, Any]],
    official_source_threshold: int,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """병합 배치에 통합 필터를 적용한다: URL중복 → 제목중복 → 기사유형 → DB기존.

    수집기 내부 필터와 겹치지만 멱등이라 비용이 거의 없고, 소스 간 교차 중복을
    여기서 잡는다.
    """

    unique_items, url_removed = remove_duplicate_by_url(items)
    unique_items, title_removed = remove_duplicate_by_title(unique_items)

    typed_items, type_removed = filter_official_source_news(
        unique_items,
        pipeline_input={},
        official_source_threshold=official_source_threshold,
    )

    new_items, existing_items = filter_new_news_by_db(typed_items)

    stats = {
        "merged": len(items),
        "duplicate_removed": len(url_removed) + len(title_removed),
        "type_removed": len(type_removed),
        "db_existing_removed": len(existing_items),
        "new": len(new_items),
    }

    return new_items, stats


def run() -> dict[str, Any]:
    settings = get_news_settings()

    # 1. 두 소스 동시 수집 (부분 실패 허용)
    merged, keyword_ids, collect_stats = collect_all_sources()

    # 2. 통합 필터: URL/제목 중복 → 기사유형 → DB 기존 제외
    new_items, filter_stats = apply_batch_filters(
        merged, official_source_threshold=settings.official_source_threshold
    )

    # 3. 전체 배치 클러스터링 + 클러스터당 최대 N개 선별 (그룹 첫 번째가 대표)
    groups, cluster_stats = select_articles_by_cluster(
        new_items,
        threshold=settings.cluster_threshold,
        description_weight=settings.cluster_description_weight,
        max_per_cluster=settings.cluster_max_articles,
    )
    selected = [item for group in groups for item in group]

    # 4. 선별된 기사만 본문 크롤링
    enriched = enrich_items_with_article_body(selected)
    storable = [item for item in enriched if has_article_body(item)]

    # 5. 저장 (relation_extracted=NULL이 미추출 상태). save_news_items가 각 item에 _news_id를 채운다
    save_result = save_news_items(items=storable, save_summary=False, skip_existing=True)

    # 6. 클러스터 대표 기록: 본문 실패로 저장 안 된 기사는 제외되고, 그룹에서
    #    가장 먼저 저장에 성공한 기사(메도이드 우선순)가 대표가 된다
    saved_groups = [
        saved_ids
        for group in groups
        if (saved_ids := [item["_news_id"] for item in group if item.get("_news_id")])
    ]
    clustered_count = assign_cluster_representatives(saved_groups)

    # 7. 후처리: 수집 경로 태깅 + 검색 키워드 마킹 (검색 실패 시 keyword_ids가 비어 자연 스킵)
    for source_type in ("headline", "search"):
        source_ids = [
            item["_news_id"]
            for item in storable
            if item.get("_news_id") and item.get("_source_type") == source_type
        ]
        mark_news_source_type(source_ids, source_type)

    searched_count = mark_keywords_searched(keyword_ids)

    print("\n" + "=" * 70)
    print("뉴스 통합 수집·클러스터링 결과")
    print("=" * 70)
    if collect_stats["failed_sources"]:
        print(f"- 수집 실패 소스: {', '.join(collect_stats['failed_sources'])}")
    print(
        f"- 수집: 헤드라인 {collect_stats['headline_collected']}개 "
        f"+ 검색 {collect_stats['search_collected']}개 = {filter_stats['merged']}개"
    )
    print(
        f"- 통합 필터: 중복 {filter_stats['duplicate_removed']}, "
        f"유형 {filter_stats['type_removed']}, DB기존 {filter_stats['db_existing_removed']} "
        f"→ 신규 {filter_stats['new']}개"
    )
    print(
        f"- 군집 {cluster_stats['cluster_count']}개 → 선별 {cluster_stats['selected']}개 "
        f"(cap 제외 {cluster_stats['dropped_by_cluster_cap']}개)"
    )
    print(f"- 본문 성공 {len(storable)} / 실패 {len(enriched) - len(storable)}")
    print(f"- 저장 결과 {save_result}, 클러스터 대표 기록 {clustered_count}건")
    print(f"- 키워드 마킹 {searched_count}개")
    print("=" * 70)

    return {
        "collect": collect_stats,
        "filter": filter_stats,
        "cluster": cluster_stats,
        "saved": save_result,
        "clustered": clustered_count,
        "keywords_marked": searched_count,
    }


if __name__ == "__main__":
    run()
