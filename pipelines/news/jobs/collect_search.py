"""뉴스 수집 + 클러스터링 job (news_pipeline DAG의 collect_articles task).

수집 → 중복 제거 → 기사 유형 필터 → DB 기존 기사 제외 → 클러스터링(클러스터당
최대 N개 선별) → 본문 크롤링 → news INSERT 순서로 진행한다. 클러스터링을 본문
수집보다 앞에 두어, 버려질 기사의 본문은 크롤링하지 않는다.
"""

from __future__ import annotations

from typing import Any

from pipelines.news.config import get_news_settings
from pipelines.news.extractors.search_collector import (
    iter_search_news_pages,
    validate_search_settings,
)
from pipelines.news.extractors.text_fetcher import enrich_items_with_article_body
from pipelines.news.loaders.postgres import (
    assign_cluster_representatives,
    fetch_search_keywords,
    filter_new_news_by_db,
    has_article_body,
    mark_keywords_searched,
    save_news_items,
)
from pipelines.news.transformers.clustering import select_articles_by_cluster
from pipelines.news.transformers.duplicate_filter import (
    remove_duplicate_by_title,
    remove_duplicate_by_url,
)
from pipelines.news.transformers.news_type_filter import filter_official_source_news


def collect_search_news(queries: list[str]) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """쿼리 목록으로 네이버 검색 API를 순회 수집하고 배치 내 중복을 제거한다."""

    collected: list[dict[str, Any]] = []
    raw_count = 0

    for query in queries:
        for page_items in iter_search_news_pages(keyword=query):
            raw_count += len(page_items)
            collected.extend(page_items)

    unique_items, url_removed = remove_duplicate_by_url(collected)
    unique_items, title_removed = remove_duplicate_by_title(unique_items)

    stats = {
        "queries": len(queries),
        "raw": raw_count,
        "duplicate_removed": len(url_removed) + len(title_removed),
        "collected": len(unique_items),
    }

    return unique_items, stats


def run() -> dict[str, Any]:
    validate_search_settings()
    settings = get_news_settings()

    # 1. 검색 쿼리는 search_keywords 테이블에서 불러온다
    keywords = fetch_search_keywords()

    if not keywords:
        print("search_keywords 테이블에 검색 쿼리가 없습니다.")
        return {
            "collect": {"queries": 0, "raw": 0, "duplicate_removed": 0, "collected": 0},
            "type_filtered": 0,
            "existing": 0,
            "cluster": {
                "collected": 0,
                "cluster_count": 0,
                "selected": 0,
                "dropped_by_cluster_cap": 0,
            },
            "saved": {},
            "clustered": 0,
        }

    # 2. 수집 + 배치 내 중복 제거
    collected, collect_stats = collect_search_news([keyword["keyword"] for keyword in keywords])

    # 3. 기사 유형 필터 (포토/표/오피니언·기획성 기사 제외)
    typed_items, type_removed = filter_official_source_news(
        collected, pipeline_input={}, official_source_threshold=settings.official_source_threshold
    )

    # 4. DB에 이미 있는 기사 제외
    new_items, existing_items = filter_new_news_by_db(typed_items)

    # 5. 클러스터링 + 클러스터당 최대 N개 선별 (그룹 첫 번째가 대표)
    groups, cluster_stats = select_articles_by_cluster(
        new_items,
        threshold=settings.cluster_threshold,
        description_weight=settings.cluster_description_weight,
        max_per_cluster=settings.cluster_max_articles,
    )
    selected = [item for group in groups for item in group]

    # 6. 선별된 기사만 본문 크롤링
    enriched = enrich_items_with_article_body(selected)
    storable = [item for item in enriched if has_article_body(item)]

    # 7. 저장 (relation_extracted=NULL이 미추출 상태). save_news_items가 각 item에 _news_id를 채운다
    save_result = save_news_items(items=storable, save_summary=False, skip_existing=True)

    # 8. 클러스터 대표 기록: 본문 실패로 저장 안 된 기사는 제외되고, 그룹에서
    #    가장 먼저 저장에 성공한 기사(메도이드 우선순)가 대표가 된다
    saved_groups = [
        saved_ids
        for group in groups
        if (saved_ids := [item["_news_id"] for item in group if item.get("_news_id")])
    ]
    clustered_count = assign_cluster_representatives(saved_groups)

    # 9. 검색 완료 마킹
    searched_count = mark_keywords_searched([keyword["id"] for keyword in keywords])

    print("\n" + "=" * 70)
    print("뉴스 수집·클러스터링 결과")
    print("=" * 70)
    print(
        f"- 쿼리 {collect_stats['queries']}개 검색 "
        f"(raw {collect_stats['raw']}, 중복제거 {collect_stats['duplicate_removed']}, "
        f"수집 {collect_stats['collected']})"
    )
    print(f"- 유형필터 제거 {len(type_removed)}개, DB기존 {len(existing_items)}개")
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
        "type_filtered": len(type_removed),
        "existing": len(existing_items),
        "cluster": cluster_stats,
        "saved": save_result,
        "clustered": clustered_count,
    }


if __name__ == "__main__":
    run()
