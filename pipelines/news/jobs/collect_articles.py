"""뉴스 수집 + 클러스터링 job (news_scheduled_pipeline DAG의 collect_articles task).

수집 → 중복 제거 → 기사 유형 필터 → 제목 선두 태그 제거 → DB 기존 기사 제외 →
배치 간 클러스터 판정(윈도우 안 기존 클러스터 합류 또는 새 클러스터, cap 초과 버림) →
본문 크롤링 → news INSERT → news_clusters 기록 순서로 진행한다.
클러스터링을 본문 수집보다 앞에 두어, 버려질 기사의 본문은 크롤링하지 않는다.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any

from pipelines.news.config import get_news_settings
from pipelines.news.extractors.search_collector import (
    iter_search_news_pages,
    validate_search_settings,
)
from pipelines.news.extractors.text_fetcher import enrich_items_with_article_body
from pipelines.news.loaders.clusters import (
    create_news_cluster,
    fetch_active_cluster_seeds,
    fetch_cluster_member_terms,
    update_news_cluster,
)
from pipelines.news.loaders.postgres import (
    fetch_search_keywords,
    filter_new_news_by_db,
    has_article_body,
    mark_keywords_searched,
    parse_anchor_pub_date,
    save_news_items,
)
from pipelines.news.transformers.clustering import (
    ClusterAssignment,
    Terms,
    assign_batch,
    document_terms,
    merge_term_weights,
    pick_representative,
    sum_terms,
    top_keywords,
)
from pipelines.news.transformers.duplicate_filter import (
    remove_duplicate_by_title,
    remove_duplicate_by_url,
)
from pipelines.news.transformers.news_type_filter import filter_official_source_news
from pipelines.news.utils.date_utils import SEOUL_TIMEZONE
from pipelines.news.utils.text_utils import remove_leading_title_brackets


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


def batch_documents(
    items: list[dict[str, Any]],
    description_weight: float,
    fallback_time: datetime,
) -> tuple[list[Terms], list[datetime]]:
    """기사 목록을 클러스터링 문서와 보도 시각으로 바꾼다.

    보도 시각을 모르는 기사는 fallback_time(실행 시각)으로 본다 — 윈도우와 cap 날짜 판정에
    쓰는 값이라 비워 둘 수 없다. news.published_at 저장값은 저장 시 따로 파싱해 NULL 을
    허용한다.
    """

    documents = [
        document_terms(item.get("title", ""), item.get("description", ""), description_weight)
        for item in items
    ]
    published_ats = [
        parse_anchor_pub_date(item.get("pubDate", "")) or fallback_time for item in items
    ]
    return documents, published_ats


def cluster_stats(
    assignments: list[ClusterAssignment], seed_count: int, collected: int
) -> dict[str, int]:
    joined = sum(1 for assignment in assignments if assignment.seed is not None)

    return {
        "collected": collected,
        "seeds": seed_count,
        "joined": joined,
        "new_clusters": len(assignments) - joined,
        "selected": sum(len(assignment.kept) for assignment in assignments),
        "dropped_by_cluster_cap": sum(len(assignment.dropped) for assignment in assignments),
    }


def _record_seed_update(
    assignment: ClusterAssignment,
    members: list[tuple[int, dict[str, float]]],
    survivor_terms: list[Terms],
    keyword_count: int,
) -> None:
    """시드 클러스터에 이번 배치의 판정을 더한다. 저장된 멤버가 있으면 대표를 다시 고른다."""

    seed = assignment.seed
    assert seed is not None
    representative_news_id: int | None = None
    cohesion: float | None = None

    if members:
        # 저장된 멤버 전체(기존 + 이번)로 메도이드를 다시 고른다.
        existing = fetch_cluster_member_terms(seed.cluster_id)
        pool_ids = [news_id for news_id, _ in existing] + [news_id for news_id, _ in members]
        pool_terms = [list(terms.items()) for _, terms in existing] + survivor_terms
        representative_index, cohesion = pick_representative(pool_terms)
        representative_news_id = pool_ids[representative_index]

    merged = merge_term_weights(seed.term_weights, assignment.term_weights)
    update_news_cluster(
        seed.cluster_id,
        term_weights=merged,
        keywords=top_keywords(merged, keyword_count),
        original_size_delta=len(assignment.members),
        first_published_at=assignment.first_published_at,
        last_published_at=assignment.last_published_at,
        representative_news_id=representative_news_id,
        cohesion=cohesion,
        members=members,
    )


def record_cluster_assignments(
    assignments: list[ClusterAssignment],
    items: list[dict[str, Any]],
    documents: list[Terms],
    keyword_count: int,
) -> dict[str, int]:
    """저장이 끝난 뒤 판정 결과를 news_clusters 와 news.cluster_id 에 기록한다.

    저장에 성공한 기사(_news_id 가 있고 기존 행 스킵이 아닌 것)만 멤버가 된다. 새 클러스터는
    저장된 기사가 하나도 없으면 만들지 않는다. 시드 클러스터는 저장된 기사가 없어도 프로필과
    판정 수, 시간 범위는 갱신한다 — 버린 기사도 같은 사건이라는 판정 자체는 유효하다.

    클러스터 하나가 트랜잭션 하나다. 실패한 클러스터는 건너뛰고 세어 돌려주며, 그 기사들은
    news 에 cluster_id 없이 남는다(news 저장은 이미 커밋됐다).
    """

    created = 0
    updated = 0
    failed = 0

    for assignment in assignments:
        survivors = [
            index
            for index in assignment.kept
            if items[index].get("_news_id")
            and items[index].get("_save_action") != "skipped_existing"
        ]
        members = [
            (int(items[index]["_news_id"]), sum_terms(documents[index])) for index in survivors
        ]

        if assignment.seed is None and not members:
            continue

        try:
            if assignment.seed is None:
                # kept 는 메도이드가 첫 번째이므로, 저장에 성공한 첫 기사가 대표다.
                create_news_cluster(
                    representative_news_id=members[0][0],
                    cohesion=assignment.cohesion,
                    term_weights=assignment.term_weights,
                    keywords=top_keywords(assignment.term_weights, keyword_count),
                    original_size=len(assignment.members),
                    first_published_at=assignment.first_published_at,
                    last_published_at=assignment.last_published_at,
                    members=members,
                )
                created += 1
            else:
                _record_seed_update(
                    assignment, members, [documents[index] for index in survivors], keyword_count
                )
                updated += 1
        except Exception as e:
            failed += 1
            logging.error(
                "클러스터 기록 실패(건너뜀): "
                f"cluster_id={assignment.seed.cluster_id if assignment.seed else None}, "
                f"news_ids={[news_id for news_id, _ in members]}, error={type(e).__name__}: {e}"
            )

    return {"created": created, "updated": updated, "failed": failed}


def run() -> dict[str, Any]:
    validate_search_settings()
    settings = get_news_settings()
    run_started_at = datetime.now(SEOUL_TIMEZONE)

    # 1. 검색 쿼리는 search_keywords 테이블에서 불러온다
    keywords = fetch_search_keywords()

    if not keywords:
        print("search_keywords 테이블에 검색 쿼리가 없습니다.")
        return {
            "collect": {"queries": 0, "raw": 0, "duplicate_removed": 0, "collected": 0},
            "type_filtered": 0,
            "existing": 0,
            "cluster": cluster_stats([], 0, 0),
            "saved": {},
            "clusters": {"created": 0, "updated": 0, "failed": 0},
        }

    # 2. 수집 + 배치 내 중복 제거
    collected, collect_stats = collect_search_news([keyword["keyword"] for keyword in keywords])

    # 3. 기사 유형 필터 (포토/표/오피니언·기획성 기사 제외)
    typed_items, type_removed = filter_official_source_news(
        collected, pipeline_input={}, official_source_threshold=settings.official_source_threshold
    )

    # 4. 제목 선두 "[속보]" 같은 브라켓 태그 제거 — 유형 필터가 태그를 봐야 하므로 그 뒤,
    #    제목 기반 DB 중복 비교·클러스터링·저장이 같은 제목을 쓰도록 그 앞에 둔다
    for item in typed_items:
        item["title"] = remove_leading_title_brackets(item.get("title", ""))

    # 5. DB에 이미 있는 기사 제외
    new_items, existing_items = filter_new_news_by_db(typed_items)

    # 6. 배치 간 클러스터 판정: 윈도우 안 기존 클러스터를 시드로 읽어 합류/신규/버림을 정한다
    documents, published_ats = batch_documents(
        new_items, settings.cluster_description_weight, run_started_at
    )
    seeds = []
    if new_items:
        window_start = min(published_ats) - timedelta(days=settings.cluster_window_days)
        seeds = fetch_active_cluster_seeds(window_start)
    assignments = assign_batch(
        documents,
        published_ats,
        seeds,
        threshold=settings.cluster_threshold,
        cap=settings.cluster_max_articles,
        decay_half_life_days=settings.cluster_decay_half_life_days,
    )
    selected = [new_items[index] for assignment in assignments for index in assignment.kept]
    stats = cluster_stats(assignments, len(seeds), len(new_items))

    # 7. 선별된 기사만 본문 크롤링
    enriched = enrich_items_with_article_body(selected)
    storable = [item for item in enriched if has_article_body(item)]

    # 8. 저장 (triple_extracted는 NULL=미시도로 남는다). save_news_items가 _news_id를 채운다
    save_result = save_news_items(items=storable, save_summary=False, skip_existing=True)

    # 9. 클러스터 기록: 본문 실패로 저장 안 된 기사는 멤버에서 빠진다
    cluster_result = record_cluster_assignments(
        assignments, new_items, documents, settings.cluster_keyword_count
    )

    # 10. 검색 완료 마킹
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
        f"- 윈도우 클러스터 {stats['seeds']}개 중 합류 {stats['joined']}개, "
        f"신규 군집 {stats['new_clusters']}개 → 선별 {stats['selected']}개 "
        f"(cap 제외 {stats['dropped_by_cluster_cap']}개)"
    )
    print(f"- 본문 성공 {len(storable)} / 실패 {len(enriched) - len(storable)}")
    print(
        f"- 저장 결과 {save_result}, 클러스터 생성 {cluster_result['created']}건 / "
        f"갱신 {cluster_result['updated']}건 / 실패 {cluster_result['failed']}건"
    )
    print(f"- 키워드 마킹 {searched_count}개")
    print("=" * 70)

    return {
        "collect": collect_stats,
        "type_filtered": len(type_removed),
        "existing": len(existing_items),
        "cluster": stats,
        "saved": save_result,
        "clusters": cluster_result,
    }


if __name__ == "__main__":
    run()
