"""task: assign_clusters — news_cluster_articles·news_backfill_cluster_articles DAG.

벡터화됐지만 cluster_id 없는 기사를 발행 시각순으로 기존 클러스터에 편입하거나 새로 만든다.

이번 런의 기사만이 아니라 미판정 기사 전부가 대상이다. 클러스터 하나가 트랜잭션 하나라 기록에 실패한
클러스터의 기사는 cluster_id 없이 남아 다음 런에 다시 판정된다. IDF 는 news_cluster_idf_terms 를
런 시작에 한 번 읽어 고정한다. 후보는 가장 최신 멤버가 배치 최초 발행 − NEWS_CLUSTER_CANDIDATE_DAYS
이후이고 첫 멤버가 배치 최초 발행 − NEWS_CLUSTER_MAX_SPAN_DAYS 이후인 클러스터다.

두 DAG 는 각자 max_active_runs=1 이지만 서로는 막지 않으므로, 백필을 돌리는 동안에는
news_cluster_articles 를 꺼 둔다 — 겹쳐 돌면 같은 사건에 클러스터가 둘 생긴다.
"""

from __future__ import annotations

from pipelines.common.logging import get_logger
from pipelines.news.config import get_news_settings
from pipelines.news.repositories.postgres.news import count_unclustered_news, fetch_unclustered_news
from pipelines.news.repositories.postgres.news_cluster_idf import load_idf_tables
from pipelines.news.repositories.postgres.news_clusters import (
    fetch_cluster_seeds,
    record_assignments,
)
from pipelines.news.transformers.clustering.batch import candidate_since, row_documents
from pipelines.news.transformers.clustering.online import assign_online

logger = get_logger(__name__)


def count_unclustered() -> int:
    """판정을 기다리는 기사 수. 백필 DAG 가 수집을 다 끝낸 뒤 하류를 깨울지 정할 때 쓴다."""

    return count_unclustered_news()


def run() -> dict[str, int]:
    settings = get_news_settings()

    rows = fetch_unclustered_news()
    if not rows:
        logger.info("[assign_clusters] 판정 대상 없음")
        return {"articles": 0, "created": 0, "updated": 0, "failed": 0}

    documents, published_ats = row_documents(rows)
    seeds = fetch_cluster_seeds(
        candidate_since(published_ats, settings.cluster_candidate_days),
        candidate_since(published_ats, settings.cluster_max_span_days),
    )
    idf_tables = load_idf_tables()

    assignments = assign_online(
        documents,
        published_ats,
        seeds,
        idf_tables,
        text_weights=settings.cluster_text_weights,
        time_weights=settings.cluster_time_weights,
        sigma_hours=settings.cluster_time_sigma_hours,
        threshold=settings.cluster_threshold,
        candidate_days=settings.cluster_candidate_days,
        max_span_days=settings.cluster_max_span_days,
    )
    result = record_assignments(assignments, rows, settings.cluster_keyword_count)

    logger.info(
        "[assign_clusters] 기사 %d건 → 후보 %d개 중 편입 %d / 신규 %d / 기록 실패 %d",
        len(rows),
        len(seeds),
        result["updated"],
        result["created"],
        result["failed"],
    )
    return {"articles": len(rows), **result}
