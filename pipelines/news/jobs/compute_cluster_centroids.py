"""task: compute_cluster_centroids — news_backfill_cluster_centroids DAG.

기존 클러스터 배정(news.cluster_id)을 그대로 두고, 멤버 기사의 term_weights 에서 news_clusters 의
centroids·mean_published_at·member_count 를 다시 유도한다. V14 직후 옛 클러스터는 centroids 가 비어
있어 새 기사가 편입되지 못하므로, 벡터화·IDF 재구축 뒤 한 번 채우면 다음 판정 런부터 옛 클러스터와도
정상적으로 비교된다. Event·트리플·요약·제목·이슈 연결은 보존되고 LLM 을 부르지 않는다.
"""

from __future__ import annotations

from pipelines.common.logging import get_logger
from pipelines.news.repositories.postgres.news_clusters import refill_cluster_centroids

logger = get_logger(__name__)


def run() -> dict[str, int]:
    clusters = refill_cluster_centroids()
    logger.info("[compute_cluster_centroids] 클러스터 %d개의 centroids·시각·멤버 수 갱신", clusters)
    return {"clusters": clusters}
