from __future__ import annotations

from pipelines.common.logging import get_logger
from pipelines.news.repositories.postgres.news_clusters import refresh_cluster_visibility

logger = get_logger(__name__)


def run() -> int:
    rows = refresh_cluster_visibility()
    logger.info("이슈 노출 여부 갱신 완료: %d개 클러스터 변경", rows)
    return rows
