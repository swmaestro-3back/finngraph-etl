from __future__ import annotations

from pipelines.common.logging import get_logger
from pipelines.news.repositories.postgres.news_clusters import refresh_event_leads

logger = get_logger(__name__)


def run() -> int:
    rows = refresh_event_leads()
    logger.info("같은 사건 대표 이슈 갱신 완료: %d개 클러스터 변경", rows)
    return rows
