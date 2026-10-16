"""task: rebuild_idf — news_rebuild_idf DAG(매일)와 news_backfill_cluster_articles DAG(판정 앞).

클러스터 판정용 IDF 사전(news_cluster_idf_terms·news_cluster_idf_meta)을 최근 NEWS_CLUSTER_IDF_DAYS
일의 벡터화된 뉴스 전체로 다시 집계해 통째로 교체한다. 한 트랜잭션이라 실패하면 이전 표가 남는다.
판정 런은 이 표를 시작 때 읽어 런 내내 고정한다 — 그날 새 기사는 다음 재구축까지 표에 없고 미등록
토큰은 최대 IDF 를 받는다(고정 사전 동작).
"""

from __future__ import annotations

from datetime import datetime, timedelta

from pipelines.common.logging import get_logger
from pipelines.news.config import get_news_settings
from pipelines.news.repositories.postgres.news_cluster_idf import rebuild_idf_tables
from pipelines.news.utils.date_utils import SEOUL_TIMEZONE

logger = get_logger(__name__)


def run() -> dict[str, int]:
    settings = get_news_settings()
    since = datetime.now(SEOUL_TIMEZONE) - timedelta(days=settings.cluster_idf_days)
    result = rebuild_idf_tables(since)
    logger.info(
        "[rebuild_idf] %s 이후 문서 %d건 → 토큰 행 %d",
        since.date(),
        result["documents"],
        result["terms"],
    )
    return result
