from __future__ import annotations

from datetime import datetime

from pipelines.common.logging import get_logger
from pipelines.news.config import get_news_settings
from pipelines.news.repositories.search_history import fetch_due_krx300_queries
from pipelines.news.utils.date_utils import SEOUL_TIMEZONE

logger = get_logger(__name__)


def run(chunk_size: int) -> list[list[int]]:
    """재검색 시점이 된 KRX300 기업을 chunk_size 개씩 나눈 company_id 청크 목록"""

    batch = fetch_due_krx300_queries(
        get_news_settings().search_interval_hours, datetime.now(SEOUL_TIMEZONE)
    )
    company_ids = [query.company_id for query in batch.queries]
    chunks = [
        company_ids[start : start + chunk_size] for start in range(0, len(company_ids), chunk_size)
    ]

    logger.info(
        "[대상] KRX300 검색 기업 %d개 → 청크 %d개 (간격 미도래 %d, company_id 없음 %d, 첫 검색 %d)",
        len(company_ids),
        len(chunks),
        batch.skipped_not_due,
        batch.skipped_no_company,
        sum(1 for q in batch.queries if q.watermark is None),
    )

    return chunks
