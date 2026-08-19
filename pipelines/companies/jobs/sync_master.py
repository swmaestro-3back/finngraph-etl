from __future__ import annotations

from pipelines.common.clients.postgres import session_scope
from pipelines.common.logging import get_logger
from pipelines.companies.loaders.postgres import sync_listed_companies

logger = get_logger(__name__)


def run() -> None:
    """국내 상장 법인을 companies에 적재하고 stocks와 연결한다.

    stocks를 읽어 쓰므로 종목 마스터 동기화(stocks_sync_master)가 끝난 뒤에 돌아야 한다.
    """

    with session_scope() as session:
        result = sync_listed_companies(session=session)

    logger.info(
        "기업 마스터 동기화 완료: upsert %d건, 종목 연결 %d건, 별칭 %d건",
        result.upserted_count,
        result.linked_count,
        result.alias_count,
    )
