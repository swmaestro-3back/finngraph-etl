from __future__ import annotations

from pipelines.common.clients.postgres import session_scope
from pipelines.common.logging import get_logger
from pipelines.companies.loaders.postgres import sync_listed_companies

logger = get_logger(__name__)


def run() -> None:
    """국내 상장 법인의 상장 여부를 갱신하고 stocks와 연결한다.

    법인 행 자체는 만들지 않는다 — 그건 DART corpCode 동기화(companies_sync_dart_corp_codes)가
    한다. 여기서는 그 목록 위에서 지금 거래되는 것을 켜고 사라진 것을 내린다.

    stocks를 읽어 쓰므로 종목 마스터 동기화(stocks_sync_master)가 끝난 뒤에 돌아야 한다.
    """

    with session_scope() as session:
        result = sync_listed_companies(session=session)

    logger.info(
        "기업 마스터 동기화 완료: 상장 %d건, 폐지 %d건, 종목 연결 %d건, 별칭 %d건",
        result.listed_count,
        result.delisted_count,
        result.linked_count,
        result.alias_count,
    )
