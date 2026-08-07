from __future__ import annotations

from pipelines.common.database import session_scope
from pipelines.common.logging import get_logger
from pipelines.stocks.extractors.kis_stock_master import fetch_kospi_kosdaq_symbols
from pipelines.stocks.loaders.symbols import sync_symbols

logger = get_logger(__name__)


def run() -> None:
    symbols = fetch_kospi_kosdaq_symbols()
    logger.info("KIS master에서 %d개 종목 수집", len(symbols))

    with session_scope() as session:
        result = sync_symbols(session=session, symbols=symbols)

    logger.info(
        "종목 마스터 동기화 완료: upsert %d건, 비활성 처리 %d건",
        result.upserted_count,
        result.inactive_count,
    )
