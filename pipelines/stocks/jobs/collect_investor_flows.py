"""투자자 수급 일일 수집.

네이버 trend API를 pageSize=1로 불러 최신 거래일 한 건만 덧붙인다. 과거 구간은
backfill_investor_flows 가 채운다. 외국인 보유율이 순매수와 같은 응답에 실려 와,
스냅샷을 따로 받아 최신 행에 붙이던 KIS 시절의 결합 로직이 없다.
"""

from __future__ import annotations

from pipelines.common.clients.postgres import session_scope
from pipelines.common.logging import get_logger
from pipelines.common.utils.batching import chunked
from pipelines.companies.loaders.diagnostics import describe_universe
from pipelines.stocks.extractors.naver import BlockSuspectedError, fetch_investor_trends
from pipelines.stocks.loaders.flows import upsert_investor_flows
from pipelines.stocks.loaders.tickers import fetch_serviceable_stocks

logger = get_logger(__name__)

CHUNK_SIZE = 100

# 일일 갱신은 최신 거래일 한 건이면 된다. 정정 소급은 백필로 처리한다.
DAILY_PAGE_SIZE = 1


def run(limit: int | None = None) -> None:
    """투자자 수급의 최신 거래일 행을 수집한다.

    대상을 자르지 않는다. 목록이 단축코드 순으로 고정이라 상한을 두면 뒤쪽 종목에 순서가
    영영 오지 않는다.

    Args:
        limit (int | None): 상한. 수동 점검용이고 배치에서는 쓰지 않는다.
    """

    with session_scope() as session:
        targets = fetch_serviceable_stocks(session, limit)
        if not targets:
            logger.warning("투자자 수급 수집 대상이 0종목이다 — %s", describe_universe(session))

    logger.info("투자자 수급 수집 시작: 대상 %d종목", len(targets))

    total_rows = 0
    failed: list[str] = []

    for chunk in chunked(targets, CHUNK_SIZE):
        flows = []
        for _, ticker in chunk:
            try:
                flows.extend(fetch_investor_trends(ticker, page_size=DAILY_PAGE_SIZE))
            except BlockSuspectedError:
                # 차단 의심은 다음 종목으로 넘어가지 않는다. task 재시도(retries)가
                # 시차를 두고 다시 시도한다.
                raise
            except Exception:
                logger.exception("수급 수집 실패: ticker=%s", ticker)
                failed.append(ticker)

        if flows:
            with session_scope() as session:
                total_rows += upsert_investor_flows(session, flows)

    logger.info("투자자 수급 수집 완료: %d행, 실패 %d종목 %s", total_rows, len(failed), failed[:10])
