"""투자자 수급 일일 수집.

네이버 trend API를 pageSize=1로 불러 최신 거래일 한 건만 덧붙인다. 과거 구간은
backfill_investor_flows 가 채운다.

외국인 보유율은 develop 방식 그대로 KIS 현재가 조회 스냅샷으로 최신 행을 덮어쓴다.
KIS 쪽은 보유주식수를 상장주식수로 직접 나눠 만든 값이라 원천이 더 명확하다. 조회가
실패하면 네이버 응답에 실려 온 보유율이 그대로 남는다. 과거 행의 보유율은 백필이
네이버 값으로 채운다.
"""

from __future__ import annotations

from dataclasses import replace

from pipelines.common.clients.kis import get_kis_client
from pipelines.common.clients.postgres import session_scope
from pipelines.common.logging import get_logger
from pipelines.common.utils.batching import chunked
from pipelines.companies.loaders.diagnostics import describe_universe
from pipelines.stocks.extractors.kis import fetch_foreign_holding
from pipelines.stocks.extractors.naver import BlockSuspectedError, fetch_investor_trends
from pipelines.stocks.loaders.flows import upsert_investor_flows
from pipelines.stocks.loaders.tickers import fetch_serviceable_stocks
from pipelines.stocks.models import ForeignHolding, InvestorFlow

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

    client = get_kis_client()
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
                symbol_flows = fetch_investor_trends(ticker, page_size=DAILY_PAGE_SIZE)
            except BlockSuspectedError:
                # 차단 의심은 다음 종목으로 넘어가지 않는다. task 재시도(retries)가
                # 시차를 두고 다시 시도한다.
                raise
            except Exception:
                logger.exception("수급 수집 실패: ticker=%s", ticker)
                failed.append(ticker)
                continue

            if symbol_flows:
                flows.extend(_with_foreign_holding(ticker, symbol_flows, client))

        if flows:
            with session_scope() as session:
                total_rows += upsert_investor_flows(session, flows)

    logger.info("투자자 수급 수집 완료: %d행, 실패 %d종목 %s", total_rows, len(failed), failed[:10])


def _with_foreign_holding(
    ticker: str,
    flows: list[InvestorFlow],
    client,
) -> list[InvestorFlow]:
    """외국인 보유 스냅샷을 최신 거래일 행에만 붙여 돌려준다.

    같은 값을 과거 행에 채우면 없는 이력을 지어내게 된다. 현재가 조회는 시점값 하나뿐이다.

    보유 조회가 실패해도 수급은 살린다 — 이쪽이 주 데이터이고, 네이버가 준 보유율이
    이미 행에 있어 스냅샷 없이도 비지 않는다.
    """

    try:
        holding = fetch_foreign_holding(ticker, client=client)
    except Exception:
        logger.exception("외국인 보유 조회 실패: ticker=%s", ticker)
        return flows

    if holding is None:
        return flows

    return _attach_holding(flows, holding)


def _attach_holding(flows: list[InvestorFlow], holding: ForeignHolding) -> list[InvestorFlow]:
    latest = max(flow.trade_date for flow in flows)
    return [
        replace(
            flow,
            foreign_hold_ratio=holding.ratio,
        )
        if flow.trade_date == latest
        else flow
        for flow in flows
    ]
