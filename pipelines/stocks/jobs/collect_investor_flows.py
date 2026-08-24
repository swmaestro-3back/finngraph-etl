"""투자자 수급 수집.

KIS `investor-trade-by-stock-daily`가 기준일에서 직전 30영업일을 한 번에 준다. 백필은
기준일을 30영업일씩 뒤로 옮기며 반복한다. `inquire-investor`는 3분류뿐이라 쓰지 않는다.

외국인 보유율은 원천이 달라 현재가 조회를 종목당 1회 더 부른다. 현재 시점 스냅샷뿐이라
최신 거래일 행에만 붙는다.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import date, timedelta

from sqlalchemy import text

from pipelines.common.clients.kis import get_kis_client
from pipelines.common.clients.postgres import session_scope
from pipelines.common.logging import get_logger
from pipelines.common.utils.batching import chunked
from pipelines.common.utils.time import now_kst
from pipelines.stocks.extractors.kis import fetch_foreign_holding, fetch_investor_flows
from pipelines.stocks.loaders.flows import upsert_investor_flows
from pipelines.stocks.loaders.tickers import fetch_serviceable_stocks
from pipelines.stocks.models import ForeignHolding, InvestorFlow

logger = get_logger(__name__)

CHUNK_SIZE = 100

# API 1회가 주는 영업일 수. 백필 커서를 이만큼씩 뒤로 옮긴다.
BUSINESS_DAYS_PER_CALL = 30

# 30영업일 ≈ 42일(주말·공휴일 포함). 커서를 달력일로 옮기므로 넉넉히 잡아 구멍을 막는다.
CALENDAR_DAYS_PER_CALL = 40

# 기준일 결정용 — 이미 적재된 일봉의 마지막 거래일.
# 기준일에 당일을 넣으면 KIS가 `OPSQ2001 TIME LIMIT`으로 거절한다.
SELECT_LAST_TRADE_DATE_SQL = text("SELECT MAX(trade_date) AS last_date FROM daily_candles")


def run(limit: int | None = None, pages: int = 1) -> None:
    """투자자 수급을 수집한다.

    대상을 자르지 않는다. 목록이 단축코드 순으로 고정이라 상한을 두면 뒤쪽 종목에 순서가
    영영 오지 않는다. 전량을 돌아도 종목당 2회(수급·보유율)라 700여 회다.

    Args:
        limit (int | None): 상한. 수동 점검용이고 배치에서는 쓰지 않는다.
        pages (int): 종목당 호출 횟수. 1이면 최근 30영업일, 2 이상이면 그만큼 과거로
            거슬러 올라간다(백필).
    """

    client = get_kis_client()
    with session_scope() as session:
        targets = fetch_serviceable_stocks(session, limit)
        base_date = _resolve_base_date(session)

    logger.info(
        "투자자 수급 수집 시작: 대상 %d종목 × %d페이지, 기준일 %s",
        len(targets),
        pages,
        base_date,
    )

    total_rows = 0
    failed: list[str] = []

    for chunk in chunked(targets, CHUNK_SIZE):
        flows = []
        for _, ticker in chunk:
            symbol_flows: list[InvestorFlow] = []
            for page in range(pages):
                cursor = base_date - timedelta(days=CALENDAR_DAYS_PER_CALL * page)
                try:
                    symbol_flows.extend(fetch_investor_flows(ticker, cursor, client=client))
                except Exception:
                    logger.exception("수급 수집 실패: ticker=%s base_date=%s", ticker, cursor)
                    failed.append(ticker)
                    break

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

    보유 조회가 실패해도 수급은 살린다 — 이쪽이 주 데이터이고, 보유는 다음 회차에 다시
    시도하면 된다.
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
            foreign_ratio=holding.ratio,
        )
        if flow.trade_date == latest
        else flow
        for flow in flows
    ]


def _resolve_base_date(session) -> date:
    """조회 기준일. 적재된 마지막 거래일을 쓰되 절대 당일을 넘기지 않는다.

    당일을 넣으면 KIS가 거절하고(OPSQ2001), 미래 날짜도 마찬가지다. 일봉이 아직 없는
    초기 상태에서는 전일로 떨어진다.
    """

    yesterday = now_kst().date() - timedelta(days=1)
    last_date = session.execute(SELECT_LAST_TRADE_DATE_SQL).scalar()
    if last_date is None:
        return yesterday
    return min(last_date, yesterday)


def backfill(start: date, limit: int | None = None) -> None:
    """start까지 거슬러 올라가며 수급을 채운다.

    Args:
        start (date): 이 날짜까지 백필한다.
        limit (int | None): 처리 종목 수 상한.
    """

    days = max((now_kst().date() - start).days, 0)
    pages = days // CALENDAR_DAYS_PER_CALL + 1
    logger.info("수급 백필: %s까지 종목당 %d회 호출", start, pages)
    run(limit=limit, pages=pages)
