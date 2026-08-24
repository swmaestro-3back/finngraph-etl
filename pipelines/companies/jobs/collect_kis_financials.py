"""KIS 상장사 재무 수집.

종목 하나에 API 6회가 든다(대차대조표·손익계산서·재무비율 × 연간·분기). 2,662종목을
매일 전부 돌면 16,000콜이라, 갱신이 가장 오래된 법인부터 배치 크기만큼만 처리하고
회차를 거듭하며 전체를 채운다. 재무는 분기에 한 번 바뀌므로 이 주기로 충분하다.
"""

from __future__ import annotations

from pipelines.common.clients.kis import get_kis_client
from pipelines.common.clients.postgres import session_scope
from pipelines.common.config import get_settings
from pipelines.common.logging import get_logger
from pipelines.common.utils.batching import chunked
from pipelines.companies.extractors.kis_finance import (
    fetch_balance_sheet,
    fetch_financial_ratio,
    fetch_income_statement,
)
from pipelines.companies.loaders.financials import fetch_stale_financial_targets, upsert_financials
from pipelines.companies.models import CompanyFinancial
from pipelines.companies.transformers.financials import (
    PERIOD_ANNUAL,
    PERIOD_QUARTER_CUMULATIVE,
    build_kis_financials,
    build_quarterly_deltas,
    infer_fiscal_month,
)

logger = get_logger(__name__)

CHUNK_SIZE = 50

# KIS API의 기간 구분. 저장하는 period_type과 다르다 — 분기 응답 하나로 QC(누적)와
# Q(개별) 두 벌을 만든다.
API_ANNUAL = "A"
API_QUARTER = "Q"


def run(limit: int | None = None) -> None:
    """재무 갱신이 오래된 법인부터 KIS 재무를 수집해 적재한다.

    Args:
        limit (int | None): 이번 회차 처리 법인 수. 생략하면 설정값을 쓴다.
    """

    settings = get_settings()
    batch_size = limit or settings.company_financial_batch_size

    client = get_kis_client()
    with session_scope() as session:
        targets = fetch_stale_financial_targets(session, batch_size, source="KIS")

    logger.info("KIS 재무 수집 시작: 대상 %d법인", len(targets))

    total_rows = 0
    failed: list[str] = []

    for chunk in chunked(targets, CHUNK_SIZE):
        financials: list[CompanyFinancial] = []
        for company_id, ticker in chunk:
            try:
                financials.extend(_collect_symbol(company_id, ticker, client))
            except Exception:
                # 상장폐지 직전 종목·신규 상장 종목은 재무가 아예 없어 실패하기도 한다.
                # 배치 전체를 죽이지 않고 종목 단위로 넘어간다.
                logger.exception("재무 수집 실패: ticker=%s", ticker)
                failed.append(ticker)

        if financials:
            with session_scope() as session:
                total_rows += upsert_financials(session, financials)

    logger.info(
        "KIS 재무 수집 완료: %d행 적재, 실패 %d종목 %s", total_rows, len(failed), failed[:10]
    )


def _collect_symbol(company_id: int, ticker: str, client) -> list[CompanyFinancial]:
    """한 종목의 연간·분기 누적·분기 개별 재무를 만든다.

    분기 개별은 API로 받는 게 아니라 **누적에서 차분해 만든다.** KIS는 누적만 주기 때문이다.
    호출 수는 늘지 않는다.

    결산월은 연간 시리즈에서 뽑는다. 분기 시리즈로 추론하면 12월 결산 법인이 3월 결산으로
    오판된다(infer_fiscal_month 참고).
    """

    annual = build_kis_financials(
        company_id=company_id,
        period_type=PERIOD_ANNUAL,
        balance_rows=fetch_balance_sheet(ticker, API_ANNUAL, client=client),
        income_rows=fetch_income_statement(ticker, API_ANNUAL, client=client),
        ratio_rows=fetch_financial_ratio(ticker, API_ANNUAL, client=client),
    )
    cumulative = build_kis_financials(
        company_id=company_id,
        period_type=PERIOD_QUARTER_CUMULATIVE,
        balance_rows=fetch_balance_sheet(ticker, API_QUARTER, client=client),
        income_rows=fetch_income_statement(ticker, API_QUARTER, client=client),
        ratio_rows=fetch_financial_ratio(ticker, API_QUARTER, client=client),
    )
    quarterly = build_quarterly_deltas(cumulative, infer_fiscal_month(annual))

    return [*annual, *cumulative, *quarterly]
