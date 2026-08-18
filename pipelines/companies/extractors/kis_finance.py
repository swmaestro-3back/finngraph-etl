"""KIS 재무제표·재무비율 수집.

국내 상장사 재무의 원천을 DART가 아니라 KIS로 정한 이유는 소급 길이다. 삼성전자 기준
연간 23개(2004~)·분기 30개를 주는데 DART 공시 API로는 2015년 이후만 닿는다. 재무비율에
ROE·EPS·BPS가 분기별로 들어 있어 직접 계산할 필요도 없다.

대신 두 가지 한계가 있다.
- **연결 기준만 준다.** 별도(OFS) 재무는 DART가 필요하다.
- **공시 시점을 주지 않는다.** point-in-time이 필요한 센티멘트 분석에는 쓸 수 없다.
"""

from __future__ import annotations

from typing import Any

from pipelines.common.kis import KisClient, get_kis_client

BALANCE_SHEET_PATH = "/uapi/domestic-stock/v1/finance/balance-sheet"
BALANCE_SHEET_TR_ID = "FHKST66430100"

INCOME_STATEMENT_PATH = "/uapi/domestic-stock/v1/finance/income-statement"
INCOME_STATEMENT_TR_ID = "FHKST66430200"

FINANCIAL_RATIO_PATH = "/uapi/domestic-stock/v1/finance/financial-ratio"
FINANCIAL_RATIO_TR_ID = "FHKST66430300"

# FID_DIV_CLS_CODE — 0=연간, 1=분기.
DIV_CLS_ANNUAL = "0"
DIV_CLS_QUARTER = "1"

PERIOD_TYPE_TO_DIV_CLS = {"A": DIV_CLS_ANNUAL, "Q": DIV_CLS_QUARTER}


def fetch_balance_sheet(
    ticker: str,
    period_type: str,
    client: KisClient | None = None,
) -> list[dict[str, Any]]:
    """대차대조표 원본 행. 최신 기간이 먼저 온다."""

    return _fetch(BALANCE_SHEET_PATH, BALANCE_SHEET_TR_ID, ticker, period_type, client)


def fetch_income_statement(
    ticker: str,
    period_type: str,
    client: KisClient | None = None,
) -> list[dict[str, Any]]:
    """손익계산서 원본 행. 분기 값은 연초부터의 누적이다."""

    return _fetch(INCOME_STATEMENT_PATH, INCOME_STATEMENT_TR_ID, ticker, period_type, client)


def fetch_financial_ratio(
    ticker: str,
    period_type: str,
    client: KisClient | None = None,
) -> list[dict[str, Any]]:
    """재무비율 원본 행. ROE·EPS·BPS가 여기 있다."""

    return _fetch(FINANCIAL_RATIO_PATH, FINANCIAL_RATIO_TR_ID, ticker, period_type, client)


def _fetch(
    path: str,
    tr_id: str,
    ticker: str,
    period_type: str,
    client: KisClient | None,
) -> list[dict[str, Any]]:
    div_cls = PERIOD_TYPE_TO_DIV_CLS.get(period_type)
    if div_cls is None:
        raise ValueError(f"period_type은 'A' 또는 'Q'여야 한다: {period_type!r}")

    client = client or get_kis_client()
    data = client.request(
        path,
        tr_id,
        {
            "FID_DIV_CLS_CODE": div_cls,
            "fid_cond_mrkt_div_code": "J",
            "fid_input_iscd": ticker,
        },
    )
    return list(data.get("output") or [])
