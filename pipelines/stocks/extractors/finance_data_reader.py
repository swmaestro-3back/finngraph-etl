"""FinanceDataReader 기반 과거 일봉 수집.

과거 일봉만 FDR로 받고 당일 이후는 KIS로 붙인다(README의 주식 파이프라인 정책).
KIS 기간별시세도 과거를 주지만 종목당 100건씩 끊겨 10년치를 받으려면 종목당 25회를
불러야 한다. 2,600종목이면 6.5만 콜이다. FDR은 종목당 1회로 전 구간을 준다.
"""

from __future__ import annotations

import math
from datetime import date
from decimal import Decimal

from pipelines.common.logging import get_logger
from pipelines.common.retry import retry_external_call
from pipelines.common.types import DailyCandle
from pipelines.stocks.models import StockTicker

logger = get_logger(__name__)

# FDR이 돌려주는 컬럼 이름. 대소문자와 표기가 버전에 따라 흔들려 소문자로 맞춰 찾는다.
_COLUMN_ALIASES = {
    "open": "open",
    "high": "high",
    "low": "low",
    "close": "close",
    "volume": "volume",
}


def fetch_tickers() -> list[StockTicker]:
    """FDR 종목 목록.

    종목 마스터의 원천은 KIS master다. FDR 목록은 상장폐지·관리종목 이력 보조용이며,
    마스터 대신 쓰면 원천이 둘로 갈린다.
    """

    raise NotImplementedError(
        "종목 목록의 원천은 KIS master다. pipelines.stocks.extractors.kis_stock_master를 쓴다."
    )


@retry_external_call()
def fetch_daily_candles(ticker: str, start: date, end: date) -> list[DailyCandle]:
    """[start, end] 구간의 일봉을 가져온다.

    Args:
        ticker (str): 단축코드.
        start (date): 시작일(포함).
        end (date): 종료일(포함).

    Returns:
        list[DailyCandle]: 거래일 오름차순 일봉. 데이터가 없으면 빈 리스트.
    """

    import FinanceDataReader as fdr

    frame = fdr.DataReader(ticker, start.isoformat(), end.isoformat())
    if frame is None or frame.empty:
        return []

    columns = {str(name).strip().lower(): name for name in frame.columns}
    missing = [key for key in _COLUMN_ALIASES if key not in columns]
    if missing:
        raise ValueError(f"FDR 응답에 필요한 컬럼이 없다: ticker={ticker} missing={missing}")

    candles: list[DailyCandle] = []
    for index, row in frame.iterrows():
        values = {key: row[columns[key]] for key in _COLUMN_ALIASES}
        if any(_is_missing(value) for value in values.values()):
            # 상장 이전 구간이나 거래정지일에 NaN 행이 섞여 나온다. 0으로 채우면
            # 수익률·밸류에이션 계산이 조용히 틀리므로 버린다.
            continue

        candles.append(
            DailyCandle(
                ticker=ticker,
                trade_date=index.date() if hasattr(index, "date") else index,
                open=Decimal(str(values["open"])),
                high=Decimal(str(values["high"])),
                low=Decimal(str(values["low"])),
                close=Decimal(str(values["close"])),
                volume=int(values["volume"]),
            )
        )

    return candles


def _is_missing(value: object) -> bool:
    if value is None:
        return True
    try:
        return math.isnan(float(value))
    except (TypeError, ValueError):
        return True
