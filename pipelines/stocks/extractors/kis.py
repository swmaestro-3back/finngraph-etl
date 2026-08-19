"""KIS 시세 수집.

인증·레이트리밋은 `pipelines.common.clients.kis.KisClient`가 담당하고, 여기서는 엔드포인트별
파라미터와 응답 파싱만 한다.

분봉(inquire-time-itemchartprice)은 이번 범위 밖이다. 장중 주기 실행이라 운영 성격이
다르고, 일봉·기간봉이 자리 잡은 뒤에 붙인다.

응답 파싱에서 공통으로 조심할 것 두 가지.

- KIS는 결측을 빈 문자열로 준다. `int("")`는 예외라 파싱 헬퍼로 감싼다.
- 기간 조회는 **최신순으로 최대 100건**만 준다. 더 긴 구간이 필요하면 가장 오래된
  날짜 직전으로 커서를 옮겨 다시 부른다.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any

from pipelines.common.clients.kis import KisClient, get_kis_client
from pipelines.common.logging import get_logger
from pipelines.stocks.types import DailyCandle, PeriodCandle

logger = get_logger(__name__)

CHART_PATH = "/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice"
CHART_TR_ID = "FHKST03010100"

# 기간 조회 1회 최대 건수. 이 수만큼 돌아오면 더 있을 수 있다는 신호다.
CHART_PAGE_SIZE = 100


def fetch_daily_candles(
    ticker: str,
    start: date,
    end: date,
    client: KisClient | None = None,
) -> list[DailyCandle]:
    """KIS 기간별시세로 일봉을 가져온다. 당일·최근 구간 갱신용이다."""

    rows = _fetch_chart_rows(ticker, "D", start, end, client)
    candles = [
        DailyCandle(
            ticker=ticker,
            trade_date=parsed_date,
            open=open_,
            high=high,
            low=low,
            close=close,
            volume=volume,
            trade_value=trade_value,
        )
        for parsed_date, open_, high, low, close, volume, trade_value in rows
    ]
    return sorted(candles, key=lambda candle: candle.trade_date)


def fetch_period_candles(
    ticker: str,
    period: str,
    start: date,
    end: date,
    client: KisClient | None = None,
) -> list[PeriodCandle]:
    """주봉·월봉을 가져온다.

    Args:
        ticker (str): 단축코드.
        period (str): 'W'(주) 또는 'M'(월).
        start (date): 시작일(포함).
        end (date): 종료일(포함).
        client (KisClient | None): 재사용할 클라이언트.

    Returns:
        list[PeriodCandle]: base_date 오름차순.
    """

    if period not in ("W", "M"):
        raise ValueError(f"period는 'W' 또는 'M'이어야 한다: {period!r}")

    rows = _fetch_chart_rows(ticker, period, start, end, client)
    candles = [
        PeriodCandle(
            ticker=ticker,
            period=period,
            base_date=parsed_date,
            open=open_,
            high=high,
            low=low,
            close=close,
            volume=volume,
            trade_value=trade_value,
        )
        for parsed_date, open_, high, low, close, volume, trade_value in rows
    ]
    return sorted(candles, key=lambda candle: candle.base_date)


# -- 내부 --------------------------------------------------------------------


def _fetch_chart_rows(
    ticker: str,
    period: str,
    start: date,
    end: date,
    client: KisClient | None,
) -> list[tuple[date, Decimal, Decimal, Decimal, Decimal, int, int | None]]:
    """기간별시세를 100건 페이지 단위로 끝까지 읽는다."""

    client = client or get_kis_client()
    parsed: dict[date, tuple[date, Decimal, Decimal, Decimal, Decimal, int, int | None]] = {}
    cursor_end = end

    while cursor_end >= start:
        data = client.request(
            CHART_PATH,
            CHART_TR_ID,
            {
                "FID_COND_MRKT_DIV_CODE": "J",
                "FID_INPUT_ISCD": ticker,
                "FID_INPUT_DATE_1": start.strftime("%Y%m%d"),
                "FID_INPUT_DATE_2": cursor_end.strftime("%Y%m%d"),
                "FID_PERIOD_DIV_CODE": period,
                # 0=수정주가 반영. 액면분할·병합 전후 가격이 이어져야 수익률이 맞는다.
                "FID_ORG_ADJ_PRC": "0",
            },
        )

        rows = data.get("output2") or []
        page: list[tuple[date, Decimal, Decimal, Decimal, Decimal, int, int | None]] = []
        for row in rows:
            record = _parse_chart_row(row)
            if record is not None:
                page.append(record)

        if not page:
            break

        for record in page:
            parsed.setdefault(record[0], record)

        oldest = min(record[0] for record in page)
        if oldest <= start or len(rows) < CHART_PAGE_SIZE:
            break

        # 같은 구간을 다시 받지 않도록 가장 오래된 날짜의 하루 전으로 커서를 옮긴다.
        cursor_end = oldest - timedelta(days=1)

    return sorted(parsed.values(), key=lambda record: record[0])


def _parse_chart_row(
    row: dict[str, Any],
) -> tuple[date, Decimal, Decimal, Decimal, Decimal, int, int | None] | None:
    trade_date = _parse_date(row.get("stck_bsop_date"))
    open_ = _parse_decimal(row.get("stck_oprc"))
    high = _parse_decimal(row.get("stck_hgpr"))
    low = _parse_decimal(row.get("stck_lwpr"))
    close = _parse_decimal(row.get("stck_clpr"))
    volume = _parse_int(row.get("acml_vol"))

    if trade_date is None or None in (open_, high, low, close) or volume is None:
        return None

    return (trade_date, open_, high, low, close, volume, _parse_int(row.get("acml_tr_pbmn")))


def _parse_date(value: object) -> date | None:
    text = str(value or "").strip().replace("/", "").replace("-", "")
    if len(text) != 8 or not text.isdigit():
        return None
    try:
        return datetime.strptime(text, "%Y%m%d").date()
    except ValueError:
        return None


def _parse_int(value: object) -> int | None:
    text = str(value or "").strip().replace(",", "")
    if not text:
        return None
    try:
        return int(Decimal(text))
    except (InvalidOperation, ValueError):
        return None


def _parse_decimal(value: object) -> Decimal | None:
    text = str(value or "").strip().replace(",", "")
    if not text:
        return None
    try:
        return Decimal(text)
    except InvalidOperation:
        return None
