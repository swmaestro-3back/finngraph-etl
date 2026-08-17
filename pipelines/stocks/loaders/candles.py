"""시세 적재.

extractor는 단축코드로 말하고 테이블은 stock_id를 키로 쓴다. 그 변환을 여기서 한다.
매핑에 없는 단축코드(비활성 종목·신규 상장 직후 등)는 조용히 버리지 않고 세어서
반환한다 — 조용히 빠지면 "수집은 됐는데 화면에 없는" 상태를 추적할 수 없다.
"""

from __future__ import annotations

from datetime import date

from sqlalchemy import text
from sqlalchemy.orm import Session

from pipelines.stocks.loaders.tickers import fetch_active_stock_ids
from pipelines.stocks.types import DailyCandle, MinuteCandle, PeriodCandle

UPSERT_DAILY_CANDLE_SQL = text(
    """
    INSERT INTO daily_candles (
      stock_id, trade_date, open, high, low, close, volume, trade_value, source, updated_at
    )
    VALUES (
      :stock_id, :trade_date, :open, :high, :low, :close, :volume, :trade_value, :source, now()
    )
    ON CONFLICT (stock_id, trade_date) DO UPDATE SET
      open = EXCLUDED.open,
      high = EXCLUDED.high,
      low = EXCLUDED.low,
      close = EXCLUDED.close,
      volume = EXCLUDED.volume,
      -- 거래대금은 원천에 따라 없을 수 있다. NULL로 덮어써서 기존 값을 지우지 않는다.
      trade_value = COALESCE(EXCLUDED.trade_value, daily_candles.trade_value),
      source = EXCLUDED.source,
      updated_at = now()
    """
)

UPSERT_PERIOD_CANDLE_SQL = text(
    """
    INSERT INTO stock_period_candles (
      stock_id, period, base_date, open, high, low, close, volume, trade_value, updated_at
    )
    VALUES (
      :stock_id, :period, :base_date, :open, :high, :low, :close, :volume, :trade_value, now()
    )
    ON CONFLICT (stock_id, period, base_date) DO UPDATE SET
      open = EXCLUDED.open,
      high = EXCLUDED.high,
      low = EXCLUDED.low,
      close = EXCLUDED.close,
      volume = EXCLUDED.volume,
      trade_value = COALESCE(EXCLUDED.trade_value, stock_period_candles.trade_value),
      updated_at = now()
    """
)

SELECT_LATEST_DAILY_CANDLE_DATES_SQL = text(
    """
    SELECT stock_id, MAX(trade_date) AS latest
      FROM daily_candles
     GROUP BY stock_id
    """
)


def upsert_daily_candles(session: Session, candles: list[DailyCandle], source: str = "KIS") -> int:
    """일봉을 적재한다.

    Args:
        session (Session): DB 세션.
        candles (list[DailyCandle]): 적재할 일봉.
        source (str): 원천 표기. 'FDR'(과거 백필) 또는 'KIS'(일별 갱신).

    Returns:
        int: 적재한 행 수. stock_id를 찾지 못한 종목은 제외된다.
    """

    if not candles:
        return 0

    stock_ids = fetch_active_stock_ids(session)
    payload = [
        {
            "stock_id": stock_ids[candle.ticker],
            "trade_date": candle.trade_date,
            "open": candle.open,
            "high": candle.high,
            "low": candle.low,
            "close": candle.close,
            "volume": candle.volume,
            "trade_value": candle.trade_value,
            "source": source,
        }
        for candle in candles
        if candle.ticker in stock_ids
    ]
    if not payload:
        return 0

    session.execute(UPSERT_DAILY_CANDLE_SQL, payload)
    return len(payload)


def upsert_minute_candles(session: Session, interval: str, candles: list[MinuteCandle]) -> int:
    """분봉 적재. 분봉 수집은 이번 범위 밖이라 아직 구현하지 않는다."""

    raise NotImplementedError(f"{interval} candle upsert is not implemented yet.")


def upsert_period_candles(session: Session, candles: list[PeriodCandle]) -> int:
    """주봉·월봉을 적재한다."""

    if not candles:
        return 0

    stock_ids = fetch_active_stock_ids(session)
    payload = [
        {
            "stock_id": stock_ids[candle.ticker],
            "period": candle.period,
            "base_date": candle.base_date,
            "open": candle.open,
            "high": candle.high,
            "low": candle.low,
            "close": candle.close,
            "volume": candle.volume,
            "trade_value": candle.trade_value,
        }
        for candle in candles
        if candle.ticker in stock_ids
    ]
    if not payload:
        return 0

    session.execute(UPSERT_PERIOD_CANDLE_SQL, payload)
    return len(payload)


def fetch_latest_daily_candle_dates(session: Session) -> dict[int, date]:
    """종목별로 이미 적재된 마지막 일봉 날짜. 백필 시작점을 정하는 데 쓴다."""

    return {
        row.stock_id: row.latest for row in session.execute(SELECT_LATEST_DAILY_CANDLE_DATES_SQL)
    }
