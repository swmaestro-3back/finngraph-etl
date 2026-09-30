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
from pipelines.stocks.types import DailyCandle, PeriodCandle

UPSERT_DAILY_CANDLE_SQL = text(
    """
    INSERT INTO stock_candles_daily (
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
      trade_value = COALESCE(EXCLUDED.trade_value, stock_candles_daily.trade_value),
      source = EXCLUDED.source,
      updated_at = now()
    """
)

UPSERT_PERIOD_CANDLE_SQL = text(
    """
    INSERT INTO stock_candles_period (
      stock_id, period, base_date, open, high, low, close, volume, trade_value, updated_at
    )
    VALUES (
      :stock_id, :period,
      date_trunc(
        CASE :period WHEN 'W' THEN 'week' ELSE 'month' END, CAST(:base_date AS date)
      )::date,
      :open, :high, :low, :close, :volume, :trade_value, now()
    )
    ON CONFLICT (stock_id, period, base_date) DO UPDATE SET
      open = EXCLUDED.open,
      high = EXCLUDED.high,
      low = EXCLUDED.low,
      close = EXCLUDED.close,
      volume = EXCLUDED.volume,
      trade_value = COALESCE(EXCLUDED.trade_value, stock_candles_period.trade_value),
      updated_at = now()
    """
)


AGGREGATE_CURRENT_PERIOD_SQL = text(
    """
    WITH latest AS (
      SELECT stock_id, max(trade_date) AS last_date
        FROM stock_candles_daily
       GROUP BY stock_id
      HAVING max(trade_date) >= :since
    ),
    bucket AS (
      SELECT d.stock_id, p.period,
             date_trunc(CASE p.period WHEN 'W' THEN 'week' ELSE 'month' END, l.last_date)::date
               AS base_date,
             d.trade_date, d.open, d.high, d.low, d.close, d.volume, d.trade_value
        FROM latest AS l
        JOIN stock_candles_daily AS d ON d.stock_id = l.stock_id
        CROSS JOIN (VALUES ('W'), ('M')) AS p(period)
       WHERE d.trade_date <= l.last_date
         AND d.trade_date >= date_trunc(
               CASE p.period WHEN 'W' THEN 'week' ELSE 'month' END, l.last_date
             )::date
    )
    INSERT INTO stock_candles_period (
      stock_id, period, base_date, open, high, low, close, volume, trade_value, updated_at
    )
    SELECT stock_id, period, base_date,
           (array_agg(open ORDER BY trade_date))[1],
           max(high), min(low),
           (array_agg(close ORDER BY trade_date DESC))[1],
           sum(volume), sum(trade_value), now()
      FROM bucket
     GROUP BY stock_id, period, base_date
    ON CONFLICT (stock_id, period, base_date) DO UPDATE SET
      open = EXCLUDED.open,
      high = EXCLUDED.high,
      low = EXCLUDED.low,
      close = EXCLUDED.close,
      volume = EXCLUDED.volume,
      trade_value = COALESCE(EXCLUDED.trade_value, stock_candles_period.trade_value),
      updated_at = now()
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


def upsert_period_candles(session: Session, candles: list[PeriodCandle]) -> int:
    """주봉·월봉을 적재한다. base_date는 구간 시작일(주: 월요일, 월: 1일)로 정규화한다."""

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


def aggregate_current_period_candles(session: Session, since: date) -> int:
    """최신 일봉이 속한 이번 주·이번 달 봉을 일봉으로 계산해 적재한다."""

    return session.execute(AGGREGATE_CURRENT_PERIOD_SQL, {"since": since}).rowcount
