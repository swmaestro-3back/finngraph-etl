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


# 등락률(change_rate)은 직전 봉 종가 대비 %다. upsert 문 안에서 계산하지 않고 적재 뒤 별도
# 단계(jobs/calculate_change_rates)로 갱신한다 — 직전 봉이 배치 밖(DB)에 있을 수 있고, 수정주가
# 반영으로 과거 종가가 바뀌면 그 뒤 봉의 등락률도 함께 바뀌기 때문이다. since 이후를 끝까지
# 다시 계산하고 값이 달라진 행만 고친다. stock_ids 가 NULL 이면 전 종목이다.
REFRESH_DAILY_CHANGE_RATE_SQL = text(
    """
    UPDATE stock_candles_daily AS c
       SET change_rate = r.change_rate
      FROM (
        SELECT d.stock_id, d.trade_date,
               ROUND((d.close / NULLIF(prev.close, 0) - 1) * 100, 2) AS change_rate
          FROM stock_candles_daily AS d
          LEFT JOIN LATERAL (
                 SELECT p.close FROM stock_candles_daily AS p
                  WHERE p.stock_id = d.stock_id AND p.trade_date < d.trade_date
                  ORDER BY p.trade_date DESC LIMIT 1
               ) AS prev ON true
         WHERE d.trade_date >= :since
           AND (
                 CAST(:stock_ids AS bigint[]) IS NULL
                 OR d.stock_id = ANY(CAST(:stock_ids AS bigint[]))
               )
      ) AS r
     WHERE c.stock_id = r.stock_id
       AND c.trade_date = r.trade_date
       AND c.change_rate IS DISTINCT FROM r.change_rate
    """
)

# since 가 속한 주·월의 봉부터 다시 계산한다.
REFRESH_PERIOD_CHANGE_RATE_SQL = text(
    """
    UPDATE stock_candles_period AS c
       SET change_rate = r.change_rate
      FROM (
        SELECT d.stock_id, d.period, d.base_date,
               ROUND((d.close / NULLIF(prev.close, 0) - 1) * 100, 2) AS change_rate
          FROM stock_candles_period AS d
          LEFT JOIN LATERAL (
                 SELECT p.close FROM stock_candles_period AS p
                  WHERE p.stock_id = d.stock_id
                    AND p.period = d.period
                    AND p.base_date < d.base_date
                  ORDER BY p.base_date DESC LIMIT 1
               ) AS prev ON true
         WHERE d.base_date >= date_trunc(
                 CASE d.period WHEN 'W' THEN 'week' ELSE 'month' END, CAST(:since AS date)
               )::date
           AND (
                 CAST(:stock_ids AS bigint[]) IS NULL
                 OR d.stock_id = ANY(CAST(:stock_ids AS bigint[]))
               )
      ) AS r
     WHERE c.stock_id = r.stock_id
       AND c.period = r.period
       AND c.base_date = r.base_date
       AND c.change_rate IS DISTINCT FROM r.change_rate
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


def refresh_daily_change_rates(
    session: Session, since: date, stock_ids: list[int] | None = None
) -> int:
    """since 이후 일봉의 등락률(직전 거래일 종가 대비 %)을 다시 계산한다.

    Args:
        session (Session): DB 세션.
        since (date): 재계산 시작일(포함). 이 날의 직전 봉은 DB 에서 찾는다.
        stock_ids (list[int] | None): 대상 종목. 생략하면 전 종목.

    Returns:
        int: 값이 바뀐 행 수.
    """

    return session.execute(
        REFRESH_DAILY_CHANGE_RATE_SQL,
        {"since": since, "stock_ids": list(stock_ids) if stock_ids else None},
    ).rowcount


def refresh_period_change_rates(
    session: Session, since: date, stock_ids: list[int] | None = None
) -> int:
    """since 가 속한 주·월부터 주봉·월봉의 등락률(직전 주·월 봉 종가 대비 %)을 다시 계산한다.

    Returns:
        int: 값이 바뀐 행 수 (W·M 합계).
    """

    return session.execute(
        REFRESH_PERIOD_CHANGE_RATE_SQL,
        {"since": since, "stock_ids": list(stock_ids) if stock_ids else None},
    ).rowcount
