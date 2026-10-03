"""테마 봉(theme_candles_daily · theme_candles_period) 조회·적재.

지수 규칙은 transformers/theme_index.py 에 있고 여기는 테이블만 안다.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from sqlalchemy import text
from sqlalchemy.orm import Session

from pipelines.themes.types import ThemeCandle

FETCH_ANCHORS_SQL = text(
    """
    SELECT DISTINCT ON (theme_id) theme_id, close
      FROM theme_candles_daily
     WHERE theme_id = ANY(:theme_ids) AND trade_date < :before
     ORDER BY theme_id, trade_date DESC
    """
)

UPSERT_THEME_DAILY_SQL = text(
    """
    INSERT INTO theme_candles_daily (
      theme_id, trade_date, open, high, low, close, volume, trade_value, source, updated_at
    )
    VALUES (
      :theme_id, :trade_date, :open, :high, :low, :close, :volume, :trade_value, 'CALC', now()
    )
    ON CONFLICT (theme_id, trade_date) DO UPDATE SET
      open = EXCLUDED.open,
      high = EXCLUDED.high,
      low = EXCLUDED.low,
      close = EXCLUDED.close,
      volume = EXCLUDED.volume,
      trade_value = EXCLUDED.trade_value,
      source = EXCLUDED.source,
      updated_at = now()
    """
)

# 등락률(change_rate)은 직전 봉 종가 대비 %다. 체인 지수라 지수 종가의 비가 곧 구성 종목
# 등락률의 가중 평균이므로 종목 봉과 같은 식을 쓴다. 참여 종목이 없어 행이 빠진 날이 있으면
# 그 앞의 마지막 봉이 기준이다. since 이후를 끝까지 다시 계산하고 값이 달라진 행만 고친다.
# 적재와 분리된 단계(jobs/calculate_theme_candles.run_change_rates)가 부른다.
REFRESH_THEME_DAILY_CHANGE_RATE_SQL = text(
    """
    UPDATE theme_candles_daily AS c
       SET change_rate = r.change_rate
      FROM (
        SELECT d.theme_id, d.trade_date,
               ROUND((d.close / NULLIF(prev.close, 0) - 1) * 100, 2) AS change_rate
          FROM theme_candles_daily AS d
          LEFT JOIN LATERAL (
                 SELECT p.close FROM theme_candles_daily AS p
                  WHERE p.theme_id = d.theme_id AND p.trade_date < d.trade_date
                  ORDER BY p.trade_date DESC LIMIT 1
               ) AS prev ON true
         WHERE d.trade_date >= :since
           AND (
                 CAST(:theme_ids AS bigint[]) IS NULL
                 OR d.theme_id = ANY(CAST(:theme_ids AS bigint[]))
               )
      ) AS r
     WHERE c.theme_id = r.theme_id
       AND c.trade_date = r.trade_date
       AND c.change_rate IS DISTINCT FROM r.change_rate
    """
)


def fetch_anchors(session: Session, theme_ids: list[int], before: date) -> dict[int, Decimal]:
    """테마별로 before 이전 마지막 테마 종가. 행이 없는 테마는 빠진다."""

    if not theme_ids:
        return {}
    rows = session.execute(FETCH_ANCHORS_SQL, {"theme_ids": list(theme_ids), "before": before})
    return {row.theme_id: row.close for row in rows}


# since 가 속한 주·월의 시작일부터 끝까지 모든 구간을 다시 집계한다. 경계에 걸친 주·월이
# 부분 집계로 덮이지 않게 하려는 것이다. base_date 는 구간 시작일(stock_candles_period 규칙).
REBUILD_THEME_PERIOD_SQL = text(
    """
    WITH bucket AS (
      SELECT d.theme_id, p.period,
             date_trunc(CASE p.period WHEN 'W' THEN 'week' ELSE 'month' END, d.trade_date)::date
               AS base_date,
             d.trade_date, d.open, d.high, d.low, d.close, d.volume, d.trade_value
        FROM theme_candles_daily AS d
        CROSS JOIN (VALUES ('W'), ('M')) AS p(period)
       WHERE d.trade_date >= date_trunc(
               CASE p.period WHEN 'W' THEN 'week' ELSE 'month' END, CAST(:since AS date)
             )::date
         AND (
               CAST(:theme_ids AS bigint[]) IS NULL
               OR d.theme_id = ANY(CAST(:theme_ids AS bigint[]))
             )
    )
    INSERT INTO theme_candles_period (
      theme_id, period, base_date, open, high, low, close, volume, trade_value, updated_at
    )
    SELECT theme_id, period, base_date,
           (array_agg(open ORDER BY trade_date))[1],
           max(high), min(low),
           (array_agg(close ORDER BY trade_date DESC))[1],
           sum(volume), sum(trade_value), now()
      FROM bucket
     GROUP BY theme_id, period, base_date
    ON CONFLICT (theme_id, period, base_date) DO UPDATE SET
      open = EXCLUDED.open,
      high = EXCLUDED.high,
      low = EXCLUDED.low,
      close = EXCLUDED.close,
      volume = EXCLUDED.volume,
      trade_value = EXCLUDED.trade_value,
      updated_at = now()
    """
)

# since 가 속한 주·월의 봉부터 등락률을 직전 주·월 봉 대비로 다시 계산한다.
REFRESH_THEME_PERIOD_CHANGE_RATE_SQL = text(
    """
    UPDATE theme_candles_period AS c
       SET change_rate = r.change_rate
      FROM (
        SELECT d.theme_id, d.period, d.base_date,
               ROUND((d.close / NULLIF(prev.close, 0) - 1) * 100, 2) AS change_rate
          FROM theme_candles_period AS d
          LEFT JOIN LATERAL (
                 SELECT p.close FROM theme_candles_period AS p
                  WHERE p.theme_id = d.theme_id
                    AND p.period = d.period
                    AND p.base_date < d.base_date
                  ORDER BY p.base_date DESC LIMIT 1
               ) AS prev ON true
         WHERE d.base_date >= date_trunc(
                 CASE d.period WHEN 'W' THEN 'week' ELSE 'month' END, CAST(:since AS date)
               )::date
           AND (
                 CAST(:theme_ids AS bigint[]) IS NULL
                 OR d.theme_id = ANY(CAST(:theme_ids AS bigint[]))
               )
      ) AS r
     WHERE c.theme_id = r.theme_id
       AND c.period = r.period
       AND c.base_date = r.base_date
       AND c.change_rate IS DISTINCT FROM r.change_rate
    """
)


def rebuild_theme_period_candles(
    session: Session, since: date, theme_ids: list[int] | None = None
) -> int:
    """since 가 속한 주·월부터의 테마 주봉·월봉을 일봉으로 다시 집계한다.

    Returns:
        int: upsert 된 행 수 (W·M 합계).
    """

    return session.execute(
        REBUILD_THEME_PERIOD_SQL,
        {"since": since, "theme_ids": list(theme_ids) if theme_ids else None},
    ).rowcount


def upsert_theme_daily_candles(session: Session, candles: list[ThemeCandle]) -> int:
    """테마 일봉을 적재한다. 같은 (theme_id, trade_date) 는 덮어쓴다."""

    if not candles:
        return 0
    session.execute(
        UPSERT_THEME_DAILY_SQL,
        [
            {
                "theme_id": c.theme_id,
                "trade_date": c.trade_date,
                "open": c.open,
                "high": c.high,
                "low": c.low,
                "close": c.close,
                "volume": c.volume,
                "trade_value": c.trade_value,
            }
            for c in candles
        ],
    )
    return len(candles)


def refresh_theme_change_rates(
    session: Session, since: date, theme_ids: list[int] | None = None
) -> int:
    """since 이후 테마 일봉과, since 가 속한 주·월부터의 주봉·월봉 등락률을 다시 계산한다.

    Returns:
        int: 값이 바뀐 행 수 (일봉·주봉·월봉 합계).
    """

    params = {"since": since, "theme_ids": list(theme_ids) if theme_ids else None}
    daily = session.execute(REFRESH_THEME_DAILY_CHANGE_RATE_SQL, params).rowcount
    period = session.execute(REFRESH_THEME_PERIOD_CHANGE_RATE_SQL, params).rowcount
    return daily + period
