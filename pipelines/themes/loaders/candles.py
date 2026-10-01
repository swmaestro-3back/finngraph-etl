"""테마 봉 조회·적재 SQL.

지수 규칙은 transformers/theme_index.py 에 있고 여기는 테이블만 안다. 구성 종목 조회는
theme_stocks ⋈ stocks ⋈ stock_candles_daily 를 그대로 돌려주고, 참여 여부 판정(상장주식수,
전일 봉 유무)은 transformer 가 한다 — 규칙이 한 곳에만 있게 하려는 것이다.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from sqlalchemy import text
from sqlalchemy.orm import Session

from pipelines.themes.types import ConstituentCandle, ThemeCandle

FETCH_THEME_IDS_SQL = text("SELECT DISTINCT theme_id FROM theme_stocks ORDER BY theme_id")

FETCH_FIRST_TRADE_DATE_SQL = text("SELECT min(trade_date) FROM stock_candles_daily")

FETCH_TRADING_CALENDAR_SQL = text(
    """
    -- 시장 캘린더는 이 테이블 전체(현재 KR 만)의 거래일이다.
    -- 다른 시장 일봉이 섞이면 테마별 캘린더로 나눠야 한다.
    SELECT DISTINCT trade_date FROM stock_candles_daily
     WHERE trade_date BETWEEN :start AND :end
     ORDER BY trade_date
    """
)

FETCH_ANCHORS_SQL = text(
    """
    SELECT DISTINCT ON (theme_id) theme_id, close
      FROM theme_candles_daily
     WHERE theme_id = ANY(:theme_ids) AND trade_date < :before
     ORDER BY theme_id, trade_date DESC
    """
)

FETCH_CONSTITUENT_CANDLES_SQL = text(
    """
    SELECT c.stock_id, c.trade_date, c.open, c.high, c.low, c.close,
           c.volume, c.trade_value, s.listed_shares
      FROM theme_stocks AS ts
      JOIN stocks AS s ON s.id = ts.stock_id
      JOIN stock_candles_daily AS c ON c.stock_id = ts.stock_id
     WHERE ts.theme_id = :theme_id
       AND c.trade_date BETWEEN :since AND :end
     ORDER BY c.trade_date, c.stock_id
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


def fetch_theme_ids(session: Session) -> list[int]:
    """편입 종목이 하나라도 있는 테마 id."""

    return [row[0] for row in session.execute(FETCH_THEME_IDS_SQL)]


def fetch_first_trade_date(session: Session) -> date | None:
    """종목 일봉의 가장 이른 거래일. 백필 기본 시작일이다."""

    return session.execute(FETCH_FIRST_TRADE_DATE_SQL).scalar()


def fetch_trading_calendar(session: Session, start: date, end: date) -> list[date]:
    """[start, end] 의 시장 거래일 오름차순. 첫 원소는 호출자가 t-1 로만 쓴다."""

    return [
        row[0] for row in session.execute(FETCH_TRADING_CALENDAR_SQL, {"start": start, "end": end})
    ]


def fetch_anchors(session: Session, theme_ids: list[int], before: date) -> dict[int, Decimal]:
    """테마별로 before 이전 마지막 테마 종가. 행이 없는 테마는 빠진다."""

    if not theme_ids:
        return {}
    rows = session.execute(FETCH_ANCHORS_SQL, {"theme_ids": list(theme_ids), "before": before})
    return {row.theme_id: row.close for row in rows}


def fetch_constituent_candles(
    session: Session, theme_id: int, since: date, end: date
) -> list[ConstituentCandle]:
    """테마 구성 종목의 [since, end] 일봉. since 는 보통 캘린더 첫 거래일(t-1 로만 쓰는 날)이다."""

    rows = session.execute(
        FETCH_CONSTITUENT_CANDLES_SQL, {"theme_id": theme_id, "since": since, "end": end}
    )
    return [
        ConstituentCandle(
            stock_id=row.stock_id,
            trade_date=row.trade_date,
            open=row.open,
            high=row.high,
            low=row.low,
            close=row.close,
            volume=row.volume,
            trade_value=row.trade_value,
            listed_shares=row.listed_shares,
        )
        for row in rows
    ]


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
