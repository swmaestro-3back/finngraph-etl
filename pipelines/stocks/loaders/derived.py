"""밸류에이션·수익률 파생 계산.

파이썬으로 종목을 돌지 않고 SQL 한 문장으로 계산한다. 입력(일봉·재무·배당·상장주식수)이
전부 DB에 있어 밖으로 꺼낼 이유가 없고, 전 종목 × 여러 날짜를 왕복하면 수만 번이 된다.
"""

from __future__ import annotations

from datetime import date

from sqlalchemy import text
from sqlalchemy.orm import Session

# 일별 밸류에이션
#
# EPS는 TTM이다. period_type='Q'(분기 개별) 네 개를 더한다 — 누적값 하나를 1년치로
# 나누면 PER이 최대 9배 부푼다. 기준일 이후 기간의 재무는 쓰지 않는다.
# 시가총액은 종가 × 상장주식수로 만든다. master 값은 전일 기준가라 당일 종가와 어긋난다.
UPSERT_VALUATION_SQL = text(
    """
    INSERT INTO valuation_daily (
      listing_id, trade_date, market_cap, per, pbr, eps, bps, dividend_yield
    )
    SELECT
      c.stock_id,
      c.trade_date,
      CASE WHEN s.listed_shares IS NOT NULL THEN (c.close * s.listed_shares)::bigint END,
      CASE WHEN f.eps IS NOT NULL AND f.eps > 0 THEN ROUND(c.close / f.eps, 2) END,
      CASE WHEN bp.bps IS NOT NULL AND bp.bps > 0 THEN ROUND(c.close / bp.bps, 2) END,
      f.eps,
      bp.bps,
      CASE
        WHEN c.close > 0 AND d.dps_sum IS NOT NULL AND d.dps_sum > 0
        THEN ROUND(d.dps_sum / c.close * 100, 2)
      END
      FROM daily_candles AS c
      JOIN stocks AS s ON s.id = c.stock_id
      LEFT JOIN LATERAL (
             -- TTM(최근 12개월) EPS = 분기 개별 EPS 네 개의 합.
             --
             -- 네 개가 다 모여야 값을 낸다. 신규 상장이라 세 분기밖에 없으면 NULL이다 —
             -- 모자란 기간을 1년치처럼 쓰면 PER이 그만큼 부풀기 때문이다.
             SELECT CASE WHEN count(t.eps) = 4 THEN sum(t.eps) END AS eps
               FROM (
                     SELECT cf.eps
                       FROM company_financials AS cf
                      WHERE cf.company_id = s.company_id
                        AND cf.source = 'KIS'
                        AND cf.period_type = 'Q'
                        AND cf.eps IS NOT NULL
                        AND cf.fiscal_yymm <= to_char(c.trade_date, 'YYYYMM')
                      ORDER BY cf.fiscal_yymm DESC
                      LIMIT 4
                    ) AS t
           ) AS f ON true
      LEFT JOIN LATERAL (
             -- BPS는 합치지 않는다. 자본총계 ÷ 주식수라 그 시점의 잔액이고
             -- 누적 개념이 없다. 가장 최근 값을 그대로 쓴다.
             --
             -- 누적(QC)과 개별(Q) 중 아무거나 읽는다. 잔액이라 두 행의 BPS가 같고,
             -- 개별 행이 아직 생성되지 않은 법인에서도 PBR이 나온다. Q만 읽으면
             -- 분기 차분이 불가능한 법인(직전 분기 누락·신규 상장)의 PBR이
             -- 이유 없이 비어버린다.
             SELECT cf.bps
               FROM company_financials AS cf
              WHERE cf.company_id = s.company_id
                AND cf.source = 'KIS'
                AND cf.period_type IN ('Q', 'QC')
                AND cf.bps IS NOT NULL
                AND cf.fiscal_yymm <= to_char(c.trade_date, 'YYYYMM')
              ORDER BY cf.fiscal_yymm DESC
              LIMIT 1
           ) AS bp ON true
      LEFT JOIN LATERAL (
             -- 최근 1년 배당 합. 분기배당이면 4건이 더해지고, 무배당(0)은 제외한다.
             SELECT SUM(dv.dps) AS dps_sum
               FROM dividends AS dv
              WHERE dv.listing_id = c.stock_id
                AND dv.dps IS NOT NULL
                AND dv.dps > 0
                AND dv.record_date <= c.trade_date
                AND dv.record_date > c.trade_date - INTERVAL '1 year'
           ) AS d ON true
     WHERE c.trade_date >= :start_date
    ON CONFLICT (listing_id, trade_date) DO UPDATE SET
      market_cap = EXCLUDED.market_cap,
      per = EXCLUDED.per,
      pbr = EXCLUDED.pbr,
      eps = EXCLUDED.eps,
      bps = EXCLUDED.bps,
      dividend_yield = EXCLUDED.dividend_yield
    """
)

# 기간 수익률
#
# valuation_daily 에 함께 담는다. 별도 테이블이면 같은 키를 두 번 관리하게 된다.
# 비교 대상은 N일 전 이전의 가장 가까운 거래일이다 — 그 날이 휴장이면 봉이 없다.
UPDATE_RETURNS_SQL = text(
    """
    UPDATE valuation_daily AS v
       SET r_1w = r.r_1w,
           r_1m = r.r_1m,
           r_3m = r.r_3m
      FROM (
        SELECT
          c.stock_id,
          c.trade_date,
          ROUND((c.close / NULLIF(w.close, 0) - 1) * 100, 2) AS r_1w,
          ROUND((c.close / NULLIF(m.close, 0) - 1) * 100, 2) AS r_1m,
          ROUND((c.close / NULLIF(q.close, 0) - 1) * 100, 2) AS r_3m
          FROM daily_candles AS c
          LEFT JOIN LATERAL (
                 SELECT p.close FROM daily_candles AS p
                  WHERE p.stock_id = c.stock_id AND p.trade_date <= c.trade_date - INTERVAL '7 days'
                  ORDER BY p.trade_date DESC LIMIT 1
               ) AS w ON true
          LEFT JOIN LATERAL (
                 SELECT p.close FROM daily_candles AS p
                  WHERE p.stock_id = c.stock_id
                    AND p.trade_date <= c.trade_date - INTERVAL '1 month'
                  ORDER BY p.trade_date DESC LIMIT 1
               ) AS m ON true
          LEFT JOIN LATERAL (
                 SELECT p.close FROM daily_candles AS p
                  WHERE p.stock_id = c.stock_id
                    AND p.trade_date <= c.trade_date - INTERVAL '3 months'
                  ORDER BY p.trade_date DESC LIMIT 1
               ) AS q ON true
         WHERE c.trade_date >= :start_date
      ) AS r
     WHERE v.listing_id = r.stock_id
       AND v.trade_date = r.trade_date
    """
)


def compute_valuations(session: Session, start_date: date) -> int:
    """start_date 이후 일봉에 대해 밸류에이션을 계산해 적재한다.

    Returns:
        int: 적재한 행 수.
    """

    result = session.execute(UPSERT_VALUATION_SQL, {"start_date": start_date})
    return result.rowcount or 0


def compute_returns(session: Session, start_date: date) -> int:
    """start_date 이후 밸류에이션 행에 기간 수익률을 채운다.

    밸류에이션이 먼저 적재돼 있어야 한다 — 같은 행을 UPDATE 하기 때문이다.
    """

    result = session.execute(UPDATE_RETURNS_SQL, {"start_date": start_date})
    return result.rowcount or 0
