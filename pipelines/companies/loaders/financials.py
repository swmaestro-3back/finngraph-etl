"""기업 재무 적재. KIS·DART 양쪽 결과가 같은 테이블로 들어온다."""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.orm import Session

from pipelines.companies.models import CompanyFinancial

# conflict target이 부분 표현식 인덱스(company_financials_uk)라 인덱스 정의와 같은
# 표현식을 그대로 적어야 Postgres가 추론한다.
UPSERT_FINANCIAL_SQL = text(
    """
    INSERT INTO company_financials (
      company_id, source, fs_div, fiscal_yymm, period_type, disclosed_at, rcept_no,
      revenue, operating_income, net_income,
      total_assets, total_liabilities, total_equity,
      roe, eps, bps, created_at, updated_at
    )
    VALUES (
      :company_id, :source, :fs_div, :fiscal_yymm, :period_type, :disclosed_at, :rcept_no,
      :revenue, :operating_income, :net_income,
      :total_assets, :total_liabilities, :total_equity,
      :roe, :eps, :bps, now(), now()
    )
    ON CONFLICT (company_id, source, fs_div, fiscal_yymm, period_type, COALESCE(rcept_no, ''))
    DO UPDATE SET
      disclosed_at = EXCLUDED.disclosed_at,
      revenue = EXCLUDED.revenue,
      operating_income = EXCLUDED.operating_income,
      net_income = EXCLUDED.net_income,
      total_assets = EXCLUDED.total_assets,
      total_liabilities = EXCLUDED.total_liabilities,
      total_equity = EXCLUDED.total_equity,
      roe = EXCLUDED.roe,
      eps = EXCLUDED.eps,
      bps = EXCLUDED.bps,
      updated_at = now()
    """
)

# 재무 수집 대상 — 법인에 연결된 활성 상장 종목
#
# stocks를 거치는 이유는 KIS 재무 API가 법인이 아니라 종목코드로 조회되기 때문이다.
# 우선주·ETP·SPAC는 company_id가 NULL이라 조인에서 자연히 빠진다.
SELECT_LISTED_COMPANY_SYMBOLS_SQL = text(
    """
    SELECT s.company_id, s.ticker
      FROM stocks AS s
     WHERE EXISTS (SELECT 1 FROM service_companies AS u WHERE u.company_id = s.company_id)
       AND s.is_active
       AND s.company_id IS NOT NULL
     ORDER BY s.ticker
    """
)

# 이번 회차에 처리할 법인 — 재무가 가장 오래 갱신되지 않은 순
#
# 종목별 조회라 전 종목을 매일 돌 수 없다. 갱신이 오래된 것부터 batch_size만큼 처리하면
# 회차를 거듭하며 전체가 고르게 채워진다(재무는 분기에 한 번 바뀌므로 이 정도로 충분하다).
#
# **service_companies로 대상을 좁힌다.** 1차 MVP는 반도체·2차전지 361법인이라 상장사 전량을
# 돌 이유가 없다. KIS 재무는 종목코드로 조회하므로 법인 → 종목으로 한 번 내려간다.#
# **JOIN이 아니라 EXISTS다.** service_companies는 (company_id, theme_id) 복합 PK라
# 두 테마에 걸친 법인이 두 행이다(실측 25곳). JOIN하면 그 종목을 두 번 수집한다.
SELECT_STALE_FINANCIAL_TARGETS_SQL = text(
    """
    SELECT s.company_id, s.ticker
      FROM stocks AS s
      LEFT JOIN (
             SELECT company_id, MAX(updated_at) AS updated_at
               FROM company_financials
              WHERE source = :source
              GROUP BY company_id
           ) AS f ON f.company_id = s.company_id
     WHERE EXISTS (SELECT 1 FROM service_companies AS u WHERE u.company_id = s.company_id)
       AND s.is_active
       AND s.company_id IS NOT NULL
     ORDER BY f.updated_at ASC NULLS FIRST, s.ticker
     LIMIT :limit
    """
)


def upsert_financials(session: Session, financials: list[CompanyFinancial]) -> int:
    """재무 행을 적재한다.

    Returns:
        int: 적재 시도한 행 수.
    """

    if not financials:
        return 0

    session.execute(
        UPSERT_FINANCIAL_SQL,
        [
            {
                "company_id": financial.company_id,
                "source": financial.source,
                "fs_div": financial.fs_div,
                "fiscal_yymm": financial.fiscal_yymm,
                "period_type": financial.period_type,
                "disclosed_at": financial.disclosed_at,
                "rcept_no": financial.rcept_no,
                "revenue": financial.revenue,
                "operating_income": financial.operating_income,
                "net_income": financial.net_income,
                "total_assets": financial.total_assets,
                "total_liabilities": financial.total_liabilities,
                "total_equity": financial.total_equity,
                "roe": financial.roe,
                "eps": financial.eps,
                "bps": financial.bps,
            }
            for financial in financials
        ],
    )
    return len(financials)


def fetch_listed_company_symbols(session: Session) -> list[tuple[int, str]]:
    """법인에 연결된 활성 상장 종목 (company_id, ticker) 전체."""

    return [
        (row.company_id, row.ticker) for row in session.execute(SELECT_LISTED_COMPANY_SYMBOLS_SQL)
    ]


def fetch_stale_financial_targets(
    session: Session,
    limit: int,
    source: str = "KIS",
) -> list[tuple[int, str]]:
    """재무 갱신이 가장 오래된 법인부터 limit개."""

    rows = session.execute(SELECT_STALE_FINANCIAL_TARGETS_SQL, {"limit": limit, "source": source})
    return [(row.company_id, row.ticker) for row in rows]
