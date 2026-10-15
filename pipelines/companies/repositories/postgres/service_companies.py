"""service_companies 테이블 적재와 수집 대상이 0건일 때의 진단."""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.orm import Session

SYNC_SERVICE_COMPANIES_SQL = text(
    """
    INSERT INTO service_companies (company_id, name)
    SELECT DISTINCT s.company_id, c.name
      FROM stocks AS s
      JOIN companies AS c ON c.id = s.company_id
     WHERE s.is_active
       AND s.market IN ('KOSPI', 'KOSDAQ')
    ON CONFLICT (company_id) DO NOTHING
    """
)


def sync_service_companies(session: Session) -> int:
    result = session.execute(SYNC_SERVICE_COMPANIES_SQL)
    return result.rowcount or 0


def count_service_companies(session: Session) -> int:
    """수집 대상 법인 수."""

    return session.execute(text("SELECT count(*) FROM service_companies")).scalar() or 0


SERVICE_UNIVERSE_SQL = text(
    """
    SELECT (SELECT count(*) FROM service_companies)                    AS service_companies,
           (SELECT count(*) FROM stocks
             WHERE is_active AND company_id IS NOT NULL
               AND market IN ('KOSPI', 'KOSDAQ'))                      AS linked_stocks
    """
)


def describe_universe(session: Session) -> str:
    """수집 대상 목록의 규모를 한 줄로. 0건의 원인이 어느 단계인지 가른다."""

    row = session.execute(SERVICE_UNIVERSE_SQL).one()
    return f"service_companies {row.service_companies}, 연결된 종목 {row.linked_stocks}"
