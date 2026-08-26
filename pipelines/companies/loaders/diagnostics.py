"""수집 대상이 0건일 때의 진단."""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.orm import Session

SERVICE_UNIVERSE_SQL = text(
    """
    SELECT (SELECT count(*) FROM service_companies)                    AS service_companies,
           (SELECT count(*) FROM theme_stocks)                         AS theme_stocks,
           (SELECT count(*) FROM stocks
             WHERE is_active AND company_id IS NOT NULL)               AS linked_stocks
    """
)


def describe_universe(session: Session) -> str:
    """수집 대상 목록의 규모를 한 줄로. 0건의 원인이 어느 단계인지 가른다."""

    row = session.execute(SERVICE_UNIVERSE_SQL).one()
    return (
        f"service_companies {row.service_companies}, "
        f"theme_stocks {row.theme_stocks}, "
        f"연결된 종목 {row.linked_stocks}"
    )
