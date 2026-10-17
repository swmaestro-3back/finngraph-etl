"""법인 마스터 참조 조회. companies 는 companies 도메인이 채우고 여기서는 읽기만 한다."""

from __future__ import annotations

from collections.abc import Sequence

from sqlalchemy import text
from sqlalchemy.orm import Session

from pipelines.market_calendar.models import CompanyRef

SELECT_COMPANY_REFS_SQL = text(
    """
    SELECT corp_code, id,
           (ceo_name IS NOT NULL OR established_on IS NOT NULL OR address IS NOT NULL)
             AS has_profile,
           COALESCE(description, '') <> '' AS has_description
      FROM companies
     WHERE corp_code = ANY(CAST(:codes AS text[]))
    """
)


def select_company_refs(session: Session, codes: Sequence[str]) -> dict[str, CompanyRef]:
    if not codes:
        return {}
    rows = session.execute(SELECT_COMPANY_REFS_SQL, {"codes": list(codes)})
    return {
        row.corp_code: CompanyRef(
            corp_code=row.corp_code,
            company_id=row.id,
            has_profile=row.has_profile,
            has_description=row.has_description,
        )
        for row in rows
    }
