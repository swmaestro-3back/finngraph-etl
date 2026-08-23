"""기업 설명 적재."""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.orm import Session

UPDATE_DESCRIPTION_SQL = text(
    """
    UPDATE companies
       SET description = :description,
           description_source = :description_source,
           updated_at = now()
     WHERE id = :company_id
    """
)

# 설명 생성 대상 — 설명이 아직 없는 국내 서비스 대상 법인
#
# 한 번 채워지면 다시 만들지 않는다. 법인당 DART 2회 + LLM 1회라 비싸고, 회사가 무엇을
# 하는지는 자주 바뀌지 않는다. 다시 만들려면 description을 NULL로 되돌린다.
SELECT_DESCRIPTION_TARGETS_SQL = text(
    """
    SELECT c.id, c.name, c.corp_code
      FROM companies AS c
     WHERE c.country = 'KR'
       AND c.corp_code IS NOT NULL
       AND c.description IS NULL
       AND EXISTS (SELECT 1 FROM service_companies AS u WHERE u.company_id = c.id)
     ORDER BY c.updated_at
     LIMIT :limit
    """
)


def update_description(
    session: Session,
    company_id: int,
    description: str,
    description_source: str,
) -> int:
    """법인 설명을 갱신한다."""

    result = session.execute(
        UPDATE_DESCRIPTION_SQL,
        {
            "company_id": company_id,
            "description": description,
            "description_source": description_source,
        },
    )
    return result.rowcount or 0


def fetch_description_targets(session: Session, limit: int) -> list[tuple[int, str, str]]:
    """설명이 필요한 법인 (id, name, corp_code)."""

    rows = session.execute(SELECT_DESCRIPTION_TARGETS_SQL, {"limit": limit})
    return [(row.id, row.name, row.corp_code) for row in rows]
