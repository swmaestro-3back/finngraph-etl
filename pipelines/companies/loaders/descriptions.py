"""기업 설명 적재."""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.orm import Session

UPDATE_DESCRIPTION_SQL = text(
    """
    UPDATE companies
       SET description = :description,
           description_source = :description_source,
           description_rcept_no = :description_rcept_no,
           updated_at = now()
     WHERE id = :company_id
    """
)

# 설명 생성 대상 — 국내 서비스 대상 법인
#
# 새 보고서가 올라왔는지는 DART 목록을 봐야 알 수 있어 전량을 넘긴다. 넘길지 말지는
# 잡이 description_rcept_no와 대조해 정한다.
SELECT_DESCRIPTION_TARGETS_SQL = text(
    """
    SELECT c.id, c.name, c.corp_code, c.description_rcept_no
      FROM companies AS c
     WHERE c.country = 'KR'
       AND c.corp_code IS NOT NULL
       AND EXISTS (SELECT 1 FROM service_companies AS u WHERE u.company_id = c.id)
     ORDER BY (c.description IS NOT NULL), c.id
     LIMIT :limit
    """
)


def update_description(
    session: Session,
    company_id: int,
    description: str,
    description_source: str,
    description_rcept_no: str,
) -> int:
    """법인 설명을 갱신한다."""

    result = session.execute(
        UPDATE_DESCRIPTION_SQL,
        {
            "company_id": company_id,
            "description": description,
            "description_source": description_source,
            "description_rcept_no": description_rcept_no,
        },
    )
    return result.rowcount or 0


def fetch_description_targets(
    session: Session, limit: int | None = None
) -> list[tuple[int, str, str, str | None]]:
    """설명 후보 법인 (id, name, corp_code, 직전 원천 접수번호)."""

    rows = session.execute(SELECT_DESCRIPTION_TARGETS_SQL, {"limit": limit})
    return [(row.id, row.name, row.corp_code, row.description_rcept_no) for row in rows]
