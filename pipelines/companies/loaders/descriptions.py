"""기업 설명·업종 적재."""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.orm import Session

# 업종명 갱신 — 단축코드로 stocks를 거쳐 법인을 찾는다.
UPDATE_INDUSTRY_NAME_SQL = text(
    """
    UPDATE companies AS c
       SET industry_name = :industry_name,
           updated_at = now()
      FROM stocks AS s
     WHERE s.ticker = :ticker
       AND s.is_active
       AND s.company_id = c.id
       AND c.industry_name IS DISTINCT FROM :industry_name
    """
)

UPDATE_DESCRIPTION_SQL = text(
    """
    UPDATE companies
       SET description = :description,
           description_source = :description_source,
           updated_at = now()
     WHERE id = :company_id
    """
)

# 설명 생성 대상 — 국내 법인 중 설명이 비었거나 폴백으로만 채워진 곳
#
# 폴백(INDUSTRY_FALLBACK)으로 채워진 법인도 다시 대상에 넣는다. 사업보고서 기반 설명이
# 훨씬 좋으므로, 다음 회차에 보고서가 올라오면 갈아끼울 기회를 준다.
SELECT_DESCRIPTION_TARGETS_SQL = text(
    """
    SELECT c.id, c.name, c.corp_code, c.industry_name, s.ticker
      FROM companies AS c
      LEFT JOIN stocks AS s ON s.company_id = c.id AND s.is_active
     WHERE c.country = 'KR'
       AND c.corp_code IS NOT NULL
       AND (c.description IS NULL OR c.description_source = 'INDUSTRY_FALLBACK')
       AND EXISTS (SELECT 1 FROM service_companies AS u WHERE u.company_id = c.id)
     ORDER BY (c.description IS NOT NULL), c.updated_at
     LIMIT :limit
    """
)


def update_industry_names(
    session: Session, profiles: dict[str, tuple[str | None, str | None]]
) -> int:
    """단축코드별 업종명을 법인에 반영한다.

    Args:
        session (Session): DB 세션.
        profiles (dict): 단축코드 → (업종명, 주요제품).

    Returns:
        int: 값이 바뀐 법인 수.
    """

    payload = [
        {"ticker": ticker, "industry_name": industry}
        for ticker, (industry, _) in profiles.items()
        if industry
    ]
    if not payload:
        return 0

    updated = 0
    for row in payload:
        result = session.execute(UPDATE_INDUSTRY_NAME_SQL, row)
        updated += result.rowcount or 0
    return updated


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


def fetch_description_targets(
    session: Session,
    limit: int,
) -> list[tuple[int, str, str, str | None, str | None]]:
    """설명이 필요한 법인 (id, name, corp_code, industry_name, ticker)."""

    rows = session.execute(
        SELECT_DESCRIPTION_TARGETS_SQL,
        {"limit": limit},
    )
    return [(row.id, row.name, row.corp_code, row.industry_name, row.ticker) for row in rows]
