from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.orm import Session

from pipelines.companies.models import CompanySyncResult

ALIAS_SOURCE_KIS_MASTER = "KIS_MASTER"

# 상장 표시 켜기
MARK_LISTED_SQL = text(
    """
    UPDATE companies AS c
       SET is_listed = true,
           delisted_at = NULL,
           name = s.name,
           updated_at = now()
      FROM stocks AS s
     WHERE c.ticker = s.ticker
       AND s.is_active
       AND BTRIM(s.name) <> ''
       AND (
             c.is_listed IS DISTINCT FROM true
          OR c.delisted_at IS NOT NULL
          OR c.name IS DISTINCT FROM s.name
       )
    """
)

# 상장 표시 끄기 (상장폐지)
MARK_DELISTED_SQL = text(
    """
    UPDATE companies AS c
       SET is_listed = false,
           delisted_at = COALESCE(c.delisted_at, CURRENT_DATE),
           updated_at = now()
     WHERE c.country = 'KR'
       AND c.ticker IS NOT NULL
       AND c.is_listed
       AND NOT EXISTS (
             SELECT 1
               FROM stocks AS s
              WHERE s.ticker = c.ticker
                AND s.is_active
           )
    """
)

# stocks.company_id 연결
# 상장 종목의 단축코드인, companies.ticker로 매핑
LINK_STOCKS_TO_COMPANIES_SQL = text(
    """
    UPDATE stocks AS s
       SET company_id = c.id,
           updated_at = now()
      FROM companies AS c
     WHERE c.ticker = s.ticker
       AND c.delisted_at IS NULL
       AND s.is_active
       AND s.company_id IS DISTINCT FROM c.id
    """
)

# 종목명을 별칭으로 적재
# companies_alias에 종목명 저장
INSERT_COMPANY_ALIASES_SQL = text(
    """
    INSERT INTO company_aliases (company_id, alias, lang, source)
    SELECT s.company_id, s.name, 'ko', :source
      FROM stocks AS s
     WHERE s.is_active
       AND s.company_id IS NOT NULL
       AND BTRIM(s.name) <> ''
    ON CONFLICT (alias, company_id) DO NOTHING
    """
)


# 국내 상장 법인 상태 조회
def sync_listed_companies(session: Session) -> CompanySyncResult:

    listed = session.execute(MARK_LISTED_SQL)
    delisted = session.execute(MARK_DELISTED_SQL)
    linked = session.execute(LINK_STOCKS_TO_COMPANIES_SQL)
    aliases = session.execute(
        INSERT_COMPANY_ALIASES_SQL,
        {"source": ALIAS_SOURCE_KIS_MASTER},
    )

    return CompanySyncResult(
        listed_count=listed.rowcount or 0,
        delisted_count=delisted.rowcount or 0,
        linked_count=linked.rowcount or 0,
        alias_count=aliases.rowcount or 0,
    )
