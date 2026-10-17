"""법인 마스터 참조 조회.

companies 는 companies 도메인(sync_dart_corp_codes)이 채우고 disclosures 는 읽기만 한다.
"""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.orm import Session

from pipelines.disclosures.models import CorpMasterRow

# sync_dart_corp_codes 가 DART corpCode.xml 전량(상장+비상장 약 11.8만)을 적재해 둔다.
SELECT_CORP_MASTER_SQL = text(
    """
    SELECT id, corp_code, name, ticker
      FROM companies
     WHERE corp_code IS NOT NULL
    """
)


def fetch_corp_master(session: Session) -> list[CorpMasterRow]:
    """corp_code 가 있는 법인 전량. job 시작 시 한 번만 부른다."""

    rows = session.execute(SELECT_CORP_MASTER_SQL)
    return [
        CorpMasterRow(
            company_id=row.id,
            corp_code=row.corp_code,
            name=row.name,
            ticker=row.ticker,
        )
        for row in rows
    ]
