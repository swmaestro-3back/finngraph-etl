"""법인 마스터 참조 조회.

references/ 는 이 파이프라인이 **읽기만 하는** 참조 데이터 조회 전용 계층이다. 쓰기가
있는 loaders/ 와 갈라 둔 이유는, 여기 있는 테이블의 소유권이 다른 도메인에 있음을
디렉토리 이름만으로 드러내기 위해서다 — companies 는 companies 도메인
(sync_dart_corp_codes)이 채우고 disclosures 는 절대 쓰지 않는다.
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


# 계약상대 역매칭용 별칭 — 출처를 가리지 않고 전량 읽는다.
SELECT_COUNTERPARTY_ALIASES_SQL = text(
    """
    SELECT a.alias, a.company_id
      FROM company_aliases AS a
    """
)


def fetch_counterparty_aliases(session: Session) -> list[tuple[str, int]]:
    """역매칭용 별칭 (별칭, company_id) 전량. job 시작 시 한 번만 부른다."""

    rows = session.execute(SELECT_COUNTERPARTY_ALIASES_SQL)
    return [(row.alias, row.company_id) for row in rows]
