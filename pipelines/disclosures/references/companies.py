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


# 계약상대 역매칭용 별칭 — sync_dart_corp_codes 가 채운다.
#
#     DART      DART 법인명. 상장사의 companies.name 은 KIS 종목명("현대차")으로 매일
#               덮여 법인명("현대자동차")이 마스터에서 사라지므로 별칭이 유일한 보존처다.
#     CURATED   사명변경·통용표기 수동 시드(CURATED_ALIASES). 현행 등록부 어디에도 없는
#               표기라 사람이 관리한다.
#
# KIS_MASTER 는 쓰지 않는다 — 종목명은 마스터명과 같고 단축코드는 계약상대 표기로
# 나오지 않으며, 향후 다른 출처의 별칭이 섞이면 품질을 보증할 수 없다.
SELECT_COUNTERPARTY_ALIASES_SQL = text(
    """
    SELECT a.alias, a.company_id
      FROM company_aliases AS a
      JOIN companies AS c ON c.id = a.company_id
     WHERE a.source IN ('DART', 'CURATED')
       AND c.corp_code IS NOT NULL
    """
)


def fetch_counterparty_aliases(session: Session) -> list[tuple[str, int]]:
    """역매칭용 별칭 (별칭, company_id) 전량. job 시작 시 한 번만 부른다."""

    rows = session.execute(SELECT_COUNTERPARTY_ALIASES_SQL)
    return [(row.alias, row.company_id) for row in rows]
