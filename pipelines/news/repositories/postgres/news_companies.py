"""news_companies 테이블 쓰기 (뉴스-기업 연결).

수집 단계가 유일한 적재 주체다. 기사를 저장하는 트랜잭션 안에서(news.save_news), 제목에 나와
관련성 판정(is_company·GATE 1~3)을 통과한 기업과 본문에만 나와 엔티티 판정을 통과한 기업
전부(_linked_companies, 검색 종목 포함)에 연결한다. 클러스터·Event·삼중항 단계는 이 연결을
읽기만 한다.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import text

from pipelines.common.clients.postgres import session_scope

INSERT_NEWS_COMPANY_SQL = text(
    """
    INSERT INTO news_companies (news_id, company_id)
    VALUES (:news_id, :company_id)
    ON CONFLICT (news_id, company_id) DO NOTHING;
    """
)


def link_news_companies(session, news_id: int, company_ids: list[int]) -> int:
    """호출자의 트랜잭션 안에서 뉴스-기업 매핑을 멱등 적재한다. 신규 행 수를 반환한다.

    save_news 가 기사 INSERT 와 같은 SAVEPOINT 안에서 부른다 — 기사와 연결이 함께 저장되거나 함께
    버려져, 기업 연결 없는 기사가 남지 않는다.
    """

    unique_ids = sorted({int(company_id) for company_id in company_ids if company_id})

    inserted_count = 0
    for company_id in unique_ids:
        result = session.execute(
            INSERT_NEWS_COMPANY_SQL, {"news_id": news_id, "company_id": company_id}
        )
        inserted_count += result.rowcount or 0

    return inserted_count


def insert_news_companies(news_id: int, company_ids: list[int]) -> int:
    """뉴스-기업 매핑을 news_companies에 멱등 적재한다. 신규 행 수를 반환한다."""

    if not any(company_ids):
        return 0

    with session_scope() as session:
        return link_news_companies(session, news_id, company_ids)


def linked_company_ids(item: dict[str, Any]) -> list[int]:
    """기사에 연결할 기업. 관련성 필터를 거치지 않은 기사는 검색 대상 기업 하나다."""

    linked = item.get("_linked_companies")
    if linked is not None:
        return [int(company["company_id"]) for company in linked]
    company = item.get("_query_company")
    return [int(company["company_id"])] if company else []
