"""news_companies 테이블 쓰기 (뉴스-기업 연결).

수집 단계가 유일한 적재 주체다. 관련성 필터를 통과해 저장된 기사를, 제목에 나와 같은
기준(GATE 1~3)을 통과한 기업 전부(_linked_companies, 검색 종목 포함)에 연결한다.
"""

from __future__ import annotations

import logging
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


def insert_news_companies(news_id: int, company_ids: list[int]) -> int:
    """뉴스-기업 매핑을 news_companies에 멱등 적재한다. 신규 행 수를 반환한다."""

    unique_ids = sorted({int(company_id) for company_id in company_ids if company_id})

    if not unique_ids:
        return 0

    inserted_count = 0

    with session_scope() as session:
        for company_id in unique_ids:
            result = session.execute(
                INSERT_NEWS_COMPANY_SQL, {"news_id": news_id, "company_id": company_id}
            )
            inserted_count += result.rowcount or 0

    return inserted_count


def linked_company_ids(item: dict[str, Any]) -> list[int]:
    """기사에 연결할 기업. 관련성 필터를 거치지 않은 기사는 검색 대상 기업 하나다."""

    linked = item.get("_linked_companies")
    if linked is not None:
        return [int(company["company_id"]) for company in linked]
    company = item.get("_query_company")
    return [int(company["company_id"])] if company else []


def link_saved_items(items: list[dict[str, Any]]) -> dict[str, int]:
    """이번 런에 저장된 기사를 연결 대상 기업에 연결한다.

    기사 하나의 INSERT 실패는 건너뛰고 세어 돌려준다.
    """

    rows = 0
    failed = 0
    for item in items:
        if not item.get("_news_id") or item.get("_save_action") == "skipped_existing":
            continue
        company_ids = linked_company_ids(item)
        if not company_ids:
            continue
        news_id = int(item["_news_id"])
        try:
            rows += insert_news_companies(news_id, company_ids)
        except Exception as e:
            failed += 1
            logging.error(
                "기업 연결 실패(건너뜀): news_id=%s, %s: %s", news_id, type(e).__name__, e
            )

    return {"rows": rows, "failed": failed}
