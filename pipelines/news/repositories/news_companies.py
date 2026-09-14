"""news_companies 테이블 쓰기 (뉴스-기업 연결).

관련성 필터가 고른 주체 종목명을 stocks.name → companies.id 로 해석해 저장된 기사에
연결한다. 행 INSERT 자체는 삼중항 파이프라인과 같은 insert_news_companies 를 쓴다 — 두
경로가 같은 기사에서 같은 종목을 넣으면 UNIQUE 로 한 행이 된다.
"""

from __future__ import annotations

import logging
from typing import Any

from sqlalchemy import text

from pipelines.common.clients.postgres import session_scope
from pipelines.triples.loaders.postgres import insert_news_companies

SELECT_COMPANY_IDS_BY_STOCK_NAME_SQL = text(
    """
    SELECT name, company_id
      FROM stocks
     WHERE name = ANY(:names)
       AND is_active
       AND company_id IS NOT NULL
       AND NOT preferred_stock
       AND NOT etp
       AND NOT spac
     ORDER BY name, id;
    """
)


def fetch_company_ids_by_stock_names(names: list[str]) -> dict[str, int]:
    """종목명으로 company_id 조회"""

    unique_names = sorted({name for name in names if name})

    if not unique_names:
        return {}

    with session_scope() as session:
        rows = session.execute(
            SELECT_COMPANY_IDS_BY_STOCK_NAME_SQL, {"names": unique_names}
        ).fetchall()

    resolved: dict[str, int] = {}
    for name, company_id in rows:
        resolved.setdefault(name, int(company_id))

    return resolved


def link_saved_items(items: list[dict[str, Any]]) -> dict[str, int]:
    """이번 런에 저장된 기사를 LLM 이 주체로 판정한 상장사에 연결한다.

    주체 이름(_subject_names)은 KRX gazetteer 정식명이라 stocks.name 으로 한 번에 해석한다.
    검색 출처 종목은 company_id 를 이미 알고 있어 해석 결과와 무관하게 연결된다. 해석되지 않은
    이름은 경고로 남기고 센다. 기사 하나의 INSERT 실패는 건너뛰고 세어 돌려준다.
    """

    saved = [
        item
        for item in items
        if item.get("_news_id") and item.get("_save_action") != "skipped_existing"
    ]
    names = sorted({name for item in saved for name in item.get("_subject_names", [])})
    resolved = fetch_company_ids_by_stock_names(names) if names else {}
    for item in saved:
        for company in item.get("_query_companies", []):
            resolved.setdefault(company["name"], int(company["company_id"]))

    rows = 0
    unresolved: set[str] = set()
    failed = 0
    for item in saved:
        company_ids: list[int] = []
        for name in item.get("_subject_names", []):
            if name in resolved:
                company_ids.append(resolved[name])
            else:
                unresolved.add(name)
        news_id = int(item["_news_id"])
        try:
            rows += insert_news_companies(news_id, company_ids)
        except Exception as e:
            failed += 1
            logging.error(
                "기업 연결 실패(건너뜀): news_id=%s, %s: %s", news_id, type(e).__name__, e
            )

    if unresolved:
        logging.warning("company_id 로 해석되지 않은 종목명(연결 생략): %s", sorted(unresolved))

    return {"rows": rows, "unresolved_names": len(unresolved), "failed": failed}
