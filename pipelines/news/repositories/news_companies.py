"""news_companies 테이블 쓰기 (뉴스-기업 연결).

관련성 필터를 통과해 저장된 기사를 검색 대상 기업에 연결한다. 행 INSERT 자체는 삼중항
파이프라인과 같은 insert_news_companies 를 쓴다 — 두 경로가 같은 기사에서 같은 기업을 넣으면
UNIQUE 로 한 행이 된다.
"""

from __future__ import annotations

import logging
from typing import Any

from pipelines.triples.loaders.postgres import insert_news_companies


def link_saved_items(items: list[dict[str, Any]]) -> dict[str, int]:
    """이번 런에 저장된 기사를 검색 대상 기업(_query_company)에 연결한다.

    기사 하나의 INSERT 실패는 건너뛰고 세어 돌려준다.
    """

    rows = 0
    failed = 0
    for item in items:
        if not item.get("_news_id") or item.get("_save_action") == "skipped_existing":
            continue
        company = item.get("_query_company")
        if not company:
            continue
        news_id = int(item["_news_id"])
        try:
            rows += insert_news_companies(news_id, [int(company["company_id"])])
        except Exception as e:
            failed += 1
            logging.error(
                "기업 연결 실패(건너뜀): news_id=%s, %s: %s", news_id, type(e).__name__, e
            )

    return {"rows": rows, "failed": failed}
