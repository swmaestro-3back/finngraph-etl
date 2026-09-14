"""저장된 news 를 관련성 필터(relevance_single_system 프롬프트)로 다시 판정해 무효 기사를 지운다.

수집 초기 기사들은 LLM 필터 없이 저장돼 시황·주가 반응 기사가 섞여 있다. 제목 + 본문 리드를
스니펫 대신 넣고, 후보 상장사는 gazetteer ∪ news_companies 연결로 잡는다. `valid=false` 인
기사만 삭제한다(news_companies · relation_sources 는 CASCADE). 판정 실패 기사는 남긴다.
삭제 뒤에는 클러스터가 어긋나므로 recluster_news 를 이어서 돈다.

실행: .venv/bin/python scripts/backfill_relevance.py
"""

from __future__ import annotations

import logging
import pathlib
from collections import defaultdict

from sqlalchemy import text

from pipelines.common.clients.postgres import session_scope
from pipelines.news.config import get_news_settings
from pipelines.news.transformers.company_candidates import attach_candidate_companies
from pipelines.news.transformers.relevance_filter import filter_relevant_news

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logging.getLogger("langchain_aws").setLevel(logging.WARNING)
log = logging.getLogger("backfill_relevance")

LEAD_CHARS = 300


def load_items() -> list[dict]:
    with session_scope() as session:
        rows = session.execute(text("SELECT id, title, text FROM news ORDER BY id")).fetchall()
        links = session.execute(
            text(
                """
                SELECT nc.news_id, c.id, c.name
                  FROM news_companies nc
                  JOIN companies c ON c.id = nc.company_id
                """
            )
        ).fetchall()

    companies: dict[int, list[dict]] = defaultdict(list)
    for news_id, company_id, name in links:
        companies[int(news_id)].append({"company_id": int(company_id), "name": name})

    return [
        {
            "_news_id": int(news_id),
            "title": title or "",
            "description": " ".join((body or "").split())[:LEAD_CHARS],
            "_query_companies": companies.get(int(news_id), []),
        }
        for news_id, title, body in rows
    ]


def main() -> None:
    items = load_items()
    attach_candidate_companies(items)
    log.info(
        "[대상] 기사 %d건 (후보 없음 %d건은 LLM 없이 무관 처리)",
        len(items),
        sum(1 for item in items if not item["_candidate_companies"]),
    )

    result = filter_relevant_news(
        items, max_concurrency=get_news_settings().news_llm_max_concurrency
    )
    log.info(
        "[판정] 통과 %d / 무효 %d / 무관 %d / 실패 %d",
        len(result.passed),
        len(result.invalid),
        len(result.irrelevant),
        len(result.failed),
    )
    for item in result.invalid[:15]:
        log.info("  무효 예시: %s", item["title"][:70])

    invalid_ids = [item["_news_id"] for item in result.invalid]
    if invalid_ids:
        with session_scope() as session:
            deleted = session.execute(
                text("DELETE FROM news WHERE id = ANY(:ids)"), {"ids": invalid_ids}
            ).rowcount
        log.info("[삭제] news %d행 (news_companies · relation_sources 는 CASCADE)", deleted)

    import runpy

    runpy.run_path(str(pathlib.Path(__file__).with_name("recluster_news.py")), run_name="__main__")


if __name__ == "__main__":
    main()
