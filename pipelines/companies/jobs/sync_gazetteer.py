"""task: sync_gazetteer — 상장 기업 개체 사전(entity_gazetteer)을 재생성한다.

companies·stocks·company_aliases 가 원천이라 국내 마스터 동기화(companies_sync_master)와
미국 적재(companies_load_us)가 끝날 때마다 돈다. 소비자(triples·events)는
pipelines/common/gazetteer.py 로 이 테이블을 읽는다.
"""

from __future__ import annotations

from collections import Counter

from pipelines.common.clients.postgres import session_scope
from pipelines.common.logging import get_logger
from pipelines.companies.extractors.gazetteer import fetch_alias_candidates
from pipelines.companies.loaders.gazetteer import (
    replace_gazetteer,
    seed_curated_aliases_by_ticker,
)
from pipelines.companies.transformers.gazetteer import build_entries

logger = get_logger(__name__)

# 충돌 로그에 남길 개수
COLLISION_LOG_LIMIT = 20


def run() -> dict[str, int]:
    with session_scope() as session:
        curated = seed_curated_aliases_by_ticker(session)
        entries, collisions = build_entries(fetch_alias_candidates(session))
        previous = replace_gazetteer(session, entries)

    for alias, names in list(collisions.items())[:COLLISION_LOG_LIMIT]:
        logger.warning("개체 사전 별칭 충돌로 제외: %s → %s", alias, ", ".join(names))

    stats = {
        "aliases": len(entries),
        "companies": len({entry.company_id for entry in entries}),
        "collisions": len(collisions),
        "curated_inserted": curated,
    }
    logger.info(
        "개체 사전 재생성 완료: 별칭 %d개(기업 %d개, 이전 %d개), 충돌 제외 %d개, 출처 %s",
        stats["aliases"],
        stats["companies"],
        previous,
        stats["collisions"],
        dict(sorted(Counter(entry.source for entry in entries).items())),
    )

    return stats


if __name__ == "__main__":
    run()
