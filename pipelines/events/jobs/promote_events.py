"""news_clusters → Neo4j Event 승격 job (news_pipeline DAG 의 promote_events task).

후보 조회(RDB) → 존재 조회(Neo4j) → 분기 → 갱신(전량, LLM 없음) → 생성(후보 추출 →
상한 → LLM → 검증 → 쓰기) → 집계. 클러스터 하나가 실패 단위다. RDB 에는 아무것도 쓰지 않는다.

EntityExtractor(비공개 gazetteer)와 EventGenerator(비공개 프롬프트)는 run() 안에서 지연
import 한다 — 헬퍼 함수들이 CI 에서 import 되게 하기 위해서다.
"""

from __future__ import annotations

import asyncio
from datetime import date, timedelta
from typing import Any

from pipelines.common.clients.neo4j import neo4j_database
from pipelines.common.logging import get_logger
from pipelines.common.utils.time import now_kst
from pipelines.events.config import get_event_settings
from pipelines.events.loaders.neo4j import create_event, refresh_events
from pipelines.events.models import ClusterCandidate, EventRecord, EventRefresh, MemberArticle
from pipelines.events.references.graph import fetch_existing_event_ids
from pipelines.events.references.rdb import (
    fetch_cluster_members,
    fetch_cluster_news_ids,
    fetch_promotable_clusters,
)
from pipelines.events.transformers.candidates import extract_company_candidates
from pipelines.events.transformers.generator import validate_draft
from pipelines.events.transformers.planner import plan_actions
from pipelines.events.transformers.source_text import member_date, member_source_text

logger = get_logger(__name__)

STAT_KEYS = (
    "scanned",
    "created",
    "created_without_edges",
    "refreshed",
    "refresh_failed",
    "skipped_no_candidates",
    "skipped_over_limit",
    "failed",
)

DatedTexts = list[tuple[date | None, str]]
# (클러스터, 멤버, LLM 입력, 후보)
Prepared = tuple[ClusterCandidate, list[MemberArticle], DatedTexts, list[str]]


def build_create_input(
    members: list[MemberArticle], extractor: Any, lead_chars: int
) -> tuple[DatedTexts, list[str]]:
    """멤버 기사들을 (보도일, 정규화 텍스트) 목록과 후보 정규명으로 바꾼다."""

    texts = [member_source_text(member, lead_chars) for member in members]
    canonical_texts, candidates = extract_company_candidates(texts, extractor)
    dates = [member_date(member) for member in members]
    dated_texts = list(zip(dates, canonical_texts, strict=True))
    return dated_texts, candidates


def select_for_llm(prepared: list[Prepared], limit: int) -> tuple[list[Prepared], int, int]:
    """후보 0 을 빼고 앞에서 limit 개만 남긴다. (선택, 후보0 수, 상한초과 수)."""

    with_candidates = [item for item in prepared if item[3]]
    skipped_no_candidates = len(prepared) - len(with_candidates)
    eligible = with_candidates[:limit]
    skipped_over_limit = len(with_candidates) - len(eligible)
    return eligible, skipped_no_candidates, skipped_over_limit


async def create_one(
    cluster: ClusterCandidate,
    members: list[MemberArticle],
    dated_texts: DatedTexts,
    candidates: list[str],
    generator: Any,
    semaphore: asyncio.Semaphore,
    title_max_chars: int,
) -> str:
    """LLM → 검증 → 쓰기.

    어느 단계든 실패하면 이 클러스터만 건너뛴다. 노드가 없으니 다음 런에 재시도된다.
    """

    try:
        async with semaphore:
            draft = await generator.draft(dated_texts, candidates)
        draft = validate_draft(draft, candidates, title_max_chars)
        record = EventRecord(
            **cluster.model_dump(),
            news_ids=sorted(member.news_id for member in members),
            title=draft.title,
            companies=draft.companies,
        )
        edges = await create_event(record)
    except Exception as e:
        logger.warning(
            "Event 생성 실패(건너뜀): cluster_id=%s, %s: %s",
            cluster.cluster_id,
            type(e).__name__,
            e,
        )
        return "failed"

    return "created" if edges > 0 else "created_without_edges"


def summarize_stats(stats: dict[str, int]) -> dict[str, int]:
    """집계 불변식을 검사한다. created_without_edges 는 created 의 부분집합이라 합에 없다."""

    accounted = (
        stats["created"]
        + stats["refreshed"]
        + stats["refresh_failed"]
        + stats["skipped_no_candidates"]
        + stats["skipped_over_limit"]
        + stats["failed"]
    )
    if stats["scanned"] != accounted:
        raise ValueError(f"집계 불일치: scanned={stats['scanned']} != 합={accounted} ({stats})")
    return stats


async def _run(extractor: Any, generator: Any) -> dict[str, int]:
    settings = get_event_settings()
    stats = dict.fromkeys(STAT_KEYS, 0)

    # 1. 후보 (RDB)
    since = now_kst() - timedelta(days=settings.scan_days)
    clusters = fetch_promotable_clusters(settings.min_size, since)
    stats["scanned"] = len(clusters)
    if not clusters:
        return stats

    async with neo4j_database:
        # 2~3. 존재 조회, 분기
        existing = await fetch_existing_event_ids([c.cluster_id for c in clusters])
        to_create, to_refresh = plan_actions(clusters, existing)

        # 4. 갱신 — 전량, LLM 없음
        if to_refresh:
            news_ids = fetch_cluster_news_ids([c.cluster_id for c in to_refresh])
            refreshes = [
                EventRefresh(**c.model_dump(), news_ids=news_ids.get(c.cluster_id, []))
                for c in to_refresh
            ]
            try:
                stats["refreshed"] = await refresh_events(refreshes)
                stats["refresh_failed"] = len(refreshes) - stats["refreshed"]
            except Exception as e:
                logger.error(
                    "Event 갱신 배치 실패: %d건, %s: %s",
                    len(refreshes),
                    type(e).__name__,
                    e,
                )
                stats["refresh_failed"] = len(refreshes)

        # 5~6. 생성 — 후보 추출 뒤 상한
        members_by_cluster = fetch_cluster_members([c.cluster_id for c in to_create])
        prepared: list[Prepared] = []
        for cluster in to_create:
            members = members_by_cluster.get(cluster.cluster_id, [])
            dated_texts, candidates = build_create_input(members, extractor, settings.lead_chars)
            prepared.append((cluster, members, dated_texts, candidates))
        eligible, stats["skipped_no_candidates"], stats["skipped_over_limit"] = select_for_llm(
            prepared, settings.max_items_per_run
        )

        # 7~9. LLM → 검증 → 쓰기
        semaphore = asyncio.Semaphore(max(settings.llm_max_concurrency, 1))
        outcomes = await asyncio.gather(
            *(
                create_one(
                    cluster,
                    members,
                    dated_texts,
                    candidates,
                    generator,
                    semaphore,
                    settings.title_max_chars,
                )
                for cluster, members, dated_texts, candidates in eligible
            )
        )
        for outcome in outcomes:
            if outcome == "failed":
                stats["failed"] += 1
                continue
            stats["created"] += 1
            if outcome == "created_without_edges":
                stats["created_without_edges"] += 1

    return stats


def run() -> dict[str, int]:
    from pipelines.events.transformers.generator import EventGenerator
    from pipelines.triples.nodes.entity_extractor import EntityExtractor

    stats = summarize_stats(asyncio.run(_run(EntityExtractor(), EventGenerator())))

    print("\n" + "=" * 70)
    print("뉴스 클러스터 → Event 승격 결과")
    print("=" * 70)
    print(f"- 후보 클러스터 {stats['scanned']}개")
    print(
        f"- 생성 {stats['created']}개 (간선 없음 {stats['created_without_edges']}개), "
        f"실패 {stats['failed']}개"
    )
    print(f"- 갱신 {stats['refreshed']}개, 갱신 실패 {stats['refresh_failed']}개")
    print(
        f"- 건너뜀: 후보 없음 {stats['skipped_no_candidates']}개, "
        f"상한 초과 {stats['skipped_over_limit']}개"
    )
    print("=" * 70)

    return stats


if __name__ == "__main__":
    run()
