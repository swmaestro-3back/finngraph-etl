"""신규 Event 노드 생성 job (events_pipeline 의 generate_events task).

후보 클러스터 중 Neo4j 에 Event 가 없는 것만 골라 멤버 기사 → gazetteer 후보 → LLM(제목·
당사자) → 검증 → 노드·간선 생성 순으로 처리한다. 클러스터 하나가 실패 단위다. RDB 에는
아무것도 쓰지 않는다. `scanned` 는 이 task 의 몫(Event 가 없는 클러스터 수)이다.

sync_events 와 같은 DAG 런에서 병렬로 돈다. 둘 다 scan_promotable 로 자기 몫을 고르므로
task 사이에 XCom 이 없다.

EntityExtractor(비공개 gazetteer)와 EventGenerator(비공개 프롬프트)는 run() 안에서 지연
import 하고 팩토리로 넘긴다 — 헬퍼가 CI 에서 import 되게 하고, 처리할 클러스터가 없는
런에서는 Bedrock 클라이언트를 만들지 않기 위해서다.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import date, timedelta
from typing import Any

from pipelines.common.clients.neo4j import neo4j_database
from pipelines.common.logging import get_logger
from pipelines.common.utils.time import now_kst
from pipelines.events.config import get_event_settings
from pipelines.events.loaders.neo4j import create_event
from pipelines.events.models import ClusterCandidate, EventRecord, MemberArticle
from pipelines.events.references.rdb import fetch_cluster_members
from pipelines.events.references.scan import scan_promotable
from pipelines.events.stats import check_total
from pipelines.events.transformers.candidates import extract_company_candidates
from pipelines.events.transformers.generator import validate_draft
from pipelines.events.transformers.source_text import member_date, member_source_text

logger = get_logger(__name__)

STAT_KEYS = (
    "scanned",
    "created",
    "created_without_edges",
    "skipped_no_candidates",
    "skipped_over_limit",
    "failed",
)

Factory = Callable[[], Any]
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
    """created_without_edges 는 created 의 부분집합이라 합에 없다."""

    return check_total(
        stats, "scanned", ("created", "skipped_no_candidates", "skipped_over_limit", "failed")
    )


async def _run(extractor_factory: Factory, generator_factory: Factory) -> dict[str, int]:
    settings = get_event_settings()
    stats = dict.fromkeys(STAT_KEYS, 0)
    since = now_kst() - timedelta(days=settings.scan_days)

    async with neo4j_database:
        # 1. 후보 스캔 — Event 가 없는 클러스터만 이 task 의 몫이다
        to_create, _ = await scan_promotable(settings.min_size, since)
        stats["scanned"] = len(to_create)
        if not to_create:
            return stats

        # 2. 멤버 텍스트 → 후보 추출 → 상한 (LLM 전에 걸러 슬롯을 낭비하지 않는다)
        extractor = extractor_factory()
        members_by_cluster = fetch_cluster_members([c.cluster_id for c in to_create])
        prepared: list[Prepared] = []
        for cluster in to_create:
            members = members_by_cluster.get(cluster.cluster_id, [])
            dated_texts, candidates = build_create_input(members, extractor, settings.lead_chars)
            prepared.append((cluster, members, dated_texts, candidates))
        eligible, stats["skipped_no_candidates"], stats["skipped_over_limit"] = select_for_llm(
            prepared, settings.max_items_per_run
        )
        if not eligible:
            return stats

        # 3. LLM → 검증 → 쓰기 (클러스터별 격리, 동시성 상한)
        generator = generator_factory()
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

    # 둘 다 인자 없는 생성자라 클래스 자체가 팩토리다. 생성은 _run 안에서 필요할 때 일어난다.
    stats = summarize_stats(asyncio.run(_run(EntityExtractor, EventGenerator)))

    print("\n" + "=" * 70)
    print("Event 생성 결과 (generate_events)")
    print("=" * 70)
    print(f"- Event 없는 후보 클러스터 {stats['scanned']}개")
    print(
        f"- 생성 {stats['created']}개 (간선 없음 {stats['created_without_edges']}개), "
        f"실패 {stats['failed']}개"
    )
    print(
        f"- 건너뜀: 후보 없음 {stats['skipped_no_candidates']}개, "
        f"상한 초과 {stats['skipped_over_limit']}개"
    )
    print("=" * 70)

    return stats


if __name__ == "__main__":
    run()
