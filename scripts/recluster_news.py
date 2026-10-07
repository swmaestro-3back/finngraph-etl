"""저장된 news 만으로 클러스터를 다시 판정한다.

조회 모드(기본): DB 를 바꾸지 않는다. 저장된 기사를 발행 시각순으로 새 판정에 통과시키고
클러스터 크기 분포를 출력한다 — NEWS_CLUSTER_THRESHOLD 를 정할 때 쓴다.

  .venv/bin/python scripts/recluster_news.py --threshold 0.30 --grep 해상변전소
  .venv/bin/python scripts/recluster_news.py --threshold 0.30 --grep 해상변전소 --title-only

적용 모드(--apply): 일회성 백필이다.
1. Neo4j Event 노드·HAS_EVENT 간선 삭제, news_clusters 전부 삭제,
   news.cluster_id·cluster_terms 초기화
2. news 를 발행 시각순으로 새 규칙으로 판정해 기록
3. 후보가 다 찬 클러스터 승격 → 제목 생성 → Event 생성

  .venv/bin/python scripts/recluster_news.py --apply

news_clusters 를 지우면 이슈 타임라인 연결도 모두 사라진다. NEWS_ISSUE_LINK_ENABLED 를 켠 채 두면
스케줄 연결이 lookback 안의 이슈만 승격 순서대로 다시 잇고, 그보다 오래된 이슈는 판정하지 않는다.
그래서 --apply 전에 이 설정을 끄고, 요약까지 끝난 뒤 scripts/backfill_issue_timeline.py --links
--apply 로 전체를 이은 다음 다시 켠다(pipelines/news/README.md "연결을 다시 만들어야 할 때").

판정 입력은 운영(cluster_articles.assign)과 같은 제목 + 본문 리드다. 본문이 없는 옛 행은
제목만으로 판정한다. IDF 는 저장된 기사 전체에서 센다 — 운영(후보 기사만 센다)보다 큰 사건의
토큰이 조금 더 흔하게 잡힌다.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
from collections import Counter

from sqlalchemy import text

from pipelines.common.clients.neo4j import neo4j_database
from pipelines.common.clients.postgres import session_scope
from pipelines.news.config import get_news_settings
from pipelines.news.repositories.postgres.news_clusters import record_assignments
from pipelines.news.transformers.clustering import (
    ClusterAssignment,
    IdfTable,
    assign_online,
    row_documents,
)
from pipelines.news.utils.date_utils import SEOUL_TIMEZONE

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logging.getLogger("langchain_aws").setLevel(logging.WARNING)
log = logging.getLogger("recluster")

SIZE_BUCKETS = ((1, 1), (2, 4), (5, 9), (10, 19), (20, 49), (50, None))


def load_news() -> list[dict]:
    """저장된 기사 전부 (발행 시각순). 발행 시각이 없으면 수집 시각을 쓴다."""

    with session_scope() as session:
        rows = session.execute(
            text(
                """
                SELECT id, title, text, COALESCE(published_at, collected_at, now()) AS published_at
                  FROM news
                 ORDER BY 4, id;
                """
            )
        ).fetchall()

    return [
        {
            "_news_id": int(news_id),
            "title": title or "",
            "text": body or "",
            "published_at": published_at.astimezone(SEOUL_TIMEZONE),
        }
        for news_id, title, body, published_at in rows
    ]


def corpus_idf(documents: list[list[tuple[str, float]]]) -> IdfTable:
    frequency: Counter[str] = Counter()
    for terms in documents:
        frequency.update({token for token, _ in terms})
    return IdfTable(document_frequency=dict(frequency), document_count=len(documents))


def judge(
    items: list[dict], threshold: float, title_only: bool
) -> tuple[list[ClusterAssignment], list]:
    settings = get_news_settings()
    documents, published_ats = row_documents(
        items, 0 if title_only else settings.cluster_lead_chars, settings.cluster_description_weight
    )
    assignments = assign_online(
        documents,
        published_ats,
        [],
        corpus_idf(documents),
        threshold=threshold,
        promote_size=settings.cluster_promote_size,
        window_days=settings.cluster_window_days,
        backward_days=settings.cluster_backward_days,
    )
    return assignments, documents


def _size(assignment: ClusterAssignment) -> int:
    return len(assignment.candidates) + len(assignment.followers)


def report(assignments: list[ClusterAssignment], items: list[dict], grep: str | None) -> None:
    promote_size = get_news_settings().cluster_promote_size
    sizes = [_size(assignment) for assignment in assignments]
    promoted = [size for size in sizes if size >= promote_size]

    log.info("[판정] 기사 %d건 → 클러스터 %d개", len(items), len(assignments))
    for low, high in SIZE_BUCKETS:
        label = f"{low}" if low == high else f"{low}~{high}" if high else f"{low}+"
        count = sum(1 for size in sizes if size >= low and (high is None or size <= high))
        log.info("  크기 %-6s 클러스터 %d개", label, count)
    log.info(
        "[승격] 기준 %d건 이상 클러스터 %d개 (기사 %d건, 전체의 %.1f%%)",
        promote_size,
        len(promoted),
        sum(promoted),
        100.0 * sum(promoted) / max(len(items), 1),
    )

    if not grep:
        return

    log.info("[grep] 제목에 '%s' 가 든 기사가 속한 클러스터", grep)
    for assignment in sorted(assignments, key=_size, reverse=True):
        members = assignment.candidates + assignment.followers
        hits = [index for index in members if grep in items[index]["title"]]
        if not hits:
            continue
        log.info(
            "  크기 %d (그중 '%s' %d건), 시작 %s",
            len(members),
            grep,
            len(hits),
            assignment.first_published_at.date(),
        )
        for index in members[:5]:
            log.info("    - %s", items[index]["title"])


def reset() -> None:
    async def wipe_graph() -> None:
        async with neo4j_database:
            [row] = await neo4j_database.execute(
                "MATCH (e:Event) OPTIONAL MATCH ()-[h:HAS_EVENT]->(e) "
                "RETURN count(DISTINCT e) AS events, count(h) AS edges"
            )
            await neo4j_database.execute("MATCH (e:Event) DETACH DELETE e")
            log.info("[초기화] Neo4j Event %d개, HAS_EVENT %d개 삭제", row["events"], row["edges"])

    asyncio.run(wipe_graph())

    with session_scope() as session:
        session.execute(text("UPDATE news SET cluster_id = NULL, cluster_terms = NULL"))
        deleted = session.execute(text("DELETE FROM news_clusters")).rowcount
    log.info("[초기화] news_clusters %d행 삭제, news.cluster_id·cluster_terms 초기화", deleted)


def apply(assignments: list[ClusterAssignment], items: list[dict], documents: list) -> None:
    from pipelines.events.jobs import generate_events
    from pipelines.news.jobs import cluster_articles

    reset()
    result = record_assignments(
        assignments, items, documents, get_news_settings().cluster_keyword_count
    )
    log.info("[기록] %s", result)
    log.info("[승격·제목] %s", cluster_articles.promote())
    log.info("[Event] %s", generate_events.run())


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--threshold", type=float, help="이번 실행에만 쓸 문턱 (기본: 설정값)")
    parser.add_argument(
        "--title-only", action="store_true", help="본문 리드를 빼고 제목만으로 판정한다"
    )
    parser.add_argument("--grep", help="제목에 이 말이 든 기사가 속한 클러스터를 출력한다")
    parser.add_argument(
        "--apply", action="store_true", help="클러스터와 Event 를 지우고 다시 만든다"
    )
    args = parser.parse_args()

    if args.apply and args.title_only:
        parser.error("--apply 는 운영과 같은 입력(제목 + 본문 리드)으로만 돈다")

    threshold = args.threshold or get_news_settings().cluster_threshold
    source = "제목" if args.title_only else "제목+리드"
    log.info("[설정] threshold=%.2f, 입력=%s", threshold, source)

    items = load_news()
    assignments, documents = judge(items, threshold, args.title_only)
    report(assignments, items, args.grep)

    if args.apply:
        apply(assignments, items, documents)


if __name__ == "__main__":
    main()
