"""저장된 news 만으로 클러스터를 다시 판정한다.

조회 모드(--dry-run): DB 를 바꾸지 않는다. 저장된 기사 전부를 시드 없이 발행 시각순으로 새 판정에
통과시키고 클러스터 크기 분포를 출력한다 — NEWS_CLUSTER_THRESHOLD·가중치를 고를 때 쓴다. IDF 는
저장된 기사 전체에서 즉석 집계한다(운영은 최근 NEWS_CLUSTER_IDF_DAYS 일만 센다).

  .venv/bin/python scripts/recluster_news.py --dry-run
  .venv/bin/python scripts/recluster_news.py --dry-run --threshold 1.2 --text-weights 1,1,1,0.5
  .venv/bin/python scripts/recluster_news.py --dry-run --grep 해상변전소

적용 모드(--apply): V14 전환이나 모델 변경 뒤의 일회성 백필이다. 실제 DAG 가 부르는 job 의 run() 을
순서대로 호출한다.

  reset        Neo4j Event 노드·HAS_EVENT 간선 삭제, news_clusters 전부 삭제(news.cluster_id 는
               FK SET NULL), news 의 term_weights·cluster_score·cluster_features NULL, IDF 두 표
               삭제. 기사 본문과 news_companies 는 보존
  vectorize    vectorize_articles.run()
  rebuild_idf  rebuild_idf_tables(since=가장 오래된 기사 − 1일)   — 저장된 기사 전체가 코퍼스
  assign       assign_clusters.run()
  promote      promote_clusters.run()      (LLM: 클러스터 제목)
  events       generate_events.run()       (Neo4j Event)

  .venv/bin/python scripts/recluster_news.py --apply
  .venv/bin/python scripts/recluster_news.py --apply --from assign   # 중간부터 (reset 없이)

news_clusters 를 지우면 이슈 타임라인 연결도 모두 사라진다. --apply 뒤 요약(news_cluster_articles 의
다음 런)까지 끝나면 news_cluster_articles 를 멈추고 백필 DAG news_backfill_issue_timeline 을
reset=true, links=true, apply=true 로 실행해 전체를 처음부터 이은 다음 다시 켠다
(pipelines/news/README.md "연결을 다시 만들어야 할 때").
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
from pipelines.news.transformers.clustering.document import FIELDS, build_term_weights, with_all
from pipelines.news.transformers.clustering.online import ClusterAssignment, assign_online
from pipelines.news.transformers.clustering.vectorize import IdfTable, IdfTables
from pipelines.news.utils.date_utils import SEOUL_TIMEZONE

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logging.getLogger("langchain_aws").setLevel(logging.WARNING)
log = logging.getLogger("recluster")

STEPS = ("reset", "vectorize", "rebuild_idf", "assign", "promote", "events")
SIZE_BUCKETS = ((1, 1), (2, 4), (5, 9), (10, 19), (20, 49), (50, None))

SELECT_ALL_NEWS_SQL = text(
    """
    SELECT n.id, n.title, n.text, COALESCE(n.published_at, n.collected_at, now()) AS published_at,
           COALESCE(ARRAY_AGG(nc.company_id ORDER BY nc.company_id)
                    FILTER (WHERE nc.company_id IS NOT NULL), '{}') AS company_ids
      FROM news n
      LEFT JOIN news_companies nc ON nc.news_id = n.id
     WHERE n.text IS NOT NULL AND BTRIM(n.text) <> ''
     GROUP BY n.id
     ORDER BY 4, n.id;
    """
)
RESET_SQL = (
    "DELETE FROM news_clusters",  # news.cluster_id 는 FK ON DELETE SET NULL
    "UPDATE news SET term_weights = NULL, cluster_score = NULL, cluster_features = NULL",
    "DELETE FROM news_cluster_idf_terms",
    "DELETE FROM news_cluster_idf_meta",
)


# ---------------------------------------------------------------- 조회 모드


def load_news() -> list[dict]:
    """본문이 있는 저장 기사 전부(발행 시각순)와 연결 기업."""

    with session_scope() as session:
        rows = session.execute(SELECT_ALL_NEWS_SQL).fetchall()
    return [
        {
            "_news_id": int(news_id),
            "title": title or "",
            "text": body or "",
            "published_at": published_at.astimezone(SEOUL_TIMEZONE),
            "company_ids": [int(c) for c in company_ids or []],
        }
        for news_id, title, body, published_at, company_ids in rows
    ]


def corpus_idf(documents: list[dict]) -> IdfTables:
    """문서 전체에서 필드별 df 를 센다. all 은 title∪body 다."""

    frequency: dict[str, Counter[str]] = {field: Counter() for field in FIELDS}
    for document in documents:
        full = with_all(document)
        for field in FIELDS:
            frequency[field].update(set(full[field]))
    return {
        field: IdfTable(document_frequency=dict(frequency[field]), article_count=len(documents))
        for field in FIELDS
    }


def parse_weights(value: str | None, default: tuple[float, ...]) -> tuple[float, ...]:
    if not value:
        return default
    return tuple(float(part) for part in value.split(",") if part.strip())


def judge(items: list[dict], args) -> list[ClusterAssignment]:
    settings = get_news_settings()
    log.info("[토큰화] 기사 %d건 (Kiwi, 몇 분 걸릴 수 있다)", len(items))
    documents = [
        build_term_weights(item["title"], item["text"], item["company_ids"]) for item in items
    ]
    return assign_online(
        documents,
        [item["published_at"] for item in items],
        [],
        corpus_idf(documents),
        text_weights=parse_weights(args.text_weights, settings.cluster_text_weights),
        time_weights=parse_weights(args.time_weights, settings.cluster_time_weights),
        sigma_hours=settings.cluster_time_sigma_hours,
        threshold=args.threshold or settings.cluster_threshold,
        candidate_days=settings.cluster_candidate_days,
        max_span_days=settings.cluster_max_span_days,
    )


def report(assignments: list[ClusterAssignment], items: list[dict], grep: str | None) -> None:
    promote_size = get_news_settings().cluster_promote_size
    sizes = [assignment.article_count for assignment in assignments]
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
    for assignment in sorted(assignments, key=lambda a: a.article_count, reverse=True):
        hits = [index for index in assignment.members if grep in items[index]["title"]]
        if not hits:
            continue
        log.info(
            "  크기 %d (그중 '%s' %d건), 시작 %s",
            assignment.article_count,
            grep,
            len(hits),
            assignment.first_published_at.date(),
        )
        for index in assignment.members[:5]:
            log.info("    - %s", items[index]["title"])


# ---------------------------------------------------------------- 적용 모드


def reset() -> None:
    async def wipe_graph() -> None:
        async with neo4j_database:
            [row] = await neo4j_database.execute(
                "MATCH (e:Event) OPTIONAL MATCH ()-[h:HAS_EVENT]->(e) "
                "RETURN count(DISTINCT e) AS events, count(h) AS edges"
            )
            await neo4j_database.execute("MATCH (e:Event) DETACH DELETE e")
            log.info("[reset] Neo4j Event %d개, HAS_EVENT %d개 삭제", row["events"], row["edges"])

    asyncio.run(wipe_graph())
    with session_scope() as session:
        deleted = session.execute(text(RESET_SQL[0])).rowcount
        for sql in RESET_SQL[1:]:
            session.execute(text(sql))
    log.info("[reset] news_clusters %d행 삭제, news 파생 컬럼·IDF 표 초기화", deleted)


def rebuild_idf() -> None:
    from datetime import timedelta

    from pipelines.news.repositories.postgres.news_cluster_idf import rebuild_idf_tables

    with session_scope() as session:
        earliest = session.execute(
            text("SELECT min(COALESCE(published_at, collected_at)) FROM news")
        ).scalar_one()
    since = earliest - timedelta(days=1)
    log.info("[rebuild_idf] since=%s %s", since.date(), rebuild_idf_tables(since))


def apply(start: str) -> None:
    from pipelines.events.jobs import generate_events
    from pipelines.news.jobs import assign_clusters, promote_clusters, vectorize_articles

    for step in STEPS[STEPS.index(start) :]:
        if step == "reset":
            reset()
        elif step == "vectorize":
            log.info("[vectorize] %s", vectorize_articles.run())
        elif step == "rebuild_idf":
            rebuild_idf()
        elif step == "assign":
            log.info("[assign] %s", assign_clusters.run())
        elif step == "promote":
            log.info("[promote] %s", promote_clusters.run())
        elif step == "events":
            log.info("[events] %s", generate_events.run())


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true", help="DB 를 바꾸지 않고 분포만 출력한다")
    mode.add_argument("--apply", action="store_true", help="클러스터와 Event 를 지우고 다시 만든다")
    parser.add_argument("--from", dest="start", choices=STEPS, default=STEPS[0])
    parser.add_argument("--threshold", type=float, help="dry-run 에만 쓸 Γ 문턱 (기본: 설정값)")
    parser.add_argument("--text-weights", help="dry-run 에만 쓸 title,body,all,entities 가중치")
    parser.add_argument("--time-weights", help="dry-run 에만 쓸 last,mean,first 가중치")
    parser.add_argument("--grep", help="제목에 이 말이 든 기사가 속한 클러스터를 출력한다")
    args = parser.parse_args()

    if args.apply:
        apply(args.start)
        return

    items = load_news()
    assignments = judge(items, args)
    report(assignments, items, args.grep)


if __name__ == "__main__":
    main()
