"""트리플관계 Neo4j 적재.

읽기(ticker 참조 조회)는 `references/graph.py`, 엣지 변환 규칙은 `edges.py`에 있다.
"""

from __future__ import annotations

from collections import defaultdict

from pipelines.common.clients.neo4j import neo4j_database
from pipelines.triples.edges import NODE_LABEL, edge_of
from pipelines.triples.models import Triplet

# 간선 누적 최대 개수
# 넘어갈 경우 FIFO로 동작
_MAX_PROVENANCE = 10


async def upsert_triplets(news_id: str, triplets: list[Triplet]) -> list[dict[str, str]]:
    """추출된 트리플관계를 Neo4j에 반영하고, 간선별 (elementId, evidence)를 반환한다.

    1. 동일한 news_id에서 동일한 트리플관계가 추가되는 경우에는 배척
    2. 동일한 트리플관계에 대해서는 총 _MAX_PROVENANCE 개수만큼만 저장가능
       (그 이후부터는 FIFO 전략으로 관리)
    3. 하나의 간선이 추가될때, first_mentioned_at, last_mentioned_at, mention_count등
       메타데이터가 저장된다.
    4. 언급 단위 메타데이터(source_sentence, evidence, polarity, tense, item)는
       provenance 리스트와 같은 FIFO 규칙으로 나란히 누적된다. item이 없는 언급은
       빈 문자열로 채워 리스트 간 인덱스 정합을 유지한다.
    5. 반환값은 relation_source 적재용 [{"neo4j_id": 간선 elementId, "reason": evidence}]
       목록이다. 이미 존재하던 간선(is_dup 포함)도 elementId는 반환한다.
    """

    # predicate(관계 타입)가 같은 엣지끼리 묶어 UNWIND 한 번으로 MERGE 한다.
    # 같은 뉴스 안에서 여러 문장이 동일 간선으로 수렴하면 첫 문장만 대표 근거로 남긴다.
    # 쿼리의 is_dup 가드는 쿼리 시작 시점의 news_ids만 보므로(플래너가 WITH와 SET 사이에
    # Eager를 끼워 모든 row의 is_dup을 먼저 평가한다) 배치 내 중복은 여기서 걸러야 한다.
    grouped: dict[str, list[dict]] = defaultdict(list)
    seen_edges: set[tuple[str, str, str]] = set()
    for triplet in triplets:
        edge = edge_of(triplet)
        if edge is None:
            continue
        if edge in seen_edges:
            continue
        seen_edges.add(edge)
        subject_name, rel, object_name = edge
        grouped[rel].append(
            {
                "subject_name": subject_name,
                "object_name": object_name,
                "source_sentence": triplet.source_sentence,
                "evidence": triplet.evidence,
                "polarity": triplet.polarity,
                "tense": triplet.tense,
                "item": triplet.item or "",
            }
        )

    edge_rows: list[dict[str, str]] = []

    for rel, rows in grouped.items():
        records = await neo4j_database.execute(
            f"""
            UNWIND $rows AS row
            MERGE (s:{NODE_LABEL} {{name: row.subject_name}})
            MERGE (o:{NODE_LABEL} {{name: row.object_name}})
            MERGE (s)-[r:{rel}]->(o)
            WITH r, row,
                 $news_id IN coalesce(r.news_ids, []) AS is_dup,
                 size(coalesce(r.news_ids, [])) >= $max_provenance AS at_cap
            SET r.first_mentioned_at = coalesce(r.first_mentioned_at, date()),
                r.last_mentioned_at = CASE WHEN is_dup THEN coalesce(r.last_mentioned_at, date())
                                           ELSE date() END,
                r.mention_count = coalesce(r.mention_count, 0)
                    + (CASE WHEN is_dup THEN 0 ELSE 1 END),
                r.news_ids = CASE
                    WHEN is_dup THEN r.news_ids
                    WHEN at_cap THEN r.news_ids[1..] + $news_id
                    ELSE coalesce(r.news_ids, []) + $news_id END,
                r.source_sentences = CASE
                    WHEN is_dup THEN r.source_sentences
                    WHEN at_cap THEN coalesce(r.source_sentences, [])[1..] + row.source_sentence
                    ELSE coalesce(r.source_sentences, []) + row.source_sentence END,
                r.mentioned_ats = CASE
                    WHEN is_dup THEN r.mentioned_ats
                    WHEN at_cap THEN coalesce(r.mentioned_ats, [])[1..] + date()
                    ELSE coalesce(r.mentioned_ats, []) + date() END,
                r.evidences = CASE
                    WHEN is_dup THEN r.evidences
                    WHEN at_cap THEN coalesce(r.evidences, [])[1..] + row.evidence
                    ELSE coalesce(r.evidences, []) + row.evidence END,
                r.polarities = CASE
                    WHEN is_dup THEN r.polarities
                    WHEN at_cap THEN coalesce(r.polarities, [])[1..] + row.polarity
                    ELSE coalesce(r.polarities, []) + row.polarity END,
                r.tenses = CASE
                    WHEN is_dup THEN r.tenses
                    WHEN at_cap THEN coalesce(r.tenses, [])[1..] + row.tense
                    ELSE coalesce(r.tenses, []) + row.tense END,
                r.items = CASE
                    WHEN is_dup THEN r.items
                    WHEN at_cap THEN coalesce(r.items, [])[1..] + row.item
                    ELSE coalesce(r.items, []) + row.item END
            RETURN elementId(r) AS neo4j_id, row.evidence AS reason
            """,
            {"rows": rows, "news_id": news_id, "max_provenance": _MAX_PROVENANCE},
        )
        edge_rows.extend(
            {"neo4j_id": record["neo4j_id"], "reason": record["reason"]} for record in records
        )

    return edge_rows
