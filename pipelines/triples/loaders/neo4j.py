"""간선 요약의 Neo4j 적재.

근거 상세(evidence, source_sentence, polarity, ...)는 relation_sources 원장(RDB)에
있고, 간선에는 그래프 탐색·필터에 쓰는 요약값(카운트·기간·공시 근거 배열)만 남는다.
요약값의 원천은
entities_relations 뷰(references/rdb.py의 fetch_edge_summaries)라, 간선 속성은
언제든 뷰 기준으로 재작성해 복구할 수 있다.

뉴스(jobs/extract)와 공시(disclosures/jobs/link_supply_contracts) 파이프라인이
같은 writer를 쓴다 — 간선 어휘(predicate 화이트리스트)의 소유자가 triples라 여기 둔다.

읽기(ticker 참조 조회)는 `references/graph.py`, 엣지 변환 규칙은 `edges.py`에 있다.
"""

from __future__ import annotations

from collections import defaultdict

from pipelines.common.clients.neo4j import neo4j_database
from pipelines.triples.edges import NODE_LABEL
from pipelines.triples.ontology.predicate_dict import PREDICATE_DICT


async def sync_edge_summaries(summaries: list[dict]) -> int:
    """간선 요약 행들을 Neo4j에 반영하고, 반영된 간선 수를 반환한다.

    summaries 항목 형식은 fetch_edge_summaries 반환값:
    {subject_name, relation, object_name,
     news_mention_count, disclosure_count, first_mentioned_at, last_mentioned_at,
     disclosure_rcept_nos, disclosure_items, news_ids, news_items}

    노드는 정규명으로 MERGE한다 — 시드된 노드(name, ticker)와 정규명이 일치하고,
    미시드 기업은 name만 가진 노드가 생겼다가 시드 시 병합된다. 값을 통째로 SET하는
    전량 덮어쓰기라 몇 번을 다시 돌려도 뷰와 같은 상태로 수렴한다(멱등).
    """

    if not summaries:
        return 0

    # relation은 Cypher 관계 타입으로 직접 삽입되므로 화이트리스트로 방어한다.
    grouped: dict[str, list[dict]] = defaultdict(list)
    for summary in summaries:
        relation = summary["relation"]
        if relation not in PREDICATE_DICT:
            continue
        grouped[relation].append(summary)

    synced_count = 0

    for relation, rows in grouped.items():
        records = await neo4j_database.execute(
            f"""
            UNWIND $rows AS row
            MERGE (s:{NODE_LABEL} {{name: row.subject_name}})
            MERGE (o:{NODE_LABEL} {{name: row.object_name}})
            MERGE (s)-[r:{relation}]->(o)
            SET r.news_mention_count = row.news_mention_count,
                r.disclosure_count = row.disclosure_count,
                r.first_mentioned_at = row.first_mentioned_at,
                r.last_mentioned_at = row.last_mentioned_at,
                r.disclosure_rcept_nos = row.disclosure_rcept_nos,
                r.disclosure_items = row.disclosure_items,
                r.news_ids = row.news_ids,
                r.news_items = row.news_items
            RETURN count(r) AS synced
            """,
            {"rows": rows},
        )
        synced_count += records[0]["synced"] if records else 0

    return synced_count


async def delete_edges(keys: list[tuple[str, str, str]]) -> int:
    """근거가 모두 사라진 간선을 삭제하고, 삭제한 간선 수를 반환한다.

    원장(entities_relations 뷰)에 더 이상 행이 없는 키는 sync_edge_summaries가
    갱신할 수 없으므로, 호출자가 뷰 결과에서 빠진 키를 여기로 넘겨 정리한다.
    노드는 남긴다 — 다른 간선이나 시드가 여전히 쓸 수 있다.
    """

    grouped: dict[str, list[dict]] = defaultdict(list)
    for subject_name, relation, object_name in keys:
        if relation not in PREDICATE_DICT:
            continue
        grouped[relation].append({"subject_name": subject_name, "object_name": object_name})

    deleted_count = 0

    for relation, rows in grouped.items():
        records = await neo4j_database.execute(
            f"""
            UNWIND $rows AS row
            MATCH (s:{NODE_LABEL} {{name: row.subject_name}})
                  -[r:{relation}]->
                  (o:{NODE_LABEL} {{name: row.object_name}})
            DELETE r
            RETURN count(r) AS deleted
            """,
            {"rows": rows},
        )
        deleted_count += records[0]["deleted"] if records else 0

    return deleted_count
