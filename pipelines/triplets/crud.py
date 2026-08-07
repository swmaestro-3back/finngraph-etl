# Neo4j CRUD
from __future__ import annotations

from collections import defaultdict
from typing import get_args

from pipelines.common.neo4j import neo4j_database
from pipelines.triplets.graph.models import Entity, EntityLabel, Triplet
from pipelines.triplets.graph.ontology.predicate_dict import PREDICATE_DICT

# NER 태그를 Neo4j Label 태그로 변환
# (COMPANY → Company, COUNTRY → Country)
_TYPE_TO_LABEL: dict[EntityLabel, str] = {
    label: "".join(part.capitalize() for part in label.split("_"))
    for label in get_args(EntityLabel)
}

# 하나의 삼중항관계를 두 개의 삼중항관계로 분해할때 사용
_ITEM_DECOMPOSITION: dict[str, tuple[str, str]] = {
    "SUPPLIES_TO": ("SUPPLIES", "SUPPLIED_TO"),
    "EXPORTS_TO": ("EXPORTS", "EXPORTED_TO"),
}

# 간선 누적 최대 개수
# 넘어갈 경우 FIFO로 동작
_MAX_PROVENANCE = 10


def _binary_edges(triplet: Triplet) -> list[tuple[Entity, str, Entity]]:
    """하나의 Triplet을 (subject, rel, object) 이항 엣지 목록으로 분해한다.

    item이 없으면 (subject)-[predicate]->(object) 단일 엣지로,
    item이 있으면 _ITEM_DECOMPOSITION 규칙에 따라 (subject-item), (item-object)
    두 개의 이항 엣지로 분해한다. 분해된 엣지도 각각 하나의 (subject, rel, object)
    트리플로 취급하므로, item은 첫 엣지의 object이자 둘째 엣지의 subject가 된다.

    각 엔드포인트는 Entity(text/label) 그대로 보존하므로, 호출부에서 Neo4j Label로
    매핑하거나(_edge_specs) EntityLabel을 그대로 사용할 수 있다(news_relations 적재).
    """

    if triplet.item is None:
        # predicate는 FPDF가 PREDICATE_DICT에 등록된 술어에만 트리플을 만들어주므로 항상
        # 화이트리스트 안이지만, 관계 타입으로 직접 삽입되는 값이라 한 번 더 방어적으로 검증한다.
        if triplet.predicate not in PREDICATE_DICT:
            return []
        return [(triplet.subject, triplet.predicate, triplet.object)]

    decomposition = _ITEM_DECOMPOSITION.get(triplet.predicate)
    # item이 있으나 분해 규칙이 없는 술어면 item을 버리고 이항 관계로만 저장한다.
    if decomposition is None:
        if triplet.predicate not in PREDICATE_DICT:
            return []
        return [(triplet.subject, triplet.predicate, triplet.object)]

    rel_subject_item, rel_item_object = decomposition
    return [
        (triplet.subject, rel_subject_item, triplet.item),
        (triplet.item, rel_item_object, triplet.object),
    ]


def _edge_specs(triplet: Triplet) -> list[tuple[str, str, str, str, str]]:
    """하나의 Triplet을 (subject_label, subject_name, rel, object_label, object_name)
    Neo4j 엣지 목록으로 변환한다. NER Label을 Neo4j Label로 매핑한다.
    """

    return [
        (_TYPE_TO_LABEL[subject.label], subject.text, rel, _TYPE_TO_LABEL[obj.label], obj.text)
        for subject, rel, obj in _binary_edges(triplet)
    ]


async def upsert_triplets(news_id: str, triplets: list[Triplet]) -> None:
    """추출된 삼중항관계를 Neo4j에 반영

    1. 3rd Argument인 Item을 가진 술어는 하나의 삼중항관계를 두 개의 삼중항관계로 분해
    2. 동일한 news_id에서 동일한 삼중항관계가 추가되는 경우에는 배척
    3. 동일한 삼중항관계에 대해서는 총 _MAX_PROVENANCE 개수만큼만 저장가능
       (그 이후부터는 FIFO 전략으로 관리)
    4. 하나의 간선이 추가될때, first_mentioned_at, last_mentioned_at, mention_count등
       메타데이터가 저장된다.
    """

    # (subject_label, rel, object_label) 시그니처가 같은 엣지끼리 묶어 UNWIND 한 번으로 MERGE 한다.
    # 같은 뉴스 안에서 여러 문장이 동일 간선으로 수렴하면 첫 문장만 대표 근거로 남긴다.
    # 쿼리의 is_dup 가드는 쿼리 시작 시점의 news_ids만 보므로(플래너가 WITH와 SET 사이에
    # Eager를 끼워 모든 row의 is_dup을 먼저 평가한다) 배치 내 중복은 여기서 걸러야 한다.
    grouped: dict[tuple[str, str, str], list[dict]] = defaultdict(list)
    seen_edges: set[tuple[str, str, str, str, str]] = set()
    for triplet in triplets:
        for subject_label, subject_name, rel, object_label, object_name in _edge_specs(triplet):
            edge_key = (subject_label, subject_name, rel, object_label, object_name)
            if edge_key in seen_edges:
                continue
            seen_edges.add(edge_key)
            grouped[(subject_label, rel, object_label)].append(
                {
                    "subject_name": subject_name,
                    "object_name": object_name,
                    "source_sentence": triplet.source_sentence,
                }
            )

    for (subject_label, rel, object_label), rows in grouped.items():
        await neo4j_database.execute(
            f"""
            UNWIND $rows AS row
            MERGE (s:{subject_label} {{name: row.subject_name}})
            MERGE (o:{object_label} {{name: row.object_name}})
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
                    WHEN at_cap THEN r.source_sentences[1..] + row.source_sentence
                    ELSE coalesce(r.source_sentences, []) + row.source_sentence END,
                r.mentioned_ats = CASE
                    WHEN is_dup THEN r.mentioned_ats
                    WHEN at_cap THEN r.mentioned_ats[1..] + date()
                    ELSE coalesce(r.mentioned_ats, []) + date() END
            """,
            {"rows": rows, "news_id": news_id, "max_provenance": _MAX_PROVENANCE},
        )


async def _fetch_company_tickers(names: list[str]) -> dict[str, str]:
    """KRX 상장 기업명 목록에 대한 ticker를 Neo4j에서 한 번에 조회한다.

    삼중항 추출로 만들어지는 Company 노드는 name만 가지지만, 정규화 단계에서 표면형을
    KRX 사전 정식명으로 재작성하므로 시드된 (:Company:KOSPI|KOSDAQ {name, ticker}) 노드와
    name이 일치한다. 상장사가 아니면 매핑에 담기지 않아 code는 NULL로 남는다.
    """

    if not names:
        return {}

    records = await neo4j_database.execute(
        """
        UNWIND $names AS name
        MATCH (c:Company {name: name})
        WHERE c:KOSPI OR c:KOSDAQ
        RETURN c.name AS name, c.ticker AS ticker
        """,
        {"names": names},
    )
    return {record["name"]: record["ticker"] for record in records}


async def build_news_relation_rows(triplets: list[Triplet]) -> list[dict]:
    """삼중항을 news_relations 테이블 행(dict) 목록으로 변환한다.

    1. Neo4j 적재와 동일하게 _binary_edges로 이항 엣지 분해 (item 술어는 2개로 분해)
    2. 동일 (subject_name, relation, object_name) 엣지는 배치 내에서 중복 제거
       (news_relations의 UNIQUE 제약과 동일한 키)
    3. COMPANY 엔티티는 Neo4j에서 ticker를 조회해 code로 채운다. 그 외 타입(COUNTRY ISO 등)은
       현재 NULL로 두고 이후 백필한다.
    """

    edges: list[tuple[Entity, str, Entity]] = []
    seen: set[tuple[str, str, str]] = set()
    for triplet in triplets:
        for subject, rel, obj in _binary_edges(triplet):
            edge_key = (subject.text, rel, obj.text)
            if edge_key in seen:
                continue
            seen.add(edge_key)
            edges.append((subject, rel, obj))

    # COMPANY 엔드포인트 이름을 모아 한 번의 쿼리로 ticker 조회
    company_names = sorted(
        {
            endpoint.text
            for subject, _, obj in edges
            for endpoint in (subject, obj)
            if endpoint.label == "COMPANY"
        }
    )
    name_to_ticker = await _fetch_company_tickers(company_names)

    def _code(entity: Entity) -> str | None:
        return name_to_ticker.get(entity.text) if entity.label == "COMPANY" else None

    return [
        {
            "subject_name": subject.text,
            "subject_type": subject.label,
            "subject_code": _code(subject),
            "relation": rel,
            "object_name": obj.text,
            "object_type": obj.label,
            "object_code": _code(obj),
        }
        for subject, rel, obj in edges
    ]
