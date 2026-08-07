from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

EntityLabel = Literal[
    "COMPANY",
    "GOVERNMENT",
    "COUNTRY",
    "COMMODITY",
    "PRODUCT",
]

# 확정된 사실만 최종 트리플로 남기기 위한 시제·양태 분류.
TenseLabel = Literal[
    # 과거 또는 현재 확정 사실형
    "past_or_present_fact",
    # 미래 계획형
    "future_or_planned",
    # 미래 가능성형
    "modal_possibility",
]


# NER
class Entity(BaseModel):
    text: str = Field(description="개체명 표면형")
    label: EntityLabel = Field(description="개체명 태그")


# LLM이 NER 결과와 무관하게 subject/object/item에 라벨을 지어낼 수 있으므로
# 할루시네이션을 방지용 모델 생성
# 추후 Label 매핑해서 반환
class RawRelation(BaseModel):
    # Bedrock structured output(json_schema)은 모든 object 스키마에
    # additionalProperties:false를 요구한다. extra="forbid"를 주면 Pydantic이
    # model_json_schema()에 이 키를 자동으로 넣어준다.
    model_config = ConfigDict(extra="forbid")

    # source_sentence/clause를 predicate보다 먼저 선언: structured output은 필드 선언
    # 순서대로 채워지므로, "근거 문장을 먼저 찾고 → 절로 재구성하고 → 그 다음에 술어를 고르는"
    # 순서가 프레임마다 강제된다. 기존 전역 clauses 필드의 CoT 역할을 프레임 단위로 옮긴 것.
    # source_sentence는 이후 로직에서 원문 대조로 검증한다.
    source_sentence: str = Field(
        description=(
            "The sentence from the original text that expresses this relationship, "
            "copied VERBATIM — character-for-character, no paraphrasing, no omission. "
            "This is the evidence for the frame."
        )
    )
    clause: str = Field(
        description=(
            "The source_sentence rewritten as a short standalone clause that directly matches "
            "the chosen predicate's direction: resolve pronouns to their referent entity, "
            "restate counterparty phrasing in the predicate's direction "
            "(e.g. 'A buys X from B' becomes 'B supplies X to A'), "
            "and keep only the single relationship this frame expresses."
        )
    )
    predicate: str = Field(
        description="Must be strictly selected from the registered_predicates list."
    )
    is_negated: bool = Field(
        description="True if the relationship involves a negative expression, otherwise False."
    )
    tense: TenseLabel = Field(
        description=(
            "Temporal/modal status of the relationship: "
            "past_or_present_fact, future_or_planned, or modal_possibility."
        )
    )
    subject: str = Field(description="Must exactly match an entity from the provided NER results.")
    object: str = Field(description="Must exactly match an entity from the provided NER results.")
    item: str | None = Field(
        default=None,
        description=(
            "Must exactly match an entity from the provided NER results."
            "Leave null if the predicate has no item argument, or if the text does not name "
            "a specific item and the item argument is marked [optional]."
        ),
    )


class RawRelationList(BaseModel):
    model_config = ConfigDict(extra="forbid")

    frames: list[RawRelation] = Field(
        description=(
            "List of extracted frames, each containing source_sentence, clause, subject, "
            "predicate, object, and (if applicable) item. "
            "One frame per relationship; a single sentence may yield multiple frames, "
            "each repeating the same source_sentence."
        )
    )


# Relation (관계 후보 프레임)
class RelationFrame(BaseModel):
    subject: Entity = Field(
        description="Must exactly match an entity from the provided NER results."
    )
    object: Entity = Field(
        description="Must exactly match an entity from the provided NER results."
    )
    # PREDICATE_DICT_NARY에서 3번째 argument(item)를 등록한 술어(공급하다 등)에만 채워지는
    # 선택적 필드. 해당 술어라도 본문에 품목이 명시되지 않으면 None으로 남는다.
    item: Entity | None = Field(
        default=None,
        description="Only set for predicates with a third 'item' argument in PREDICATE_DICT_NARY.",
    )
    predicate: str = Field(
        description="Must be strictly selected from the registered_predicates list."
    )
    is_negated: bool = Field(
        description="True if the relationship involves a negative expression, otherwise False."
    )
    tense: TenseLabel = Field(
        description=(
            "Temporal/modal status of the relationship: "
            "past_or_present_fact, future_or_planned, or modal_possibility."
        )
    )
    # 이 프레임의 근거가 된 원문 문장 (provenance). 원문 대조 검증(공백 정규화 후
    # substring 매칭)을 통과한 프레임만 생성되므로 항상 채워진다.
    source_sentence: str = Field(
        description="The verbatim sentence from the source text that expresses this relationship.",
    )


# Triplet (확정 삼중항)
class Triplet(BaseModel):
    subject: Entity = Field(description="주체")
    predicate: str = Field(description="술어 원형")
    object: Entity = Field(description="객체")
    item: Entity | None = Field(
        default=None, description="품목 (3rd Argument가 필수적인 술어에서만 채워짐)"
    )
    source_sentence: str = Field(description="관계의 근거가 된 원문 문장 (provenance)")
