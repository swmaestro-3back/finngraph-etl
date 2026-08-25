from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

# ==============================================================================
# Literals
# ==============================================================================

# Temporal and modal status of a relation (UI labels: 확정 / 예정 / 관측).
# Modality is the stance the text takes: fact, conjecture, possibility, intent.
Tense = Literal[
    "past_or_present_fact",
    "future_or_planned",
    "modal_possibility",
]

# Polarity of a relation (UI labels: 성립 / 부인 / 종료).
# Polarity says whether the relation holds, is denied outright, or has ended.
Polarity = Literal[
    "affirmed",  # the relation holds
    "denied",  # the relation is denied outright
    "terminated",  # the relation held once and has since been cancelled
]


# ==============================================================================
# EntityExtractor
# ==============================================================================


class Entity(BaseModel):
    """An anchor-slot value: a company that matched the gazetteer."""

    text: str = Field(description="기업 표면형 (gazetteer canonical)")


# ==============================================================================
# RelationExtractor
# ==============================================================================


class RawRelation(BaseModel):
    """
    This is a model to prevent hallucination when creating labels by model itself,
    not refering to NER results.
    The field declaration is intended, just to make model go through CoT when giving out the model
        source sentence first, then triples, finally predicate
    """

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
    # List of RawRelations above
    # Just for `structured_output` since it requires pydantic BaseModel type
    frames: list[RawRelation] = Field(
        description=(
            "List of extracted frames, each containing source_sentence, clause, subject, "
            "predicate, object, and (if applicable) item. "
            "One frame per relationship; a single sentence may yield multiple frames, "
            "each repeating the same source_sentence."
        )
    )


class CandidateFrame(BaseModel):
    # The Final Result of RelationExtractor
    # This holds datas that needs to be processed by FrameAnnotator
    subject: Entity = Field(
        description="Must exactly match an entity from the provided NER results."
    )
    object: Entity = Field(
        description="Must exactly match an entity from the provided NER results."
    )
    item: str | None = Field(
        default=None,
        description=(
            "Free-text product phrase copied verbatim from the article. Only set for "
            "predicates with a third 'item' argument in PREDICATE_DICT."
        ),
    )
    predicate: str = Field(
        description="Must be strictly selected from the registered_predicates list."
    )

    # Always populated since only frames passing verbatim validation
    # (whitespace-normalized substring match) are generated.
    source_sentence: str = Field(
        description="The verbatim sentence from the source text that expresses this relationship.",
    )

    # Minimal clause matching predicate direction
    # Keeps directional inference intact to prevent predicate misselection.
    clause: str = Field(
        description=(
            "The relationship restated as a single clause matching the predicate's direction."
        ),
    )


# ==============================================================================
# FrameAnnotator
# ==============================================================================


class RawAnnotation(BaseModel):
    # The field declaration is intended, like RawRelation model
    # first generates evidence, then determine polarity and tense
    frame_index: int = Field(
        description="The 0-based index of the frame being annotated, copied from the input list."
    )
    subject: str = Field(description="Copy the subject of the input frame back EXACTLY as given.")
    predicate: str = Field(
        description="Copy the predicate of the input frame back EXACTLY as given."
    )
    object: str = Field(description="Copy the object of the input frame back EXACTLY as given.")
    evidence: str = Field(
        description=(
            "A single self-contained Korean sentence that a reader can understand on its own: "
            "resolve pronouns to entity names, restore the omitted subject, and pull in the "
            "time, condition, and attribution stated elsewhere in the article. "
            "Never add facts not in the text."
        )
    )
    polarity: Polarity = Field(
        description="Polarity of the relationship: affirmed, denied, or terminated."
    )
    tense: Tense = Field(
        description=(
            "Temporal/modal status of the relationship: "
            "past_or_present_fact, future_or_planned, or modal_possibility."
        )
    )


class RawAnnotationList(BaseModel):
    # List of RawAnnotations above
    # Just for `structured_output` since it requires pydantic BaseModel type
    annotations: list[RawAnnotation] = Field(
        description="One annotation per input frame, in the same order as the input list."
    )


# A relation frame that has been through FrameAnnotator.
class RelationFrame(CandidateFrame):
    # Evidence with context restored, so one line reads on its own. It is generated, so it
    # cannot be matched against the source; grounding checks surface forms and length instead.
    evidence: str = Field(
        description=(
            "A self-contained Korean sentence describing the relationship with restored context."
        ),
    )
    polarity: Polarity = Field(description="affirmed / denied / terminated")
    tense: Tense = Field(description="past_or_present_fact / future_or_planned / modal_possibility")


# ==============================================================================
# TripletBuilder
# ==============================================================================


class Triplet(BaseModel):
    subject: Entity = Field(description="주체")
    predicate: str = Field(description="술어 원형")
    object: Entity = Field(description="객체")
    item: str | None = Field(
        default=None, description="원문 품목 구 (3rd Argument가 필수적인 술어에서만 채워짐)"
    )
    source_sentence: str = Field(description="관계의 근거가 된 원문 문장 (provenance)")
    evidence: str = Field(description="맥락이 복원된 자립적 근거 문장 (UI 노출용)")
    polarity: Polarity = Field(description="affirmed / denied / terminated")
    tense: Tense = Field(description="past_or_present_fact / future_or_planned / modal_possibility")
