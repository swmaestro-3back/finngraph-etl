from langchain_aws import ChatBedrockConverse

from pipelines.common.clients.bedrock import ensure_bedrock_token
from pipelines.common.config import get_settings
from pipelines.triples.models import (
    CandidateFrame,
    Entity,
    RawRelation,
    RawRelationList,
)
from pipelines.triples.ontology.predicate_dict import PREDICATE_DICT, REGISTERED_PREDICATES
from pipelines.triples.prompts.relation_extraction import PROMPT
from pipelines.triples.utils.text import normalize_whitespace


def _product_item_slot(predicate: str) -> dict | None:
    """Return the item argument spec when the predicate declares a product slot.

    Slot kind is derived from the ontology rather than hardcoded, so adding a predicate
    with an item argument later needs no change here.
    """
    entry = PREDICATE_DICT.get(predicate)
    if entry is None:
        return None
    arguments = list(entry["arguments"].values())
    if len(arguments) < 3:
        return None
    item_argument = arguments[2]
    return item_argument if item_argument["types"] == ["PRODUCT"] else None


def build_candidate_frames(
    raw_frames: list[RawRelation],
    entities: list[Entity],
    text: str,
) -> list[CandidateFrame]:
    """
    Validate raw LLM output and assemble grounded relation candidates

    Pure function, kept apart from the LLM call so it can be unit tested without an API key
    """

    # Anchor slots are the only ones checked against NER output. Product slots are free text.
    entity_texts = {entity.text.strip() for entity in entities}
    normalized_text = normalize_whitespace(text)

    frames: list[CandidateFrame] = []
    for raw_frame in raw_frames:
        # Guardrail for predicate hallucination
        if raw_frame.predicate not in REGISTERED_PREDICATES:
            continue

        # Discard if subject or object is not in extracted entities
        subject_text = raw_frame.subject.strip()
        object_text = raw_frame.object.strip()
        if subject_text not in entity_texts or object_text not in entity_texts:
            continue

        # Product slot: accept any span the article actually contains. The substring check is
        # the only defence against an invented item, since there is no dictionary to match.
        item: str | None = None
        item_slot = _product_item_slot(raw_frame.predicate)
        if item_slot is not None:
            item_text = (raw_frame.item or "").strip()
            if item_text and normalize_whitespace(item_text) in normalized_text:
                item = item_text
            if item_slot["required"] and item is None:
                continue

        # Discard if source_sentence is not in the article
        source_sentence = raw_frame.source_sentence.strip()
        if not source_sentence or normalize_whitespace(source_sentence) not in normalized_text:
            continue

        clause = raw_frame.clause.strip()

        frames.append(
            CandidateFrame(
                predicate=raw_frame.predicate,
                subject=Entity(text=subject_text),
                object=Entity(text=object_text),
                item=item,
                source_sentence=source_sentence,
                clause=clause,
            )
        )

    # Deduplicate frames
    seen: set[str] = set()
    deduped: list[CandidateFrame] = []
    for frame in frames:
        key = frame.model_dump_json()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(frame)

    return deduped


class RelationExtractor:
    def __init__(self):
        ensure_bedrock_token()
        settings = get_settings()
        self._model = ChatBedrockConverse(
            model=settings.bedrock_chat_model,
            region_name=settings.bedrock_region,
            temperature=0,
        )

        self._chain = PROMPT | self._model.with_structured_output(
            schema=RawRelationList,
            method="json_schema",
        )

    async def extract(
        self,
        text: str,
        entities: list[Entity],
    ) -> list[CandidateFrame]:
        """
        Extract relation candidates from the article
        """
        entity_lines = [f"- {entity.text}" for entity in entities]
        entities_str = "\n".join(entity_lines) if entity_lines else "없음"

        # The predicate dictionary and few-shot examples already live in PROMPT's fixed system
        # prefix, so text and entities are the only per-request variables.
        invoke_input = {
            "text": text,
            "entities": entities_str,
        }

        # The call is network I/O, so await it rather than blocking the event loop
        result = await self._chain.ainvoke(invoke_input)
        return build_candidate_frames(result.frames, entities, text)
