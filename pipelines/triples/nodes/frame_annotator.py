from langchain_aws import ChatBedrockConverse

from pipelines.common.clients.bedrock import ensure_bedrock_token
from pipelines.common.config import get_settings
from pipelines.triples.models import (
    CandidateFrame,
    RawAnnotation,
    RawAnnotationList,
    RelationFrame,
)
from pipelines.triples.prompts.frame_annotation import PROMPT
from pipelines.triples.utils.text import normalize_whitespace

_MAX_EVIDENCE_LENGTH = 200


def format_candidates(candidates: list[CandidateFrame]) -> str:
    """
    Format candidate frames as the numbered text block for prompt.
    The [n] prefix serves as the frame index for LLM echo validation.
    """

    lines: list[str] = []
    for index, candidate in enumerate(candidates):
        item_text = candidate.item if candidate.item is not None else "없음"
        lines.append(
            f"[{index}] subject={candidate.subject.text} | predicate={candidate.predicate} "
            f"| object={candidate.object.text} | item={item_text}"
        )
        lines.append(f"    source_sentence: {candidate.source_sentence}")
        lines.append(f"    clause: {candidate.clause}")
    return "\n".join(lines)


def _is_grounded(evidence: str, candidate: CandidateFrame) -> bool:
    """
    Verify if the generated evidence grounds the candidate frame.
    Checks surface-form inclusion of subject/object/item within the maximum length limit.
    """

    if not evidence or len(evidence) > _MAX_EVIDENCE_LENGTH:
        return False

    required = [candidate.subject.text, candidate.object.text]
    if candidate.item is not None:
        required.append(candidate.item)

    normalized_evidence = normalize_whitespace(evidence)
    return all(normalize_whitespace(term) in normalized_evidence for term in required)


def merge_annotations(
    candidates: list[CandidateFrame],
    annotations: list[RawAnnotation],
) -> tuple[list[RelationFrame], dict[str, int]]:
    """
    Merge annotations into their frames, dropping any frame that fails to line up

    Filling a default instead would turn a denial into a fact, which costs far more
    than losing one relation.
    """

    annotation_by_index: dict[int, RawAnnotation] = {}
    for annotation in annotations:
        if annotation.frame_index in annotation_by_index:
            continue
        annotation_by_index[annotation.frame_index] = annotation

    frames: list[RelationFrame] = []
    dropped_mismatch = 0
    dropped_grounding = 0

    for index, candidate in enumerate(candidates):
        annotation = annotation_by_index.get(index)

        # Drop if annotation is missing or out-of-bounds
        if annotation is None:
            dropped_mismatch += 1
            continue

        # Echo validation: catch LLM index misalignment
        if (
            annotation.subject.strip() != candidate.subject.text
            or annotation.predicate.strip() != candidate.predicate
            or annotation.object.strip() != candidate.object.text
        ):
            dropped_mismatch += 1
            continue

        evidence = annotation.evidence.strip()
        if not _is_grounded(evidence, candidate):
            dropped_grounding += 1
            continue

        frames.append(
            RelationFrame(
                subject=candidate.subject,
                object=candidate.object,
                item=candidate.item,
                predicate=candidate.predicate,
                source_sentence=candidate.source_sentence,
                clause=candidate.clause,
                evidence=evidence,
                polarity=annotation.polarity,
                tense=annotation.tense,
            )
        )

    stats = {
        "dropped_annotation_mismatch": dropped_mismatch,
        "dropped_evidence_grounding": dropped_grounding,
    }
    return frames, stats


class FrameAnnotator:
    def __init__(self):
        ensure_bedrock_token()
        settings = get_settings()
        self._model = ChatBedrockConverse(
            model=settings.bedrock_chat_model,
            region_name=settings.bedrock_region,
            temperature=0,
        )

        self._chain = PROMPT | self._model.with_structured_output(
            schema=RawAnnotationList,
            method="json_schema",
        )

    async def annotate(
        self,
        text: str,
        candidates: list[CandidateFrame],
    ) -> tuple[list[RelationFrame], dict[str, int]]:
        """
        Annotate every candidate frame extracted by frame_annotator
        """

        if not candidates:
            return [], {"dropped_annotation_mismatch": 0, "dropped_evidence_grounding": 0}

        invoke_input = {
            "text": text,
            "frames": format_candidates(candidates),
        }

        result = await self._chain.ainvoke(invoke_input)
        return merge_annotations(candidates, result.annotations)
