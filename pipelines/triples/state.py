from typing import TypedDict

from pipelines.triples.models import (
    CandidateFrame,
    Entity,
    RelationFrame,
    Triplet,
)


class GraphState(TypedDict, total=False):
    news_id: str
    article: str
    entities: list[Entity]  # Result of EntityExtractor
    candidate_frames: list[CandidateFrame]  # Result of RelationExtractor
    annotated_frames: list[RelationFrame]  # Result of FrameAnnotator
    annotation_stats: dict
    triplets: list[Triplet]  # Result of TripletBuilder
    triplet_stats: dict
