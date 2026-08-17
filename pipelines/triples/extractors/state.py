from typing import TypedDict

from pipelines.triples.models import (
    Entity,
    RelationFrame,
    Triple,
)


class GraphState(TypedDict, total=False):
    news_id: str
    article: str
    entities: list[Entity]
    relations: list[RelationFrame]
    triples: list[Triple]
    triple_stats: dict
