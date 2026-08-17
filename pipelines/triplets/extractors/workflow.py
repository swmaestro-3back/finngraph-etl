import asyncio

from langgraph.graph import END, StateGraph

from pipelines.triplets.extractors.nodes.entity_extractor import EntityExtractor
from pipelines.triplets.extractors.nodes.relation_extractor import RelationExtractor
from pipelines.triplets.extractors.nodes.triplet_builder import TripletBuilder
from pipelines.triplets.extractors.state import GraphState
from pipelines.triplets.models import Entity


class GraphRunner:
    def __init__(self):
        self.entity_extractor = EntityExtractor()
        self._relation_extractor = RelationExtractor()
        self._triplet_builder = TripletBuilder()
        self._graph = self._compile_graph(
            self.entity_extractor,
            self._relation_extractor,
            self._triplet_builder,
        )

    def _compile_graph(
        self,
        entity_extractor: EntityExtractor,
        relation_extractor: RelationExtractor,
        triplet_builder: TripletBuilder,
    ):

        async def normalize_article(state: GraphState) -> dict:
            """
            gazetteer를 토대로 등록된 엔티티 명칭 표준화
            """
            normalized_article = await asyncio.to_thread(
                entity_extractor.normalize, state["article"]
            )
            return {"article": normalized_article}

        async def extract_entities(state: GraphState) -> dict:
            """
            gazetteer를 바탕으로 엔티티 추출
            """
            gazetteer_entities = await asyncio.to_thread(
                entity_extractor.extract_entities, state["article"]
            )

            seen: set[tuple[str, str]] = set()
            deduped: list[Entity] = []
            for entity in gazetteer_entities:
                key = (entity.text, entity.label)
                if key in seen:
                    continue
                seen.add(key)
                deduped.append(entity)
            return {"entities": deduped}

        async def extract_relations(state: GraphState) -> dict:
            relations = await relation_extractor.label(state["article"], state["entities"])
            return {"relations": relations}

        async def build_triplets(state: GraphState) -> dict:
            relations = state["relations"]
            return {
                "triplets": triplet_builder.filter(relations),
                "triplet_stats": triplet_builder.stats(relations),
            }

        workflow = StateGraph(GraphState)

        workflow.add_node("normalizer", normalize_article)
        workflow.add_node("entity_extractor", extract_entities)
        workflow.add_node("relation_extractor", extract_relations)
        workflow.add_node("triplet_builder", build_triplets)

        workflow.set_entry_point("normalizer")

        workflow.add_edge("normalizer", "entity_extractor")
        workflow.add_edge("entity_extractor", "relation_extractor")
        workflow.add_edge("relation_extractor", "triplet_builder")
        workflow.add_edge("triplet_builder", END)

        return workflow.compile()

    async def ainvoke(self, news_id: str, article: str) -> GraphState:
        """LangGraph 워크플로우를 비동기로 실행한다."""
        final_state = await self._graph.ainvoke(
            GraphState(
                news_id=news_id,
                article=article,
            )
        )
        return final_state
