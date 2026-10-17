from langgraph.graph import END, StateGraph

from pipelines.triples.models import Entity
from pipelines.triples.nodes.frame_annotator import FrameAnnotator
from pipelines.triples.nodes.relation_extractor import RelationExtractor
from pipelines.triples.nodes.triplet_builder import TripletBuilder
from pipelines.triples.state import GraphState

# A relation needs two endpoint companies, so fewer distinct companies than this cannot yield a
# triplet. Counted by company, not by surface form: two spellings of one company are two entities
# but still one endpoint. The job checks it before invoking the graph, so no LLM call is spent.
MIN_COMPANIES = 2


def merge_stats(triplet_stats: dict, annotation_stats: dict) -> dict:
    """Merge TripletBuilder statistics with FrameAnnotator drop counts.

    Drop counts are captured during annotation, whereas stats() only measures surviving frames.
    Merging both is required to compute the drop rate.
    """

    merged = dict(triplet_stats)
    merged["dropped_annotation_mismatch"] = annotation_stats.get("dropped_annotation_mismatch", 0)
    merged["dropped_evidence_grounding"] = annotation_stats.get("dropped_evidence_grounding", 0)
    return merged


class GraphRunner:
    def __init__(self):
        self._relation_extractor = RelationExtractor()
        self._frame_annotator = FrameAnnotator()
        self._triplet_builder = TripletBuilder()
        self._graph = self._compile_graph(
            self._relation_extractor,
            self._frame_annotator,
            self._triplet_builder,
        )

    def _compile_graph(
        self,
        relation_extractor: RelationExtractor,
        frame_annotator: FrameAnnotator,
        triplet_builder: TripletBuilder,
    ):

        async def extract_relations(state: GraphState) -> dict:
            """Extract relation frame candidates by refering to base ontology"""
            candidate_frames = await relation_extractor.extract(state["article"], state["entities"])
            return {"candidate_frames": candidate_frames}

        async def annotate_frames(state: GraphState) -> dict:
            """Define evidence/polarity/tense refering to the article"""
            annotated_frames, annotation_stats = await frame_annotator.annotate(
                state["article"], state["candidate_frames"]
            )
            return {
                "annotated_frames": annotated_frames,
                "annotation_stats": annotation_stats,
            }

        async def build_triplets(state: GraphState) -> dict:
            annotated_frames = state["annotated_frames"]
            return {
                "triplets": triplet_builder.build(annotated_frames),
                "triplet_stats": merge_stats(
                    triplet_builder.stats(annotated_frames),
                    state.get("annotation_stats", {}),
                ),
            }

        workflow = StateGraph(GraphState)

        workflow.add_node("extract_relations", extract_relations)
        workflow.add_node("annotate_frames", annotate_frames)
        workflow.add_node("build_triplets", build_triplets)

        # Entities arrive with the article: the news pipeline already matched and verified them
        workflow.set_entry_point("extract_relations")
        workflow.add_edge("extract_relations", "annotate_frames")
        workflow.add_edge("annotate_frames", "build_triplets")
        workflow.add_edge("build_triplets", END)

        return workflow.compile()

    async def ainvoke(self, news_id: str, article: str, entities: list[Entity]) -> GraphState:

        final_state = await self._graph.ainvoke(
            GraphState(
                news_id=news_id,
                article=article,
                entities=entities,
            )
        )
        return final_state
