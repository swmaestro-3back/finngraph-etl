from typing import get_args

from pipelines.triples.models import Polarity, RelationFrame, Tense, Triplet
from pipelines.triples.ontology.predicate_dict import PREDICATE_DICT


class TripletBuilder:
    def __init__(self):
        self._predicate_dict: dict = PREDICATE_DICT

    def build(self, relation_frames: list[RelationFrame]) -> list[Triplet]:
        """
        Validate frames against the ontology, convert them to triplets and drop duplicates
        """
        triples: list[Triplet] = []
        seen: set[str] = set()
        for frame in relation_frames:
            triple = self._to_triplet(frame)
            if triple is None:
                continue
            key = triple.model_dump_json()
            if key in seen:
                continue
            seen.add(key)
            triples.append(triple)
        return triples

    def _to_triplet(self, frame: RelationFrame) -> Triplet | None:
        """
        Convert one frame to a triplet, or None if the predicate is not registered

        Argument type validation used to live here. With a single anchor kind (COMPANY) and a
        free-text product slot, every check was vacuously true, so it was removed rather than
        left as code that reads like a guard but guards nothing.
        """
        # Polarity is deliberately not filtered here. A denied or terminated relation keeps its
        # edge and is distinguished by its label, so the UI can show the full history.
        if frame.predicate not in self._predicate_dict:
            return None

        if self._violates_irreflexivity(frame):
            return None

        return Triplet(
            subject=frame.subject,
            predicate=frame.predicate,
            object=frame.object,
            item=frame.item,
            source_sentence=frame.source_sentence,
            evidence=frame.evidence,
            polarity=frame.polarity,
            tense=frame.tense,
        )

    def _violates_irreflexivity(self, frame: RelationFrame) -> bool:
        """
        True if the predicate is marked irreflexive in the ontology and subject == object
        """
        spec = self._predicate_dict.get(frame.predicate, {})
        return bool(spec.get("irreflexive")) and frame.subject.text == frame.object.text

    def stats(self, relation_frames: list[RelationFrame]) -> dict:
        """
        Report per-stage counts for debugging and evaluation
        """
        total = len(relation_frames)
        not_in_dict = sum(1 for f in relation_frames if f.predicate not in self._predicate_dict)
        self_referential = sum(1 for f in relation_frames if self._violates_irreflexivity(f))

        polarity_counts: dict[str, int] = {polarity: 0 for polarity in get_args(Polarity)}
        tense_counts: dict[str, int] = {tense: 0 for tense in get_args(Tense)}
        for frame in relation_frames:
            polarity_counts[frame.polarity] += 1
            tense_counts[frame.tense] += 1

        passed = len(self.build(relation_frames))

        return {
            "total_frames": total,
            "filtered_not_in_dict": not_in_dict,
            "filtered_self_referential": self_referential,
            "polarity_counts": polarity_counts,
            "tense_counts": tense_counts,
            "passed": passed,
        }
