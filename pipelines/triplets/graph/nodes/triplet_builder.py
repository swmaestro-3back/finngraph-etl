from typing import get_args

from pipelines.triplets.graph.models import Entity, RelationFrame, TenseLabel, Triplet
from pipelines.triplets.graph.ontology.predicate_dict import PREDICATE_DICT


class TripletBuilder:
    def __init__(self):
        self._predicate_dict: dict = PREDICATE_DICT

    def filter(self, relation_frames: list[RelationFrame]) -> list[Triplet]:
        """
        삼중항관계 중복제거
        """
        triples: list[Triplet] = []
        seen: set[str] = set()
        for frame in relation_frames:
            triple = self._filter_triplets(frame)
            if triple is None:
                continue
            key = triple.model_dump_json()
            if key in seen:
                continue
            seen.add(key)
            triples.append(triple)
        return triples

    def _filter_triplets(self, frame: RelationFrame) -> Triplet | None:
        # 조건 1: 부정 표현 프레임 제거
        if frame.is_negated:
            return None

        # 조건 2: 술어 사전 미등록 술어 제거
        entry = self._predicate_dict.get(frame.predicate)
        if entry is None:
            return None

        # PREDICATE_DICT_NARY는 argument를 subject/object(/item) 순서로 등록해두므로,
        # dict 순서를 그대로 역할 순서로 사용한다 (첫 번째=subject, 두 번째=object, 세 번째=item).
        arg_names = list(entry["arguments"].keys())
        agent_key, counterparty_key = arg_names[0], arg_names[1]
        item_key = arg_names[2] if len(arg_names) > 2 else None

        # 조건 3: subject(행위자) 개체명 타입이 술어의 subject argument 타입 목록에 없으면 제거.
        # 목록이 빈 리스트면 타입 제약이 없는 술어이므로 통과시킨다.
        agent_types = entry["arguments"][agent_key]["types"]
        if agent_types and frame.subject.label not in agent_types:
            return None

        # 조건 4: object(피행위자) 개체명 타입이 술어의 object argument 타입 목록에 없으면 제거
        # (위와 동일한 규칙)
        counterparty_types = entry["arguments"][counterparty_key]["types"]
        if counterparty_types and frame.object.label not in counterparty_types:
            return None

        # item은 optional argument이므로, 술어에 item argument가 없거나 타입이 맞지 않으면
        # (subject/object와 달리) 프레임 전체를 버리지 않고 item만 비워서 통과시킨다.
        item: Entity | None = None
        if item_key is not None and frame.item is not None:
            item_types = entry["arguments"][item_key]["types"]
            if not item_types or frame.item.label in item_types:
                item = frame.item

        return Triplet(
            subject=frame.subject,
            predicate=frame.predicate,
            object=frame.object,
            item=item,
            source_sentence=frame.source_sentence,
        )

    def stats(self, relation_frames: list[RelationFrame]) -> dict:
        """삼중항 필터링 통계를 반환한다 (디버깅·평가용)."""
        total = len(relation_frames)
        negated = sum(1 for f in relation_frames if f.is_negated)
        not_in_dict = sum(
            1
            for f in relation_frames
            if not f.is_negated and f.predicate not in self._predicate_dict
        )
        # 전체 프레임의 tense(시제·양태)별 개수 분포. 필터링과 무관하게 모든 프레임을 집계하며,
        # 한 번도 등장하지 않은 tense도 0으로 남도록 TenseLabel의 모든 라벨을 미리 채워둔다.
        tense_counts: dict[str, int] = {tense: 0 for tense in get_args(TenseLabel)}
        for frame in relation_frames:
            tense_counts[frame.tense] += 1
        passed = len(self.filter(relation_frames))

        return {
            "total_frames": total,
            "filtered_negated": negated,
            "filtered_not_in_dict": not_in_dict,
            # tense별 프레임 수 (past_or_present_fact / future_or_planned / modal_possibility)
            "tense_counts": tense_counts,
            "passed": passed,
        }
