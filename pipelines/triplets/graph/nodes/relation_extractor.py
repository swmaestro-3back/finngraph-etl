import re

from langchain_google_genai import ChatGoogleGenerativeAI

from pipelines.common.config import get_settings
from pipelines.triplets.graph.models import (
    Entity,
    RawRelationList,
    RelationFrame,
)
from pipelines.triplets.graph.ontology.predicate_dict import PREDICATE_DICT
from pipelines.triplets.graph.prompts.relation_extraction import PROMPT

settings = get_settings()

# LLM이 프롬프트 지시를 어기고 미등록 술어를 지어냈을 때를 걸러내기 위한 검증용 술어 집합.
_REGISTERED_PREDICATES: set[str] = set(PREDICATE_DICT.keys())


# source_sentence 검증용 정규화: LLM이 verbatim 지시를 어기고 공백/개행만 미묘하게 바꿔도
# 멀쩡한 근거 문장이 탈락하지 않도록, 모든 공백을 제거한 뒤 substring으로 비교한다.
def _normalize_for_match(s: str) -> str:
    return re.sub(r"\s+", "", s)


class RelationExtractor:
    def __init__(self):

        self._model = ChatGoogleGenerativeAI(
            model=settings.gemini_model, temperature=0, google_api_key=settings.google_api_key
        )

        self._chain = PROMPT | self._model.with_structured_output(
            schema=RawRelationList,
            method="json_schema",
        )

    async def label(
        self,
        text: str,
        entities: list[Entity],
    ) -> list[RelationFrame]:
        entity_lines = [f"- {e.text} ({e.label})" for e in entities]
        entities_str = "\n".join(entity_lines) if entity_lines else "없음"

        # 그라운딩 검증용 조회 테이블: 이후 subject/object/item 텍스트가 실제 NER 결과에
        # 존재하는지, 존재한다면 라벨이 무엇인지 확인하는 유일한 소스가 된다
        entity_label_by_text = {e.text.strip(): e.label for e in entities}

        # 술어 사전·few-shot은 PROMPT의 고정 prefix(system 지시)에 이미 들어있으므로,
        # 요청마다 채우는 변수는 text/entities만이다.
        invoke_input = {
            "text": text,
            "entities": entities_str,
        }

        # LLM 호출은 네트워크 I/O라 ainvoke로 await 해서 이벤트 루프를 막지 않는다.
        result = await self._chain.ainvoke(invoke_input)
        normalized_text = _normalize_for_match(text)

        frames: list[RelationFrame] = []
        for raw_frame in result.frames:
            # 술어 사전 미등록 술어는 즉시 제외
            # (LLM이 프롬프트 지시를 어기고 새 술어를 지어냈을 가능성에 대한 가드레일)
            if raw_frame.predicate not in _REGISTERED_PREDICATES:
                continue

            # 그라운딩 체크: subject/object 텍스트가 NER 결과에 없으면(None) 할루시네이션으로
            # 간주하고 프레임 전체를 버린다. 라벨은 LLM이 아니라 항상 NER 결과에서만 가져온다.
            subject_label = entity_label_by_text.get(raw_frame.subject.strip())
            object_label = entity_label_by_text.get(raw_frame.object.strip())
            if subject_label is None or object_label is None:
                continue

            # item이 할루시네이션이어도 프레임 전체를 버리지 않고 item만 누락시킨다
            # (subject/object로 이루어진 관계 자체는 여전히 유효하기 때문). item이 mandatory인
            # 술어인데 결과적으로 None이 되는 경우, 현재 FPDF는 이를 별도로 걸러내지 않고
            # item=None인 채로 통과시킨다 — required 강제는 아직 프롬프트 레벨(위 [Core
            # Principles] 2번)에만 있고 FPDF에는 구현돼 있지 않다.
            item_entity: Entity | None = None
            if raw_frame.item is not None:
                item_label = entity_label_by_text.get(raw_frame.item.strip())
                if item_label is not None:
                    item_entity = Entity(text=raw_frame.item.strip(), label=item_label)

            # 근거 문장 검증: 공백 정규화 후 원문에 substring으로 존재해야 통과.
            # source_sentence는 필수이므로 검증에 실패한 프레임은 근거 없는 관계로
            # 간주하고 전체를 버린다 (subject/object 그라운딩 실패와 같은 정책).
            source_sentence = raw_frame.source_sentence.strip()
            if not source_sentence or _normalize_for_match(source_sentence) not in normalized_text:
                continue

            frames.append(
                RelationFrame(
                    predicate=raw_frame.predicate,
                    is_negated=raw_frame.is_negated,
                    tense=raw_frame.tense,
                    subject=Entity(text=raw_frame.subject.strip(), label=subject_label),
                    object=Entity(text=raw_frame.object.strip(), label=object_label),
                    item=item_entity,
                    source_sentence=source_sentence,
                )
            )

        # 중복 제거: LLM이 동일한 관계를 여러 프레임으로 반복 추출할 수 있으므로
        # (원칙 5의 프레임 분리 지시를 과하게 적용하는 경우 등), 모든 필드가 완전히 동일한
        # 프레임은 하나만 남긴다. model_dump_json()은 중첩된 Entity까지 포함해 모든 필드를
        # 안정적인 순서로 직렬화하므로 완전 동치 판별 키로 사용한다. 최초 등장 순서는 보존한다.
        seen: set[str] = set()
        deduped: list[RelationFrame] = []
        for frame in frames:
            key = frame.model_dump_json()
            if key in seen:
                continue
            seen.add(key)
            deduped.append(frame)

        return deduped
