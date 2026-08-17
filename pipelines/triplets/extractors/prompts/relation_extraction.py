import json
from typing import Any

from pipelines.triplets.ontology.predicate_dict import PREDICATE_DICT

_PREDICATE_DICT: dict = PREDICATE_DICT


def _format_predicate_entry(predicate: str, entry: dict) -> str:
    arg_names = list(entry["arguments"].keys())
    subject_role, object_role = arg_names[0], arg_names[1]
    subject_desc = entry["arguments"][subject_role]["description"]
    object_desc = entry["arguments"][object_role]["description"]
    line = f"- {predicate}: {entry['description']} (subject={subject_desc}, object={object_desc}"
    if len(arg_names) > 2:
        item_role = arg_names[2]
        item_entry = entry["arguments"][item_role]
        item_tag = "mandatory" if item_entry["required"] else "optional"
        line += f", item={item_entry['description']} [{item_tag}]"
    line += ")"
    return line


_REGISTERED_PREDICATES_LIST = "\n".join(
    _format_predicate_entry(predicate, entry)
    for predicate, entry in sorted(_PREDICATE_DICT.items())
)

SYSTEM_MESSAGE = f"""\
### [Role]
You are an expert Information Extraction system specialized in Korean economic and financial news.
Your sole task is to extract multilateral business relationships among entities such as 'COMPANY, GOVERNMENT, COUNTRY, and MATERIAL/PRODUCT' in the form of structured frames: (subject, predicate, object), optionally with a fourth 'item' argument.

### [Core Principles]
0. Evidence First (source_sentence and clause fields)
    - For every frame, first fill "source_sentence": the sentence in the original text that states or implies the relationship, copied verbatim. Never paraphrase, trim, or merge sentences in this field. If a relationship spans context from two sentences, use the sentence containing the predicate expression.
    - Then fill "clause": the source_sentence decomposed and rewritten into the one simple clause this frame expresses, with an explicit subject and object.
      - Coordinate arguments joined by '와/과/및/그리고' (e.g., "양극재와 음극재") must be split into separate frames — one clause per entity — each repeating the same source_sentence, never treated as a single combined argument.
      - Subordinate/causal clauses (e.g., "~하기 때문에", "~하여") that themselves describe a relationship must get their own frame, not be discarded as background context.
      - Resolve pronouns (e.g., "이를") to their referent entity in the clause (the source_sentence keeps the original pronoun).
      - If the sentence is phrased from the counterparty's point of view, restate the clause in the matching predicate's direction (e.g. rephrase 'A가 B로부터 X를 구매했다' as 'B가 A에게 X를 공급한다').
    - Only after both fields are filled, choose the predicate and arguments that the clause expresses.
1. Semantic Relation Matching: Match each clause against a predicate's description and argument roles, not against a specific verb. A predicate applies whenever the relationship it describes holds between two grounded entities, however the text phrases it — synonym, passive form, nominalized phrase, or indirect wording all count equally.
   - When a clause is phrased from the counterparty's point of view, restate it in the matching predicate's direction before extracting. e.g. "A가 B로부터 X를 구매했다" restates as "B가 A에게 X를 공급한다" → extract SUPPLIES_TO(subject=B, object=A, item=X).
   - Extract implied relationships between two COMPANY/GOVERNMENT/COUNTRY entities whenever the surrounding context clearly presupposes them, even without a single verb connecting the two directly — e.g., two companies jointly founding a venture presupposes PARTNERS_WITH between them.
   - Extract a frame only when the relationship is actually stated or clearly implied. Entities that merely co-occur in a sentence with no relational content between them do not form a frame.
2. Strict Frame Structure: Every extracted frame must contain "subject", "predicate", and "object". Some predicates in the registered_predicates list additionally declare an "item" argument (shown as "item=... [mandatory]" or "item=... [optional]"). For those predicates, also fill "item" within the same frame (do not split it into a separate frame) whenever the text names a specific product/commodity. If a predicate's item argument is marked [mandatory], do not extract that frame at all unless a specific item entity is named in the text. Never extract any other additional arguments, modifiers, temporal information (time/date), monetary amounts, or percentage shares.
3. Arguments Must Match NER Entities: The values of "subject", "object", and "item" MUST exactly match the surface forms provided in the "NER Results (entities)" list below. Never use terms (e.g., general nouns, abstract concepts, or unrecognized words) that are not present in the given "entities" list. If an entity is not available in the list to fill subject or object, do not extract that frame. Do not invent any values.
4. Predicate Constraint: The "predicate" MUST be one of the exact strings in the "registered_predicates" list. Do not invent new relationship names.
5. Multiple Triplet Separation (Mandatory): If a single sentence contains multiple facts or relationships, never combine them into a single frame. Extract them as independent frames — each with its own clause, all sharing the same source_sentence — and include all of them in the "frames" list. If a predicate has an "item" argument and the text lists multiple coordinate items (e.g. "양극재와 음극재를 공급한다"), extract one frame per item, each repeating the same subject and object.
6. Negation Handling (is_negated): Set "is_negated" to true if the relationship involves a negative expression (e.g., "does not", "fails to", "rejects", "denied"). Otherwise, set it to false.
7. Tense/Modality Classification (tense): Classify the temporal/modal status of the clause containing the predicate into exactly one of:
   - "past_or_present_fact": an already-completed past event, or an ongoing/habitual present-tense fact (including implied relationships presented as presently existing, e.g. "코델코는 칠레에 위치한 광산기업이다").
   - "future_or_planned": a future, planned, or scheduled action (e.g., "~할 계획이다", "~할 예정이다", "~할 방침이다", "~할 전망이다").
   - "modal_possibility": a speculative, hypothetical, or ability/possibility expression (e.g., "~할 수도 있다", "~할 수 있다", "~것으로 보인다", "~가능성이 있다").
   Always extract the frame regardless of its tense/modality classification — do not omit future/modal relationships. Downstream filtering relies on this field to keep only confirmed facts.

[Argument Type]
- subject: The active agent (COMPANY, GOVERNMENT, or COUNTRY).
- object: The target entity. (COMPANY, GOVERNMENT, COUNTRY, COMMODITY, PRODUCT)
- item: Only for predicates that declare an "item" role (see registered_predicates list) — the specific product/commodity/material (COMMODITY, PRODUCT) involved. Fill it whenever the text names one. If the predicate's item is [mandatory] and no item is named, do not extract that frame. If [optional], extract the frame anyway and leave item null.

**Registered predicate list**:
{_REGISTERED_PREDICATES_LIST}
"""

_EXAMPLES = [
    {
        "text": (
            "앞서 유튜브를 인수한 구글은 페이스북을 인수할 계획이라고 밝혔다."
            "현대차와 LG에너지솔루션은 미국에 배터리 공장을 짓기 위한 합작법인을 설립하기로 했다."
            "네이버는 경영권 인수 없이 컬리의 지분 5%만 취득하는 지분투자를 단행했다."
            "반면 SK는 계열사 SK엔카를 매각하기로 했다."
            "삼성전자와 애플은 스마트폰 시장에서 경쟁하면서도 디스플레이 분야에서는 협력한다."
        ),
        "entities": (
            "구글(COMPANY), 페이스북(COMPANY), 유튜브(COMPANY), 테슬라(COMPANY), 리비안(COMPANY), "
            "현대차(COMPANY), LG에너지솔루션(COMPANY), 미국(COUNTRY), 배터리(COMMODITY), "
            "네이버(COMPANY), 컬리(COMPANY), SK(COMPANY), SK엔카(COMPANY), "
            "삼성전자(COMPANY), 애플(COMPANY)"
        ),
        "output": json.dumps(
            {
                "frames": [
                    {
                        "source_sentence": "앞서 유튜브를 인수한 구글은 페이스북을 인수할 계획이라고 밝혔다.",
                        "clause": "구글은 페이스북을 인수할 계획이다.",
                        "predicate": "ACQUIRES",
                        "is_negated": False,
                        "tense": "future_or_planned",
                        "subject": "구글",
                        "object": "페이스북",
                        "item": None,
                    },
                    {
                        "source_sentence": "앞서 유튜브를 인수한 구글은 페이스북을 인수할 계획이라고 밝혔다.",
                        "clause": "구글은 유튜브를 인수했다.",
                        "predicate": "ACQUIRES",
                        "is_negated": False,
                        "tense": "past_or_present_fact",
                        "subject": "구글",
                        "object": "유튜브",
                        "item": None,
                    },
                    {
                        "source_sentence": "현대차와 LG에너지솔루션은 미국에 배터리 공장을 짓기 위한 합작법인을 설립하기로 했다.",
                        "clause": "현대차는 LG에너지솔루션과 협력한다.",
                        "predicate": "PARTNERS_WITH",
                        "is_negated": False,
                        "tense": "future_or_planned",
                        "subject": "현대차",
                        "object": "LG에너지솔루션",
                        "item": None,
                    },
                    {
                        "source_sentence": "네이버는 경영권 인수 없이 컬리의 지분 5%만 취득하는 지분투자를 단행했다.",
                        "clause": "네이버는 컬리에 지분투자를 했다.",
                        "predicate": "INVESTS_IN",
                        "is_negated": False,
                        "tense": "past_or_present_fact",
                        "subject": "네이버",
                        "object": "컬리",
                        "item": None,
                    },
                    {
                        "source_sentence": "반면 SK는 계열사 SK엔카를 매각하기로 했다.",
                        "clause": "SK는 SK엔카를 매각할 계획이다.",
                        "predicate": "DIVESTS_FROM",
                        "is_negated": False,
                        "tense": "future_or_planned",
                        "subject": "SK",
                        "object": "SK엔카",
                        "item": None,
                    },
                    {
                        "source_sentence": "삼성전자와 애플은 스마트폰 시장에서 경쟁하면서도 디스플레이 분야에서는 협력한다.",
                        "clause": "삼성전자는 애플과 스마트폰 시장에서 경쟁한다.",
                        "predicate": "COMPETES_WITH",
                        "is_negated": False,
                        "tense": "past_or_present_fact",
                        "subject": "삼성전자",
                        "object": "애플",
                        "item": None,
                    },
                    {
                        "source_sentence": "삼성전자와 애플은 스마트폰 시장에서 경쟁하면서도 디스플레이 분야에서는 협력한다.",
                        "clause": "삼성전자는 애플과 디스플레이 분야에서 협력한다.",
                        "predicate": "PARTNERS_WITH",
                        "is_negated": False,
                        "tense": "past_or_present_fact",
                        "subject": "삼성전자",
                        "object": "애플",
                        "item": None,
                    },
                ]
            },
            ensure_ascii=False,
        ),
    },
    {
        "text": (
            "삼성SDI는 칠레의 코델코로부터 리튬을 대량 구매했다."
            "한편 에코프로비엠은 올 하반기부터 삼성SDI에 배터리 핵심 소재인 양극재와 음극재를 동시에 공급한다."
            "이를 통해 LFP 배터리를 생산하여 테슬라에게 30년까지 공급하는데 차질이 없을 것으로 예상된다."
            "삼성SDI는 차세대 전고체 배터리를 개발 중이며, 배터리를 미국에 수출한다."
            "미국은 중국에 반도체 장비 수출을 규제하고 있다."
        ),
        "entities": (
            "삼성SDI(COMPANY), 칠레(COUNTRY), 코델코(COMPANY), 리튬(COMMODITY), "
            "에코프로비엠(COMPANY), 양극재(COMMODITY), 음극재(COMMODITY), "
            "테슬라(COMPANY), LFP 배터리(COMMODITY), "
            "전고체 배터리(PRODUCT), 배터리(COMMODITY), 미국(COUNTRY), 중국(COUNTRY)"
        ),
        "output": json.dumps(
            {
                "frames": [
                    {
                        "source_sentence": "삼성SDI는 칠레의 코델코로부터 리튬을 대량 구매했다.",
                        "clause": "코델코가 삼성SDI에 리튬을 공급한다.",
                        "predicate": "SUPPLIES_TO",
                        "is_negated": False,
                        "tense": "past_or_present_fact",
                        "subject": "코델코",
                        "object": "삼성SDI",
                        "item": "리튬",
                    },
                    {
                        "source_sentence": "삼성SDI는 칠레의 코델코로부터 리튬을 대량 구매했다.",
                        "clause": "코델코는 리튬을 생산한다.",
                        "predicate": "PRODUCES",
                        "is_negated": False,
                        "tense": "past_or_present_fact",
                        "subject": "코델코",
                        "object": "리튬",
                        "item": None,
                    },
                    {
                        "source_sentence": "삼성SDI는 칠레의 코델코로부터 리튬을 대량 구매했다.",
                        "clause": "코델코는 칠레에 위치한다.",
                        "predicate": "LOCATED_IN",
                        "is_negated": False,
                        "tense": "past_or_present_fact",
                        "subject": "코델코",
                        "object": "칠레",
                        "item": None,
                    },
                    {
                        "source_sentence": "한편 에코프로비엠은 올 하반기부터 삼성SDI에 배터리 핵심 소재인 양극재와 음극재를 동시에 공급한다.",
                        "clause": "에코프로비엠은 올 하반기부터 삼성SDI에 양극재를 공급한다.",
                        "predicate": "SUPPLIES_TO",
                        "is_negated": False,
                        "tense": "future_or_planned",
                        "subject": "에코프로비엠",
                        "object": "삼성SDI",
                        "item": "양극재",
                    },
                    {
                        "source_sentence": "한편 에코프로비엠은 올 하반기부터 삼성SDI에 배터리 핵심 소재인 양극재와 음극재를 동시에 공급한다.",
                        "clause": "에코프로비엠은 올 하반기부터 삼성SDI에 음극재를 공급한다.",
                        "predicate": "SUPPLIES_TO",
                        "is_negated": False,
                        "tense": "future_or_planned",
                        "subject": "에코프로비엠",
                        "object": "삼성SDI",
                        "item": "음극재",
                    },
                    {
                        "source_sentence": "이를 통해 LFP 배터리를 생산하여 테슬라에게 30년까지 공급하는데 차질이 없을 것으로 예상된다.",
                        "clause": "삼성SDI는 양극재와 음극재를 통해 LFP 배터리를 생산할 것으로 예상된다.",
                        "predicate": "PRODUCES",
                        "is_negated": False,
                        "tense": "modal_possibility",
                        "subject": "삼성SDI",
                        "object": "LFP 배터리",
                        "item": None,
                    },
                    {
                        "source_sentence": "이를 통해 LFP 배터리를 생산하여 테슬라에게 30년까지 공급하는데 차질이 없을 것으로 예상된다.",
                        "clause": "삼성SDI는 테슬라에게 LFP 배터리를 공급할 것으로 예상된다.",
                        "predicate": "SUPPLIES_TO",
                        "is_negated": False,
                        "tense": "modal_possibility",
                        "subject": "삼성SDI",
                        "object": "테슬라",
                        "item": "LFP 배터리",
                    },
                    {
                        "source_sentence": "삼성SDI는 차세대 전고체 배터리를 개발 중이며, 배터리를 미국에 수출한다.",
                        "clause": "삼성SDI는 전고체 배터리를 개발하고 있다.",
                        "predicate": "DEVELOPS",
                        "is_negated": False,
                        "tense": "past_or_present_fact",
                        "subject": "삼성SDI",
                        "object": "전고체 배터리",
                        "item": None,
                    },
                    {
                        "source_sentence": "삼성SDI는 차세대 전고체 배터리를 개발 중이며, 배터리를 미국에 수출한다.",
                        "clause": "삼성SDI는 미국에 배터리를 수출한다.",
                        "predicate": "EXPORTS_TO",
                        "is_negated": False,
                        "tense": "past_or_present_fact",
                        "subject": "삼성SDI",
                        "object": "미국",
                        "item": "배터리",
                    },
                    {
                        "source_sentence": "미국은 중국에 반도체 장비 수출을 규제하고 있다.",
                        "clause": "미국은 중국을 규제한다.",
                        "predicate": "SANCTIONS",
                        "is_negated": False,
                        "tense": "past_or_present_fact",
                        "subject": "미국",
                        "object": "중국",
                        "item": None,
                    },
                ]
            },
            ensure_ascii=False,
        ),
    },
]


def _user_text(text: str, entities: str) -> str:
    return f"**Text**:\n{text}\n\n**NER results (entities)**:\n{entities}"


# Bedrock에서 few-shot prompt를 전달하기 위한 템플릿 생성 로직
FEW_SHOT_MESSAGES: list[dict[str, Any]] = []
for _example in _EXAMPLES:
    FEW_SHOT_MESSAGES.append(
        {"role": "user", "content": [{"text": _user_text(_example["text"], _example["entities"])}]}
    )
    FEW_SHOT_MESSAGES.append({"role": "assistant", "content": [{"text": _example["output"]}]})


# User Message 생성 로직
def build_user_message(text: str, entities: str) -> dict[str, Any]:
    """이번 요청의 실제 user 턴 하나를 만든다. 호출부에서 FEW_SHOT_MESSAGES와 조합한다."""
    return {"role": "user", "content": [{"text": _user_text(text, entities)}]}
