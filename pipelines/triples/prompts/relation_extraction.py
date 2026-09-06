import json

from langchain_core.messages import SystemMessage
from langchain_core.prompts import ChatPromptTemplate, FewShotChatMessagePromptTemplate

from pipelines.triples.ontology.predicate_dict import PREDICATE_DICT

_PREDICATE_DICT: dict = PREDICATE_DICT


# Render each predicate with its description and per-argument roles rather than the bare name.
# Type constraints alone cannot separate near-synonyms that differ only in degree of control
# (INVESTS_IN vs ACQUIRES), so the role text carries that distinction.
# Three-argument predicates show the item role separately and expose the dictionary's required
# flag as [mandatory] or [optional], which tells the model whether a frame missing an item
# should be dropped or extracted anyway.
# The rendering never changes between requests, so build it once at import time.
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

_SYSTEM = """\
### [Role]
You are an expert Information Extraction system specialized in Korean economic and financial news.
Your sole task is to extract business relationships between COMPANY entities in the form of structured frames: (subject, predicate, object), optionally with a fourth 'item' argument naming a supplied product.

### [Core Principles]
0. Evidence First (source_sentence and clause fields)
    - For every frame, first fill "source_sentence": the sentence in the original text that states or implies the relationship, copied verbatim. Never paraphrase, trim, or merge sentences in this field. If a relationship spans context from two sentences, use the sentence containing the predicate expression.
    - Then fill "clause": the source_sentence decomposed and rewritten into the one simple clause this frame expresses, with an explicit subject and object.
      - Coordinate arguments joined by '와/과/및/그리고' (e.g., "양극재와 음극재") must be split into separate frames — one clause per item — each repeating the same source_sentence.
      - Subordinate/causal clauses (e.g., "~하기 때문에", "~하여") that themselves describe a relationship must get their own frame, not be discarded as background context.
      - Resolve pronouns (e.g., "이를") to their referent entity in the clause (the source_sentence keeps the original pronoun).
      - If the sentence is phrased from the counterparty's point of view, restate the clause in the matching predicate's direction (e.g. rephrase 'A가 B로부터 X를 구매했다' as 'B가 A에게 X를 공급한다').
    - Only after both fields are filled, choose the predicate and arguments that the clause expresses.
1. Semantic Relation Matching: Match each clause against a predicate's description and argument roles, not against a specific verb. A predicate applies whenever the relationship it describes holds between two grounded entities, however the text phrases it — synonym, passive form, nominalized phrase, or indirect wording all count equally.
   - When a clause is phrased from the counterparty's point of view, restate it in the matching predicate's direction before extracting. e.g. "A가 B로부터 X를 구매했다" restates as "B가 A에게 X를 공급한다" → extract SUPPLIES_TO(subject=B, object=A, item=X).
   - Extract a frame only when the relationship is actually stated or clearly implied. Entities that merely co-occur in a sentence with no relational content between them do not form a frame.
   - "Stated or clearly implied" includes the article making a claim that a relation does NOT hold, no longer holds, or was denied/refuted. Extract the frame identifying the entity pair and predicate exactly as you would for an affirmed relationship. Do not decide whether the relation is affirmed, denied, or terminated — that judgment belongs to a downstream annotation stage.
2. Strict Frame Structure: Every extracted frame must contain "subject", "predicate", and "object". SUPPLIES_TO additionally requires "item". Never extract any other additional arguments, modifiers, temporal information (time/date), monetary amounts, or percentage shares.
3. Subject and Object Must Match NER Entities: The values of "subject" and "object" MUST exactly match a surface form in the "NER Results (entities)" list below. That list contains companies only. If either party of a relationship is missing from the list — a government body, a public agency, an unlisted counterparty — do not extract that frame at all. Never invent entity names.
4. Item Is Free Text From the Article: "item" is NOT constrained by the entity list. Copy the product, material, or service phrase verbatim from the source_sentence, exactly as the article writes it, including qualifiers ("하이니켈 양극재", "차량용 5나노 AI 칩", "2층 전동차 개조작업"). Do not normalize it to a familiar or shorter term, do not translate it, and do not substitute a generic category name. Never write an item phrase that does not literally appear in the text. If SUPPLIES_TO is expressed but the text names no specific item, do not extract that frame.
   - "item" must be a NOUN PHRASE that names the product/material/service — never a clause or sentence fragment, and never a bare generic category noun.
   - Granularity rule — how to cut the span: locate the head noun of the supplied item, then extend LEFT to include every modifier that describes WHAT THE ITEM IS (용도·재질·세대·모델명·규격 등의 수식어: "차량용", "하이니켈", "차세대", "메탈 플레이트 내"). STOP before anything that describes effects, expectations, amounts, or the deal itself (기대·전망·회복·상승·수주·계약 규모 등), and never include a verb or connective ending (~하며, ~하여, ~함에 따라).
   - Both failure directions are wrong:
     - Too LONG (a clause): "메탈 플레이트 내 레이저 에칭 적용 확대에 따른 점유율 회복과 평균판매단가 상승이 기대" → correct item: "메탈 플레이트 내 레이저 에칭".
     - Too SHORT (a bare category): "장비", "부품", "소재", "칩" alone → keep the modifiers the article attaches: "HBM", "차세대 중앙처리장치 베라 루빈", "반도체 검사 장비".
   - Litmus test before writing the item: (a) can a reader tell WHICH specific product it is from the item alone? — "장비" fails; (b) does it still read as the NAME of a thing rather than a statement about it? — a clause fails. Both must pass.
5. Predicate Constraint: The "predicate" MUST be one of the exact strings in the "registered_predicates" list. Do not invent new relationship names.
6. Multiple Triplet Separation (Mandatory): If a single sentence contains multiple facts, never combine them into a single frame. Extract them as independent frames — each with its own clause, all sharing the same source_sentence. If the text lists multiple coordinate items (e.g. "양극재와 음극재를 공급한다"), extract one frame per item, each repeating the same subject and object.

[Argument Type]
- subject: The acting company. Must appear in the entity list.
- object: The counterparty company. Must appear in the entity list.
- item: Only for SUPPLIES_TO — the supplied product/material/service, copied verbatim from the article. Not constrained by the entity list. The full noun phrase naming the specific item, with its modifiers — never a clause, never a bare category noun.
"""

_EXAMPLES = [
    {
        "text": (
            "앞서 유튜브를 인수한 구글은 페이스북을 인수할 계획이라고 밝혔다."
            "네이버는 경영권 인수 없이 컬리의 지분 5%만 취득하는 지분투자를 단행했다."
            "에코프로비엠은 LG에너지솔루션과 협력을 논의 중이라는 보도에 대해 사실무근이라고 밝혔다."
        ),
        "entities": ("구글, 페이스북, 유튜브, 네이버, 컬리, 에코프로비엠, LG에너지솔루션"),
        "output": json.dumps(
            {
                "frames": [
                    {
                        "source_sentence": "앞서 유튜브를 인수한 구글은 페이스북을 인수할 계획이라고 밝혔다.",
                        "clause": "구글은 페이스북을 인수할 계획이다.",
                        "predicate": "ACQUIRES",
                        "subject": "구글",
                        "object": "페이스북",
                        "item": None,
                    },
                    {
                        "source_sentence": "앞서 유튜브를 인수한 구글은 페이스북을 인수할 계획이라고 밝혔다.",
                        "clause": "구글은 유튜브를 인수했다.",
                        "predicate": "ACQUIRES",
                        "subject": "구글",
                        "object": "유튜브",
                        "item": None,
                    },
                    {
                        "source_sentence": "네이버는 경영권 인수 없이 컬리의 지분 5%만 취득하는 지분투자를 단행했다.",
                        "clause": "네이버는 컬리에 지분투자를 했다.",
                        "predicate": "INVESTS_IN",
                        "subject": "네이버",
                        "object": "컬리",
                        "item": None,
                    },
                ]
            },
            ensure_ascii=False,
        ),
    },
    {
        "text": (
            "에코프로비엠은 올 하반기부터 삼성SDI에 하이니켈 양극재와 단결정 양극재를 동시에 공급한다."
            "삼성전자는 테슬라에 차량용 5나노 AI 칩을 공급하기로 했다."
            "삼성SDI는 칠레의 코델코로부터 리튬을 대량 구매했다."
            "현대로템은 호주 뉴사우스웨일즈 주 교통부로부터 2층 전동차 개조작업을 수주했다."
        ),
        "entities": ("에코프로비엠, 삼성SDI, 삼성전자, 테슬라, 코델코, 현대로템"),
        "output": json.dumps(
            {
                "frames": [
                    {
                        "source_sentence": "에코프로비엠은 올 하반기부터 삼성SDI에 하이니켈 양극재와 단결정 양극재를 동시에 공급한다.",
                        "clause": "에코프로비엠은 삼성SDI에 하이니켈 양극재를 공급한다.",
                        "predicate": "SUPPLIES_TO",
                        "subject": "에코프로비엠",
                        "object": "삼성SDI",
                        "item": "하이니켈 양극재",
                    },
                    {
                        "source_sentence": "에코프로비엠은 올 하반기부터 삼성SDI에 하이니켈 양극재와 단결정 양극재를 동시에 공급한다.",
                        "clause": "에코프로비엠은 삼성SDI에 단결정 양극재를 공급한다.",
                        "predicate": "SUPPLIES_TO",
                        "subject": "에코프로비엠",
                        "object": "삼성SDI",
                        "item": "단결정 양극재",
                    },
                    {
                        "source_sentence": "삼성전자는 테슬라에 차량용 5나노 AI 칩을 공급하기로 했다.",
                        "clause": "삼성전자는 테슬라에 차량용 5나노 AI 칩을 공급한다.",
                        "predicate": "SUPPLIES_TO",
                        "subject": "삼성전자",
                        "object": "테슬라",
                        "item": "차량용 5나노 AI 칩",
                    },
                    {
                        "source_sentence": "삼성SDI는 칠레의 코델코로부터 리튬을 대량 구매했다.",
                        "clause": "코델코가 삼성SDI에 리튬을 공급한다.",
                        "predicate": "SUPPLIES_TO",
                        "subject": "코델코",
                        "object": "삼성SDI",
                        "item": "리튬",
                    },
                ]
            },
            ensure_ascii=False,
        ),
    },
]

_EXAMPLE_PROMPT = ChatPromptTemplate.from_messages(
    [
        ("human", "**Text**:\n{text}\n\n**NER results (entities)**:\n{entities}"),
        ("ai", "{output}"),
    ]
)

# Assemble the few-shot prompt
_FEW_SHOT_PROMPT = FewShotChatMessagePromptTemplate(
    example_prompt=_EXAMPLE_PROMPT,
    examples=_EXAMPLES,
)

# System prompt + the rendered predicate list
# SYSTEM_PROMPT + REGISTERED_PREDICATES_STR
_SYSTEM_MESSAGE = _SYSTEM + "\n\n**Registered predicate list**:\n" + _REGISTERED_PREDICATES_LIST

# Final prompt: system instructions, the few-shot examples (expanded into human/ai turns), and
# the per-request human message. Passing a SystemMessage instance keeps the system body out of
# template-variable parsing, so braces inside predicate descriptions are safe.
PROMPT = ChatPromptTemplate.from_messages(
    [
        SystemMessage(content=_SYSTEM_MESSAGE),
        _FEW_SHOT_PROMPT,
        ("human", "**Text**:\n{text}\n\n**NER results (entities)**:\n{entities}"),
    ]
)
