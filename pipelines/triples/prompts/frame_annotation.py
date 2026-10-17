import json

from langchain_core.messages import SystemMessage
from langchain_core.prompts import ChatPromptTemplate, FewShotChatMessagePromptTemplate

_SYSTEM = """\
### [Role]
You are an expert annotator for a knowledge graph built from Korean economic and financial news.
You are given the FULL article text and a list of relation frames that were already extracted from it.
For each frame you produce exactly four things: a self-contained Korean evidence sentence, a polarity label, a tense label, and a per-company impact judgment (subject_impact / object_impact).

### [Hard Rules]
- Annotate EVERY frame you are given, exactly once, in the same order. Never add, remove, merge, split, or re-interpret frames.
- "frame_index", "subject", "predicate", and "object" must be copied back EXACTLY as given. They are used to verify alignment; any mismatch causes the annotation to be discarded.
- Never introduce facts, numbers, dates, or causal links that are not stated in the article.
- The "clause" is a direction-normalized restatement of the relation and carries no polarity; judge polarity from the article text, not from the clause.

### [1. evidence]
Write ONE Korean sentence that a reader can understand on its own, without seeing the rest of the article.
This is not a summary — it is the frame's source sentence made self-contained.
- Preserve the original wording as much as possible, but pull missing context INTO the sentence.
- You MUST restore:
  - pronouns and referring expressions ("이를", "이 회사", "양사", "동사") replaced by the actual entity name,
  - the omitted subject,
  - and, when the article states them: the time ("올 하반기부터", "2027년까지"), the condition or premise ("계약이 체결되면"), and the attribution ("업계에 따르면", "회사 측은").
- The surface forms of the frame's subject, object, and item MUST appear literally in the evidence sentence.
- Target length is roughly 40-120 Korean characters. Never exceed 200 characters, and never copy a whole paragraph.

### [2. polarity]
Classify the relationship into exactly one of:
- "affirmed": the article states that the relationship holds.
- "denied": the article denies that the relationship holds at all (e.g. "부인했다", "사실무근", "공급하지 않는다", "협의한 바 없다").
- "terminated": a relationship that previously held has been ended, cancelled, dissolved, or halted (e.g. "계약을 해지했다", "합작법인을 청산했다", "공급을 중단했다").

Decision rule: if the text presupposes that the relationship previously held, choose "terminated". If it denies that the relationship ever held, choose "denied". If you cannot decide, choose "denied".

### [3. tense]
Classify the temporal/modal status into exactly one of:
- "past_or_present_fact": an already-completed past event, or an ongoing/habitual present-tense fact.
- "future_or_planned": a future, planned, or scheduled action ("~할 계획이다", "~할 예정이다", "~할 방침이다").
- "modal_possibility": a speculative, hypothetical, ability, or hearsay expression ("~할 수도 있다", "~것으로 보인다", "~가능성이 있다", "~것으로 전해진다").

polarity and tense are independent axes. "내년부터 공급을 중단할 예정이다" is polarity="terminated" AND tense="future_or_planned".

### [4. impact]
Judge the expected impact of THIS frame's event on each of the two companies SEPARATELY:
- "subject_impact": impact on the subject company — "positive" / "negative" / "neutral".
- "object_impact": impact on the object company — "positive" / "negative" / "neutral".

Apply the rules below in priority order, independently for each company.

Rule 1 — a stated market reaction takes priority.
If the article explicitly reports that company's stock-price move AS A REACTION to this
frame's event ("이 소식에 주가 급등", "인수설에 5% 하락", "상한가", "신저가 경신"), label that
company by the DIRECTION of the move: rise → "positive", fall → "negative".
- Only that company's own price counts. A price move of the counterparty never transfers
  to the other side — the other side is still judged by Rule 2.
- The move must be tied to this frame's event. Background price history not attributed to
  it ("최근 주가가 부진했던", "지난달 급락했던 바 있다") is NOT a market reaction — skip to Rule 2.

Rule 2 — otherwise, judge the event itself.
When no market reaction for that company is stated, judge the fundamental business impact
of this frame's event from that company's own perspective — not the article's overall tone.
- Materiality: if the event is routine or negligible relative to that company's size
  (e.g. a small supplier winning a 삼성전자 contract is major for the supplier but
  immaterial for 삼성전자), label that side "neutral".
- Judge the event AS IT ACTUALLY HAPPENED, combined with polarity: an affirmed supply
  contract is "positive" for the supplier, while a terminated one is "negative" for it.
  A denial ("denied") of a rumored relationship is "neutral" for both sides unless the
  article states concrete consequences for a company.
- When the impact is indirect, ambiguous, or the frame is merely descriptive, choose "neutral".
"""

_EXAMPLES = [
    {
        "text": (
            "에코프로비엠은 국내 대표 양극재 제조사다. "
            "이 회사는 올 하반기부터 삼성SDI에 초저온 내구성 코팅 소재를 공급하기로 했다. "
            "다만 회사 측은 LG에너지솔루션을 인수한다는 보도에 대해 사실무근이라고 밝혔다. "
            "한편 에코프로비엠은 지난달 체결했던 포스코케미칼과의 양극재 공급 계약을 해지했다. "
            "계약 규모가 시장 기대에 못 미친다는 평가 속에 이날 공급 소식에도 에코프로비엠 주가는 4% 하락했다."
        ),
        "frames": (
            "[0] subject=에코프로비엠 | predicate=SUPPLIES_TO | object=삼성SDI | item=초저온 내구성 코팅 소재\n"
            "    source_sentence: 이 회사는 올 하반기부터 삼성SDI에 초저온 내구성 코팅 소재를 공급하기로 했다.\n"
            "    clause: 에코프로비엠은 삼성SDI에 초저온 내구성 코팅 소재를 공급한다.\n"
            "[1] subject=에코프로비엠 | predicate=ACQUIRES | object=LG에너지솔루션 | item=없음\n"
            "    source_sentence: 다만 회사 측은 LG에너지솔루션을 인수한다는 보도에 대해 사실무근이라고 밝혔다.\n"
            "    clause: 에코프로비엠은 LG에너지솔루션을 인수한다.\n"
            "[2] subject=에코프로비엠 | predicate=SUPPLIES_TO | object=포스코케미칼 | item=양극재\n"
            "    source_sentence: 한편 에코프로비엠은 지난달 체결했던 포스코케미칼과의 양극재 공급 계약을 해지했다.\n"
            "    clause: 에코프로비엠은 포스코케미칼에 양극재를 공급한다."
        ),
        "output": json.dumps(
            {
                "annotations": [
                    {
                        "frame_index": 0,
                        "subject": "에코프로비엠",
                        "predicate": "SUPPLIES_TO",
                        "object": "삼성SDI",
                        "evidence": "에코프로비엠은 올 하반기부터 삼성SDI에 초저온 내구성 코팅 소재를 공급하기로 했다.",
                        "polarity": "affirmed",
                        "tense": "future_or_planned",
                        # 공급 체결은 이벤트만 보면 공급사 호재지만, 기사에 이 소식에 대한
                        # 주가 하락 반응이 명시되어 Rule 1이 이벤트 판단을 덮는다.
                        "subject_impact": "negative",
                        "object_impact": "neutral",
                    },
                    {
                        "frame_index": 1,
                        "subject": "에코프로비엠",
                        "predicate": "ACQUIRES",
                        "object": "LG에너지솔루션",
                        "evidence": "에코프로비엠은 LG에너지솔루션을 인수한다는 보도에 대해 사실무근이라고 밝혔다.",
                        "polarity": "denied",
                        "tense": "past_or_present_fact",
                        "subject_impact": "neutral",
                        "object_impact": "neutral",
                    },
                    {
                        "frame_index": 2,
                        "subject": "에코프로비엠",
                        "predicate": "SUPPLIES_TO",
                        "object": "포스코케미칼",
                        "evidence": "에코프로비엠은 지난달 체결했던 포스코케미칼과의 양극재 공급 계약을 해지했다.",
                        "polarity": "terminated",
                        "tense": "past_or_present_fact",
                        "subject_impact": "negative",
                        "object_impact": "neutral",
                    },
                ]
            },
            ensure_ascii=False,
        ),
    },
]

_EXAMPLE_PROMPT = ChatPromptTemplate.from_messages(
    [
        ("human", "**Article**:\n{text}\n\n**Frames to annotate**:\n{frames}"),
        ("ai", "{output}"),
    ]
)

_FEW_SHOT_PROMPT = FewShotChatMessagePromptTemplate(
    example_prompt=_EXAMPLE_PROMPT,
    examples=_EXAMPLES,
)

# Passing a SystemMessage instance keeps the system body out of template-variable parsing
PROMPT = ChatPromptTemplate.from_messages(
    [
        SystemMessage(content=_SYSTEM),
        _FEW_SHOT_PROMPT,
        ("human", "**Article**:\n{text}\n\n**Frames to annotate**:\n{frames}"),
    ]
)
