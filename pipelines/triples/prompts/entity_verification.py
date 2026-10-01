import json

from langchain_core.messages import SystemMessage
from langchain_core.prompts import ChatPromptTemplate, FewShotChatMessagePromptTemplate

_SYSTEM = """\
### [Role]
You are an entity verifier for a knowledge graph built from Korean economic and financial news.
You are given the FULL article text and a list of candidate company names that a dictionary matcher found in it.
The matcher is purely lexical, so some candidates are false hits. For each candidate you decide whether to keep it.

### [Hard Rules]
- Judge EVERY candidate you are given, exactly once, in the same order. Never add, rename, or merge candidates.
- "entity" must be copied back EXACTLY as given.
- "mention" must be a sentence copied verbatim from the article that contains the candidate's surface form. When it appears more than once, pick the occurrence that best shows how the article uses it.
- Fill "mention" and "reason" BEFORE deciding "keep".

### [Keep Criteria]
Keep a candidate only when BOTH hold:
1. Company reference: in this article the surface form refers to that company — not a common noun, a word fragment, a person, a place, a product line, or a different organization that happens to share the string.
   - Korean particles attach directly to nouns, so a short company name often appears inside an ordinary word or phrase: "대상으로" (대상), "태양 에너지" (태양), "나노 기술" (나노), "비자 발급" (비자). These are NOT company references.
2. Substantive mention: the company takes part in at least one concrete business action or fact in the article — as the actor, the counterparty, or the target (e.g. supplies, buys, invests, acquires, partners, is a customer, is supplied by, signs, sues, is acquired).
   - A company that appears ONLY inside a list of industry players, market leaders, or peers, with no action or fact specific to it, is background and fails this criterion (e.g. "에코프로비엠 엘앤에프 포스코케미칼 등이 주요 업체로 꼽힌다").
   - If the same company is listed in the background somewhere AND takes part in an action elsewhere, keep it.

### [Bias]
Dropping a real company loses every relation it takes part in, while keeping a weak mention only costs a little downstream work.
When you cannot decide, keep the candidate.
"""

_EXAMPLES = [
    {
        "text": (
            "정부는 중소기업을 대상으로 태양 에너지 설비 보조금을 확대한다. "
            "에코프로비엠 엘앤에프 포스코케미칼 등이 주요 양극재 업체로 꼽힌다. "
            "이 가운데 포스코케미칼은 LG에너지솔루션에 하이니켈 양극재를 공급한다. "
            "코스모신소재는 지난해 에스티엠의 전구체 공장을 인수했다."
        ),
        "entities": (
            "- 대상\n- 태양\n- 에코프로비엠\n- 엘앤에프\n- 포스코케미칼\n"
            "- LG에너지솔루션\n- 코스모신소재"
        ),
        "output": json.dumps(
            {
                "judgements": [
                    {
                        "entity": "대상",
                        "mention": "정부는 중소기업을 대상으로 태양 에너지 설비 보조금을 확대한다.",
                        "reason": "'대상으로'는 '목표로'라는 뜻의 일반명사로, 기업 대상을 가리키지 않는다.",
                        "keep": False,
                    },
                    {
                        "entity": "태양",
                        "mention": "정부는 중소기업을 대상으로 태양 에너지 설비 보조금을 확대한다.",
                        "reason": "'태양 에너지'의 일반명사로, 기업 태양을 가리키지 않는다.",
                        "keep": False,
                    },
                    {
                        "entity": "에코프로비엠",
                        "mention": "에코프로비엠 엘앤에프 포스코케미칼 등이 주요 양극재 업체로 꼽힌다.",
                        "reason": "업계 주요 업체 나열에만 등장하고 구체적 사업 행위가 없다.",
                        "keep": False,
                    },
                    {
                        "entity": "엘앤에프",
                        "mention": "에코프로비엠 엘앤에프 포스코케미칼 등이 주요 양극재 업체로 꼽힌다.",
                        "reason": "업계 주요 업체 나열에만 등장하고 구체적 사업 행위가 없다.",
                        "keep": False,
                    },
                    {
                        "entity": "포스코케미칼",
                        "mention": "이 가운데 포스코케미칼은 LG에너지솔루션에 하이니켈 양극재를 공급한다.",
                        "reason": "나열에도 나오지만 LG에너지솔루션에 양극재를 공급하는 주체다.",
                        "keep": True,
                    },
                    {
                        "entity": "LG에너지솔루션",
                        "mention": "이 가운데 포스코케미칼은 LG에너지솔루션에 하이니켈 양극재를 공급한다.",
                        "reason": "양극재 공급을 받는 상대방이다.",
                        "keep": True,
                    },
                    {
                        "entity": "코스모신소재",
                        "mention": "코스모신소재는 지난해 에스티엠의 전구체 공장을 인수했다.",
                        "reason": "전구체 공장을 인수한 주체다.",
                        "keep": True,
                    },
                ]
            },
            ensure_ascii=False,
        ),
    },
]

_EXAMPLE_PROMPT = ChatPromptTemplate.from_messages(
    [
        ("human", "**Article**:\n{text}\n\n**Candidate entities**:\n{entities}"),
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
        ("human", "**Article**:\n{text}\n\n**Candidate entities**:\n{entities}"),
    ]
)
