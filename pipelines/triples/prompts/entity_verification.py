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
1. Company reference: in this article the surface form refers to that company — not a common noun, a word fragment, a person, a place, a product line, or a different organization that happens to share the string. The matcher has no notion of word boundaries in Korean, so check each of these false-hit patterns:
   - Common noun: Korean particles attach directly to nouns, so a short company name often appears as an ordinary word: "대상으로", "조사 대상이라" (대상), "태양 에너지" (태양), "나노 기술" (나노), "비자 발급" (비자). These are NOT company references.
   - Part of a longer proper noun: the candidate string sits inside the name of a different company, institution, brand, or product — "플로하이브컴퍼니" contains 하이브 but is a different company. This is NOT a reference to the candidate, even though the surrounding context is about a company. Do not assume the longer name is an affiliate of the candidate.
   - Not a noun at all: the string is an adverb, a pronoun plus particle, a verb or adjective stem, or an ending — "한창 진행 중" (한창), "우리로서는" (우리로). These are NOT company references.
   - If the candidate fails in one place but ALSO appears elsewhere in the article as a standalone reference to the company, judge it by the standalone occurrence.
2. Substantive mention: the company takes part in at least one concrete business action or fact in the article — as the actor, the counterparty, or the target (e.g. supplies, buys, invests, acquires, partners, is a customer, is supplied by, signs, sues, is acquired).
   - A company that appears ONLY inside a list of industry players, market leaders, or peers, with no action or fact specific to it, is background and fails this criterion (e.g. "에코프로비엠 엘앤에프 포스코케미칼 등이 주요 업체로 꼽힌다").
   - If the same company is listed in the background somewhere AND takes part in an action elsewhere, keep it.

### [Bias]
The two criteria fail in different directions, so resolve doubt differently for each:
- Criterion 1 (is it the company at all?): a false hit that is kept becomes a wrong edge on a real company in the graph. If no occurrence in the article clearly refers to the candidate company itself, drop it.
- Criterion 2 (is the mention substantive?): dropping a real company loses every relation it takes part in, while keeping a weak mention only costs a little downstream work. When the candidate clearly is the company and only its weight is in doubt, keep it.
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
    {
        "text": (
            "플로하이브컴퍼니는 카카오와 음원 유통 계약을 체결했다고 밝혔다. "
            "두 회사는 지난해 공정거래위원회 조사 대상이라 과징금을 부과받은 바 있다. "
            "신규 플랫폼 개발이 한창 진행 중이며, 회사 관계자는 우리로서는 큰 기회라고 말했다."
        ),
        "entities": "- 하이브\n- 카카오\n- 대상\n- 한창\n- 우리로",
        "output": json.dumps(
            {
                "judgements": [
                    {
                        "entity": "하이브",
                        "mention": "플로하이브컴퍼니는 카카오와 음원 유통 계약을 체결했다고 밝혔다.",
                        "reason": "'플로하이브컴퍼니'라는 다른 회사명의 일부일 뿐이고, 하이브가 단독으로 등장하는 곳이 없다.",
                        "keep": False,
                    },
                    {
                        "entity": "카카오",
                        "mention": "플로하이브컴퍼니는 카카오와 음원 유통 계약을 체결했다고 밝혔다.",
                        "reason": "음원 유통 계약을 체결한 상대방이다.",
                        "keep": True,
                    },
                    {
                        "entity": "대상",
                        "mention": "두 회사는 지난해 공정거래위원회 조사 대상이라 과징금을 부과받은 바 있다.",
                        "reason": "'조사 대상'의 일반명사로, 기업 대상을 가리키지 않는다.",
                        "keep": False,
                    },
                    {
                        "entity": "한창",
                        "mention": "신규 플랫폼 개발이 한창 진행 중이며, 회사 관계자는 우리로서는 큰 기회라고 말했다.",
                        "reason": "'한창 진행 중'의 부사로, 기업 한창을 가리키지 않는다.",
                        "keep": False,
                    },
                    {
                        "entity": "우리로",
                        "mention": "신규 플랫폼 개발이 한창 진행 중이며, 회사 관계자는 우리로서는 큰 기회라고 말했다.",
                        "reason": "대명사 '우리'에 조사 '로서는'이 붙은 표현으로, 기업 우리로를 가리키지 않는다.",
                        "keep": False,
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
