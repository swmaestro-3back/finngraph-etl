"""본문 기업 판정(엔티티 필터) 프롬프트.

후보는 본문에만 나온 기업이다 — 제목에 나온 기업은 관련성 필터가 이미 판정해 후보에 없다. 결과가
기업 피드와 Event 당사자에 그대로 노출되므로 애매하면 버린다.

오탐 패턴과 예시는 저장된 기사 본문의 실제 후보에서 뽑았다. 가장 흔한 후보는 긴 단어의 일부
(레이·비자·하이브·테스), 일반명사(대상·태양), 페이지 상용구(공유 버튼의 카카오·페이스북, 저작권
문구의 DB), 원화 표기(한화), 그룹명(SK·한화), 목표주가를 낸 증권사, 주가 나열이다. 예시는 기사
한 건의 모양을 그대로 흉내 낸다: 제목 기업(엘앤에프, SK이노베이션)은 본문에 나오지만 후보 목록에는
없다.
"""

import json

from langchain_core.messages import SystemMessage
from langchain_core.prompts import ChatPromptTemplate, FewShotChatMessagePromptTemplate

_SYSTEM = """\
### [Role]
You are an entity verifier for a Korean stock-news service.
You are given an article body and a list of candidate company names that a dictionary matcher found in it. Companies named in the headline were judged separately and are not in the list, so every candidate appears only in the body.
The matcher is purely lexical, so many candidates are false hits. For each candidate you decide whether to keep it.

### [Hard Rules]
- Judge EVERY candidate you are given, exactly once, in the same order. Never add, rename, or merge candidates.
- "entity" must be copied back EXACTLY as given.
- "mention" must be a sentence or line copied verbatim from the article that contains the candidate's surface form. When it appears more than once, pick the occurrence that best shows how the article uses it.
- Fill "mention" and "reason" BEFORE deciding "keep".

### [Keep Criteria]
Keep a candidate only when BOTH hold:
1. Company reference: in this article the surface form refers to that company — not a common noun, a word fragment, a person, a place, a product line, or a different organization that happens to share the string. The matcher has no notion of word boundaries in Korean, so check each of these false-hit patterns:
   - Common noun: Korean particles attach directly to nouns, so a short company name often appears as an ordinary word: "대상으로", "조사 대상이라" (대상), "태양광", "태양 에너지" (태양), "전방산업", "전방위" (전방). These are NOT company references.
   - Part of a longer word or name: the candidate string sits inside a longer ordinary word — "인플레이션", "디스플레이" (레이), "소비자" (비자), "하이브리드" (하이브), "테스트" (테스), "애플리케이션" (애플), "스튜디오" (디오) — or inside the name of a different company, institution, brand, or product — "플로하이브컴퍼니" contains 하이브 but is a different company. These are NOT references to the candidate. Do not assume the longer name is an affiliate of the candidate.
   - Not a noun at all: the string is an adverb, a pronoun plus particle, a verb or adjective stem, or an ending — "한창 진행 중" (한창), "우리로서는" (우리로). These are NOT company references.
   - Won notation: "(한화 약 1조2200억원)" converts an amount into Korean won. It is NOT the company 한화.
   - Conglomerate label: "SK그룹", "한화그룹 계열사", "최태원 SK그룹 회장" name a business group or give a person's affiliation. That is NOT a reference to the listed company SK or 한화 unless the company itself is the one acting.
   - Page boilerplate, not article text: share buttons ("페이스북 트위터 카카오톡 URL복사"), copyright lines ("재판매 및 DB 금지"), reporter bylines and e-mail lines, photo credits, and headlines of other articles appended to the body ("[관련기사] ..."). A candidate that appears ONLY there is NOT part of this article.
   - If the candidate fails in one place but ALSO appears elsewhere in the article as a standalone reference to the company, judge it by the standalone occurrence.
2. Substantive mention: the company takes part in at least one concrete business action or fact in the article — as the actor, the counterparty, or the target (e.g. supplies, buys, invests, acquires, partners, is a customer, is supplied by, signs, sues, is acquired).
   - A company that appears ONLY inside a list of industry players, market leaders, or peers, with no action or fact specific to it, is background and fails this criterion (e.g. "에코프로비엠 엘앤에프 포스코케미칼 등이 주요 업체로 꼽힌다").
   - A stock price move is not a business action. A company that appears ONLY in a list of stocks that rose or fell together, or in a market summary, fails this criterion.
   - A securities firm, research house, or data provider cited as the source of a target price, forecast, rating, or comment is not a participant. Keep it only when the article reports a business action of that firm itself.
   - If the same company is listed in the background somewhere AND takes part in an action elsewhere, keep it.

### [Bias]
The kept companies are shown to users as the companies this article is about, so a wrong keep is worse than a miss. Resolve doubt the same way for both criteria:
- Criterion 1 (is it the company at all?): if no occurrence in the article clearly refers to the candidate company itself, drop it.
- Criterion 2 (is the mention substantive?): if you cannot point to a concrete business action or fact that the company itself takes part in, drop it.
"""

# 예시 1 — 공급 계약 기사. 제목 기업 엘앤에프는 후보에 없다.
_CONTRACT_COUNTERPARTY = "엘앤에프는 삼성SDI와 하이니켈 양극재 공급 계약을 체결했다고 5일 공시했다."
_CONTRACT_AMOUNT = "계약 규모는 7억9000만 달러(한화 약 1조2200억원)다."
_CONTRACT_SUPPLIER = "원료인 전구체는 에코프로머티가 댄다."
_CONTRACT_PLAN = (
    "회사는 북미 소비자 수요와 인플레이션 감축법(IRA) 요건을 고려해 미국 공장 증설도 검토한다."
)
_CONTRACT_BROKER = "키움증권은 이날 엘앤에프 목표주가를 15만원으로 올렸다."
_CONTRACT_STOCKS = (
    "이날 증시에서는 포스코퓨처엠(2.1%), 에코프로비엠(1.4%) 등 2차전지주가 함께 올랐다."
)
_CONTRACT_SHARE = "페이스북 트위터 카카오톡 URL복사"
_CONTRACT_COPYRIGHT = "*재판매 및 DB 금지"

# 예시 2 — 합병 기사. 제목 기업 SK이노베이션·SKIET 는 후보에 없다.
_MERGER_LEAD = "SK이노베이션이 자회사 SKIET를 흡수합병한다."
_MERGER_GROUP = "최태원 SK그룹 회장은 그동안 배터리 소재 사업 재편을 주문해 왔다."
_MERGER_CUSTOMER = "SKIET의 분리막 주요 고객사는 LG에너지솔루션이다."
_MERGER_DEMAND = "전방산업 수요가 둔화하면서 하이브리드 차량용 분리막 출하가 줄었다."
_MERGER_FORECAST = "에프앤가이드에 따르면 SK이노베이션의 올해 영업이익 전망치는 3000억원이다."
_MERGER_RELATED = "[관련기사] 한화오션 원청교섭 새 국면…법원, 집행정지 기각"

_EXAMPLES = [
    {
        "text": " ".join(
            (
                _CONTRACT_COUNTERPARTY,
                _CONTRACT_AMOUNT,
                _CONTRACT_SUPPLIER,
                _CONTRACT_PLAN,
                _CONTRACT_BROKER,
                _CONTRACT_STOCKS,
                _CONTRACT_SHARE,
                _CONTRACT_COPYRIGHT,
            )
        ),
        "entities": (
            "- 삼성SDI\n- 한화\n- 에코프로머티\n- 비자\n- 레이\n- 키움증권\n"
            "- 포스코퓨처엠\n- 에코프로비엠\n- 페이스북\n- 카카오\n- DB"
        ),
        "output": json.dumps(
            {
                "judgements": [
                    {
                        "entity": "삼성SDI",
                        "mention": _CONTRACT_COUNTERPARTY,
                        "reason": "양극재 공급 계약을 체결한 상대방이다.",
                        "keep": True,
                    },
                    {
                        "entity": "한화",
                        "mention": _CONTRACT_AMOUNT,
                        "reason": "'한화 약 1조2200억원'은 원화 환산 표기로, 기업 한화를 가리키지 않는다.",
                        "keep": False,
                    },
                    {
                        "entity": "에코프로머티",
                        "mention": _CONTRACT_SUPPLIER,
                        "reason": "이 계약 물량의 전구체를 공급하는 주체다.",
                        "keep": True,
                    },
                    {
                        "entity": "비자",
                        "mention": _CONTRACT_PLAN,
                        "reason": "'소비자'라는 단어의 일부로, 기업 비자를 가리키지 않는다.",
                        "keep": False,
                    },
                    {
                        "entity": "레이",
                        "mention": _CONTRACT_PLAN,
                        "reason": "'인플레이션'이라는 단어의 일부로, 기업 레이를 가리키지 않는다.",
                        "keep": False,
                    },
                    {
                        "entity": "키움증권",
                        "mention": _CONTRACT_BROKER,
                        "reason": "목표주가를 낸 증권사일 뿐, 기사가 다루는 사업 행위의 당사자가 아니다.",
                        "keep": False,
                    },
                    {
                        "entity": "포스코퓨처엠",
                        "mention": _CONTRACT_STOCKS,
                        "reason": "함께 오른 종목 나열에만 등장하고 구체적 사업 행위가 없다.",
                        "keep": False,
                    },
                    {
                        "entity": "에코프로비엠",
                        "mention": _CONTRACT_STOCKS,
                        "reason": "함께 오른 종목 나열에만 등장하고 구체적 사업 행위가 없다.",
                        "keep": False,
                    },
                    {
                        "entity": "페이스북",
                        "mention": _CONTRACT_SHARE,
                        "reason": "기사 공유 버튼 문구로, 기사 내용이 아니다.",
                        "keep": False,
                    },
                    {
                        "entity": "카카오",
                        "mention": _CONTRACT_SHARE,
                        "reason": "공유 버튼의 '카카오톡' 일부로, 기사 내용이 아니다.",
                        "keep": False,
                    },
                    {
                        "entity": "DB",
                        "mention": _CONTRACT_COPYRIGHT,
                        "reason": "'재판매 및 DB 금지'라는 저작권 문구로, 기업 DB를 가리키지 않는다.",
                        "keep": False,
                    },
                ]
            },
            ensure_ascii=False,
        ),
    },
    {
        "text": " ".join(
            (
                _MERGER_LEAD,
                _MERGER_GROUP,
                _MERGER_CUSTOMER,
                _MERGER_DEMAND,
                _MERGER_FORECAST,
                _MERGER_RELATED,
            )
        ),
        "entities": "- SK\n- LG에너지솔루션\n- 전방\n- 하이브\n- 에프앤가이드\n- 한화오션",
        "output": json.dumps(
            {
                "judgements": [
                    {
                        "entity": "SK",
                        "mention": _MERGER_GROUP,
                        "reason": "'SK그룹 회장'은 인물의 소속 표기이고, 상장사 SK가 행위자로 나오는 곳이 없다.",
                        "keep": False,
                    },
                    {
                        "entity": "LG에너지솔루션",
                        "mention": _MERGER_CUSTOMER,
                        "reason": "분리막을 공급받는 주요 고객사다.",
                        "keep": True,
                    },
                    {
                        "entity": "전방",
                        "mention": _MERGER_DEMAND,
                        "reason": "'전방산업'의 일반명사로, 기업 전방을 가리키지 않는다.",
                        "keep": False,
                    },
                    {
                        "entity": "하이브",
                        "mention": _MERGER_DEMAND,
                        "reason": "'하이브리드'라는 단어의 일부로, 기업 하이브를 가리키지 않는다.",
                        "keep": False,
                    },
                    {
                        "entity": "에프앤가이드",
                        "mention": _MERGER_FORECAST,
                        "reason": "실적 전망치의 출처일 뿐, 사업 행위의 당사자가 아니다.",
                        "keep": False,
                    },
                    {
                        "entity": "한화오션",
                        "mention": _MERGER_RELATED,
                        "reason": "본문 끝에 붙은 다른 기사의 제목에만 나오고, 이 기사의 내용이 아니다.",
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
