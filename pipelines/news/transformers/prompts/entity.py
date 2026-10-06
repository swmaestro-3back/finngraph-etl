"""본문 기업 판정(엔티티 필터) 프롬프트.

후보는 본문에만 나온 기업이다 — 제목에 나온 기업은 관련성 필터가 이미 판정해 후보에 없다. 모델은
제목과 제목 기업(이 기사의 주인공)을 함께 보고, 후보마다 제목이 전하는 주된 뉴스에서 맡은 역할을
고른다. 주된 뉴스의 당사자(party)만 통과다. 결과가 기업 피드와 Event 당사자에 그대로 노출되므로
애매하면 버린다.

오탐 패턴과 예시는 저장된 기사 본문의 실제 후보에서 뽑았다. 가장 흔한 후보는 긴 단어의 일부
(레이·비자·하이브·테스), 일반명사(대상·태양), 페이지 상용구(공유 버튼의 카카오·페이스북, 저작권
문구의 DB), 원화 표기(한화), 그룹명(SK·한화), 목표주가를 낸 증권사, 주가 나열, 그리고 주인공
기업의 뉴스를 설명하려고 인용된 다른 기업의 사실(그룹사의 계획, 다른 기업의 실적)이다. 예시는
기사 한 건의 모양을 그대로 흉내 낸다: 제목 기업은 본문에 나오지만 후보 목록에는 없다.
"""

import json

from langchain_core.messages import SystemMessage
from langchain_core.prompts import ChatPromptTemplate, FewShotChatMessagePromptTemplate

_SYSTEM = """\
### [Role]
You are an entity verifier for a Korean stock-news service.
You are given an article's headline, the headline companies, the article body, and a list of candidate company names that a dictionary matcher found in the body.
The headline companies were judged separately: they are the companies this article is about, and they are not in the candidate list. Every candidate appears only in the body.
The matcher is purely lexical, and a body names many companies besides the ones the article is about. For each candidate you decide its role in this article. Only a party to the main news is linked to the article.

### [Hard Rules]
- Judge EVERY candidate you are given, exactly once, in the same order. Never add, rename, or merge candidates.
- "entity" must be copied back EXACTLY as given.
- "mention" must be a sentence or line copied verbatim from the article body that contains the candidate's surface form. When it appears more than once, pick the occurrence that best shows the candidate's role.
- Fill "mention" and "reason" BEFORE deciding "role".

### [Step 1 — Is it the company at all?]
In this article the surface form must refer to that company — not a common noun, a word fragment, a person, a place, a product line, or a different organization that happens to share the string. The matcher has no notion of word boundaries in Korean, so check each of these false-hit patterns:
- Common noun: Korean particles attach directly to nouns, so a short company name often appears as an ordinary word: "대상으로", "조사 대상이라" (대상), "태양광", "태양 에너지" (태양), "전방산업", "전방위" (전방).
- Part of a longer word or name: the candidate string sits inside a longer ordinary word — "인플레이션", "디스플레이" (레이), "소비자" (비자), "하이브리드" (하이브), "테스트" (테스), "애플리케이션" (애플), "스튜디오" (디오) — or inside the name of a different company, institution, brand, or product — "플로하이브컴퍼니" contains 하이브 but is a different company. Do not assume the longer name is an affiliate of the candidate.
- Not a noun at all: the string is an adverb, a pronoun plus particle, a verb or adjective stem, or an ending — "한창 진행 중" (한창), "우리로서는" (우리로).
- Won notation: "(한화 약 1조2200억원)" converts an amount into Korean won. It is NOT the company 한화.
- Conglomerate label: "SK그룹", "한화그룹 계열사", "최태원 SK그룹 회장" name a business group or give a person's affiliation. That is NOT a reference to the listed company SK or 한화 unless the company itself is the one acting.
- Affiliate label: "카카오 계열사", "LG 자회사", "롯데 그룹사" name a different, unnamed company by its parent. The actor is that affiliate, not the listed company 카카오, LG, or 롯데 — do not link the parent for an affiliate's deal, and do not use general knowledge to decide which affiliate it is.
- Page boilerplate, not article text: share buttons ("페이스북 트위터 카카오톡 URL복사"), copyright lines ("재판매 및 DB 금지"), reporter bylines and e-mail lines, photo credits, and headlines of other articles appended to the body ("[관련기사] ..."). A candidate that appears ONLY there is not part of this article.
If no occurrence passes Step 1, the role is "not_company". If the candidate fails in one place but ALSO appears elsewhere as a standalone reference to the company, judge it by that occurrence in Step 2.

### [Step 2 — Its role in the main news]
First name the main news: the event or development the headline reports about the headline companies. Then give the candidate one role:
- "party": the company itself takes part in the main news — as an actor alongside the headline company, or as its counterparty or target: the customer, supplier, partner, investor, investee, acquirer, acquired company, seller, buyer, or contract or lawsuit counterpart in that event.
- "background": the company's own action, plan, earnings, or situation is cited to explain, cause, or set the context for the main news, but the company is not a party to it. Typical cases: a parent or group company's plan cited as the reason a subsidiary will benefit; a customer's, rival's, or peer's earnings, investment, or plan cited as an industry backdrop; a past deal recalled for context; another company's news in a separate paragraph. Test: would this article belong on the candidate's own news page? If the article is not news about the candidate, it is background — even when the candidate acts in its own sentence.
- "listed": the company appears only inside a list of industry players, market leaders, peers, or stocks that rose or fell together, or in a market summary, with nothing specific to it. A stock price move is not a business action.
- "source": a securities firm, research house, or data provider cited only as the source of a target price, forecast, rating, data point, or comment.
- "not_company": the candidate failed Step 1.
If any occurrence makes the candidate a party, the role is "party". Otherwise pick the role that best fits its strongest occurrence.

### [Bias]
A party is shown to users as a company this article is about, so a wrong party is worse than a miss. If you cannot state the main news and the candidate's own part in that event, the role is not "party".
"""

_HUMAN = (
    "**Headline**: {title}\n"
    "**Headline companies**: {headline_companies}\n\n"
    "**Article**:\n{text}\n\n"
    "**Candidate entities**:\n{entities}"
)

# 예시 1 — 공급 계약 기사. 제목 기업 엘앤에프는 후보에 없다.
_CONTRACT_TITLE = "엘앤에프, 1조2천억 하이니켈 양극재 공급 계약"
_CONTRACT_COUNTERPARTY = "엘앤에프는 삼성SDI와 하이니켈 양극재 공급 계약을 체결했다고 5일 공시했다."
_CONTRACT_AMOUNT = "계약 규모는 7억9000만 달러(한화 약 1조2200억원)다."
_CONTRACT_SUPPLIER = "이 계약 물량의 원료인 전구체는 에코프로머티가 댄다."
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
_MERGER_TITLE = "SK이노베이션, SKIET 흡수합병"
_MERGER_LEAD = "SK이노베이션이 자회사 SKIET를 흡수합병한다."
_MERGER_GROUP = "최태원 SK그룹 회장은 그동안 배터리 소재 사업 재편을 주문해 왔다."
_MERGER_CUSTOMER = (
    "합병 법인은 SKIET가 LG에너지솔루션과 맺은 분리막 장기 공급 계약을 그대로 승계한다."
)
_MERGER_PRECEDENT = "앞서 LG화학도 지난해 분리막 사업을 매각하며 소재 사업을 재편했다."
_MERGER_DEMAND = "전방산업 수요가 둔화하면서 하이브리드 차량용 분리막 출하가 줄었다."
_MERGER_FORECAST = "에프앤가이드에 따르면 SK이노베이션의 올해 영업이익 전망치는 3000억원이다."
_MERGER_RELATED = "[관련기사] 한화오션 원청교섭 새 국면…법원, 집행정지 기각"

# 예시 3 — 수혜 기대 기사. 그룹사의 계획과 다른 기업의 실적이 배경으로 인용되고, 거래 상대방이
# "카카오 계열사"라는 소속 표기로만 나온다. 제목 기업 삼성에스디에스는 후보에 없다.
_BENEFIT_TITLE = "삼성SDS, 그룹 AI 전환·두나무 지분 취득 소식에 27% 급등"
_BENEFIT_MOVE = "삼성SDS 주가가 이날 27% 오르며 52주 신고가를 새로 썼다."
_BENEFIT_STAKE = "전날 삼성SDS는 카카오 계열사가 보유한 두나무 지분 1.0%를 취득하기로 결의했다."
_BENEFIT_PARENT = (
    "삼성전자는 이달 4일 2030년까지 국내외 생산 공장을 'AI 자율공장'으로 전환하겠다는 계획을 "
    "발표했다."
)
_BENEFIT_ORDER = "삼성SDS는 삼성바이오로직스의 송도 신공장 스마트팩토리 구축 사업을 수주했다."
_BENEFIT_OTHER = "SK텔레콤도 2분기 영업이익이 시장 예상치를 웃돌며 AI 사업 기대가 커졌다."
_BENEFIT_PEERS = "같은 IT서비스주인 현대오토에버도 3% 올랐다."
_BENEFIT_ANALYST = (
    'NH투자증권은 "스마트팩토리 사업을 영위하는 삼성SDS의 매출 성장이 기대된다"고 분석했다.'
)


def _example(
    title: str, headline_companies: str, sentences: tuple[str, ...], judgements: list[dict]
) -> dict:
    return {
        "title": title,
        "headline_companies": headline_companies,
        "text": " ".join(sentences),
        "entities": "\n".join(f"- {judgement['entity']}" for judgement in judgements),
        "output": json.dumps({"judgements": judgements}, ensure_ascii=False),
    }


def _judgement(entity: str, mention: str, reason: str, role: str) -> dict:
    return {"entity": entity, "mention": mention, "reason": reason, "role": role}


_EXAMPLES = [
    _example(
        _CONTRACT_TITLE,
        "엘앤에프",
        (
            _CONTRACT_COUNTERPARTY,
            _CONTRACT_AMOUNT,
            _CONTRACT_SUPPLIER,
            _CONTRACT_PLAN,
            _CONTRACT_BROKER,
            _CONTRACT_STOCKS,
            _CONTRACT_SHARE,
            _CONTRACT_COPYRIGHT,
        ),
        [
            _judgement(
                "삼성SDI",
                _CONTRACT_COUNTERPARTY,
                "주된 뉴스인 양극재 공급 계약의 상대방이다.",
                "party",
            ),
            _judgement(
                "한화",
                _CONTRACT_AMOUNT,
                "'한화 약 1조2200억원'은 원화 환산 표기로, 기업 한화를 가리키지 않는다.",
                "not_company",
            ),
            _judgement(
                "에코프로머티",
                _CONTRACT_SUPPLIER,
                "주된 뉴스인 이 계약 물량의 전구체를 공급하는 주체다.",
                "party",
            ),
            _judgement(
                "비자",
                _CONTRACT_PLAN,
                "'소비자'라는 단어의 일부로, 기업 비자를 가리키지 않는다.",
                "not_company",
            ),
            _judgement(
                "레이",
                _CONTRACT_PLAN,
                "'인플레이션'이라는 단어의 일부로, 기업 레이를 가리키지 않는다.",
                "not_company",
            ),
            _judgement(
                "키움증권",
                _CONTRACT_BROKER,
                "목표주가를 낸 증권사일 뿐, 공급 계약의 당사자가 아니다.",
                "source",
            ),
            _judgement(
                "포스코퓨처엠",
                _CONTRACT_STOCKS,
                "함께 오른 종목 나열에만 등장한다.",
                "listed",
            ),
            _judgement(
                "에코프로비엠",
                _CONTRACT_STOCKS,
                "함께 오른 종목 나열에만 등장한다.",
                "listed",
            ),
            _judgement(
                "페이스북",
                _CONTRACT_SHARE,
                "기사 공유 버튼 문구로, 기사 내용이 아니다.",
                "not_company",
            ),
            _judgement(
                "카카오",
                _CONTRACT_SHARE,
                "공유 버튼의 '카카오톡' 일부로, 기사 내용이 아니다.",
                "not_company",
            ),
            _judgement(
                "DB",
                _CONTRACT_COPYRIGHT,
                "'재판매 및 DB 금지'라는 저작권 문구로, 기업 DB를 가리키지 않는다.",
                "not_company",
            ),
        ],
    ),
    _example(
        _MERGER_TITLE,
        "SK이노베이션, SK아이이테크놀로지",
        (
            _MERGER_LEAD,
            _MERGER_GROUP,
            _MERGER_CUSTOMER,
            _MERGER_PRECEDENT,
            _MERGER_DEMAND,
            _MERGER_FORECAST,
            _MERGER_RELATED,
        ),
        [
            _judgement(
                "SK",
                _MERGER_GROUP,
                "'SK그룹 회장'은 인물의 소속 표기이고, 상장사 SK가 행위자로 나오는 곳이 없다.",
                "not_company",
            ),
            _judgement(
                "LG에너지솔루션",
                _MERGER_CUSTOMER,
                "합병 법인이 승계하는 공급 계약의 상대방으로, 주된 뉴스인 합병에 걸려 있다.",
                "party",
            ),
            _judgement(
                "LG화학",
                _MERGER_PRECEDENT,
                "지난해 자기 사업을 재편한 사례로 인용됐을 뿐, 이번 합병의 당사자가 아니다.",
                "background",
            ),
            _judgement(
                "전방",
                _MERGER_DEMAND,
                "'전방산업'의 일반명사로, 기업 전방을 가리키지 않는다.",
                "not_company",
            ),
            _judgement(
                "하이브",
                _MERGER_DEMAND,
                "'하이브리드'라는 단어의 일부로, 기업 하이브를 가리키지 않는다.",
                "not_company",
            ),
            _judgement(
                "에프앤가이드",
                _MERGER_FORECAST,
                "실적 전망치의 출처일 뿐, 합병의 당사자가 아니다.",
                "source",
            ),
            _judgement(
                "한화오션",
                _MERGER_RELATED,
                "본문 끝에 붙은 다른 기사의 제목에만 나오고, 이 기사의 내용이 아니다.",
                "not_company",
            ),
        ],
    ),
    _example(
        _BENEFIT_TITLE,
        "삼성에스디에스",
        (
            _BENEFIT_MOVE,
            _BENEFIT_STAKE,
            _BENEFIT_PARENT,
            _BENEFIT_ORDER,
            _BENEFIT_OTHER,
            _BENEFIT_PEERS,
            _BENEFIT_ANALYST,
        ),
        [
            _judgement(
                "카카오",
                _BENEFIT_STAKE,
                "'카카오 계열사'는 소속 표기로, 지분을 판 주체는 그 계열사이지 상장사 카카오가 아니다.",
                "not_company",
            ),
            _judgement(
                "삼성전자",
                _BENEFIT_PARENT,
                "삼성전자의 공장 전환 계획은 삼성SDS 수혜 기대의 근거로 인용됐을 뿐이고, 이 기사는 "
                "삼성전자의 뉴스가 아니다.",
                "background",
            ),
            _judgement(
                "삼성바이오로직스",
                _BENEFIT_ORDER,
                "삼성SDS가 수주한 스마트팩토리 사업의 발주처로, 주된 뉴스의 상대방이다.",
                "party",
            ),
            _judgement(
                "SK텔레콤",
                _BENEFIT_OTHER,
                "자기 실적이 덧붙여 소개됐을 뿐, 삼성SDS의 뉴스와 관계가 없다.",
                "background",
            ),
            _judgement(
                "현대오토에버",
                _BENEFIT_PEERS,
                "함께 오른 같은 업종 종목으로만 등장한다.",
                "listed",
            ),
            _judgement(
                "NH투자증권",
                _BENEFIT_ANALYST,
                "전망을 낸 증권사일 뿐, 주된 뉴스의 당사자가 아니다.",
                "source",
            ),
        ],
    ),
]

_EXAMPLE_PROMPT = ChatPromptTemplate.from_messages([("human", _HUMAN), ("ai", "{output}")])

_FEW_SHOT_PROMPT = FewShotChatMessagePromptTemplate(
    example_prompt=_EXAMPLE_PROMPT,
    examples=_EXAMPLES,
)

# Passing a SystemMessage instance keeps the system body out of template-variable parsing
PROMPT = ChatPromptTemplate.from_messages(
    [
        SystemMessage(content=_SYSTEM),
        _FEW_SHOT_PROMPT,
        ("human", _HUMAN),
    ]
)
