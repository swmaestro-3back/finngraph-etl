"""본문 기업 판정(엔티티 필터) 프롬프트.

후보는 본문에만 나온 기업이다 — 제목에 나온 기업은 관련성 필터가 이미 판정해 후보에 없다. 모델은
제목과 제목 기업(이 기사의 주인공)을 함께 보고, 후보마다 제목이 전하는 주된 뉴스에서 맡은 역할을
고른다. 주된 뉴스의 당사자(party)만 통과다. 결과가 기업 피드와 Event 당사자에 그대로 노출되므로
애매하면 버린다.

규칙은 시스템에 한 번만 적고 패턴은 예시가 보여 준다. 예시 두 건은 저장된 기사 본문의 실제 후보에서
뽑은 가장 흔한 오탐을 하나씩만 담는다: 긴 단어의 일부(레이), 원화 표기(한화), 공유 버튼(카카오톡),
목표주가를 낸 증권사, 주가 나열, 계열사 소속 표기(카카오 계열사), 그리고 주인공 기업의 뉴스를
설명하려고 인용된 다른 기업의 사실(그룹사의 계획, 다른 기업의 실적). 예시는 기사 한 건의 모양을
그대로 흉내 낸다: 제목 기업은 본문에 나오지만 후보 목록에는 없다. 예시의 reason 은 짧게 둔다 —
모델이 그 길이를 따라 하므로 출력 토큰이 그만큼 준다.
"""

import json

from langchain_core.messages import SystemMessage
from langchain_core.prompts import ChatPromptTemplate, FewShotChatMessagePromptTemplate

_SYSTEM = """\
### [Role]
You verify company mentions for a Korean stock-news service. You get an article's headline, the headline companies (the companies this article is about — already judged, not in the candidate list), the body, and candidate company names that a lexical dictionary matcher found only in the body. For each candidate, decide its role in this article. Only a party to the main news is linked to the article.

### [Hard Rules]
- Judge every candidate exactly once, in the given order. Never add, rename, or merge candidates; copy "entity" back exactly.
- "mention" is one sentence copied verbatim from the body that contains the candidate's surface form — the occurrence that best shows its role.
- Fill "mention" and "reason" before "role".

### [Step 1 — Does the surface form mean that company?]
The matcher ignores word boundaries, so check:
- Ordinary word or word fragment, often with a particle attached: "대상으로" (대상), "소비자" (비자), "하이브리드" (하이브), and won notation "(한화 약 1조원)" (한화).
- Affiliate or group label: "SK그룹 회장", "카카오 계열사", "LG 자회사" name a group, a person's affiliation, or a different unnamed company. The actor is not the listed parent — do not use outside knowledge to decide which affiliate it is, and do not link the parent for an affiliate's deal.
- Page boilerplate: share buttons, copyright lines, bylines, and appended headlines of other articles ("[관련기사] …") are not this article.
If no occurrence refers to the company itself, the role is "not_company".

### [Step 2 — Role in the main news]
First name the main news: the event the headline reports about the headline companies. A price move is not the event; the deal, result, or decision behind it is. Then assign one role:
- "party": the company itself takes part in that event — jointly with the headline company (co-acquirer, co-signatory, consortium member) or as its counterparty, customer, supplier, partner, investor, acquirer, target, or lawsuit counterpart.
- "background": its own action, plan, earnings, or situation is cited only to explain or give context to the main news — e.g. a parent or group company's plan behind a subsidiary's upside, or another company's earnings or investment as industry backdrop. Test: is the main event also news about the candidate? If not, it is background even when the candidate acts in its own sentence.
- "listed": appears only in a list of peers, market leaders, or stocks moving together. A price move is not a business action.
- "source": a securities firm, research house, or data provider cited only for a target price, forecast, rating, or comment.
Judge by the candidate's strongest occurrence; any occurrence that makes it a party makes the role "party".

### [Bias]
A party is shown to users as a company this article is about, so a wrong party is worse than a miss: if you cannot state the main news and the candidate's own part in it, the role is not "party".
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
_CONTRACT_PLAN = "회사는 인플레이션 감축법(IRA) 요건을 고려해 미국 공장 증설도 검토한다."
_CONTRACT_BROKER = "키움증권은 이날 엘앤에프 목표주가를 15만원으로 올렸다."
_CONTRACT_STOCKS = "이날 증시에서는 포스코퓨처엠(2.1%) 등 2차전지주가 함께 올랐다."
_CONTRACT_SHARE = "페이스북 트위터 카카오톡 URL복사"

# 예시 2 — 수혜 기대 기사. 주가 급등 뒤의 사건(지분 취득)에 공동 취득자가 있고, 그룹사의 계획과
# 다른 기업의 실적이 배경으로 인용되고, 거래 상대방이 "카카오 계열사"라는 소속 표기로만 나온다.
# 제목 기업 삼성에스디에스는 후보에 없다.
_BENEFIT_TITLE = "삼성SDS, 그룹 AI 전환·두나무 지분 취득 소식에 27% 급등"
_BENEFIT_MOVE = "삼성SDS 주가가 이날 27% 오르며 52주 신고가를 새로 썼다."
_BENEFIT_STAKE = (
    "전날 삼성SDS와 삼성카드는 카카오 계열사가 보유한 두나무 지분을 각각 1.0% 취득하기로 결의했다."
)
_BENEFIT_PARENT = (
    "삼성전자는 이달 4일 2030년까지 국내외 생산 공장을 'AI 자율공장'으로 전환하겠다는 계획을 "
    "발표했다."
)
_BENEFIT_ORDER = "삼성SDS는 삼성바이오로직스의 송도 신공장 스마트팩토리 구축 사업을 수주했다."
_BENEFIT_OTHER = "SK텔레콤도 2분기 영업이익이 시장 예상치를 웃돌며 AI 사업 기대가 커졌다."
_BENEFIT_PEERS = "같은 IT서비스주인 현대오토에버도 3% 올랐다."


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
            _CONTRACT_PLAN,
            _CONTRACT_BROKER,
            _CONTRACT_STOCKS,
            _CONTRACT_SHARE,
        ),
        [
            _judgement("삼성SDI", _CONTRACT_COUNTERPARTY, "공급 계약의 상대방.", "party"),
            _judgement("한화", _CONTRACT_AMOUNT, "원화 환산 표기.", "not_company"),
            _judgement("레이", _CONTRACT_PLAN, "'인플레이션'의 일부.", "not_company"),
            _judgement("키움증권", _CONTRACT_BROKER, "목표주가를 낸 증권사.", "source"),
            _judgement("포스코퓨처엠", _CONTRACT_STOCKS, "함께 오른 종목 나열.", "listed"),
            _judgement("카카오", _CONTRACT_SHARE, "공유 버튼의 '카카오톡'.", "not_company"),
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
        ),
        [
            _judgement("삼성카드", _BENEFIT_STAKE, "두나무 지분의 공동 취득자.", "party"),
            _judgement(
                "카카오",
                _BENEFIT_STAKE,
                "'카카오 계열사'는 소속 표기. 매도 주체는 그 계열사다.",
                "not_company",
            ),
            _judgement(
                "삼성전자",
                _BENEFIT_PARENT,
                "삼성SDS 수혜의 근거로 인용된 그룹사 계획. 삼성전자의 뉴스가 아니다.",
                "background",
            ),
            _judgement(
                "삼성바이오로직스", _BENEFIT_ORDER, "삼성SDS가 수주한 사업의 발주처.", "party"
            ),
            _judgement(
                "SK텔레콤",
                _BENEFIT_OTHER,
                "덧붙여진 다른 기업의 실적. 주된 뉴스와 무관.",
                "background",
            ),
            _judgement("현대오토에버", _BENEFIT_PEERS, "함께 오른 업종 종목.", "listed"),
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
