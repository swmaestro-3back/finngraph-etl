"""후보 기업 판정(엔티티 필터) 질문 — jev(TypeSafe System One)용.

후보(candidates)는 제목·본문에 나온 기업 중 검색 종목을 뺀 것이다 — 검색 종목은 관련성 필터가
이미 판정해 후보에 없다. 모델은 제목·검색 종목(query_stocks, 관련성을 통과한 주인공)·리드와, 후보마다
표기가 든 문장들(제목 포함)을 보고 두 질문에 답한다. 주된 뉴스의 당사자(party)만 통과다. 결과가 기업 피드와 Event 당사자에 그대로 노출되므로 애매하면 버린다.

jev 는 생성 모델이 아니다. 질문마다 확률만 돌려주므로 판정 기준은 아래 두 질문의 criteria 에 적고,
"애매하면 party 아님" 은 filters.entity_filter 의 임계값이 맡는다. 긴 단어의 일부(SK하이닉스의 SK)는
질문 전에 코드가 거른다 — 같은 모듈의 토큰 규칙.

- `is_company` (Noul): 그 표기가 이 상장사를 가리키는가. 코드 규칙이 못 거르는 동명(대상·DB·흥국생명)·
  원화 표기·페이지 보일러플레이트가 대상이다.
- `role` (Choice): source / listed / background / party. not_company 를 옵션에 두지 않는 것은 "none of
  the above 옵션은 잘 안 된다"는 공식 지침이고, party 를 맨 뒤에 둔 것은 첫 옵션 편향 때문이다. 옵션마다
  `{what, not_for, examples}` 를 적는다 — 경계가 갈리는 유형(공급계약 상대, 금융 서비스 제공자, 비상장
  계열사의 모회사, 고객 설명, 그룹 지칭, 동반 급등 속 참여사, 주인공의 다른 표기)이 여기 들어 있다.

질문은 영어(jev 의 주 학습 언어), 예시는 한국어 기사 문장이다. 규칙을 고치면 finngraph-v2 의
evals/entity_filtering 골드로 효과를 잰다.
"""

from __future__ import annotations

from typing import Any

ROLE_CRITERIA = {
    "source": {
        "what": (
            "Cited only as the source of a target price, forecast, rating, or comment: a "
            "securities firm, research house, or data provider."
        ),
        "examples": [
            "키움증권은 이날 엘앤에프 목표주가를 15만원으로 올렸다.",
            "에프앤가이드 컨센서스",
        ],
    },
    "listed": {
        "what": (
            "Appears only in a list of peers, market leaders, or stocks that rose or fell "
            "together. A price move is not a business action."
        ),
        "not_for": "Several counterparties of one contract listed in one sentence: those are parties.",
        "examples": [
            "이날 증시에서는 포스코퓨처엠(2.1%) 등 2차전지주가 함께 올랐다.",
            "같은 IT서비스주인 현대오토에버도 3% 올랐다.",
        ],
    },
    "background": {
        "what": (
            "A real mention that is not a participant in the main news: its own action, plan, "
            "earnings, or situation is cited to explain or give context. Also a group name or "
            "group facility (SK그룹, LG그룹주, LG사이언스파크); a financial service provider for "
            "the deal (IPO underwriter 상장주선인, subscription broker 청약 창구, sale advisor "
            "매각주관사, treasury-stock trustee 신탁계약 수탁사); an existing customer, supplier, "
            "or partner named only in a company profile or analyst report (주요 고객은 X, X향 "
            "매출 비중); a company named only in page boilerplate or other articles' headlines."
        ),
        "examples": [
            "삼성전자는 2030년까지 생산 공장을 'AI 자율공장'으로 전환하겠다는 계획을 발표했다. (cited to explain 삼성SDS's rally)",
            "SK텔레콤도 2분기 영업이익이 시장 예상치를 웃돌았다. (another company's result)",
            "카카오X는 카카오뱅크·카카오페이 등 계열사를 관리한다. (affiliates enumerated)",
        ],
    },
    "party": {
        "what": (
            "The company itself takes part in the event the headline reports: a counterparty of "
            "the supply, order, delivery, or sales contract (the customer as much as the "
            "supplier), the other side of a joint development, cooperation agreement (협약, "
            "MOU), consortium, or joint venture, a buyer or seller of the stake, an investor "
            "that puts its own money in (CB subscriber, block-deal buyer), a co-defendant, or "
            "the headline company itself under another spelling (English name, short name, "
            "full name behind an abbreviation). An unlisted affiliate's action is its nearest "
            "listed parent's action: when the seller or supplier is 'SK에코플랜트' or '카카오 "
            "계열사', the listed parent (SK, 카카오) is the party."
        ),
        "not_for": (
            "A company whose separate fact is cited as context, a peer in a stock list, a "
            "forecast source, a financial intermediary, or an existing customer in a profile. "
            "A wrong party is worse than a miss: if you cannot state the main news and this "
            "company's own part in it, it is not a party."
        ),
        "examples": [
            "엘앤에프는 삼성SDI와 하이니켈 양극재 공급 계약을 체결했다고 공시했다. (삼성SDI = customer)",
            "삼성SDS와 삼성카드는 카카오 계열사가 보유한 두나무 지분을 각각 취득하기로 했다. (삼성카드 = co-buyer, 카카오 = seller through its affiliate)",
            "STX엔진, 한화엔진과 한화오션 등에 공급계약을 체결했다. (each listed counterparty = party)",
        ],
    },
}

IS_COMPANY_TRUE = (
    "The surface form names the listed company itself (including an English or short name), "
    "or a group label or unlisted affiliate name that belongs to this listed company "
    "(SK그룹 → SK, 카카오 계열사 → 카카오, 한화큐셀 → 한화)."
)
IS_COMPANY_FALSE = (
    "An ordinary word that happens to be spelled like the name (대상, 전방, DB as in '재판매 및 "
    "DB 금지'), a sports team or league sponsor that shares the name (KCC in '2026-2027 KCC "
    "프로농구', KT in 'KT 이강철 감독'), won currency notation (한화 약 1조원), an unrelated company "
    "that merely shares "
    "the name (흥국생명 vs 흥국), or page boilerplate: share buttons (카카오톡 URL복사), bylines, "
    "copyright lines, other articles' headlines."
)


def build_questions(count: int, context_key: str = "lead") -> dict[str, Any]:
    """state 의 `candidates[0..count)` 에 대한 질문 2×count. context_key 는 주된 뉴스를 읽을 state 필드."""
    from typesafe_sdk import Choice, Noul, NoulCriteria

    questions: dict[str, Any] = {}
    for i in range(count):
        ref = f"candidates[{i}]"
        questions[f"c{i}_is_company"] = Noul(
            instructions={
                "question": (
                    f"In `{ref}.sentences`, does the surface form `{ref}.surface` refer to the "
                    f"listed company `{ref}.name`, directly or through its group or an "
                    f"unlisted affiliate?"
                ),
            },
            criteria=NoulCriteria(true=IS_COMPANY_TRUE, false=IS_COMPANY_FALSE),
        )
        questions[f"c{i}_role"] = Choice(
            instructions={
                "question": (
                    f"What role does the company `{ref}.name` (surface form `{ref}.surface`) "
                    f"play in the main news that `headline` reports about "
                    f"`query_stocks`? Judge by its strongest occurrence in "
                    f"`{ref}.sentences`; `{context_key}` tells what the main news is."
                ),
                "main_news": (
                    "The event the headline reports: the deal, result, or decision. A stock "
                    "price move is not the event; the business action behind it is."
                ),
                "if_not_the_company": (
                    "If the surface form does not refer to this company at all, still pick the "
                    "role the text gives the entity it names; a separate question handles that."
                ),
            },
            criteria=ROLE_CRITERIA,
        )
    return questions
