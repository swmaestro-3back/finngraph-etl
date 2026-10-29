"""관련성 필터 질문 — jev(TypeSafe System One)용.

기사 제목만 보고, 이 기사를 가져온 검색 종목마다 그 표기가 그 기업을 가리키는지(is_company)와
그 기업 페이지에 보여줄 기사인지(has_event·has_driver)를 판정한다. 결과는 기업 페이지 피드와 저장 여부를
정하므로 애매하면 버린다. 제목의 다른 상장사는 묻지 않는다 — 엔티티 필터가 본문과 함께 판정한다.

jev 는 생성 모델이 아니다. 질문마다 확률만 돌려주므로 Noul 셋으로 따로 묻는다. "다른 기업의
뉴스에 동반 움직임일 뿐이면 통과 아님" 은 각 질문의 false 기준에 들어 있다. 결합과 "애매하면
버림"(임계값)은 filters.relevance_filter 가 맡는다.
질문은 state 의 `companies[i]` 를 가리키는 경로로 쓰고, 한 요청에 기업 × 질문 셋을 모두
펼친다(speculative fan-out). 검색 종목은 대개 하나라 요청당 질문 셋이다.

- `is_company` (Noul): `companies[i].surface` 가 제목에서 상장사 `companies[i].name` 을
  가리키는가. 일반 명사(대상·태양), 다른 기업 이름의 일부, 그룹의 다른 상장사가 false 다.
- `has_event` (Noul): 주된 뉴스가 이 기업이 참여한 실행된 사건인가 — 명시된 상대와의 경제
  관계(계약·수주·지분·합병·소송·제재)와 이 기업에 일어난 사건(실적·CB·증설·규제). 제목에 상대로
  명시된 기업도 행위자와 같이 통과한다.
- `has_driver` (Noul): 사건은 아니지만 이 기업이 주체이고 자사 사업의 구체적 동인(수주 기대·정책·
  수요·애널리스트 견해, 긍정·부정 모두)이 명시됐는가.
  true·false 설명과 예시를 criteria 로 나눠 경계를 적는다.

질문은 영어(jev 의 주 학습 언어), 예시는 한국어 제목이다. 규칙을 고치면 finngraph-v2 의 관련성
골드(evals/relevance, 검색 종목 행만 유효)로 효과를 재야 한다.
"""

from __future__ import annotations

from typing import Any

# 질문마다 반복되는 판정 전제. 제목이 유일한 증거라는 점과 비상장 계열사 귀속 규칙이다.
EVIDENCE = (
    "The headline is the only evidence. Never supply an entity, relation, event, or driver from "
    "general knowledge. Judge this company on its own, as a desk editor deciding whether the "
    "article belongs on this listed company's news page."
)
MAIN_NEWS = (
    "The MAIN news is the event or relation the headline reports. A stock price move is not the "
    "news; the stated cause behind it is."
)
AFFILIATE_RULE = (
    "When the headline names this company's group or an unlisted affiliate it owns, judge as if "
    "this listed company did what the affiliate did (한화큐셀 → 한화)."
)

IS_COMPANY_TRUE = (
    "The spelling names the listed company itself (full name, short form, or nickname: LG엔솔 for "
    "LG에너지솔루션, 삼전 for 삼성전자), its group, or an unlisted affiliate it owns, whose news "
    "belongs on the listed parent's page (한화큐셀 → 한화, SK에코플랜트 → SK, LS전선 → LS). A "
    "longer company name that STARTS with this spelling also counts: corporate ties cannot be "
    "checked from a headline, so treat it as this company or its affiliate."
)
IS_COMPANY_FALSE = (
    "The spelling is an ordinary word or part of a phrase (대상 in '임직원 대상 성과급' or "
    "'조세 부과 대상', 태양 in '태양광'); a sports team that shares the name (KT in 'KT, 소노 잡고 2연승', "
    "DB in '프로농구 DB, 정관장 잡고 3연승'); it sits inside a different company's name (하이브 in "
    "폴로하이브머티리얼즈); or it names a group whose acting affiliate's listed parent is a "
    "different company in `companies` — a listed sibling or subsidiary is never the group (OCI in "
    "'OCI 폴리실리콘 증설' means the group led by OCI홀딩스, not the spun-off chemicals maker OCI)."
)

GATE_CRITERIA: dict[str, dict[str, Any]] = {
    "has_event": {
        "question": (
            "Is the MAIN news of `headline` an executed event that this company took part in?"
        ),
        "true": {
            "what": (
                "The main news is an executed event and this company is one of its participants. "
                "Either an economic relation with an explicitly named counterparty (company, "
                "government, country, commodity, or product): ownership or capital "
                "(인수·합병·분할·매각·투자·지원), contract or supply "
                "(계약·제휴·수주·선정·공급·납품·판매·위탁), production or trade "
                "(생산·증산·감산·수출·수입), or state action (승인·규제·제재·관세·소송·판결·협정) "
                "— both endpoints are named in the headline and this company is one of them; the "
                "named counterparty passes as much as the actor, and the acquired or merged company "
                "and the company whose stake is sold are participants as much as the buyer. Or an "
                "event that happened at or "
                "to this company and affects its revenue, cost, financing, ownership, production, "
                "operations, or regulation: earnings, guidance, capital raise (유상증자·CB), "
                "capacity expansion or start-up, regulatory decision, lawsuit or ruling, "
                "disclosure, recall, production halt or restart, management or labor event. Match "
                "by meaning, not verb form. The event is stated in the headline, not implied by a "
                "price move."
            ),
            "examples": [
                "SNT에너지, 남부발전 1천282억 수주에 9% 상승 → true for SNT에너지 and 남부발전",
                "엘앤에프, 삼성SDI에 양극재 공급 계약 → true for 엘앤에프 and for 삼성SDI",
                "우리로, ETRI 광검출기 기술 이전에 상한가 → true for 우리로",
                "한화큐셀, 美 조지아 솔라허브 가동 시작 "
                "→ true for 한화 (unlisted affiliate's capacity start-up)",
                "엘앤에프, 2천억 규모 CB 발행 결정 → true",
                "앤씨앤, 비투엔 피인수 소식에 상한가 → true for 앤씨앤 (the acquired company)",
            ],
        },
        "false": {
            "what": (
                "A price move with no stated event; the relation or event is only expected or "
                "discussed, not executed; the other side is a pronoun, a sector, a '관련주' "
                "grouping, or not named at all; an event that happened to another company while "
                "this one only moves with it, or this company is named alongside, as a peer, or in "
                "a roundup; routine launches, promotions, awards, appointments, CSR, or employee "
                "compensation; advertising and fragments. The company's own product or technology "
                "announcement (공개·출시·개발·선보인다), taking part in an exhibition or conference "
                "(참가·개최), a co-marketing or promotion tie-up, and a bare direction word (진출 "
                "가속·본격화·박차) are not executed events: the headline must state an order, "
                "approval, contract, financing, or a named counterparty's commitment."
            ),
            "examples": [
                "삼성전자 2% 강세, 미국발 훈풍 → false",
                "삼성전자, 임직원 대상 성과급 지급 → false (routine compensation)",
                "바이오주 일제히 급락…알테오젠 8%↓ → false (only a price move)",
                "우리로, ETRI 광검출기 기술 이전에 상한가…한국첨단소재도 동반 상승 "
                "→ false for 한국첨단소재",
                "포시에스, AI에이전트 플랫폼 공개 → false (own product announcement)",
                "일동제약, 밀라노 CPhI 2026 참가 → false (exhibition attendance)",
                "동화약품, 가마치통닭과 공동 마케팅 업무협약 → false (promotion tie-up)",
            ],
        },
    },
    "has_driver": {
        "question": (
            "Is this company a subject of `headline`, and does the headline name a concrete driver "
            "of its stock move or outlook that concerns its own business?"
        ),
        "true": {
            "what": (
                "The headline names which project, policy, product, or market drives this "
                "company's move or outlook: an order, contract, or project it may win or lose; a "
                "policy, tax, subsidy, or regulation aimed at its industry; demand, price, or "
                "capacity conditions for its products or markets; an analyst view that says what it "
                "rests on. The driver may be real or only expected (기대감·전망·수혜·우려), positive "
                "or negative. A sector-led headline (방산주, 반도체주) passes for a company it names "
                "as a mover when the driver concerns that sector's business. A geopolitical or macro "
                "event (war, sanctions, trade dispute, disaster, oil or FX shock) counts when the "
                "headline says how it reaches this company's own products, markets, or projects "
                "(방산 수출 기대, 운임·정제마진, 해외 공장 피해)."
            ),
            "examples": [
                "한화엔진, 데이터센터용 엔진 성장 기대감에 7%대 강세 "
                "→ true (demand for its own product)",
                "방산주, 폴란드 2차 계약 기대감에 강세…현대로템 6%↑ → true for 현대로템",
                "LS에코에너지, 美 태양광 전력망 시장 확대 → true (a named market for its product)",
                "한화에어로스페이스, 유럽 전쟁 장기화에 수출 기대감 강세 "
                "→ true (war reaching its own export market)",
            ],
        },
        "false": {
            "what": (
                "No driver is named, or the driver is market-wide (지수, 외국인·기관 수급, "
                "미국 증시 훈풍, 금리·환율, 차익 실현) or a bare sector or theme move; the driver "
                "is another "
                "company's contract, earnings, or event; politics with no stated link to this "
                "company's business; market roundups (시황, 수급 동향, 특징주 종합). The same "
                "geopolitical or macro event is false when the headline ties it only to the index, "
                "foreign flows, or a sector move rather than to this company's business. The "
                "company merely announcing, developing, or unveiling its own product, technology, "
                "or service (공개·출시·개발·도입·구축), attending an exhibition or conference, a "
                "marketing tie-up, or a bare direction word (진출 가속·본격화·박차) is not a "
                "driver: the headline must say what demand, order, price, policy, or approval "
                "moves this company's business."
            ),
            "examples": [
                "바이오주 일제히 급락…알테오젠 8%↓ → false (no driver named)",
                "삼성전자 2% 강세, 미국발 훈풍 → false (market-wide factor)",
                "중동 전쟁 확전에 코스피 급락…삼성전자 3%↓ → false (war reaching only the index)",
                "SK케미칼, 탄성소재 적용 유연 데코시트 개발 → false (own product development)",
                "뉴로핏, 미국 시장 진출 '가속페달' → false (direction word, no named driver)",
            ],
        },
    },
}


def build_questions(count: int) -> dict[str, Any]:
    """state 의 `companies[0..count)` 에 대한 질문 3×count. 답은 `c{i}_<질문>` 키로 돌아온다."""
    from typesafe_sdk import Noul, NoulCriteria

    questions: dict[str, Any] = {}
    for i in range(count):
        ref = f"companies[{i}]"
        subject = f"the listed company `{ref}.name`, named in `headline` as `{ref}.surface`"
        questions[f"c{i}_is_company"] = Noul(
            instructions={
                "question": (
                    f"In `headline`, does the spelling `{ref}.surface` refer to the listed "
                    f"company `{ref}.name`, directly or through its group or an unlisted "
                    f"affiliate it owns?"
                ),
                "evidence": EVIDENCE,
            },
            criteria=NoulCriteria(true=IS_COMPANY_TRUE, false=IS_COMPANY_FALSE),
        )
        for gate, spec in GATE_CRITERIA.items():
            questions[f"c{i}_{gate}"] = Noul(
                instructions={
                    "question": f"{spec['question']} The company is {subject}.",
                    "main_news": MAIN_NEWS,
                    "affiliates": AFFILIATE_RULE,
                    "evidence": EVIDENCE,
                },
                criteria=NoulCriteria(true=spec["true"], false=spec["false"]),
            )
    return questions
