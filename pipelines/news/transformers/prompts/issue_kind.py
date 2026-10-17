"""이슈 성격 분류 프롬프트다. 회사 자신의 사건(event)인지, 외부 요인·전망·증권사 의견에 따른 주가
반응(market_reaction)인지 가린다.

주가 반응 이슈는 타임라인 연결에서 모두 빠진다(부모도 후보도 되지 않는다). 지시문은 다른 연결
프롬프트처럼 영어로 쓰고, 예시는 입력과 같은 한국어 제목으로 둔다. 예시 제목은 지어낸 회사 이름으로
만든 합성 제목이다. 실제 기사 제목을 예시로 쓰면 그 이슈를 분류할 때 예시가 답을 알려 주게 되므로
쓰지 않는다. 판정은 도구 강제 호출로 받는다.
프롬프트를 바꾸면 llm.PROMPT_VERSIONS["issue_kind"] 를 올려야 캐시된 옛 응답을 다시 쓰지 않는다.
"""

from __future__ import annotations

from typing import Any

SYSTEM = """You classify one issue for a Korean stock-market news app. An issue is a cluster of news articles, and the app links issues into timelines of company events. Decide what the issue's main news is.

Decide mainly by the issue title and by what most of the article titles report. The headline point and the summary come from one representative article. They often restate earlier facts as background, such as an earlier contract, earlier results, an earlier deal or an ongoing plan, and such a fact counts only when the articles report it as their own news. They can also show only that one article's angle, such as a preview written before results came out. When the summary and most article titles disagree, follow the article titles. When the article titles disagree, follow the title and the majority of the article titles, with one exception: when the title states a fact of the company itself (something it did, decided, disclosed or reported, not an expectation) and at least one article title reports that fact as news, choose event even if the other articles are previews or stock-move reports.
The summary also tells what triggered a stock move. When it says the move came from a new fact that the company itself confirmed or disclosed right before the move, that fact is the news of these articles, not background.

event: the main news is something that happened at or to the company itself (or to the named companies) and that the articles report as new: its own announcement, disclosure or filing; actual results for a finished period; a contract, order or agreement it signed; an investment, capacity expansion, spin-off or restructuring it decided; a merger or acquisition, tender offer, share issue to an investor or stake sale it is party to; a share buyback or cancellation; share purchases by its largest shareholder or management; a product launch, development milestone, approval or certification; a lawsuit, tax charge, sanction or regulatory action against it; a statement by its management or a dispute over its own disclosure or statement; a management change, labor dispute, accident or incident; a trading halt or resumption. A deal of its subsidiary counts as its own, and so does a deal of a company it invested in when the company itself announces that deal. Title wording such as 어닝 서프라이즈, 어닝 쇼크, 컨센서스 상회·하회 or 사상 최대 실적 달성 reports actual results that came out, even when the summary is a preview written before them. When some articles preview results and others report the actual results, the actual results are the main news. It is still event when the articles report it together with the stock move it caused or with analysts' comments on it, for example "한결전자, 2분기 영업이익 41% 증가에 18% 급등" (the company's own reported results).
A stock jump triggered by a new fact that the company itself confirmed or disclosed is also event, even when the deal is not final yet and the headlines speak of 기대감: the company confirmed that it is in talks or has a meeting scheduled with a counterparty, published its own reference material, IR material or filing, or stated its own plan or timeline for commercialization, mass production or supply. Headlines such as "수주 기대감에 급등" or "협의 소식에 강세" are event when the articles or the summary show that the company itself confirmed or disclosed that fact.
Event examples:
- "서진중공업, 해외 조선사와 엔진 공급 협의 중이라고 밝혀…수주 기대감에 급등"
- "다온텔레콤, 데이터센터 합작 참고자료 공시에 기대감 급등"
- "하늘바이오, 대표가 직접 밝힌 상용화 일정에 상한가"
An issue without a company is event when its main news is an occurrence itself, such as a government policy announcement, a court ruling or the release of an economic indicator.

market_reaction: the main news is a stock or sector move or an outlook, and the company itself has no new event in these articles. This covers:
- Moves driven by outside factors: macro news, commodity prices, geopolitics, policy expectations, remarks by government officials or politicians, a theme, investor flows (including an outside fund disclosing a larger passive stake), or an event of another company such as a partner, customer, competitor or group affiliate. An outside event is not the company's own event, even when the company is said to benefit or suffer from it.
- Analyst and broker notes: target prices, ratings, earnings forecasts or previews, investment points. They stay market_reaction even when they quote figures or recap the company's earlier events and plans.
- Expectations without a fact the company itself confirmed or disclosed: expected results before the results are reported (실적 기대, 실적 전망), or hopes for an outcome that has not happened yet, such as an order not yet won, a possible role in a supply chain, or talks or a possibility reported only by unnamed sources, the industry or other parties and not confirmed by the company (기대, 가능성, ~로 알려지며, 업계에 따르면).
- Statistics and rankings compiled by outside parties, such as export or customs data, industry association figures or a research firm's market share or sales ranking, even when they are about the company's own products. The company's own announcement or filing of its results or sales is event.
- Business feature articles: stories that profile the company's business, strategy, technology, sponsorship or marketing, or recap deliveries and contracts made earlier, without a new step that the articles date to these days. When the articles report a new step that the company took or announced in these days, such as a new plan, entry into a new market, a presentation at a trade fair or the start of a supply, it is event.
Examples:
- "대양에너지·한빛석유 유가 급등 수혜 강세"
- "중동 긴장 고조에 해운주 일제히 급등"
- "가람전자 3분기 실적 전망…목표가 상향"
- "새론바이오, 후속 기술수출 기대감에 상승"
- "누리로봇, 협력사 휴머노이드 공개에 급등"
- "미래모빌리티, 북미 수주설에 급등…회사 측 '확정된 바 없다'"
- "보람자동차, 조사업체 집계 1~9월 전기차 판매 순위 상승"
- "한빛정밀, 검사장비 한 우물 20년…해외 매출 비중 절반 넘어"
- "장관 '반도체 지원 확대' 발언에 장비주 강세"

If the articles report a new event of the company itself together with the stock move, including a fact the company itself confirmed or disclosed that triggered the move, choose event. If the stock move, an outlook, an expectation without such a fact, an analyst's view, outside statistics or a business feature is the only news about the company, choose market_reaction.

Record the result with the record_issue_kind tool. Write main_news and reason in Korean, one short sentence each."""

TOOL: dict[str, Any] = {
    "name": "record_issue_kind",
    "description": "Record whether the issue is the company's own event or a market reaction.",
    "schema": {
        "type": "object",
        "properties": {
            "main_news": {
                "type": "string",
                "description": "What the title and most article titles report, in one short sentence.",
            },
            "reason": {"type": "string", "description": "Why the kind fits, in one short sentence."},
            "kind": {"type": "string", "enum": ["event", "market_reaction"]},
        },
        "required": ["main_news", "reason", "kind"],
    },
}
