"""관련성 필터 — 이 기사를 가져온 검색 종목의 페이지에 저장할 기사인가 (jev, 제목만).

판정 대상은 검색 종목뿐이다. 제목의 다른 상장사는 여기서 묻지 않는다 — 본문 크롤링 뒤
company_matches.match_candidates 가 제목·본문의 나머지 기업을 후보로 모으고 엔티티 필터가 본문
증거로 판정한다.
질문은 셋이다 — is_company(그 표기가 이 기업인가), has_event(이 기업이 참여한 실행된 사건인가),
has_driver(사건은 아니지만 자사 사업의 구체적 동인이 명시됐는가). "동반 움직임" 질문은 따로 없다:
검색 종목이 남의 뉴스에 끌려 움직일 뿐이면 has_event·has_driver 가 둘 다 false 다.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from functools import lru_cache
from typing import Any

from pydantic import BaseModel, Field

from pipelines.common.clients.jev import aclose_judge, check_answers
from pipelines.common.logging import get_logger
from pipelines.news.config import get_news_settings

logger = get_logger(__name__)

GATES = ("has_event", "has_driver")

# 판정 임계값. 값은 NEWS_RELEVANCE_* 환경변수(기본값은 news.config.NewsSettings)로 정한다. 원시
# 확률은 item 의 `_relevance_judgements` 에 남으므로 골드로 재호출 없이 스윕할 수 있다
_settings = get_news_settings()
THRESHOLDS = {
    # is_company 가 이 밑이면 그 기업을 가리키지 않는다
    "company_min": _settings.relevance_company_min,
    "gate_min": _settings.relevance_gate_min,  # has_event·has_driver 중 최댓값이 이 이상이면 valid
}


class CompanyVerdict(BaseModel):
    """판정 기업 하나의 판정. `scores` 는 jev 원시 확률(평가·임계값 조정용)."""

    name: str  # 입력의 제목 표기
    is_company: bool
    valid: bool
    scores: dict[str, float] = Field(default_factory=dict)


class ArticleVerdict(BaseModel):
    """기사 하나의 판정. 입력 기업 순서대로 하나씩."""

    companies: list[CompanyVerdict]


@dataclass(frozen=True)
class ArticleInput:
    title: str
    # 판정 기업 [{surface: 제목 표기, name: 사전 표준명}]. 이 기사를 가져온 검색 종목들이다.
    companies: tuple[dict[str, str], ...]


Judge = Callable[[ArticleInput], Awaitable[ArticleVerdict]]


@dataclass
class RelevanceResult:
    passed: list[dict[str, Any]] = field(default_factory=list)
    invalid: list[dict[str, Any]] = field(default_factory=list)
    failed: list[dict[str, Any]] = field(default_factory=list)
    # jev 호출마다(실패 포함) 걸린 시간. 런 요약 로그가 jev.latency_summary 로 묶는다
    latencies_ms: list[float] = field(default_factory=list)


def search_companies_of(item: dict[str, Any]) -> list[dict[str, Any]]:
    """판정할 기업 — 이 기사를 가져온 검색 종목 중 제목에 나온 것. 표기(surface)는 제목 매치의 것.

    같은 기사가 여러 종목 검색에 걸리면 URL 중복 제거가 사본 하나로 합치며 `_query_companies` 에
    모두 기억한다. 그 정보가 없으면 `_query_company` 하나다. 제목 기업 매치(`_title_companies`)를
    거치지 않은 기사는 검색 종목 이름을 표기로 삼는다.
    """

    fetched = {int(company["company_id"]) for company in item.get("_query_companies") or []}
    query = item.get("_query_company")
    if query:
        fetched.add(int(query["company_id"]))
    if not fetched:
        return []

    title_companies = item.get("_title_companies")
    if title_companies:
        return [company for company in title_companies if company["company_id"] in fetched]
    if not query:
        query = item["_query_companies"][0]
    return [
        {"company_id": int(query["company_id"]), "name": query["name"], "surface": query["name"]}
    ]


def resolve_verdict(
    companies: list[dict[str, Any]], verdict: ArticleVerdict
) -> tuple[bool | None, list[dict[str, Any]]]:
    """판정을 기업으로 되돌린다. (기사 통과 여부, 통과한 기업) 을 돌려준다.

    판정은 입력 기업과 같은 순서(위치)로 짝을 맞춘다 — jev 는 질문마다 답을 돌려주므로 빠지지
    않지만, 판정이 짧으면 뒤쪽 기업은 판정 없음으로 본다.
    기업은 is_company 와 valid 가 모두 true 일 때만 통과다. 기사는 검색 종목 중 하나라도 통과하면
    통과다. 통과한 종목이 없는데 판정이 빠진 종목이 있으면 통과 여부는 None — 호출자가 실패로 본다.
    """

    judged: dict[int, bool] = {}
    for company, company_verdict in zip(companies, verdict.companies, strict=False):
        judged.setdefault(
            company["company_id"], company_verdict.is_company and company_verdict.valid
        )

    linked = [
        {"company_id": company["company_id"], "name": company["name"]}
        for company in companies
        if judged.get(company["company_id"])
    ]
    if linked:
        return True, linked
    if any(company["company_id"] not in judged for company in companies):
        return None, linked
    return False, linked


# ── jev 호출 ─────────────────────────────────────────────────────────────────


def build_state(article: ArticleInput) -> dict[str, Any]:
    """기사 하나의 jev state. 제목과 판정 기업(표기·표준명)만 — 본문은 이 단계에 없다. 순수 함수."""

    return {
        "headline": article.title,
        "companies": [
            {"surface": company["surface"], "name": company["name"]}
            for company in article.companies
        ],
    }


def decide(answers: dict[str, Any], i: int) -> tuple[bool, bool, dict[str, float]]:
    """기업 i 의 (is_company, valid, 원시 확률). THRESHOLDS 로 정한다.

    valid 는 has_event·has_driver 중 최댓값이 gate_min 이상일 때다. is_company 와 따로 판정한다 —
    연결은 둘 다 true 일 때만이고, 따로 두면 원시 확률을 읽기 쉽다.
    """

    scores = {
        "is_company": answers[f"c{i}_is_company"].noul,
        **{gate: answers[f"c{i}_{gate}"].noul for gate in GATES},
    }
    is_company = scores["is_company"] >= THRESHOLDS["company_min"]
    valid = max(scores[gate] for gate in GATES) >= THRESHOLDS["gate_min"]
    return is_company, valid, scores


class RelevanceJudge:
    """jev 호출. 클라이언트는 이벤트 루프 안에서 처음 부를 때 만들고, 런(asyncio.run)이 끝날 때
    filter_relevant_news 가 aclose() 로 닫는다 — 풀이 루프에 묶여 있어 다음 런에서 다시 연다."""

    def __init__(self):
        from pipelines.news.transformers.prompts.relevance import build_questions

        self._build_questions = build_questions
        self._client = None

    def _get_client(self):
        if self._client is None:
            from pipelines.common.clients.jev import async_client

            self._client = async_client()
        return self._client

    async def aclose(self) -> None:
        client, self._client = self._client, None
        if client is not None:
            await client.aclose()

    async def __call__(self, article: ArticleInput) -> ArticleVerdict:
        return await self.judge(article)

    async def judge(self, article: ArticleInput) -> ArticleVerdict:
        state = build_state(article)
        questions = self._build_questions(len(article.companies))
        response = await self._get_client().system_one(state, questions)
        check_answers(questions, response.answers)
        companies = []
        for i, company in enumerate(article.companies):
            is_company, valid, scores = decide(response.answers, i)
            companies.append(
                CompanyVerdict(
                    name=company["surface"], is_company=is_company, valid=valid, scores=scores
                )
            )
        return ArticleVerdict(companies=companies)


@lru_cache
def get_relevance_judge() -> Judge:
    """프로세스당 하나. 처음 부를 때 typesafe_sdk 를 로드하므로 import 시점엔 비용이 없다."""

    return RelevanceJudge()


# ── 기사 묶음 처리 ───────────────────────────────────────────────────────────


async def judge_items(
    items: list[dict[str, Any]], judge: Judge, max_concurrency: int
) -> RelevanceResult:
    buckets: dict[int, str] = {}
    linked: dict[int, list[dict[str, Any]]] = {}
    latencies: list[float] = []
    semaphore = asyncio.Semaphore(max(1, max_concurrency))

    async def one(index: int, item: dict[str, Any]) -> None:
        companies = search_companies_of(item)
        if not companies:
            buckets[index] = "invalid"
            return
        article = ArticleInput(
            title=item.get("title", ""),
            companies=tuple(
                {"surface": company["surface"], "name": company["name"]} for company in companies
            ),
        )

        async with semaphore:
            started = time.monotonic()
            try:
                verdict = await judge(article)
            except Exception as e:
                logger.warning(
                    "관련성 판정 호출 실패: %s / %s: %s", item.get("link", ""), type(e).__name__, e
                )
                buckets[index] = "failed"
                return
            finally:
                latencies.append((time.monotonic() - started) * 1000)

        # 평가가 원시 확률을 읽을 수 있게 판정을 item 에 남긴다 (저장에는 쓰이지 않는다)
        item["_relevance_judgements"] = [c.model_dump() for c in verdict.companies]
        target_valid, passed = resolve_verdict(companies, verdict)
        if target_valid is None:
            buckets[index] = "failed"
            return
        buckets[index] = "passed" if target_valid else "invalid"
        if target_valid:
            linked[index] = passed

    await asyncio.gather(*(one(index, item) for index, item in enumerate(items)))

    result = RelevanceResult(latencies_ms=latencies)
    for index, item in enumerate(items):
        bucket = buckets[index]
        if bucket == "passed":
            # 통과한 검색 종목만 연결한다 — 제목의 다른 기업은 엔티티 필터가 뒤에 더한다
            item["_linked_companies"] = linked[index]
        getattr(result, bucket).append(item)
    logger.debug(
        "[jev] 관련성 판정 %d건: 통과 %d / 무효 %d / 실패 %d",
        len(items),
        len(result.passed),
        len(result.invalid),
        len(result.failed),
    )
    return result


async def _judge_and_close(
    items: list[dict[str, Any]], judge: Judge, max_concurrency: int
) -> RelevanceResult:
    try:
        return await judge_items(items, judge, max_concurrency)
    finally:
        await aclose_judge(judge)


def filter_relevant_news(
    items: list[dict[str, Any]],
    judge: Judge | None = None,
    max_concurrency: int | None = None,
) -> RelevanceResult:
    """judge 를 안 주면 jev 싱글톤을 쓴다. 테스트는 가짜 judge 를 넘긴다."""

    if not items:
        return RelevanceResult()

    if judge is None:
        judge = get_relevance_judge()

    if max_concurrency is None:
        max_concurrency = get_news_settings().news_llm_max_concurrency

    return asyncio.run(_judge_and_close(items, judge, max_concurrency))
