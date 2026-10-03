"""
LLM 필터 - 기사 제목에 나온 기업마다 그 기업 페이지에 보여줄 기사인지(GATE 1~3) 판정한다.
저장 여부는 검색 대상 종목의 판정으로 정하고, 통과한 기업은 모두 news_companies 에 연결된다.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from functools import lru_cache
from typing import Any

from pydantic import BaseModel, Field

from pipelines.news.transformers.prompts import relevance as relevance_prompt

DEFAULT_MAX_TOKENS = 1024
RETRY_ATTEMPTS = 2


# 아래 세 모델의 docstring 과 Field description 은 JSON 스키마에 실려 호출마다 LLM 에 전달된다.
# 스키마 안의 한글은 시스템 프롬프트보다 글자당 토큰이 몇 배 든다 — 판정 기준은 시스템 프롬프트
# (GATE 1~3)에만 두고 여기에는 다시 적지 않는다.
class CompanyVerdict(BaseModel):
    """판정 기업 하나의 판정."""

    name: str = Field(description="입력의 '판정 기업' 목록에 있는 표기 그대로.")
    valid: bool = Field(description="이 기업이 GATE 1~3 중 하나를 통과하면 true.")


class ArticleVerdict(BaseModel):
    """기사 하나의 판정."""

    id: int = Field(description="입력의 [기사 N] 에서 N. 입력에 있는 번호만, 하나도 빠짐없이.")
    companies: list[CompanyVerdict] = Field(
        description="판정 기업마다 하나씩, 목록 순서대로, 하나도 빠짐없이."
    )


class BatchVerdict(BaseModel):
    """LLM 구조화 출력. 입력 기사마다 판정 하나."""

    verdicts: list[ArticleVerdict] = Field(description="입력 기사마다 하나씩, 같은 번호로.")


@dataclass(frozen=True)
class ArticleInput:
    id: int
    title: str
    description: str
    # 판정 기업의 제목 표기. 검색 종목이 첫 번째다.
    companies: tuple[str, ...]


Judge = Callable[[list[ArticleInput]], Awaitable[BatchVerdict]]


@dataclass
class RelevanceResult:
    passed: list[dict[str, Any]] = field(default_factory=list)
    invalid: list[dict[str, Any]] = field(default_factory=list)
    failed: list[dict[str, Any]] = field(default_factory=list)


def load_system_prompt() -> str:
    return relevance_prompt.SYSTEM


def build_relevance_input(articles: list[ArticleInput]) -> str:
    """기사마다 프롬프트 USER 블록을 채워 빈 줄로 잇는다."""

    return "\n\n".join(
        relevance_prompt.USER.format(
            id=article.id,
            title=article.title,
            description=article.description,
            companies=", ".join(article.companies),
        )
        for article in articles
    )


def chunked(values: list, size: int) -> list[list]:
    size = max(1, size)
    return [values[start : start + size] for start in range(0, len(values), size)]


def title_companies_of(item: dict[str, Any]) -> list[dict[str, Any]]:
    """판정할 기업 목록. 검색 종목이 첫 번째다.

    제목 기업 매치(company_matches)를 거치지 않은 기사는 검색 종목 하나로 판정한다.
    """

    companies = item.get("_title_companies")
    if companies:
        return companies
    company = item.get("_query_company")
    if not company:
        return []
    return [
        {
            "company_id": int(company["company_id"]),
            "name": company["name"],
            "surface": company["name"],
        }
    ]


def fetching_company_ids(item: dict[str, Any], companies: list[dict[str, Any]]) -> set[int]:
    """이 기사를 가져온 검색 종목 중 판정 기업에 든 것. 이 중 하나라도 통과하면 기사를 저장한다.

    같은 기사가 여러 종목 검색에 걸리면 URL 중복 제거가 사본 하나로 합치며 `_query_companies` 에
    모두 기억한다. 그 정보가 없으면 판정 기업의 첫 번째(검색 종목) 하나다.
    """

    judged_ids = {company["company_id"] for company in companies}
    fetched = {int(company["company_id"]) for company in item.get("_query_companies") or []}
    return (fetched & judged_ids) or {companies[0]["company_id"]}


def _name_key(name: str) -> str:
    return "".join(name.split())


def resolve_verdict(
    companies: list[dict[str, Any]],
    verdict: ArticleVerdict,
    search_ids: set[int] | None = None,
) -> tuple[bool | None, list[dict[str, Any]]]:
    """판정을 기업으로 되돌린다. (기사 통과 여부, 통과한 기업) 을 돌려준다.

    기사는 search_ids(이 기사를 가져온 검색 종목, 기본은 첫 번째 기업) 중 하나라도 통과하면 통과다.
    통과한 검색 종목이 없는데 판정이 빠진 검색 종목이 있으면 통과 여부는 None — 그 종목이 통과일 수
    있으니 호출자가 누락으로 보고 다시 판정한다.
    목록에 없는 이름은 버린다(LLM 이 기업을 더하지 못한다). 표기 대신 정식명으로 답해도 받는다.
    이름은 공백을 빼고 비교한다 — 'LG 엔솔' 을 'LG엔솔' 로 붙여 답해도 같은 기업이다.
    같은 기업의 판정이 여러 번이면 첫 판정이 이긴다. 판정이 빠진 다른 기업은 연결하지 않는다.
    """

    lookup: dict[str, dict[str, Any]] = {}
    for company in companies:
        lookup.setdefault(_name_key(company["surface"]), company)
        lookup.setdefault(_name_key(company["name"]), company)

    judged: dict[int, bool] = {}
    for company_verdict in verdict.companies:
        company = lookup.get(_name_key(company_verdict.name))
        if company is None or company["company_id"] in judged:
            continue
        judged[company["company_id"]] = company_verdict.valid

    linked = [
        {"company_id": company["company_id"], "name": company["name"]}
        for company in companies
        if judged.get(company["company_id"])
    ]
    if search_ids is None:
        search_ids = {companies[0]["company_id"]}
    search_verdicts = [judged.get(company_id) for company_id in search_ids]
    if any(search_verdicts):
        return True, linked
    if any(valid is None for valid in search_verdicts):
        return None, linked
    return False, linked


class RelevanceJudge:
    """Bedrock 구조화 출력 체인. events/transformers/generator.py 와 같은 구성."""

    def __init__(self, max_tokens: int | None = None):
        from langchain_aws import ChatBedrockConverse
        from langchain_core.messages import SystemMessage
        from langchain_core.prompts import ChatPromptTemplate

        from pipelines.common.clients.bedrock import ensure_bedrock_token
        from pipelines.common.config import get_settings
        from pipelines.news.config import get_news_settings

        ensure_bedrock_token()
        settings = get_settings()
        # langchain_aws 가 호출마다 찍는 "Using Bedrock Converse API ..." INFO 를 끈다
        logging.getLogger("langchain_aws").setLevel(logging.WARNING)

        # 기사 하나의 판정이 기업 수만큼 길어져 묶음 크기에 비례해 잡는다. 모자라면 응답이 잘려
        # 검증에 실패하고 묶음 전체가 개별 fallback 으로 떨어져 오히려 비싸진다.
        if max_tokens is None:
            max_tokens = max(DEFAULT_MAX_TOKENS, 128 * get_news_settings().news_llm_batch_size)

        model = ChatBedrockConverse(
            model=settings.bedrock_chat_model,
            region_name=settings.bedrock_region,
            temperature=0,
            max_tokens=max_tokens,
            timeout=settings.bedrock_request_timeout,
        )
        prompt = ChatPromptTemplate.from_messages(
            [SystemMessage(content=load_system_prompt()), ("human", "{articles}")]
        )
        structured = model.with_structured_output(schema=BatchVerdict, method="json_schema")
        self._chain = (prompt | structured).with_retry(stop_after_attempt=RETRY_ATTEMPTS)

    async def judge(self, articles: list[ArticleInput]) -> BatchVerdict:
        return await self._chain.ainvoke({"articles": build_relevance_input(articles)})


@lru_cache
def get_relevance_judge() -> Judge:
    """프로세스당 하나. 처음 부를 때 langchain·Bedrock 을 로드하므로 import 시점엔 비용이 없다."""

    return RelevanceJudge().judge


async def judge_items(
    items: list[dict[str, Any]], judge: Judge, max_concurrency: int, batch_size: int
) -> RelevanceResult:
    buckets: dict[int, str] = {}
    linked: dict[int, list[dict[str, Any]]] = {}
    companies_by_index: dict[int, list[dict[str, Any]]] = {}
    search_ids_by_index: dict[int, set[int]] = {}
    inputs: list[ArticleInput] = []

    for index, item in enumerate(items):
        companies = title_companies_of(item)
        if not companies:
            buckets[index] = "invalid"
            continue
        companies_by_index[index] = companies
        search_ids_by_index[index] = fetching_company_ids(item, companies)
        inputs.append(
            ArticleInput(
                id=index,
                title=item.get("title", ""),
                description=item.get("description", ""),
                companies=tuple(company["surface"] for company in companies),
            )
        )

    semaphore = asyncio.Semaphore(max(1, max_concurrency))
    batches = chunked(inputs, batch_size)
    progress = {"batches": 0, "articles": 0}

    async def call(articles: list[ArticleInput]) -> dict[int, tuple[bool, list[dict[str, Any]]]]:
        """한 번 호출하고 번호로 짝을 맞춘다. 통과 여부를 정할 수 없는 기사는 결과에 넣지 않는다.

        호출 예외는 빈 결과 — 호출자가 fallback 한다.
        """

        async with semaphore:
            try:
                batch = await judge(articles)
            except Exception as e:
                logging.warning(
                    "관련성 판정 호출 실패 (%d건): %s: %s", len(articles), type(e).__name__, e
                )
                return {}

        wanted = {article.id for article in articles}
        resolved: dict[int, tuple[bool, list[dict[str, Any]]]] = {}
        seen: set[int] = set()
        for verdict in batch.verdicts:
            if verdict.id not in wanted or verdict.id in seen:
                continue
            seen.add(verdict.id)
            target_valid, companies = resolve_verdict(
                companies_by_index[verdict.id], verdict, search_ids_by_index[verdict.id]
            )
            if target_valid is not None:
                resolved[verdict.id] = (target_valid, companies)
        return resolved

    async def judge_batch(articles: list[ArticleInput]) -> None:
        resolved = await call(articles)

        missing = [article for article in articles if article.id not in resolved]
        if missing and len(articles) > 1:
            # 묶음에서 빠졌거나 검색 종목 판정이 빠졌거나 묶음 자체가 실패한 기사는 개별로 한 번 더
            # 판정한다.
            logging.warning("묶음 판정 누락 %d/%d건 — 개별 재판정", len(missing), len(articles))
            for article in missing:
                resolved.update(await call([article]))

        for article in articles:
            outcome = resolved.get(article.id)
            if outcome is None:
                buckets[article.id] = "failed"
                continue
            target_valid, companies = outcome
            buckets[article.id] = "passed" if target_valid else "invalid"
            if target_valid:
                linked[article.id] = companies

        # 묶음은 동시에 돌아 완료 순서가 섞이므로 완료 수만 센다
        progress["batches"] += 1
        progress["articles"] += len(articles)
        logging.debug(
            "[LLM] 판정 %d/%d 묶음 (기사 %d/%d건)",
            progress["batches"],
            len(batches),
            progress["articles"],
            len(inputs),
        )

    await asyncio.gather(*(judge_batch(batch) for batch in batches))

    result = RelevanceResult()
    for index, item in enumerate(items):
        bucket = buckets[index]
        if bucket == "passed":
            # 가져온 검색 종목이 통과한 기사만 연결한다 — 버려지는 기사는 연결할 곳이 없다
            item["_linked_companies"] = linked[index]
        getattr(result, bucket).append(item)
    return result


def filter_relevant_news(
    items: list[dict[str, Any]],
    judge: Judge | None = None,
    max_concurrency: int | None = None,
    batch_size: int | None = None,
) -> RelevanceResult:
    """judge 를 안 주면 Bedrock 싱글톤을 쓴다. 테스트는 가짜 judge 를 넘긴다."""

    if not items:
        return RelevanceResult()

    if judge is None:
        judge = get_relevance_judge()

    if max_concurrency is None or batch_size is None:
        from pipelines.news.config import get_news_settings

        settings = get_news_settings()
        if max_concurrency is None:
            max_concurrency = settings.news_llm_max_concurrency
        if batch_size is None:
            batch_size = settings.news_llm_batch_size

    return asyncio.run(judge_items(items, judge, max_concurrency, batch_size))
