"""
LLM 필터 - 기사가 검색 대상 종목 하나에 대한 유효한 기사인지 판정
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field

PROMPT_DIRECTORY = Path(__file__).resolve().parents[1] / "prompts"

DEFAULT_MAX_TOKENS = 1024
DEFAULT_BATCH_SIZE = 10
RETRY_ATTEMPTS = 2


class ArticleVerdict(BaseModel):
    """기사 하나의 판정. Field description 이 곧 프롬프트 규칙이다."""

    id: int = Field(description="입력의 [기사 N] 에서 N. 입력에 있는 번호만, 하나도 빠짐없이.")
    valid: bool = Field(
        description=(
            "대상 종목이 주어나 목적어인 등록 predicate 관계(수주·공급·인수·투자·계약 등)가 "
            "제목·요약에 명시되어 있거나(GATE 1), 대상 종목 자체의 사건(실적·유상증자·인허가·"
            "소송·공시·증설 등)이 매출·비용·생산·공급·규제 등에 직접 영향을 주면(GATE 2) true. "
            "둘 중 하나만 통과해도 true. 대상 종목이 시세 변동만 서술되거나 지나가며 언급된 기사, "
            "지수·타사·업황·거시 요인만으로 설명되는 시세 변동, 수혜주 전망, 여러 종목 나열, "
            "광고·홍보면 false."
        )
    )


class BatchVerdict(BaseModel):
    """LLM 구조화 출력. 입력 기사마다 판정 하나."""

    verdicts: list[ArticleVerdict] = Field(description="입력 기사마다 하나씩, 같은 번호로.")


@dataclass(frozen=True)
class ArticleInput:
    id: int
    title: str
    description: str
    company: str


Judge = Callable[[list[ArticleInput]], Awaitable[BatchVerdict]]


@dataclass
class RelevanceResult:
    passed: list[dict[str, Any]] = field(default_factory=list)
    invalid: list[dict[str, Any]] = field(default_factory=list)
    failed: list[dict[str, Any]] = field(default_factory=list)


@lru_cache
def load_system_prompt() -> str:
    return (PROMPT_DIRECTORY / "relevance_single_system.txt").read_text(encoding="utf-8").strip()


def build_relevance_input(articles: list[ArticleInput]) -> str:
    blocks: list[str] = []
    for article in articles:
        blocks.append(
            f"[기사 {article.id}]\n제목: {article.title}\n요약: {article.description}\n"
            f"대상 종목: {article.company}"
        )
    return "\n\n".join(blocks)


def chunked(values: list, size: int) -> list[list]:
    size = max(1, size)
    return [values[start : start + size] for start in range(0, len(values), size)]


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

        # 판정 하나가 20 토큰 안팎이라 묶음 크기에 비례해 잡는다. 모자라면 응답이 잘려 검증에
        # 실패하고 묶음 전체가 개별 fallback 으로 떨어져 오히려 비싸진다.
        if max_tokens is None:
            max_tokens = max(DEFAULT_MAX_TOKENS, 64 * get_news_settings().news_llm_batch_size)

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
    inputs: list[ArticleInput] = []

    for index, item in enumerate(items):
        company = item.get("_query_company")
        if not company:
            buckets[index] = "invalid"
            continue
        inputs.append(
            ArticleInput(
                id=index,
                title=item.get("title", ""),
                description=item.get("description", ""),
                company=company["name"],
            )
        )

    semaphore = asyncio.Semaphore(max(1, max_concurrency))
    batches = chunked(inputs, batch_size)
    progress = {"batches": 0, "articles": 0}

    async def call(articles: list[ArticleInput]) -> dict[int, ArticleVerdict]:
        """한 번 호출하고 번호로 짝을 맞춘다. 호출 예외는 빈 결과 — 호출자가 fallback 한다."""

        async with semaphore:
            try:
                batch = await judge(articles)
            except Exception as e:
                logging.warning(
                    "관련성 판정 호출 실패 (%d건): %s: %s", len(articles), type(e).__name__, e
                )
                return {}

        wanted = {article.id for article in articles}
        matched: dict[int, ArticleVerdict] = {}
        for verdict in batch.verdicts:
            if verdict.id in wanted and verdict.id not in matched:
                matched[verdict.id] = verdict
        return matched

    async def judge_batch(articles: list[ArticleInput]) -> None:
        matched = await call(articles)

        missing = [article for article in articles if article.id not in matched]
        if missing and len(articles) > 1:
            # 묶음에서 빠졌거나 묶음 자체가 실패한 기사는 개별로 한 번 더 판정한다.
            logging.warning("묶음 판정 누락 %d/%d건 — 개별 재판정", len(missing), len(articles))
            for article in missing:
                matched.update(await call([article]))

        for article in articles:
            verdict = matched.get(article.id)
            if verdict is None:
                buckets[article.id] = "failed"
                continue
            buckets[article.id] = "passed" if verdict.valid else "invalid"

        # 묶음은 동시에 돌아 완료 순서가 섞이므로 완료 수만 센다
        progress["batches"] += 1
        progress["articles"] += len(articles)
        logging.info(
            "[LLM] 판정 %d/%d 묶음 (기사 %d/%d건)",
            progress["batches"],
            len(batches),
            progress["articles"],
            len(inputs),
        )

    await asyncio.gather(*(judge_batch(batch) for batch in batches))

    result = RelevanceResult()
    for index, item in enumerate(items):
        getattr(result, buckets[index]).append(item)
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
