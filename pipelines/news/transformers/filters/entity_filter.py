"""
LLM 엔티티 필터 - 기사 본문을 보고, 본문에만 나온 기업마다 그 표기가 실제로 그 기업을 가리키고
기사가 서술하는 구체적 사업 행위·사실의 당사자인지 판정한다.

제목에 나온 기업은 관련성 필터(relevance_filter)가 이미 판정했다. 여기서는 본문 기업 매치
(company_matches.match_body_companies)가 붙인 `_body_companies` 만 보고, 통과한 기업을
`_linked_companies` 에 더한다. 결과는 기업 피드와 Event 당사자에 그대로 노출되므로 애매하면
버린다 — LLM 이 판정을 빠뜨린 표기도, 인용한 문장에 표기가 없는 유지 판정도 버린다.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from functools import lru_cache
from typing import Any

from pydantic import BaseModel, Field

RETRY_ATTEMPTS = 2


class EntityJudgement(BaseModel):
    # 필드 순서는 의도한 것이다 — 문장을 먼저 인용하고, 근거를 쓰고, 마지막에 keep 을 정한다
    entity: str = Field(description="Copy the candidate entity back EXACTLY as given.")
    mention: str = Field(
        description=(
            "The sentence from the article where this surface form appears, copied verbatim. "
            "Pick the occurrence that best shows how the article uses the entity."
        )
    )
    reason: str = Field(
        description=(
            "One short sentence: does the surface form refer to the company here, and does the "
            "company take part in a concrete business action or fact in the article?"
        )
    )
    keep: bool = Field(
        description=(
            "True only if the surface form refers to the company AND the company is a "
            "participant in a concrete business action or fact. When unsure, false."
        )
    )


class EntityJudgementList(BaseModel):
    """LLM 구조화 출력. 후보 표기마다 판정 하나."""

    judgements: list[EntityJudgement] = Field(
        description="One judgement per candidate entity, in the same order as the input list."
    )


# (본문, 후보 표기 목록) → 판정
Judge = Callable[[str, list[str]], Awaitable[EntityJudgementList]]


@dataclass
class EntityFilterResult:
    # LLM 을 부른 기사 수
    judged: int = 0
    # 그중 호출이 실패한 기사 수. 그 기사는 제목 기업만 연결된다
    failed: int = 0
    # 통과한 본문 기업 수
    kept: int = 0


def resolve_judgements(
    companies: list[dict[str, Any]], judgements: list[EntityJudgement]
) -> list[dict[str, Any]]:
    """판정을 기업으로 되돌린다. 통과한 기업을 본문 등장 순서로 돌려준다.

    keep 이 true 인 표기가 하나라도 있는 기업만 통과다. 판정이 빠진 표기는 버린다. 입력에 없는
    이름은 무시한다(LLM 이 기업을 더하지 못한다). 같은 표기의 판정이 여러 번이면 첫 판정이 이긴다.

    keep 이 true 여도 인용한 문장(mention)에 그 표기가 없으면 버린다 — 모델이 표기의 위치를 못
    찾고 다른 문장을 보고 판정한 것이라 근거가 없다.
    """

    keep_by_surface: dict[str, bool] = {}
    for judgement in judgements:
        surface = judgement.entity.strip()
        if surface in keep_by_surface:
            continue
        keep_by_surface[surface] = judgement.keep and surface in judgement.mention

    passed: dict[int, dict[str, Any]] = {}
    for company in companies:
        if keep_by_surface.get(company["surface"].strip(), False):
            passed.setdefault(
                company["company_id"],
                {"company_id": company["company_id"], "name": company["name"]},
            )

    return list(passed.values())


def merge_linked(
    title_linked: list[dict[str, Any]], body_passed: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """제목 통과 기업 뒤에 본문 통과 기업을 잇는다. 같은 기업은 한 번만 든다."""

    merged: dict[int, dict[str, Any]] = {}
    for company in [*title_linked, *body_passed]:
        merged.setdefault(int(company["company_id"]), company)
    return list(merged.values())


class EntityJudge:
    """Bedrock 구조화 출력 체인. relevance_filter.RelevanceJudge 와 같은 구성."""

    def __init__(self):
        from langchain_aws import ChatBedrockConverse

        from pipelines.common.clients.bedrock import ensure_bedrock_token
        from pipelines.common.config import get_settings
        from pipelines.news.transformers.prompts.entity import PROMPT

        ensure_bedrock_token()
        settings = get_settings()
        logging.getLogger("langchain_aws").setLevel(logging.WARNING)

        model = ChatBedrockConverse(
            model=settings.bedrock_chat_model,
            region_name=settings.bedrock_region,
            temperature=0,
            timeout=settings.bedrock_request_timeout,
        )
        structured = model.with_structured_output(schema=EntityJudgementList, method="json_schema")
        self._chain = (PROMPT | structured).with_retry(stop_after_attempt=RETRY_ATTEMPTS)

    async def judge(self, text: str, surfaces: list[str]) -> EntityJudgementList:
        return await self._chain.ainvoke(
            {"text": text, "entities": "\n".join(f"- {surface}" for surface in surfaces)}
        )


@lru_cache
def get_entity_judge() -> Judge:
    """프로세스당 하나. 처음 부를 때 langchain·Bedrock 을 로드하므로 import 시점엔 비용이 없다."""

    return EntityJudge().judge


async def judge_items(
    items: list[dict[str, Any]], judge: Judge, max_concurrency: int, body_limit: int
) -> EntityFilterResult:
    semaphore = asyncio.Semaphore(max(1, max_concurrency))
    result = EntityFilterResult()

    async def one(item: dict[str, Any]) -> None:
        companies = item["_body_companies"]
        result.judged += 1

        async with semaphore:
            try:
                verdict = await judge(
                    (item.get("_text") or "")[:body_limit],
                    [company["surface"] for company in companies],
                )
            except Exception as e:
                result.failed += 1
                logging.warning(
                    "엔티티 판정 호출 실패 (제목 기업만 연결): %s / %s: %s",
                    item.get("link", ""),
                    type(e).__name__,
                    e,
                )
                return

        passed = resolve_judgements(companies, verdict.judgements)
        result.kept += len(passed)
        item["_linked_companies"] = merge_linked(item.get("_linked_companies") or [], passed)

    await asyncio.gather(*(one(item) for item in items))
    return result


def filter_body_entities(
    items: list[dict[str, Any]],
    judge: Judge | None = None,
    max_concurrency: int | None = None,
    body_limit: int | None = None,
) -> EntityFilterResult:
    """본문 기업을 판정해 통과한 기업을 `_linked_companies` 에 더한다.

    `_body_companies` 가 빈 기사는 LLM 을 부르지 않는다. judge 를 안 주면 Bedrock 싱글톤을 쓴다.
    테스트는 가짜 judge 를 넘긴다.
    """

    targets = [item for item in items if item.get("_body_companies")]
    if not targets:
        return EntityFilterResult()

    if judge is None:
        judge = get_entity_judge()

    if max_concurrency is None or body_limit is None:
        from pipelines.news.config import get_news_settings

        settings = get_news_settings()
        if max_concurrency is None:
            max_concurrency = settings.news_llm_max_concurrency
        if body_limit is None:
            body_limit = settings.news_llm_body_limit

    return asyncio.run(judge_items(targets, judge, max_concurrency, body_limit))
