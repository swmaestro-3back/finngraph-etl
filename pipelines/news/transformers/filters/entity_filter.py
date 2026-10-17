"""
LLM 엔티티 필터 - 기사 제목과 본문을 보고, 본문에만 나온 기업마다 기사에서 맡은 역할(role)을
판정한다. 제목이 전하는 주된 뉴스의 당사자(party)만 통과다.

제목에 나온 기업은 관련성 필터(relevance_filter)가 이미 판정했다. 여기서는 본문 기업 매치
(company_matches.match_body_companies)가 붙인 `_body_companies` 만 보고, 통과한 기업을
`_linked_companies` 에 더한다. 모델에는 제목과 제목 기업(관련성 필터를 통과한 기업)도 함께
준다 — 주된 뉴스를 모르면 주인공 기업의 뉴스를 설명하려고 인용된 다른 기업의 사실(그룹사 계획,
고객사 실적)을 당사자와 가리지 못한다.

결과는 기업 피드와 Event 당사자에 그대로 노출되므로 애매하면 버린다 — LLM 이 판정을 빠뜨린
표기도, 인용한 문장에 표기가 없는 통과 판정도 버린다.
"""

from __future__ import annotations

import asyncio
import logging
from collections import Counter
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from functools import lru_cache
from typing import Any, Literal

from pydantic import BaseModel, Field

RETRY_ATTEMPTS = 2

# 후보 표기가 기사에서 맡은 역할. 프롬프트의 [Roles] 와 같은 이름·순서다
Role = Literal["party", "background", "listed", "source", "not_company"]

# 기업에 연결하는 역할. 나머지는 버린다
KEPT_ROLES = frozenset({"party"})


class EntityJudgement(BaseModel):
    # 필드 순서는 의도한 것이다 — 문장을 먼저 인용하고, 근거를 쓰고, 마지막에 role 을 정한다
    entity: str = Field(description="Copy the candidate entity back EXACTLY as given.")
    mention: str = Field(
        description=(
            "The sentence from the article where this surface form appears, copied verbatim. "
            "Pick the occurrence that best shows how the article uses the entity."
        )
    )
    reason: str = Field(
        description=(
            "One short sentence: does the surface form refer to the company here, and how does "
            "the company relate to the main news the headline reports?"
        )
    )
    role: Role = Field(
        description=(
            "party: a party to the headline's main news. background: its own fact is cited only "
            "as cause or context. listed: only in a list of peers or stock moves. source: cited "
            "as the source of a forecast or rating. not_company: the surface form does not refer "
            "to the company here. When unsure, never party."
        )
    )


class EntityJudgementList(BaseModel):
    """LLM 구조화 출력. 후보 표기마다 판정 하나."""

    judgements: list[EntityJudgement] = Field(
        description="One judgement per candidate entity, in the same order as the input list."
    )


# (제목, 제목 기업 이름, 본문, 후보 표기 목록) → 판정
Judge = Callable[[str, list[str], str, list[str]], Awaitable[EntityJudgementList]]


@dataclass
class EntityFilterResult:
    # LLM 을 부른 기사 수
    judged: int = 0
    # 그중 호출이 실패한 기사 수. 그 기사는 제목 기업만 연결된다
    failed: int = 0
    # 통과한 본문 기업 수
    kept: int = 0
    # 판정받은 후보 표기의 역할별 수. 어떤 이유로 버려지는지 로그로 본다
    roles: Counter[str] = field(default_factory=Counter)


def resolve_judgements(
    companies: list[dict[str, Any]], judgements: list[EntityJudgement]
) -> list[dict[str, Any]]:
    """판정을 기업으로 되돌린다. 통과한 기업을 본문 등장 순서로 돌려준다.

    역할이 KEPT_ROLES 인 표기가 하나라도 있는 기업만 통과다. 판정이 빠진 표기는 버린다. 입력에
    없는 이름은 무시한다(LLM 이 기업을 더하지 못한다). 같은 표기의 판정이 여러 번이면 첫 판정이
    이긴다.

    통과 역할이어도 인용한 문장(mention)에 그 표기가 없으면 버린다 — 모델이 표기의 위치를 못
    찾고 다른 문장을 보고 판정한 것이라 근거가 없다.
    """

    keep_by_surface: dict[str, bool] = {}
    for judgement in judgements:
        surface = judgement.entity.strip()
        if surface in keep_by_surface:
            continue
        keep_by_surface[surface] = judgement.role in KEPT_ROLES and surface in judgement.mention

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
            model=settings.chat_model("entity"),
            region_name=settings.bedrock_region,
            temperature=0,
            timeout=settings.bedrock_request_timeout,
        )
        structured = model.with_structured_output(schema=EntityJudgementList, method="json_schema")
        self._chain = (PROMPT | structured).with_retry(stop_after_attempt=RETRY_ATTEMPTS)

    async def judge(
        self, title: str, headline_companies: list[str], text: str, surfaces: list[str]
    ) -> EntityJudgementList:
        return await self._chain.ainvoke(
            {
                "title": title,
                "headline_companies": ", ".join(headline_companies),
                "text": text,
                "entities": "\n".join(f"- {surface}" for surface in surfaces),
            }
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
        surfaces = [company["surface"] for company in companies]
        result.judged += 1

        async with semaphore:
            try:
                verdict = await judge(
                    item.get("title", ""),
                    # 관련성 필터를 통과한 제목 기업(검색 종목 포함) — 이 기사의 주인공이다
                    [company["name"] for company in item.get("_linked_companies") or []],
                    (item.get("_text") or "")[:body_limit],
                    surfaces,
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

        candidates = set(surfaces)
        result.roles.update(
            judgement.role
            for judgement in verdict.judgements
            if judgement.entity.strip() in candidates
        )
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

    judge 에는 제목과 기존 `_linked_companies`(제목 통과 기업)의 이름도 넘긴다 — 이 함수는
    관련성 필터 뒤에 돌아야 한다.

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
