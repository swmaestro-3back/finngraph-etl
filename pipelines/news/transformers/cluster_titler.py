"""클러스터 이름 생성 — 멤버 기사 묶음으로 사건 이름을 짓는다.

판정 기사 수가 NEWS_CLUSTER_TITLE_MIN_SIZE 를 넘은 클러스터에만 부른다. 실패한 클러스터는
title 이 NULL 로 남아 다음 런에 다시 시도된다. Bedrock 은 부르지 않고 titler 를 갈아끼울 수
있어 테스트는 가짜로 돈다.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from functools import lru_cache
from pathlib import Path

from pydantic import BaseModel, Field

from pipelines.news.repositories.news_clusters import ClusterArticle
from pipelines.news.utils.date_utils import SEOUL_TIMEZONE
from pipelines.news.utils.text_utils import remove_leading_title_brackets

PROMPT_PATH = Path(__file__).with_name("prompts") / "cluster_title_system.txt"

LEAD_CHARS = 600
DEFAULT_MAX_TOKENS = 128
RETRY_ATTEMPTS = 2


class ClusterTitle(BaseModel):
    title: str = Field(description="사건 이름. 한국어 명사구 한 줄")


Titler = Callable[[str], Awaitable[ClusterTitle]]


def _collapse(text: str) -> str:
    return " ".join((text or "").split())


def build_title_input(articles: list[ClusterArticle], lead_chars: int = LEAD_CHARS) -> str:
    """`[기사 N] 날짜 | 제목` 과 본문 리드로 LLM 입력을 만든다."""

    blocks: list[str] = []
    for index, article in enumerate(articles, start=1):
        day = (
            article.published_at.astimezone(SEOUL_TIMEZONE).date().isoformat()
            if article.published_at
            else "날짜 미상"
        )
        lead = _collapse(article.text)[:lead_chars].rstrip()
        blocks.append(f"[기사 {index}] {day} | {_collapse(article.title)}\n{lead}".rstrip())

    return "\n\n".join(blocks)


def validate_cluster_title(title: str, max_chars: int) -> str:
    """공백·선두 태그를 정리하고 비었거나 너무 길면 ValueError."""

    cleaned = remove_leading_title_brackets(_collapse(title))
    if not cleaned:
        raise ValueError("빈 제목")
    if len(cleaned) > max_chars:
        raise ValueError(f"제목 길이 초과: {len(cleaned)} > {max_chars}")
    return cleaned


class ClusterTitler:
    """Bedrock 구조화 출력 체인. filters.relevance_filter.RelevanceJudge 와 같은 구성."""

    def __init__(self, max_tokens: int = DEFAULT_MAX_TOKENS):
        from langchain_aws import ChatBedrockConverse
        from langchain_core.messages import SystemMessage
        from langchain_core.prompts import ChatPromptTemplate

        from pipelines.common.clients.bedrock import ensure_bedrock_token
        from pipelines.common.config import get_settings

        ensure_bedrock_token()
        settings = get_settings()
        logging.getLogger("langchain_aws").setLevel(logging.WARNING)

        model = ChatBedrockConverse(
            model=settings.bedrock_chat_model,
            region_name=settings.bedrock_region,
            temperature=0,
            max_tokens=max_tokens,
            timeout=settings.bedrock_request_timeout,
        )
        prompt = ChatPromptTemplate.from_messages(
            [
                SystemMessage(content=PROMPT_PATH.read_text(encoding="utf-8")),
                ("human", "{articles}"),
            ]
        )
        structured = model.with_structured_output(schema=ClusterTitle, method="json_schema")
        self._chain = (prompt | structured).with_retry(stop_after_attempt=RETRY_ATTEMPTS)

    async def title(self, articles_text: str) -> ClusterTitle:
        return await self._chain.ainvoke({"articles": articles_text})


@lru_cache
def get_cluster_titler() -> Titler:
    return ClusterTitler().title


async def _title_all(
    clusters: dict[int, list[ClusterArticle]],
    titler: Titler,
    max_concurrency: int,
    max_chars: int,
) -> dict[int, str]:
    semaphore = asyncio.Semaphore(max(1, max_concurrency))
    titles: dict[int, str] = {}

    async def one(cluster_id: int, articles: list[ClusterArticle]) -> None:
        async with semaphore:
            try:
                draft = await titler(build_title_input(articles))
                titles[cluster_id] = validate_cluster_title(draft.title, max_chars)
            except Exception as e:
                logging.warning(
                    "클러스터 이름 생성 실패(다음 런에 재시도): cluster_id=%s, %s: %s",
                    cluster_id,
                    type(e).__name__,
                    e,
                )

    await asyncio.gather(*(one(cid, arts) for cid, arts in clusters.items() if arts))
    return titles


def title_clusters(
    clusters: dict[int, list[ClusterArticle]],
    titler: Titler | None = None,
    max_concurrency: int = 4,
    max_chars: int = 30,
) -> dict[int, str]:
    """클러스터별 이름. 실패한 클러스터는 결과에 없다. titler 를 안 주면 Bedrock 싱글톤."""

    if not clusters:
        return {}
    if titler is None:
        titler = get_cluster_titler()
    return asyncio.run(_title_all(clusters, titler, max_concurrency, max_chars))
