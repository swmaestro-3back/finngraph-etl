"""클러스터 이름 생성 — 후보 기사 제목들로 사건 이름을 짓는다.

승격된(대표 기사가 정해진) 클러스터에만 부른다. 입력은 후보 기사 전부의 제목이고 본문은 넣지
않는다 — 주가 반응형 제목들에서 공통된 사건 하나를 골라 라벨로 다듬는 역할이다. 길이 상한
(NEWS_CLUSTER_TITLE_MAX_CHARS)은 시스템 프롬프트에 주입되고, 그래도 넘치면 같은 입력에 축약
지시를 붙여 한 번 더 요청한다(온도 0 이라 그냥 재시도하면 같은 답이 나온다). 실패한 클러스터는
title 이 NULL 로 남아 다음 런에 다시 시도된다. Bedrock 은 부르지 않고 titler 를 갈아끼울 수 있어
테스트는 가짜로 돈다.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime
from functools import lru_cache

from pydantic import BaseModel

from pipelines.news.transformers.prompts import cluster_title as cluster_title_prompt
from pipelines.news.utils.date_utils import SEOUL_TIMEZONE
from pipelines.news.utils.text_utils import remove_leading_title_brackets

DEFAULT_MAX_TOKENS = 128
RETRY_ATTEMPTS = 2


class ClusterTitle(BaseModel):
    # 형식 규칙은 시스템 프롬프트에만 둔다 — 스키마 설명의 한글은 호출마다 토큰이 비싸게 든다
    title: str


Titler = Callable[[str], Awaitable[ClusterTitle]]


@dataclass(frozen=True)
class Headline:
    """제목 생성 입력 한 줄 — 후보 기사의 제목과 발행 시각."""

    title: str
    published_at: datetime | None


def _collapse(text: str) -> str:
    return " ".join((text or "").split())


def build_title_input(headlines: list[Headline]) -> str:
    """후보 기사마다 프롬프트 USER 줄(`[기사 N] 날짜 | 제목`)을 줄바꿈으로 잇는다.

    빈 제목은 건너뛴다. 전부 비면 빈 문자열이다.
    """

    lines: list[str] = []
    for headline in headlines:
        title = _collapse(headline.title)
        if not title:
            continue
        day = (
            headline.published_at.astimezone(SEOUL_TIMEZONE).date().isoformat()
            if headline.published_at
            else "날짜 미상"
        )
        lines.append(cluster_title_prompt.USER.format(index=len(lines) + 1, date=day, title=title))

    return "\n".join(lines)


class TitleTooLong(ValueError):
    def __init__(self, title: str, max_chars: int):
        super().__init__(f"제목 길이 초과: {len(title)} > {max_chars}")
        self.title = title


def validate_cluster_title(title: str, max_chars: int) -> str:
    """공백·선두 태그를 정리하고 비었으면 ValueError, 너무 길면 TitleTooLong."""

    cleaned = remove_leading_title_brackets(_collapse(title))
    if not cleaned:
        raise ValueError("빈 제목")
    if len(cleaned) > max_chars:
        raise TitleTooLong(cleaned, max_chars)
    return cleaned


def build_shorten_input(articles_text: str, too_long: str, max_chars: int) -> str:
    """길이 초과 제목을 돌려주며 같은 사건을 더 짧게 짓도록 하는 재요청 입력."""

    return cluster_title_prompt.RETRY.format(
        articles=articles_text, title=too_long, length=len(too_long), max_chars=max_chars
    )


def load_system_prompt(max_chars: int) -> str:
    return cluster_title_prompt.SYSTEM.format(max_chars=max_chars)


class ClusterTitler:
    """Bedrock 구조화 출력 체인. filters.relevance_filter.RelevanceJudge 와 같은 구성."""

    def __init__(self, max_chars: int, max_tokens: int = DEFAULT_MAX_TOKENS):
        from langchain_aws import ChatBedrockConverse
        from langchain_core.messages import SystemMessage
        from langchain_core.prompts import ChatPromptTemplate

        from pipelines.common.clients.bedrock import ensure_bedrock_token
        from pipelines.common.config import get_settings

        ensure_bedrock_token()
        settings = get_settings()
        logging.getLogger("langchain_aws").setLevel(logging.WARNING)

        model = ChatBedrockConverse(
            model=settings.chat_model("cluster_title"),
            region_name=settings.bedrock_region,
            temperature=0,
            max_tokens=max_tokens,
            timeout=settings.bedrock_request_timeout,
        )
        prompt = ChatPromptTemplate.from_messages(
            [
                SystemMessage(content=load_system_prompt(max_chars)),
                ("human", "{articles}"),
            ]
        )
        structured = model.with_structured_output(schema=ClusterTitle, method="json_schema")
        self._chain = (prompt | structured).with_retry(stop_after_attempt=RETRY_ATTEMPTS)

    async def title(self, articles_text: str) -> ClusterTitle:
        return await self._chain.ainvoke({"articles": articles_text})


@lru_cache
def get_cluster_titler(max_chars: int) -> Titler:
    return ClusterTitler(max_chars=max_chars).title


async def _title_one(titler: Titler, articles_text: str, max_chars: int) -> str:
    """제목 하나. 길이 초과면 축약 지시를 붙여 한 번 더 묻고, 그래도 넘치면 TitleTooLong."""

    draft = await titler(articles_text)
    try:
        return validate_cluster_title(draft.title, max_chars)
    except TitleTooLong as e:
        retry = await titler(build_shorten_input(articles_text, e.title, max_chars))
        return validate_cluster_title(retry.title, max_chars)


async def _title_all(
    inputs: dict[int, str], titler: Titler, max_concurrency: int, max_chars: int
) -> dict[int, str]:
    semaphore = asyncio.Semaphore(max(1, max_concurrency))
    titles: dict[int, str] = {}

    async def one(cluster_id: int, articles_text: str) -> None:
        async with semaphore:
            try:
                titles[cluster_id] = await _title_one(titler, articles_text, max_chars)
            except Exception as e:
                logging.debug(
                    "클러스터 이름 생성 실패(다음 런에 재시도): cluster_id=%s, %s: %s",
                    cluster_id,
                    type(e).__name__,
                    e,
                )

    await asyncio.gather(*(one(cluster_id, text) for cluster_id, text in inputs.items()))
    return titles


def title_clusters(
    clusters: dict[int, list[Headline]],
    titler: Titler | None = None,
    max_concurrency: int = 4,
    max_chars: int = 25,
) -> dict[int, str]:
    """클러스터별 사건 제목. 입력은 후보 기사 제목 전부이고 본문은 넣지 않는다.

    실패했거나 제목이 하나도 없는 클러스터는 결과에 없다. titler 를 안 주면 Bedrock 싱글톤이다.
    """

    inputs = {
        cluster_id: build_title_input(headlines) for cluster_id, headlines in clusters.items()
    }
    inputs = {cluster_id: text for cluster_id, text in inputs.items() if text}
    if not inputs:
        return {}
    if titler is None:
        titler = get_cluster_titler(max_chars)
    return asyncio.run(_title_all(inputs, titler, max_concurrency, max_chars))
