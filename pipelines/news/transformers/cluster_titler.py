"""클러스터 이름 생성 — 멤버 기사 묶음으로 사건 이름을 짓는다.

판정 기사 수가 NEWS_CLUSTER_TITLE_MIN_SIZE 를 넘은 클러스터에만 부른다. 길이 상한
(NEWS_CLUSTER_TITLE_MAX_CHARS)은 시스템 프롬프트에 주입되고, 그래도 넘치면 같은 입력에
축약 지시를 붙여 한 번 더 요청한다(온도 0 이라 그냥 재시도하면 같은 답이 나온다). 실패한
클러스터는 title 이 NULL 로 남아 다음 런에 다시 시도된다. Bedrock 은 부르지 않고 titler 를
갈아끼울 수 있어 테스트는 가짜로 돈다.
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

    return (
        f"{articles_text}\n\n[재요청] 앞서 만든 제목 '{too_long}'은 {len(too_long)}자로 "
        f"{max_chars}자를 넘는다. 같은 사건을 {max_chars}자 이내로 다시 짓는다. "
        "고유명사는 남기고 꼬리 명사·수식어부터 뺀다."
    )


def load_system_prompt(max_chars: int) -> str:
    return PROMPT_PATH.read_text(encoding="utf-8").replace("{max_chars}", str(max_chars))


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
            model=settings.bedrock_chat_model,
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
                titles[cluster_id] = await _title_one(
                    titler, build_title_input(articles), max_chars
                )
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
    max_chars: int = 25,
) -> dict[int, str]:
    """클러스터별 이름. 실패한 클러스터는 결과에 없다. titler 를 안 주면 Bedrock 싱글톤."""

    if not clusters:
        return {}
    if titler is None:
        titler = get_cluster_titler(max_chars)
    return asyncio.run(_title_all(clusters, titler, max_concurrency, max_chars))
