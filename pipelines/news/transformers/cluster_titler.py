"""클러스터 이름·요약 생성 — 멤버 기사 묶음으로 사건 이름과 1~2문장 요약을 짓는다.

판정 기사 수가 NEWS_CLUSTER_TITLE_MIN_SIZE 를 넘은 클러스터에만 부른다. 길이 상한
(NEWS_CLUSTER_TITLE_MAX_CHARS)은 시스템 프롬프트에 주입되고, 그래도 넘치면 같은 입력에
축약 지시를 붙여 한 번 더 요청한다(온도 0 이라 그냥 재시도하면 같은 답이 나온다). 실패한
클러스터는 title 이 NULL 로 남아 다음 런에 다시 시도된다. Bedrock 은 부르지 않고 titler 를
갈아끼울 수 있어 테스트는 가짜로 돈다.

요약은 이슈 타임라인 노드 설명이다. 추가 호출 없이 제목과 같은 구조화 출력에서 받는다.
요약이 비었거나 NEWS_CLUSTER_SUMMARY_MAX_CHARS 를 넘으면 요약만 버린다 — 제목은 요약 때문에
실패하지 않는다.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from functools import lru_cache, partial
from pathlib import Path
from typing import Any, TypeVar

from pydantic import BaseModel, ConfigDict, Field

from pipelines.news.repositories.news_clusters import ClusterArticle
from pipelines.news.utils.date_utils import SEOUL_TIMEZONE
from pipelines.news.utils.text_utils import remove_leading_title_brackets

PROMPT_PATH = Path(__file__).with_name("prompts") / "cluster_title_system.txt"

LEAD_CHARS = 600
# 제목(~20자)에 요약(~150자)까지 한 JSON 으로 받는다. 한국어는 글자당 토큰이 많아 넉넉히 둔다 —
# 잘리면 JSON 이 깨져 제목까지 잃는다.
DEFAULT_MAX_TOKENS = 512
RETRY_ATTEMPTS = 2

T = TypeVar("T")


def _require_summary(schema: dict[str, Any]) -> None:
    schema["required"] = ["title", "summary"]
    schema["properties"]["summary"].pop("default", None)


class ClusterTitle(BaseModel):
    # 스키마에는 summary 를 필수로 내보내 모델이 늘 쓰게 하고, 파싱은 기본값으로 받는다 —
    # 응답에 요약이 빠져도 제목은 살린다.
    model_config = ConfigDict(json_schema_extra=_require_summary)

    title: str = Field(description="사건 이름. 한국어 명사구 한 줄")
    summary: str = Field(default="", description="사건 요약. 해요체 1~2문장")


@dataclass(frozen=True)
class ClusterLabel:
    """검증을 통과한 클러스터 이름과 요약. 요약은 버려졌으면 None."""

    title: str
    summary: str | None = None


Titler = Callable[[str], Awaitable[ClusterTitle]]


def _collapse(text: str | None) -> str:
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


class SummaryTooLong(ValueError):
    def __init__(self, summary: str, max_chars: int):
        super().__init__(f"요약 길이 초과: {len(summary)} > {max_chars}")
        self.summary = summary


def validate_cluster_title(title: str, max_chars: int) -> str:
    """공백·선두 태그를 정리하고 비었으면 ValueError, 너무 길면 TitleTooLong."""

    cleaned = remove_leading_title_brackets(_collapse(title))
    if not cleaned:
        raise ValueError("빈 제목")
    if len(cleaned) > max_chars:
        raise TitleTooLong(cleaned, max_chars)
    return cleaned


def validate_cluster_summary(summary: str | None, max_chars: int) -> str:
    """공백을 정리하고 비었으면 ValueError, 너무 길면 SummaryTooLong. 자르지 않는다 — 문장
    중간에서 끊긴 요약을 보여주느니 비워 둔다."""

    cleaned = _collapse(summary)
    if not cleaned:
        raise ValueError("빈 요약")
    if len(cleaned) > max_chars:
        raise SummaryTooLong(cleaned, max_chars)
    return cleaned


def pick_summary(sources: list[str | None], max_chars: int, cluster_id: int) -> str | None:
    """앞에서부터 검증을 통과한 첫 요약. 없으면 로그만 남기고 None."""

    error: ValueError | None = None
    for source in sources:
        try:
            return validate_cluster_summary(source, max_chars)
        except ValueError as e:
            error = e

    log = logging.warning if isinstance(error, SummaryTooLong) else logging.info
    log("클러스터 요약 버림(제목은 저장): cluster_id=%s, %s", cluster_id, error)
    return None


def build_shorten_input(articles_text: str, too_long: str, max_chars: int) -> str:
    """길이 초과 제목을 돌려주며 같은 사건을 더 짧게 짓도록 하는 재요청 입력."""

    return (
        f"{articles_text}\n\n[재요청] 앞서 만든 제목 '{too_long}'은 {len(too_long)}자로 "
        f"{max_chars}자를 넘는다. 같은 사건을 {max_chars}자 이내로 다시 짓는다. "
        "고유명사는 남기고 꼬리 명사·수식어부터 뺀다."
    )


def build_summary_input(articles_text: str, title: str) -> str:
    """이미 이름이 있는 클러스터의 요약만 받을 때. 기존 제목의 사건을 요약하도록 알려 준다."""

    return f"{articles_text}\n\n[기존 제목] '{title}' — 요약은 이 사건을 설명한다."


def load_system_prompt(max_chars: int, summary_max_chars: int = 150) -> str:
    return (
        PROMPT_PATH.read_text(encoding="utf-8")
        .replace("{max_chars}", str(max_chars))
        .replace("{summary_max_chars}", str(summary_max_chars))
    )


class ClusterTitler:
    """Bedrock 구조화 출력 체인. filters.relevance_filter.RelevanceJudge 와 같은 구성."""

    def __init__(
        self, max_chars: int, summary_max_chars: int, max_tokens: int = DEFAULT_MAX_TOKENS
    ):
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
                SystemMessage(content=load_system_prompt(max_chars, summary_max_chars)),
                ("human", "{articles}"),
            ]
        )
        structured = model.with_structured_output(schema=ClusterTitle, method="json_schema")
        self._chain = (prompt | structured).with_retry(stop_after_attempt=RETRY_ATTEMPTS)

    async def title(self, articles_text: str) -> ClusterTitle:
        return await self._chain.ainvoke({"articles": articles_text})


@lru_cache
def get_cluster_titler(max_chars: int, summary_max_chars: int) -> Titler:
    return ClusterTitler(max_chars=max_chars, summary_max_chars=summary_max_chars).title


async def _label_one(
    titler: Titler, articles_text: str, max_chars: int, summary_max_chars: int, cluster_id: int
) -> ClusterLabel:
    """제목과 요약 하나. 제목이 길면 축약 지시를 붙여 한 번 더 묻고, 그래도 넘치면
    TitleTooLong. 재요청 응답도 요약을 내므로 그것을 먼저 쓰고, 안 되면 첫 응답의 요약을 쓴다."""

    draft = await titler(articles_text)
    try:
        title = validate_cluster_title(draft.title, max_chars)
        summaries = [draft.summary]
    except TitleTooLong as e:
        retry = await titler(build_shorten_input(articles_text, e.title, max_chars))
        title = validate_cluster_title(retry.title, max_chars)
        summaries = [retry.summary, draft.summary]

    return ClusterLabel(title=title, summary=pick_summary(summaries, summary_max_chars, cluster_id))


async def _summary_one(
    titler: Titler,
    title: str,
    articles: list[ClusterArticle],
    summary_max_chars: int,
    cluster_id: int,
) -> str | None:
    """기존 제목의 사건 요약 하나. 응답의 제목은 쓰지 않는다."""

    draft = await titler(build_summary_input(build_title_input(articles), title))
    return pick_summary([draft.summary], summary_max_chars, cluster_id)


async def _gather_isolated(
    work: dict[int, Callable[[], Awaitable[T]]], max_concurrency: int, what: str
) -> dict[int, T]:
    """클러스터별 작업을 동시에 돌린다. 실패한 클러스터는 결과에서 빠지고 경고만 남는다."""

    semaphore = asyncio.Semaphore(max(1, max_concurrency))
    results: dict[int, T] = {}

    async def one(cluster_id: int, job: Callable[[], Awaitable[T]]) -> None:
        async with semaphore:
            try:
                results[cluster_id] = await job()
            except Exception as e:
                logging.warning(
                    "클러스터 %s 실패(다음 런에 재시도): cluster_id=%s, %s: %s",
                    what,
                    cluster_id,
                    type(e).__name__,
                    e,
                )

    await asyncio.gather(*(one(cid, job) for cid, job in work.items()))
    return results


def title_clusters(
    clusters: dict[int, list[ClusterArticle]],
    titler: Titler | None = None,
    max_concurrency: int = 4,
    max_chars: int = 25,
    summary_max_chars: int = 150,
) -> dict[int, ClusterLabel]:
    """클러스터별 이름과 요약. 실패한 클러스터는 결과에 없다. titler 를 안 주면 Bedrock 싱글톤."""

    clusters = {cid: arts for cid, arts in clusters.items() if arts}
    if not clusters:
        return {}
    if titler is None:
        titler = get_cluster_titler(max_chars, summary_max_chars)

    work = {
        cid: partial(_label_one, titler, build_title_input(arts), max_chars, summary_max_chars, cid)
        for cid, arts in clusters.items()
    }
    return asyncio.run(_gather_isolated(work, max_concurrency, "이름 생성"))


def summarize_titled_clusters(
    clusters: dict[int, tuple[str, list[ClusterArticle]]],
    titler: Titler | None = None,
    max_concurrency: int = 4,
    max_chars: int = 25,
    summary_max_chars: int = 150,
) -> dict[int, str]:
    """이미 이름이 있는 클러스터의 요약만 만든다(백필). clusters 는 id → (기존 제목, 멤버).

    titler 와 프롬프트는 그대로 쓰고 기존 제목을 알려 준다. 응답의 새 제목은 버린다. 요약이
    검증을 못 넘긴 클러스터는 결과에 없다.
    """

    clusters = {cid: pair for cid, pair in clusters.items() if pair[1]}
    if not clusters:
        return {}
    if titler is None:
        titler = get_cluster_titler(max_chars, summary_max_chars)

    work = {
        cid: partial(_summary_one, titler, title, arts, summary_max_chars, cid)
        for cid, (title, arts) in clusters.items()
    }
    summaries = asyncio.run(_gather_isolated(work, max_concurrency, "요약 생성"))
    return {cid: summary for cid, summary in summaries.items() if summary}
