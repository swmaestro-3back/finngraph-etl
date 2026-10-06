"""기사 요약 — 대표 기사 본문으로 요약 문단과 핵심 포인트를 만든다.

핵심 포인트는 고정된 항목(PointKind) 중 본문이 뒷받침하는 2~3개다. 사건이 없는 기사(시황 나열)는
빈 목록이다. 모델은 항목 키만 내고 화면 라벨은 키로 매핑한다. 개수·중복·문체·길이는 스키마가 아니라
여기서 검증하고, 어기면 같은 입력에 어긴 규칙을 붙여 한 번 더 요청한다(온도 0 이라 그냥 재시도하면
같은 답이 나온다). 그래도 어기거나 호출이 실패한 기사는 결과에서 빠져 다음 런에 다시 시도된다.
summarizer 를 갈아끼울 수 있어 테스트는 Bedrock 없이 돈다.
"""

from __future__ import annotations

import asyncio
import logging
import re
from collections.abc import Awaitable, Callable
from enum import StrEnum
from functools import lru_cache
from typing import Any

from pydantic import BaseModel

from pipelines.news.transformers.prompts import summary as summary_prompt
from pipelines.news.utils.date_utils import SEOUL_TIMEZONE
from pipelines.news.utils.text_utils import (
    clean_article_body_for_storage,
    get_printable_text,
)

DEFAULT_BODY_LIMIT = 12000
DEFAULT_MAX_TOKENS = 1024
DEFAULT_MAX_CONCURRENCY = 4
RETRY_ATTEMPTS = 2

MIN_POINTS = 2
MAX_POINTS = 3
# 길이 상한(글자 수는 공백 포함). 프롬프트에는 이보다 짧은 목표치와 함께 주입된다.
SUMMARY_MAX_SENTENCES = 4
SENTENCE_MAX_CHARS = 80
POINT_MAX_CHARS = 60
# 문장 끝의 마침표·따옴표·괄호를 떼고 종결어미를 본다
_SENTENCE_TAIL = " .!?\"'”’)…"
# 문장 경계: 종결 부호 뒤의 공백. 소수점(7.0%)처럼 공백이 따르지 않는 마침표는 경계가 아니다
_SENTENCE_BREAK = re.compile(r"(?<=[.!?])\s+")


class PointKind(StrEnum):
    """핵심 포인트 항목. 선언 순서가 화면 표시 순서다. 정의는 시스템 프롬프트에 있다."""

    CHANGE = "CHANGE"
    AFFECTED = "AFFECTED"
    SCALE = "SCALE"
    CAUSE = "CAUSE"
    RIPPLE = "RIPPLE"


_POINT_ORDER = {kind: index for index, kind in enumerate(PointKind)}


# 형식 규칙은 시스템 프롬프트에만 둔다 — 스키마 설명의 한글은 호출마다 토큰이 비싸게 든다
class KeyPoint(BaseModel):
    kind: PointKind
    text: str


class NewsSummary(BaseModel):
    summary: str
    key_points: list[KeyPoint]


Summarizer = Callable[[str], Awaitable[NewsSummary]]

# (news_id, 요약 문단, [{"kind": ..., "text": ...}])
SummaryRow = tuple[int, str, list[dict[str, str]]]


class SummaryInvalid(ValueError):
    """출력이 형식 규칙을 어겼다. 메시지는 재요청에 그대로 실린다."""


def _collapse(text: str) -> str:
    return " ".join((text or "").split())


def _ends_politely(text: str) -> bool:
    return text.rstrip(_SENTENCE_TAIL).endswith("요")


def validate_summary(draft: NewsSummary) -> NewsSummary:
    """공백을 정리하고 포인트를 표시 순서로 정렬한다. 규칙을 어기면 SummaryInvalid."""

    summary = _collapse(draft.summary)
    points = [KeyPoint(kind=point.kind, text=_collapse(point.text)) for point in draft.key_points]

    if not summary:
        raise SummaryInvalid("summary 가 비어 있다")
    if not _ends_politely(summary):
        raise SummaryInvalid("summary 가 해요체(~요)로 끝나지 않는다")
    sentences = _SENTENCE_BREAK.split(summary)
    if len(sentences) > SUMMARY_MAX_SENTENCES:
        raise SummaryInvalid(
            f"summary 가 {len(sentences)}문장이다({SUMMARY_MAX_SENTENCES}문장 이내)"
        )
    longest = max(len(sentence) for sentence in sentences)
    if longest > SENTENCE_MAX_CHARS:
        raise SummaryInvalid(
            f"summary 의 한 문장이 {longest}자다"
            f"({SENTENCE_MAX_CHARS}자 이내, 문장을 나누거나 부수 정보를 뺀다)"
        )
    if points and not MIN_POINTS <= len(points) <= MAX_POINTS:
        raise SummaryInvalid(
            f"key_points 가 {len(points)}개다"
            f"({MIN_POINTS}~{MAX_POINTS}개, 본문에 기업 사건이 없을 때만 0개)"
        )
    if len({point.kind for point in points}) != len(points):
        raise SummaryInvalid("key_points 에 같은 kind 가 두 번 나온다")
    if any(not _ends_politely(point.text) for point in points):
        raise SummaryInvalid("key_points 의 text 가 해요체(~요)로 끝나지 않는다")
    longest_point = max((len(point.text) for point in points), default=0)
    if longest_point > POINT_MAX_CHARS:
        raise SummaryInvalid(
            f"key_points 의 text 가 {longest_point}자다"
            f"({POINT_MAX_CHARS}자 이내, 한 가지 사실만 남긴다)"
        )

    return NewsSummary(
        summary=summary,
        key_points=sorted(points, key=lambda point: _POINT_ORDER[point.kind]),
    )


def build_summary_source_text(item: dict[str, Any], body_limit: int = DEFAULT_BODY_LIMIT) -> str:
    text = clean_article_body_for_storage(
        get_printable_text(item.get("_text", "")),
        article_title=get_printable_text(item.get("title", "")),
    )

    if body_limit and body_limit > 0:
        text = text[:body_limit]

    return text.strip()


def format_published_date(published_at: Any) -> str:
    """발행일을 서울 기준 'YYYY년 M월 D일'로. 상대 시점(이날·전날)을 날짜로 풀 기준점이다."""

    if not published_at:
        return "미상"
    local = published_at.astimezone(SEOUL_TIMEZONE)
    return f"{local.year}년 {local.month}월 {local.day}일"


def build_summary_prompt(item: dict[str, Any], body_limit: int = DEFAULT_BODY_LIMIT) -> str:
    return summary_prompt.USER.format(
        title=get_printable_text(item.get("title", "")),
        published_date=format_published_date(item.get("_published_at")),
        source_text=build_summary_source_text(item, body_limit),
    ).strip()


def load_system_prompt() -> str:
    return summary_prompt.SYSTEM.format(
        summary_max_sentences=SUMMARY_MAX_SENTENCES,
        sentence_max_chars=SENTENCE_MAX_CHARS,
        point_max_chars=POINT_MAX_CHARS,
    )


def build_retry_input(article_text: str, reason: str) -> str:
    """규칙을 어긴 출력 뒤에 같은 기사로 다시 쓰게 하는 재요청 입력."""

    return summary_prompt.RETRY.format(article=article_text, reason=reason)


class ArticleSummarizer:
    """Bedrock 구조화 출력 체인. cluster_titler.ClusterTitler 와 같은 구성."""

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
            model=settings.chat_model("summary"),
            region_name=settings.bedrock_region,
            temperature=0,
            max_tokens=max_tokens,
            timeout=settings.bedrock_request_timeout,
        )
        prompt = ChatPromptTemplate.from_messages(
            [
                SystemMessage(content=load_system_prompt()),
                ("human", "{article}"),
            ]
        )
        structured = model.with_structured_output(schema=NewsSummary, method="json_schema")
        self._chain = (prompt | structured).with_retry(stop_after_attempt=RETRY_ATTEMPTS)

    async def summarize(self, article_text: str) -> NewsSummary:
        return await self._chain.ainvoke({"article": article_text})


@lru_cache
def get_article_summarizer(max_tokens: int = DEFAULT_MAX_TOKENS) -> Summarizer:
    return ArticleSummarizer(max_tokens=max_tokens).summarize


async def _summarize_one(summarizer: Summarizer, article_text: str) -> NewsSummary:
    """요약 하나. 규칙을 어기면 어긴 규칙을 붙여 한 번 더 묻고, 그래도 어기면 SummaryInvalid."""

    draft = await summarizer(article_text)
    try:
        return validate_summary(draft)
    except SummaryInvalid as e:
        retry = await summarizer(build_retry_input(article_text, str(e)))
        return validate_summary(retry)


async def _summarize_all(
    items: list[dict[str, Any]],
    summarizer: Summarizer,
    max_concurrency: int,
    body_limit: int,
) -> list[SummaryRow]:
    semaphore = asyncio.Semaphore(max(1, max_concurrency))

    async def one(item: dict[str, Any]) -> SummaryRow | None:
        news_id = item.get("_news_id")
        if not news_id:
            return None
        async with semaphore:
            try:
                result = await _summarize_one(summarizer, build_summary_prompt(item, body_limit))
            except Exception as e:
                logging.warning(
                    "[summarize_articles] 요약 실패(다음 런에 재시도): news_id=%s, %s: %s",
                    news_id,
                    type(e).__name__,
                    e,
                )
                return None
        return (
            int(news_id),
            result.summary,
            [{"kind": point.kind.value, "text": point.text} for point in result.key_points],
        )

    results = await asyncio.gather(*(one(item) for item in items))
    return [row for row in results if row is not None]


def summarize_news_items(
    items: list[dict[str, Any]],
    summarizer: Summarizer | None = None,
    max_concurrency: int | None = None,
) -> list[SummaryRow]:
    """기사별 요약과 핵심 포인트. 실패한 기사는 결과에 없다.

    summarizer 를 안 주면 Bedrock 싱글톤이고, 동시성·본문 길이·토큰 상한은 뉴스 설정을 따른다.
    """

    if not items:
        return []

    body_limit = DEFAULT_BODY_LIMIT
    if summarizer is None or max_concurrency is None:
        from pipelines.news.config import get_news_settings

        settings = get_news_settings()
        body_limit = settings.news_llm_body_limit
        if max_concurrency is None:
            max_concurrency = settings.news_llm_max_concurrency
        if summarizer is None:
            summarizer = get_article_summarizer(settings.news_llm_max_tokens)

    return asyncio.run(_summarize_all(items, summarizer, max_concurrency, body_limit))
