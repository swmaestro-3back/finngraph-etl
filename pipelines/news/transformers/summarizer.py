from __future__ import annotations

import asyncio
import logging
import re
from functools import lru_cache
from pathlib import Path
from string import Template
from typing import Any

from pipelines.common.bedrock import extract_bedrock_text, get_bedrock_client
from pipelines.common.config import get_settings
from pipelines.news.utils.text_utils import (
    clean_article_body_for_storage,
    get_printable_text,
)

SUMMARY_PROMPT_DIRECTORY = Path(__file__).with_name("prompts")

DEFAULT_BODY_LIMIT = 12000
DEFAULT_MAX_TOKENS = 512
DEFAULT_MAX_CONCURRENCY = 4
DEFAULT_TIMEOUT = 300

Triplet = tuple[str, str, str]


def get_summarizer_config() -> dict[str, Any]:

    try:
        from pipelines.news.config import get_news_settings

        settings = get_settings()
        news_settings = get_news_settings()

        return {
            "body_limit": news_settings.news_llm_body_limit,
            "max_tokens": news_settings.news_llm_max_tokens,
            "max_concurrency": news_settings.news_llm_max_concurrency,
            "bedrock_region": settings.bedrock_region,
            "bedrock_model": settings.bedrock_chat_model,
            "bedrock_timeout": settings.bedrock_request_timeout,
        }
    except Exception:
        return {
            "body_limit": DEFAULT_BODY_LIMIT,
            "max_tokens": DEFAULT_MAX_TOKENS,
            "max_concurrency": DEFAULT_MAX_CONCURRENCY,
            "bedrock_region": "",
            "bedrock_model": "",
            "bedrock_timeout": DEFAULT_TIMEOUT,
        }


@lru_cache
def load_summary_prompt_text(filename: str) -> str:
    return (SUMMARY_PROMPT_DIRECTORY / filename).read_text(encoding="utf-8").strip()


def render_summary_prompt(filename: str, **values: str) -> str:
    return Template(load_summary_prompt_text(filename)).safe_substitute(**values).strip()


def build_summary_source_text(item: dict[str, Any], body_limit: int = DEFAULT_BODY_LIMIT) -> str:
    text = clean_article_body_for_storage(
        get_printable_text(item.get("_text", "")),
        article_title=get_printable_text(item.get("title", "")),
    )

    if body_limit and body_limit > 0:
        text = text[:body_limit]

    return text.strip()


def format_triplets_for_prompt(triplets: list[Triplet] | None) -> str:
    if not triplets:
        return "(none)"

    lines = []
    seen: set[Triplet] = set()

    for triplet in triplets:
        if not triplet or len(triplet) < 3:
            continue

        subject, relation, obj = (str(part or "").strip() for part in triplet[:3])

        if not subject or not relation or not obj:
            continue

        key = (subject, relation, obj)

        if key in seen:
            continue

        seen.add(key)
        lines.append(f"- {subject} —[{relation}]→ {obj}")

    return "\n".join(lines) if lines else "(none)"


def build_summary_prompt(item: dict[str, Any], body_limit: int = DEFAULT_BODY_LIMIT) -> str:
    return render_summary_prompt(
        "summary_single.txt",
        title=get_printable_text(item.get("title", "")),
        triplets=format_triplets_for_prompt(item.get("_triplets")),
        source_text=build_summary_source_text(item, body_limit),
    )


def clean_summary_text(content: str) -> str:
    content = re.sub(r"<think>.*?</think>", "", content or "", flags=re.DOTALL | re.IGNORECASE)

    return content.replace("```", "").strip()


def build_bedrock_summary_request(
    item: dict[str, Any], config: dict[str, Any]
) -> tuple[str, str, str, dict[str, Any]]:
    model_id = str(config.get("bedrock_model") or "")

    if not model_id:
        raise RuntimeError("Bedrock 설정이 비어있음(모델 ID 필요)")

    return (
        model_id,
        load_summary_prompt_text("summary_single_system.txt"),
        build_summary_prompt(
            item,
            body_limit=int(config.get("body_limit") or DEFAULT_BODY_LIMIT),
        ),
        {
            "temperature": 0.0,
            "maxTokens": int(config.get("max_tokens") or DEFAULT_MAX_TOKENS),
        },
    )


def parse_bedrock_summary_response(response_data: dict[str, Any]) -> str:
    raw_response = extract_bedrock_text(response_data)

    if not raw_response:
        raise RuntimeError("Bedrock 응답이 비어있음")

    return clean_summary_text(raw_response)


def summarize_with_bedrock(item: dict[str, Any], config: dict[str, Any]) -> str:
    model_id, system_text, user_text, inference_config = build_bedrock_summary_request(
        item=item, config=config
    )
    client = get_bedrock_client(
        str(config.get("bedrock_region") or ""),
        int(config.get("bedrock_timeout") or DEFAULT_TIMEOUT),
    )
    response = client.converse(
        modelId=model_id,
        system=[{"text": system_text}],
        messages=[{"role": "user", "content": [{"text": user_text}]}],
        inferenceConfig=inference_config,
    )

    return parse_bedrock_summary_response(response)


async def summarize_with_bedrock_async(item: dict[str, Any], config: dict[str, Any]) -> str:
    # boto3는 동기 클라이언트라 to_thread로 감싸 asyncio.Semaphore 동시성만 활용한다.
    return await asyncio.to_thread(summarize_with_bedrock, item, config)


async def build_summary_for_item_async(
    item: dict[str, Any],
    config: dict[str, Any],
    semaphore: asyncio.Semaphore,
) -> tuple[int, str] | None:

    news_id = item.get("_news_id")

    try:
        async with semaphore:
            summary = await summarize_with_bedrock_async(item=item, config=config)
    except Exception as e:
        title = get_printable_text(item.get("title", ""))
        logging.warning(
            f"Bedrock 비동기 실패: news_id={news_id}, title={title}, error={type(e).__name__}: {e}"
        )
        return None

    if not news_id or not summary:
        return None

    return (int(news_id), summary)


async def build_summaries_async(
    items: list[dict[str, Any]],
    config: dict[str, Any],
    max_concurrency: int,
) -> list[tuple[int, str]]:
    semaphore = asyncio.Semaphore(max(int(max_concurrency or 1), 1))
    tasks = [
        build_summary_for_item_async(
            item=item,
            config=config,
            semaphore=semaphore,
        )
        for item in items
    ]
    results = await asyncio.gather(*tasks)

    return [result for result in results if result is not None]


def summarize_news_items(
    items: list[dict[str, Any]],
    mode: str = "async",
    max_concurrency: int | None = None,
) -> list[tuple[int, str]]:

    if not items:
        return []

    if mode != "async":
        raise ValueError(f"지원하지 않는 모드: {mode} (async만 지원)")

    config = get_summarizer_config()
    concurrency = max_concurrency or int(config.get("max_concurrency") or DEFAULT_MAX_CONCURRENCY)

    return asyncio.run(
        build_summaries_async(
            items=items,
            config=config,
            max_concurrency=concurrency,
        )
    )
