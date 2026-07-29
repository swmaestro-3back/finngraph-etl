from __future__ import annotations

import asyncio
import logging
import re
from functools import lru_cache
from pathlib import Path
from string import Template
from typing import Any

import httpx

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
        from pipelines.news import config

        return {
            "body_limit": getattr(config, "NEWS_SUMMARY_BODY_LIMIT", DEFAULT_BODY_LIMIT),
            "max_tokens": getattr(config, "NEWS_SUMMARY_MAX_TOKENS", DEFAULT_MAX_TOKENS),
            "max_concurrency": getattr(
                config,
                "NEWS_SUMMARY_MAX_CONCURRENCY",
                DEFAULT_MAX_CONCURRENCY,
            ),
            "vllm_base_url": getattr(config, "VLLM_BASE_URL", ""),
            "vllm_model": getattr(config, "VLLM_CHAT_MODEL", ""),
            "vllm_api_key": getattr(config, "VLLM_API_KEY", "EMPTY"),
            "vllm_timeout": getattr(config, "VLLM_REQUEST_TIMEOUT", DEFAULT_TIMEOUT),
        }
    except Exception:
        return {
            "body_limit": DEFAULT_BODY_LIMIT,
            "max_tokens": DEFAULT_MAX_TOKENS,
            "max_concurrency": DEFAULT_MAX_CONCURRENCY,
            "vllm_base_url": "",
            "vllm_model": "",
            "vllm_api_key": "EMPTY",
            "vllm_timeout": DEFAULT_TIMEOUT,
        }


@lru_cache
def load_summary_prompt_text(filename: str) -> str:
    return (SUMMARY_PROMPT_DIRECTORY / filename).read_text(encoding="utf-8").strip()


def render_summary_prompt(filename: str, **values: str) -> str:
    return Template(load_summary_prompt_text(filename)).safe_substitute(**values).strip()


def build_summary_source_text(item: dict[str, Any], body_limit: int = DEFAULT_BODY_LIMIT) -> str:
    body_text = clean_article_body_for_storage(
        get_printable_text(item.get("_body_text", "")),
        article_title=get_printable_text(item.get("title", "")),
    )

    if body_limit and body_limit > 0:
        body_text = body_text[:body_limit]

    return body_text.strip()


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


def build_summary_request(
    item: dict[str, Any], config: dict[str, Any]
) -> tuple[str, dict[str, str], dict[str, Any], int]:
    base_url = str(config.get("vllm_base_url") or "").rstrip("/")
    model = str(config.get("vllm_model") or "")

    if not base_url or not model:
        raise RuntimeError("vLLM 요약 설정이 비어있음")

    return (
        f"{base_url}/chat/completions",
        {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {config.get('vllm_api_key') or 'EMPTY'}",
        },
        {
            "model": model,
            "messages": [
                {
                    "role": "system",
                    "content": load_summary_prompt_text("summary_single_system.txt"),
                },
                {
                    "role": "user",
                    "content": build_summary_prompt(
                        item,
                        body_limit=int(config.get("body_limit") or DEFAULT_BODY_LIMIT),
                    ),
                },
            ],
            "temperature": 0.0,
            "max_tokens": int(config.get("max_tokens") or DEFAULT_MAX_TOKENS),
            "chat_template_kwargs": {
                "enable_thinking": False,
            },
            "stream": False,
        },
        int(config.get("vllm_timeout") or DEFAULT_TIMEOUT),
    )


def parse_summary_response(response_data: dict[str, Any]) -> str:

    choices = response_data.get("choices", [])

    if not choices:
        raise RuntimeError("vLLM 요약 choices가 비어있음")

    content = choices[0].get("message", {}).get("content", "") or ""
    content = re.sub(r"<think>.*?</think>", "", content, flags=re.DOTALL | re.IGNORECASE)
    content = content.replace("```", "").strip()

    return content


async def summarize_with_vllm_async(
    item: dict[str, Any],
    config: dict[str, Any],
    client: httpx.AsyncClient,
) -> str:
    url, headers, payload, timeout = build_summary_request(item=item, config=config)
    response = await client.post(url, headers=headers, json=payload, timeout=timeout)
    response.raise_for_status()

    return parse_summary_response(response.json())


async def build_summary_for_item_async(
    item: dict[str, Any],
    config: dict[str, Any],
    client: httpx.AsyncClient,
    semaphore: asyncio.Semaphore,
) -> tuple[int, str] | None:

    news_id = item.get("_news_id")

    try:
        async with semaphore:
            summary = await summarize_with_vllm_async(item=item, config=config, client=client)
    except Exception as e:
        title = get_printable_text(item.get("title", ""))
        logging.warning(
            f"vLLM 비동기 요약 실패: news_id={news_id}, title={title}, "
            f"error={type(e).__name__}: {e}"
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
    max_concurrency = max(int(max_concurrency or 1), 1)
    semaphore = asyncio.Semaphore(max_concurrency)
    limits = httpx.Limits(
        max_connections=max_concurrency,
        max_keepalive_connections=max_concurrency,
    )

    async with httpx.AsyncClient(limits=limits) as client:
        tasks = [
            build_summary_for_item_async(
                item=item,
                config=config,
                client=client,
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
        raise ValueError(f"지원하지 않는 요약 모드: {mode} (async만 지원)")

    config = get_summarizer_config()
    concurrency = max_concurrency or int(config.get("max_concurrency") or DEFAULT_MAX_CONCURRENCY)

    return asyncio.run(
        build_summaries_async(
            items=items,
            config=config,
            max_concurrency=concurrency,
        )
    )
