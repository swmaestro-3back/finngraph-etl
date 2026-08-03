from __future__ import annotations

import asyncio
import logging
import re
from functools import lru_cache
from pathlib import Path
from string import Template
from typing import Any

import httpx

# Bedrock 클라이언트/응답 파싱은 material_event_filter의 검증된 헬퍼를 공유한다.
from pipelines.news.transformers.material_event_filter import (
    _get_bedrock_client,
    extract_bedrock_text,
)
from pipelines.news.utils.text_utils import (
    clean_article_body_for_storage,
    get_printable_text,
)

SUMMARY_PROMPT_DIRECTORY = Path(__file__).with_name("prompts")

DEFAULT_PROVIDER = "bedrock"
DEFAULT_BODY_LIMIT = 12000
DEFAULT_MAX_TOKENS = 512
DEFAULT_MAX_CONCURRENCY = 4
DEFAULT_TIMEOUT = 300

Triplet = tuple[str, str, str]


def get_summarizer_config() -> dict[str, Any]:

    try:
        from pipelines.news import config

        return {
            "provider": getattr(config, "NEWS_SUMMARY_PROVIDER", DEFAULT_PROVIDER),
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
            "bedrock_region": getattr(config, "BEDROCK_REGION", ""),
            "bedrock_model": getattr(config, "BEDROCK_CHAT_MODEL", ""),
            "bedrock_timeout": getattr(config, "BEDROCK_REQUEST_TIMEOUT", DEFAULT_TIMEOUT),
        }
    except Exception:
        return {
            "provider": DEFAULT_PROVIDER,
            "body_limit": DEFAULT_BODY_LIMIT,
            "max_tokens": DEFAULT_MAX_TOKENS,
            "max_concurrency": DEFAULT_MAX_CONCURRENCY,
            "vllm_base_url": "",
            "vllm_model": "",
            "vllm_api_key": "EMPTY",
            "vllm_timeout": DEFAULT_TIMEOUT,
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


def clean_summary_text(content: str) -> str:
    content = re.sub(r"<think>.*?</think>", "", content or "", flags=re.DOTALL | re.IGNORECASE)

    return content.replace("```", "").strip()


def parse_summary_response(response_data: dict[str, Any]) -> str:

    choices = response_data.get("choices", [])

    if not choices:
        raise RuntimeError("vLLM 요약 choices가 비어있음")

    return clean_summary_text(choices[0].get("message", {}).get("content", "") or "")


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
    client = _get_bedrock_client(
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
    provider = str(config.get("provider") or DEFAULT_PROVIDER).lower()

    try:
        async with semaphore:
            if provider == "bedrock":
                summary = await summarize_with_bedrock_async(item=item, config=config)
            elif provider == "vllm":
                summary = await summarize_with_vllm_async(item=item, config=config, client=client)
            else:
                raise ValueError(f"지원하지 않는 provider: {provider}")
    except Exception as e:
        title = get_printable_text(item.get("title", ""))
        logging.warning(
            f"{provider} 비동기 실패: news_id={news_id}, title={title}, "
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
        raise ValueError(f"지원하지 않는 모드: {mode} (async만 지원)")

    config = get_summarizer_config()
    provider = str(config.get("provider") or DEFAULT_PROVIDER).lower()

    if provider not in {"bedrock", "vllm"}:
        raise ValueError(f"지원하지 않는 provider: {provider} (bedrock/vllm만 지원)")

    concurrency = max_concurrency or int(config.get("max_concurrency") or DEFAULT_MAX_CONCURRENCY)

    return asyncio.run(
        build_summaries_async(
            items=items,
            config=config,
            max_concurrency=concurrency,
        )
    )
