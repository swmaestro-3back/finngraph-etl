from __future__ import annotations

from typing import Any

from pipelines.common.logging import get_logger
from pipelines.news.repositories.news import (
    fetch_unsummarized_news_items,
    save_news_summaries,
)
from pipelines.news.transformers.summarizer import summarize_news_items

logger = get_logger(__name__)


def summarize_unsummarized_news(
    mode: str = "async",
    max_concurrency: int | None = None,
) -> dict[str, Any]:

    source_news = fetch_unsummarized_news_items()

    results = summarize_news_items(
        items=source_news,
        mode=mode,
        max_concurrency=max_concurrency,
    )

    return {
        "rows": results,
        "fetched": len(source_news),
        "summarized": len(results),
    }


def save_summaries(rows: list[tuple[int, str]]) -> dict[str, int]:

    return save_news_summaries(rows=rows)


def run(
    apply: bool = True,
    mode: str = "async",
    max_concurrency: int | None = None,
) -> dict[str, Any]:

    result = summarize_unsummarized_news(
        mode=mode,
        max_concurrency=max_concurrency,
    )

    rows = result["rows"]

    summary: dict[str, Any] = {
        "fetched": result["fetched"],
        "summarized": result["summarized"],
        "applied": False,
        "saved": 0,
    }

    if apply and rows:
        save_result = save_summaries(rows)
        summary["applied"] = True
        summary["saved"] = save_result["saved_count"]

    logger.info(
        "[summarize_articles] 완료: 미요약 %d → 요약 %d / 저장 %d%s",
        summary["fetched"],
        summary["summarized"],
        summary["saved"],
        "" if apply else " (dry-run)",
    )

    return summary


if __name__ == "__main__":
    run()
