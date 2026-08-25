from __future__ import annotations

from typing import Any

from pipelines.news.config import get_news_settings
from pipelines.news.loaders.news_repository import (
    fetch_unsummarized_news_items,
    save_news_summaries,
)
from pipelines.news.transformers.summarizer import summarize_news_items


def summarize_unsummarized_news(
    limit: int | None = None,
    mode: str = "async",
    max_concurrency: int | None = None,
) -> dict[str, Any]:

    if limit is None:
        limit = get_news_settings().news_llm_max_items_per_run

    source_news = fetch_unsummarized_news_items(limit=limit)

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
    limit: int | None = None,
    mode: str = "async",
    max_concurrency: int | None = None,
) -> dict[str, Any]:

    result = summarize_unsummarized_news(
        limit=limit,
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

    print("\n" + "=" * 70)
    print("뉴스 요약 결과" + ("" if apply else " (dry-run)"))
    print("=" * 70)
    print(f"조회(미요약) 수: {summary['fetched']}")
    print(f"요약 생성 수: {summary['summarized']}")
    if summary["applied"]:
        print(f"저장됨: {summary['saved']}개")
    else:
        print("dry-run: DB 변경 없음")
    print("=" * 70)

    return summary


if __name__ == "__main__":
    run()
