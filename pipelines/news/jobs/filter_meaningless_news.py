from __future__ import annotations

from typing import Any

from pipelines.news.loaders.news_repository import (
    extract_news_ids_from_removed_items,
    fetch_unchecked_news_items,
    mark_news_material_checked,
)
from pipelines.news.transformers.material_event_filter import filter_material_event_news

DEFAULT_MAX_ITEMS_PER_RUN = 300


def judge_unchecked_news(
    limit: int = DEFAULT_MAX_ITEMS_PER_RUN,
    mode: str = "sequential",
    max_concurrency: int | None = None,
) -> dict[str, Any]:

    source_news = fetch_unchecked_news_items(limit=limit)

    passed_items, removed_items = filter_material_event_news(
        items=source_news,
        use_llm=True,
        mode=mode,
        max_concurrency=max_concurrency,
    )

    kept_ids = [item.get("_news_id") for item in passed_items if item.get("_news_id")]
    dropped_ids = extract_news_ids_from_removed_items(removed_items)

    return {
        "kept_ids": kept_ids,
        "dropped_ids": dropped_ids,
        "fetched": len(source_news),
        "kept": len(passed_items),
        "dropped": len(removed_items),
        "removed_items": removed_items,
    }


def mark_material_results(
    kept_ids: list[int],
    dropped_ids: list[int],
) -> dict[str, int]:

    return mark_news_material_checked(kept_ids=kept_ids, dropped_ids=dropped_ids)


def run(
    apply: bool = True,
    limit: int = DEFAULT_MAX_ITEMS_PER_RUN,
    mode: str = "sequential",
    max_concurrency: int | None = None,
) -> dict[str, Any]:

    judged = judge_unchecked_news(
        limit=limit,
        mode=mode,
        max_concurrency=max_concurrency,
    )

    kept_ids = judged["kept_ids"]
    dropped_ids = judged["dropped_ids"]

    summary: dict[str, Any] = {
        "fetched": judged["fetched"],
        "kept": judged["kept"],
        "dropped": judged["dropped"],
        "applied": False,
        "marked_kept": 0,
        "marked_dropped": 0,
        "removed_items": judged["removed_items"],
    }

    if apply and (kept_ids or dropped_ids):
        mark_result = mark_material_results(kept_ids, dropped_ids)
        summary["applied"] = True
        summary["marked_kept"] = mark_result["kept_count"]
        summary["marked_dropped"] = mark_result["dropped_count"]

    print("\n" + "=" * 70)
    print("의미 없는 뉴스 필터링 결과" + ("" if apply else " (dry-run)"))
    print("=" * 70)
    print(f"조회(미판정) 수: {summary['fetched']}")
    print(f"유지 수: {summary['kept']}")
    print(f"소프트삭제 후보 수: {summary['dropped']}")
    if summary["applied"]:
        print(
            f"기록됨 → 유지 {summary['marked_kept']}개 / 소프트삭제 {summary['marked_dropped']}개"
        )
    else:
        print("dry-run: DB 변경 없음")
    print("=" * 70)

    return summary


if __name__ == "__main__":
    run()
