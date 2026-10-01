"""scripts/backfill_issue_timeline.py 단위 테스트. DB·LLM·Bedrock 은 가짜로 갈아끼우고 흐름만 본다.

dry-run 이 기본이고 아무것도 쓰지 않는지, 초기화 → 요약 → 연결 순서를 지키는지, 연결 반복이
진전이 없을 때 멈추는지를 본다 — 운영 DB 에 한 번 잘못 쓰면 되돌리기 어려운 부분이다.
"""

from __future__ import annotations

import importlib.util
from datetime import datetime
from pathlib import Path

import pytest

from pipelines.news.repositories.issue_timeline import LinkTarget

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "backfill_issue_timeline.py"
WRITERS = {"reset_links", "summarize", "update_cluster_summary", "link_pending"}


def _stats(scanned: int, linked: int = 0, roots: int = 0, failed: int = 0) -> dict[str, int]:
    return {"scanned": scanned, "embedded": 0, "linked": linked, "roots": roots, "failed": failed}


@pytest.fixture
def backfill(monkeypatch):
    from pipelines.news import config
    from pipelines.news.jobs import link_issues
    from pipelines.news.repositories import issue_timeline, news_clusters
    from pipelines.news.transformers import cluster_titler

    monkeypatch.setenv("NEWS_ISSUE_LINK_MAX_PER_RUN", "2")
    config.get_news_settings.cache_clear()

    spec = importlib.util.spec_from_file_location("backfill_issue_timeline", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    events: list[str] = []
    link_calls: list[tuple[datetime, int]] = []
    link_results: list[dict[str, int]] = []
    saved: dict[int, str] = {}

    def record(name, result):
        def fake(*args, **kwargs):
            events.append(name)
            return result

        return fake

    def summarize(clusters, **kwargs):
        events.append("summarize")
        return {cluster_id: f"{title} 요약이에요." for cluster_id, (title, _) in clusters.items()}

    def update_cluster_summary(cluster_id, summary):
        events.append("update_cluster_summary")
        saved[cluster_id] = summary
        return 1

    def link_pending(since, limit):
        events.append("link_pending")
        link_calls.append((since, limit))
        return link_results.pop(0) if link_results else _stats(0)

    target = LinkTarget(7, "마이크론 4분기 실적", None, datetime(2026, 9, 30, tzinfo=module.KST))
    for owner, name, fn in (
        (issue_timeline, "count_resettable_clusters", record("count_resettable", (5, 4))),
        (issue_timeline, "reset_links", record("reset_links", 5)),
        (issue_timeline, "count_link_targets", record("count_link_targets", (1, 1))),
        (issue_timeline, "fetch_link_targets", record("fetch_link_targets", [target])),
        (
            news_clusters,
            "fetch_unsummarized_clusters",
            record("fetch_unsummarized", [(1, "미국 8월 건설지출"), (2, "마이크론 실적")]),
        ),
        (news_clusters, "fetch_cluster_articles", record("fetch_articles", {1: [], 2: []})),
        (news_clusters, "update_cluster_summary", update_cluster_summary),
        (cluster_titler, "summarize_titled_clusters", summarize),
        (link_issues, "link_pending", link_pending),
    ):
        monkeypatch.setattr(owner, name, fn)

    yield module, events, link_calls, link_results, saved
    config.get_news_settings.cache_clear()


def test_dry_run_is_default_and_writes_nothing(backfill):
    module, events, link_calls, _, saved = backfill

    module.main(["--reset-links", "--summaries", "--links"])

    assert WRITERS.isdisjoint(events)
    assert events == [
        "count_resettable",
        "fetch_unsummarized",
        "count_link_targets",
        "fetch_link_targets",
    ]
    assert link_calls == [] and saved == {}


def test_apply_runs_reset_then_summaries_then_links(backfill):
    module, events, link_calls, link_results, saved = backfill
    link_results.append(_stats(2, linked=1, roots=1))

    module.main(["--links", "--summaries", "--reset-links", "--apply"])

    order = [e for e in events if e in WRITERS]
    # 임베딩에 요약이 들어가야 하므로 연결은 늘 요약 뒤, 초기화는 늘 맨 앞
    assert order[0] == "reset_links"
    assert order.index("update_cluster_summary") < order.index("link_pending")
    assert order.index("summarize") > order.index("reset_links")
    assert saved == {1: "미국 8월 건설지출 요약이에요.", 2: "마이크론 실적 요약이에요."}
    # 기간을 안 주면 전체, 배치는 런당 상한
    assert link_calls[0] == (module.ALL_TIME, 2)


def test_links_loop_stops_without_progress(backfill):
    module, events, link_calls, link_results, _ = backfill
    # 대상은 있는데 전부 실패 — linked_at 이 NULL 로 남아 같은 대상이 계속 잡힌다
    link_results.extend([_stats(2, failed=2), _stats(2, linked=2)])

    module.main(["--links", "--apply"])

    assert len(link_calls) == 1


def test_links_loop_runs_until_no_targets_and_respects_limit(backfill):
    module, _, link_calls, link_results, _ = backfill
    link_results.extend([_stats(2, linked=1, roots=1), _stats(1, roots=1), _stats(5, linked=5)])

    module.main(["--links", "--limit", "3", "--apply"])

    # 상한 3 을 배치 2 로 나눠 2 → 1, 남은 수가 0 이 되면 멈춘다
    assert [limit for _, limit in link_calls] == [2, 1]

    link_calls.clear()
    link_results[:] = [_stats(2, linked=2), _stats(0)]
    module.main(["--links", "--apply"])

    assert len(link_calls) == 2


def test_since_days_must_be_positive(backfill):
    module, events, *_ = backfill

    with pytest.raises(SystemExit):
        module.main(["--reset-links", "--since-days", "0", "--apply"])
    assert events == []
