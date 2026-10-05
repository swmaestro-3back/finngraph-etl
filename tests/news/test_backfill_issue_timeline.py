"""scripts/backfill_issue_timeline.py 단위 테스트. DB·Bedrock 은 가짜로 갈아끼우고 흐름만 본다.

dry-run 이 기본이고 아무것도 쓰지 않는지, 초기화 → 연결 순서를 지키는지, 연결 반복이 진전이 없을
때 멈추는지를 본다 — 운영 DB 에 한 번 잘못 쓰면 되돌리기 어려운 부분이다.
"""

from __future__ import annotations

import importlib.util
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from pipelines.common.utils.time import now_kst
from pipelines.news.repositories.postgres.news_clusters import LinkTarget

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "backfill_issue_timeline.py"
WRITERS = {"reset_cluster_links", "link_pending"}


def _stats(scanned: int, linked: int = 0, roots: int = 0, failed: int = 0) -> dict[str, int]:
    return {
        "scanned": scanned,
        "embedded": 0,
        "linked": linked,
        "same_event": 0,
        "follow_up": linked,
        "roots": roots,
        "failed": failed,
    }


@pytest.fixture
def backfill(monkeypatch):
    from pipelines.news import config
    from pipelines.news.jobs import link_issues
    from pipelines.news.repositories.postgres import news_clusters

    monkeypatch.setenv("NEWS_ISSUE_LINK_MAX_PER_RUN", "2")
    config.get_news_settings.cache_clear()

    spec = importlib.util.spec_from_file_location("backfill_issue_timeline", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    events: list[str] = []
    link_calls: list[tuple[datetime, int]] = []
    link_results: list[dict[str, int]] = []
    deadlines: list[datetime] = []

    def record(name, result):
        def fake(*args, **kwargs):
            events.append(name)
            return result

        return fake

    def count_link_targets(since, summary_deadline):
        events.append("count_link_targets")
        deadlines.append(summary_deadline)
        return {"targets": 1, "without_embedding": 1, "waiting_summary": 0}

    def link_pending(since, limit):
        events.append("link_pending")
        link_calls.append((since, limit))
        return link_results.pop(0) if link_results else _stats(0)

    target = LinkTarget(7, "마이크론 4분기 실적", datetime(2026, 9, 30, tzinfo=module.KST))
    for owner, name, fn in (
        (
            news_clusters,
            "count_resettable_clusters",
            record("count_resettable", {"clusters": 5, "linked": 4}),
        ),
        (news_clusters, "reset_cluster_links", record("reset_cluster_links", 5)),
        (news_clusters, "count_link_targets", count_link_targets),
        (news_clusters, "fetch_link_targets", record("fetch_link_targets", [target])),
        (link_issues, "link_pending", link_pending),
        (link_issues, "embed_texts", record("embed_texts", [])),
    ):
        monkeypatch.setattr(owner, name, fn)

    yield module, events, link_calls, link_results, deadlines
    config.get_news_settings.cache_clear()


def test_dry_run_is_default_and_writes_nothing(backfill):
    module, events, link_calls, _, deadlines = backfill

    module.main(["--reset-links", "--links"])

    assert WRITERS.isdisjoint(events)
    assert "embed_texts" not in events
    assert events == ["count_resettable", "count_link_targets", "fetch_link_targets"]
    assert link_calls == []
    # 요약 대기 기준은 job 과 같은 24시간
    assert abs(deadlines[0] - (now_kst() - timedelta(hours=24))) < timedelta(minutes=1)


def test_apply_runs_reset_before_links(backfill):
    module, events, link_calls, link_results, _ = backfill
    link_results.append(_stats(2, linked=1, roots=1))

    module.main(["--links", "--reset-links", "--apply"])

    order = [e for e in events if e in WRITERS]
    assert order[0] == "reset_cluster_links"
    assert order.index("reset_cluster_links") < order.index("link_pending")
    # 기간을 안 주면 전체, 배치는 런당 상한
    assert link_calls[0] == (module.ALL_TIME, 2)


def test_since_days_narrows_the_window(backfill):
    module, _, link_calls, link_results, _ = backfill
    link_results.append(_stats(1, roots=1))

    module.main(["--links", "--since-days", "30", "--apply"])

    since, _ = link_calls[0]
    assert abs(since - (now_kst() - timedelta(days=30))) < timedelta(minutes=1)


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


@pytest.mark.parametrize(
    "argv",
    [
        ["--reset-links", "--since-days", "0", "--apply"],
        ["--links", "--limit", "0"],
        ["--apply"],
        ["--links", "--apply", "--dry-run"],
    ],
)
def test_invalid_arguments_exit_before_touching_db(backfill, argv):
    module, events, *_ = backfill

    with pytest.raises(SystemExit):
        module.main(argv)
    assert events == []
