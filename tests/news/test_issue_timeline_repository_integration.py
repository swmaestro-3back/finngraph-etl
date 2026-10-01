"""이슈 타임라인 repository 통합 테스트.

실제 Postgres(pgvector) 에 news / news_clusters / news_companies 행을 만들고 SQL 을 직접 부른다.
다른 행과 섞이지 않게 2001년 날짜와 uuid 마커를 쓴다. V6 마이그레이션이 적용된 DB 가 필요하다.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta

import pytest
from sqlalchemy import text

from pipelines.common.clients.postgres import SessionLocal, session_scope
from pipelines.news.repositories.issue_timeline import (
    COUNT_RESETTABLE_SQL,
    RESET_LINKS_SQL,
    fetch_cluster_company_ids,
    fetch_link_candidates,
    fetch_link_targets,
    fetch_recent_member_titles,
    record_link_decision,
    save_cluster_embedding,
)
from pipelines.news.repositories.news_clusters import (
    fetch_unsummarized_clusters,
    update_cluster_label,
    update_cluster_summary,
)
from pipelines.news.transformers.issue_linker import to_vector_literal
from pipelines.news.utils.date_utils import SEOUL_TIMEZONE

pytestmark = pytest.mark.integration

BASE = datetime(2001, 3, 1, 9, tzinfo=SEOUL_TIMEZONE)
STALE = datetime(2001, 1, 1, tzinfo=SEOUL_TIMEZONE)


def _unit(index: int) -> str:
    return to_vector_literal([1.0 if i == index else 0.0 for i in range(1024)])


@pytest.fixture
def story():
    """루트 후보 root(3/1, 기업 A), 다른 기업 other(3/5, 기업 B), 대상 target(3/10, 기업 A),
    기업 없는 plain(3/3), 이름 없는 untitled(3/9)."""

    marker = uuid.uuid4().hex[:8]
    with session_scope() as session:
        company_a, company_b = (
            session.execute(
                text(
                    "INSERT INTO companies (name, is_listed, country) "
                    "VALUES (:n, true, 'KR') RETURNING id"
                ),
                {"n": f"타임라인{marker}{suffix}"},
            ).scalar_one()
            for suffix in ("A", "B")
        )

        def cluster(title: str | None, day: int) -> int:
            return session.execute(
                text(
                    """
                    INSERT INTO news_clusters (
                        first_published_at, last_published_at, original_size, member_count,
                        title, updated_at
                    )
                    VALUES (:t, :t, 3, 1, :title, :u) RETURNING id
                    """
                ),
                {"t": BASE + timedelta(days=day), "title": title, "u": STALE},
            ).scalar_one()

        news_ids: list[int] = []

        def member(cluster_id: int, title: str, hours: int, companies: tuple[int, ...]) -> None:
            news_id = session.execute(
                text(
                    "INSERT INTO news (title, text, link, published_at, cluster_id) "
                    "VALUES (:title, '본문', :link, :p, :c) RETURNING id"
                ),
                {
                    "title": title,
                    "link": f"https://example.com/it/{marker}/{len(news_ids)}",
                    "p": BASE + timedelta(hours=hours),
                    "c": cluster_id,
                },
            ).scalar_one()
            news_ids.append(news_id)
            for company_id in companies:
                session.execute(
                    text("INSERT INTO news_companies (news_id, company_id) VALUES (:n, :c)"),
                    {"n": news_id, "c": company_id},
                )

        ids = {
            "root": cluster(f"실적 발표 일정 {marker}", 0),
            "plain": cluster(f"건설지출 {marker}", 2),
            "other": cluster(f"다른 기업 {marker}", 4),
            "untitled": cluster(None, 8),
            "target": cluster(f"실적 {marker}", 9),
        }
        member(ids["root"], "첫 기사", 1, (company_a,))
        member(ids["root"], "둘째 기사", 2, (company_a, company_b))
        member(ids["root"], "셋째 기사", 3, ())
        member(ids["root"], "넷째 기사", 4, ())
        member(ids["other"], "다른 기업 기사", 100, (company_b,))
        member(ids["target"], "실적 기사", 220, (company_a,))
        member(ids["plain"], "건설지출 기사", 50, ())

    yield {**ids, "a": int(company_a), "b": int(company_b)}

    with session_scope() as session:
        session.execute(text("DELETE FROM news WHERE id = ANY(:ids)"), {"ids": news_ids})
        session.execute(
            text("DELETE FROM news_clusters WHERE id = ANY(:ids)"), {"ids": list(ids.values())}
        )
        session.execute(
            text("DELETE FROM companies WHERE id = ANY(:ids)"), {"ids": [company_a, company_b]}
        )


def _row(cluster_id: int) -> dict:
    with session_scope() as session:
        row = session.execute(
            text(
                """
                SELECT parent_cluster_id, story_root_id, link_score, linked_at, updated_at,
                       summary, title
                  FROM news_clusters
                 WHERE id = :id
                """
            ),
            {"id": cluster_id},
        ).one()
    return dict(row._mapping)


def test_targets_members_and_companies(story):
    mine = set(story.values())
    targets = [
        t for t in fetch_link_targets(BASE - timedelta(days=1), 10_000) if t.cluster_id in mine
    ]

    # 이름 있고 판정 전인 것만, 오래된 순
    assert [t.cluster_id for t in targets] == [
        story["root"],
        story["plain"],
        story["other"],
        story["target"],
    ]
    assert all(t.embedding is None for t in targets)

    # 최신 기사부터 per_cluster 개
    assert fetch_recent_member_titles([story["root"]], 3) == {
        story["root"]: ["넷째 기사", "셋째 기사", "둘째 기사"]
    }
    assert fetch_cluster_company_ids([story["root"], story["other"], story["plain"]]) == {
        story["root"]: frozenset({story["a"], story["b"]}),
        story["other"]: frozenset({story["b"]}),
    }


def test_link_flow_writes_root_then_child_without_touching_updated_at(story):
    for key in ("root", "other", "target"):
        save_cluster_embedding(story[key], _unit(0))
    save_cluster_embedding(story["plain"], _unit(1))

    [target] = [t for t in fetch_link_targets(BASE, 10_000) if t.cluster_id == story["target"]]
    assert target.embedding is not None and target.embedding.startswith("[1")

    # 판정 전 클러스터는 후보가 아니다
    assert (
        fetch_link_candidates(
            cluster_id=story["target"],
            embedding=target.embedding,
            first_published_at=target.first_published_at,
            company_ids=frozenset({story["a"]}),
            lookback_days=90,
            min_score=0.6,
            limit=20,
        )
        == []
    )

    for key in ("root", "other", "plain"):
        assert record_link_decision(story[key], None, None) is True
    assert _row(story["root"])["story_root_id"] == story["root"]

    candidates = fetch_link_candidates(
        cluster_id=story["target"],
        embedding=target.embedding,
        first_published_at=target.first_published_at,
        company_ids=frozenset({story["a"]}),
        lookback_days=90,
        min_score=0.6,
        limit=20,
    )
    # 기업 A 가 겹치는 root 만. other(기업 B)·plain(기업 없음)은 빠진다
    assert [c.cluster_id for c in candidates] == [story["root"]]
    assert candidates[0].score == pytest.approx(1.0)
    assert candidates[0].company_ids == frozenset({story["a"], story["b"]})

    # 기업 없는 대상은 기업 없는 클러스터만, lookback 밖은 빠진다
    no_company = fetch_link_candidates(
        cluster_id=-1,
        embedding=_unit(1),
        first_published_at=BASE + timedelta(days=30),
        company_ids=frozenset(),
        lookback_days=90,
        min_score=0.75,
        limit=20,
    )
    assert [c.cluster_id for c in no_company] == [story["plain"]]
    assert (
        fetch_link_candidates(
            cluster_id=-1,
            embedding=_unit(1),
            first_published_at=BASE + timedelta(days=30),
            company_ids=frozenset(),
            lookback_days=10,
            min_score=0.75,
            limit=20,
        )
        == []
    )

    assert record_link_decision(story["target"], story["root"], 0.9312345678) is True
    row = _row(story["target"])
    assert (row["parent_cluster_id"], row["story_root_id"]) == (story["root"], story["root"])
    assert float(row["link_score"]) == pytest.approx(0.931235)
    assert row["linked_at"] is not None
    # 연결 쓰기는 events 승격 스캔이 보는 updated_at 을 건드리지 않는다
    assert row["updated_at"] == STALE
    # 이미 판정된 클러스터는 다시 쓰지 않는다
    assert record_link_decision(story["target"], None, None) is False
    assert _row(story["target"])["parent_cluster_id"] == story["root"]


def test_summary_writes(story):
    pending = [cid for cid, _ in fetch_unsummarized_clusters(BASE - timedelta(days=1), 10_000)]
    # member_count 는 fixture 가 1 로 넣었다. 이름 없는 클러스터는 빠진다
    assert [cid for cid in pending if cid in set(story.values())] == [
        story["root"],
        story["plain"],
        story["other"],
        story["target"],
    ]

    assert update_cluster_summary(story["root"], "실적을 발표해요.") is True
    assert update_cluster_summary(story["root"], "덮어쓰지 않는다") is False
    row = _row(story["root"])
    assert row["summary"] == "실적을 발표해요."
    assert row["updated_at"] == STALE

    update_cluster_label(story["untitled"], "새 이름", None)
    row = _row(story["untitled"])
    assert (row["title"], row["summary"]) == ("새 이름", None)


def test_reset_links_clears_only_clusters_from_since(story):
    for key in ("root", "plain", "other", "target"):
        save_cluster_embedding(story[key], _unit(0))
    for key in ("root", "plain", "other"):
        record_link_decision(story[key], None, None)
    record_link_decision(story["target"], story["root"], 1.0)

    # since 이후 구간은 이 fixture 밖의 행도 포함하므로 커밋하지 않고 같은 트랜잭션 안에서만 본다
    session = SessionLocal()
    try:
        session.execute(RESET_LINKS_SQL, {"since": BASE + timedelta(days=3)})
        rows = {
            int(row.id): row
            for row in session.execute(
                text(
                    """
                    SELECT id, embedding IS NULL AS no_embedding, parent_cluster_id,
                           story_root_id, link_score, linked_at, updated_at
                      FROM news_clusters
                     WHERE id = ANY(:ids)
                    """
                ),
                {"ids": list(story[k] for k in ("root", "plain", "other", "target"))},
            )
        }
        remaining = session.execute(COUNT_RESETTABLE_SQL, {"since": BASE + timedelta(days=3)}).one()
    finally:
        session.rollback()
        session.close()

    # 3/1 root·3/3 plain 은 그대로, 3/5 other·3/10 target 은 임베딩·판정 모두 지워진다
    for key in ("root", "plain"):
        row = rows[story[key]]
        assert not row.no_embedding and row.linked_at is not None
        assert row.story_root_id == story[key]
    for key in ("other", "target"):
        row = rows[story[key]]
        assert row.no_embedding
        assert (row.parent_cluster_id, row.story_root_id, row.link_score, row.linked_at) == (
            None,
            None,
            None,
            None,
        )
        assert row.updated_at == STALE
    assert (remaining.clusters, remaining.linked) == (0, 0)
