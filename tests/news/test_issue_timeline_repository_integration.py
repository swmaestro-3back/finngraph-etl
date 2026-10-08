"""이슈 타임라인 repository 를 검증하는 통합 테스트다. 연결 대상·멤버·후보(페이지) 조회, 재판정
대상 조회, 임베딩·판정 쓰기(첫 판정·재판정), 초기화를 본다.

실제 Postgres(pgvector)에 news / news_clusters / news_companies 행을 만들고 SQL 을 직접 부른다.
다른 행과 섞이지 않게 2101년 날짜와 uuid 마커를 쓴다. V12 마이그레이션이 적용된 DB 가 필요하다.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from pipelines.common.clients.postgres import session_scope
from pipelines.common.utils.time import now_kst
from pipelines.news.repositories.postgres.news_clusters import (
    count_link_targets,
    count_resettable_clusters,
    fetch_cluster_members,
    fetch_link_candidates,
    fetch_link_targets,
    fetch_relink_tail,
    record_link_decision,
    reset_cluster_links,
    save_cluster_embedding,
)
from pipelines.news.transformers.issue_linker import ClusterMember, to_vector_literal
from pipelines.news.utils.date_utils import SEOUL_TIMEZONE

pytestmark = pytest.mark.integration

BASE = datetime(2101, 3, 1, 9, tzinfo=SEOUL_TIMEZONE)
STALE = datetime(2001, 1, 1, tzinfo=SEOUL_TIMEZONE)
CHANGE_POINTS = [{"kind": "CHANGE", "text": "실적을 발표해요."}]


def _unit(index: int) -> str:
    return to_vector_literal([1.0 if i == index else 0.0 for i in range(1024)])


def _deadline() -> datetime:
    return now_kst() - timedelta(hours=24)


@pytest.fixture
def story():
    """다음 클러스터를 만든다: root(3/1, 기업 A·B), plain(3/3, 기업 없음), other(3/5, 기업 B),
    stale(3/7, 요약 없음·오래됨), waiting(3/8, 요약 없음·방금 갱신), untitled(3/9),
    unpromoted(3/9), target(3/10, 기업 A), twin(3/10 같은 시각, 기업 A)."""

    marker = uuid.uuid4().hex[:8]
    news_ids: list[int] = []
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

        def cluster(title: str | None, day: int, updated_at: datetime = STALE) -> int:
            return session.execute(
                text(
                    """
                    INSERT INTO news_clusters (
                        first_published_at, last_published_at, original_size, member_count,
                        title, updated_at
                    )
                    VALUES (:t, :t, 5, 5, :title, :u) RETURNING id
                    """
                ),
                {"t": BASE + timedelta(days=day), "title": title, "u": updated_at},
            ).scalar_one()

        def member(
            cluster_id: int,
            title: str,
            hours: int,
            companies: tuple[int, ...] = (),
            *,
            summary: str | None = None,
            points: list | None = None,
            representative: bool = False,
            candidate: bool = False,
        ) -> int:
            news_id = session.execute(
                text(
                    """
                    INSERT INTO news (
                        title, text, link, published_at, cluster_id, summary, summary_points,
                        cluster_terms
                    )
                    VALUES (
                        :title, '본문', :link, :p, :c, :summary, CAST(:points AS jsonb),
                        CAST(:terms AS jsonb)
                    )
                    RETURNING id
                    """
                ),
                {
                    "title": title,
                    "link": f"https://example.com/it/{marker}/{len(news_ids)}",
                    "p": BASE + timedelta(hours=hours),
                    "c": cluster_id,
                    "summary": summary,
                    "points": None if points is None else json.dumps(points, ensure_ascii=False),
                    # 후보 기사(승격 전에 들어온 기사)만 토큰 가중치가 있다.
                    "terms": '{"실적": 1.0}' if candidate else None,
                },
            ).scalar_one()
            news_ids.append(news_id)
            for company_id in companies:
                session.execute(
                    text("INSERT INTO news_companies (news_id, company_id) VALUES (:n, :c)"),
                    {"n": news_id, "c": company_id},
                )
            if representative:
                # 요약 대기 판정이 updated_at 을 보므로 대표 기사를 지정할 때 updated_at 은 바꾸지 않는다.
                session.execute(
                    text("UPDATE news_clusters SET representative_news_id = :n WHERE id = :c"),
                    {"n": news_id, "c": cluster_id},
                )
            return news_id

        ids = {
            "root": cluster(f"실적 발표 일정 {marker}", 0),
            "plain": cluster(f"건설지출 {marker}", 2),
            "other": cluster(f"다른 기업 {marker}", 4),
            "stale": cluster(f"요약 실패 {marker}", 6),
            "waiting": cluster(f"요약 대기 {marker}", 7, updated_at=now_kst()),
            "untitled": cluster(None, 8),
            "unpromoted": cluster(f"대표 없음 {marker}", 8),
            "target": cluster(f"실적 {marker}", 9),
            "twin": cluster(f"실적 중복 {marker}", 9),
        }
        member(
            ids["root"],
            "첫 기사",
            1,
            (company_a,),
            summary="첫 문장이에요. 둘째 문장이에요.",
            points=CHANGE_POINTS,
            representative=True,
            candidate=True,
        )
        # 둘째·넷째는 승격 뒤에 붙은 기사이고, 셋째는 후보 기사다.
        member(ids["root"], "둘째 기사", 2, (company_a, company_b))
        member(ids["root"], "셋째 기사", 3, candidate=True)
        member(ids["root"], "넷째 기사", 4)
        member(
            ids["plain"], "건설지출 기사", 50, summary="건설지출이 늘었어요.", representative=True
        )
        member(ids["other"], "다른 기업 기사", 100, (company_b,), points=[], representative=True)
        member(ids["stale"], "요약 실패 기사", 150, representative=True)
        member(ids["waiting"], "요약 대기 기사", 170, representative=True)
        member(ids["untitled"], "이름 없음 기사", 190, summary="요약이에요.", representative=True)
        member(ids["unpromoted"], "대표 없음 기사", 190, summary="요약이에요.")
        member(ids["target"], "실적 기사", 220, (company_a,), summary="요약.", representative=True)
        member(
            ids["twin"], "실적 중복 기사", 221, (company_a,), summary="요약.", representative=True
        )

    yield {**ids, "a": int(company_a), "b": int(company_b)}

    with session_scope() as session:
        session.execute(
            text("UPDATE news_clusters SET representative_news_id = NULL WHERE id = ANY(:ids)"),
            {"ids": list(ids.values())},
        )
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
                SELECT parent_cluster_id, story_root_id, link_score, link_relation, linked_at,
                       updated_at, embedding IS NULL AS no_embedding
                  FROM news_clusters
                 WHERE id = :id
                """
            ),
            {"id": cluster_id},
        ).one()
    return dict(row._mapping)


def _candidates(story, cluster_key: str, embedding: str, companies, **overrides):
    [target] = [
        t
        for t in fetch_link_targets(BASE - timedelta(days=1), 10_000, _deadline())
        if t.cluster_id == story[cluster_key]
    ]
    params = {
        "cluster_id": target.cluster_id,
        "embedding": embedding,
        "first_published_at": target.first_published_at,
        "company_ids": frozenset(companies),
        "window_start": target.first_published_at - timedelta(days=90),
        "min_score": 0.45,
        "limit": 20,
    }
    params.update(overrides)
    return [c.cluster_id for c in fetch_link_candidates(**params)]


def test_targets_need_title_representative_and_summary_or_wait(story):
    targets = fetch_link_targets(BASE - timedelta(days=1), 10_000, _deadline())

    # 이름과 대표 기사가 있고 판정 전인 것만 오래된 순으로 읽는다. 요약이 없는 것은 updated_at 이
    # 하루 넘게 지나야 대상이 된다.
    assert [t.cluster_id for t in targets] == [
        story["root"],
        story["plain"],
        story["other"],
        story["stale"],
        story["target"],
        story["twin"],
    ]
    root, plain, other, stale = targets[:4]
    assert (root.summary_points, root.summary) == (CHANGE_POINTS, "첫 문장이에요. 둘째 문장이에요.")
    assert (plain.summary_points, plain.summary) == (None, "건설지출이 늘었어요.")
    assert other.summary_points == []
    assert (stale.summary_points, stale.summary) == (None, None)
    assert all(t.embedding is None for t in targets)

    assert count_link_targets(BASE - timedelta(days=1), _deadline()) == {
        "targets": 6,
        "without_embedding": 6,
        "waiting_summary": 1,
    }
    # limit 개까지만 읽는다.
    assert [t.cluster_id for t in fetch_link_targets(BASE + timedelta(days=1), 2, _deadline())] == [
        story["plain"],
        story["other"],
    ]


def test_members_candidates_first_by_published_at_with_companies(story):
    members = fetch_cluster_members([story["root"], story["plain"], -1])

    # 후보 기사(승격 뒤에 바뀌지 않는다)를 먼저, 그 안에서는 발행 시각순으로 읽는다.
    assert members == {
        story["root"]: [
            ClusterMember("첫 기사", frozenset({story["a"]})),
            ClusterMember("셋째 기사", frozenset()),
            ClusterMember("둘째 기사", frozenset({story["a"], story["b"]})),
            ClusterMember("넷째 기사", frozenset()),
        ],
        story["plain"]: [ClusterMember("건설지출 기사", frozenset())],
    }
    assert fetch_cluster_members([]) == {}


def test_link_flow_without_touching_updated_at(story):
    for key in ("root", "other", "target", "twin"):
        save_cluster_embedding(story[key], _unit(0))
    save_cluster_embedding(story["plain"], _unit(1))

    [target] = [
        t for t in fetch_link_targets(BASE, 10_000, _deadline()) if t.cluster_id == story["target"]
    ]
    assert target.embedding is not None and target.embedding.startswith("[1")

    # 판정 전 클러스터는 부모 후보가 아니다.
    assert _candidates(story, "target", target.embedding, {story["a"]}) == []

    for key in ("root", "other", "plain"):
        assert record_link_decision(story[key], None, None, None) is True
    root_row = _row(story["root"])
    assert (root_row["story_root_id"], root_row["link_relation"]) == (story["root"], None)

    # 기업 A 가 연결된 root 만 나오고, other(기업 B)·plain(기업 없음)은 빠진다.
    assert _candidates(story, "target", target.embedding, {story["a"]}) == [story["root"]]
    # 기업 B 는 root(둘째 기사)와 other 둘 다 연결돼 있다. 코사인이 같으면 늦게 시작한 쪽이 먼저다.
    assert _candidates(story, "target", target.embedding, {story["b"]}) == [
        story["other"],
        story["root"],
    ]
    # 기업 없는 대상은 기업 없는 클러스터만 후보로 보고, 하한과 lookback 도 지킨다.
    assert _candidates(story, "target", _unit(1), set(), min_score=0.75) == [story["plain"]]
    assert _candidates(story, "target", _unit(2), set(), min_score=0.75) == []
    assert (
        _candidates(
            story,
            "target",
            _unit(1),
            set(),
            window_start=target.first_published_at - timedelta(days=5),
        )
        == []
    )

    assert record_link_decision(story["target"], story["root"], 0.9312345678, "follow_up") is True
    row = _row(story["target"])
    assert (row["parent_cluster_id"], row["story_root_id"], row["link_relation"]) == (
        story["root"],
        story["root"],
        "follow_up",
    )
    assert float(row["link_score"]) == pytest.approx(0.931235)
    assert row["linked_at"] is not None
    # 연결 쓰기는 승격·제목 재시도와 Event 스캔이 보는 updated_at 을 건드리지 않는다.
    assert row["updated_at"] == STALE
    # 이미 판정된 클러스터는 다시 쓰지 않는다.
    assert record_link_decision(story["target"], None, None, None) is False
    assert _row(story["target"])["parent_cluster_id"] == story["root"]

    # 같은 시각에 시작한 twin 은 id 가 작은 target 을 후보로 본다(반대는 아니다).
    assert _candidates(story, "twin", _unit(0), {story["a"]}) == [story["target"], story["root"]]
    # story_root_id 는 부모의 루트(부모가 없는 타임라인 첫 이슈)를 물려받는다.
    assert record_link_decision(story["twin"], story["target"], 1.0, "same_event") is True
    assert _row(story["twin"])["story_root_id"] == story["root"]


def test_candidates_are_paged_by_score_start_and_id(story):
    for key in ("root", "other", "target"):
        save_cluster_embedding(story[key], _unit(0))
    for key in ("root", "other"):
        record_link_decision(story[key], None, None, None)
    [target] = [
        t for t in fetch_link_targets(BASE, 10_000, _deadline()) if t.cluster_id == story["target"]
    ]
    params = {
        "cluster_id": target.cluster_id,
        "embedding": target.embedding,
        "first_published_at": target.first_published_at,
        "company_ids": frozenset({story["b"]}),
        "window_start": target.first_published_at - timedelta(days=90),
        "min_score": 0.45,
        "limit": 1,
    }

    # 코사인이 같아 늦게 시작한 other 가 첫 페이지, root 가 다음 페이지다.
    [first] = fetch_link_candidates(**params)
    assert first.cluster_id == story["other"]
    assert first.score == pytest.approx(1.0)
    [second] = fetch_link_candidates(**params, after=first)
    assert second.cluster_id == story["root"]
    assert fetch_link_candidates(**params, after=second) == []


def test_relink_tail_and_rewrite_only_linked(story):
    for key in ("root", "plain", "other", "target", "twin"):
        save_cluster_embedding(story[key], _unit(0))
    for key in ("root", "plain", "other"):
        record_link_decision(story[key], None, None, None)
    record_link_decision(story["target"], story["root"], 0.9, "follow_up")
    record_link_decision(story["twin"], story["target"], 1.0, "same_event")

    # 판정된 클러스터 중 root(BASE 시작) 뒤에 시작한 것을 오래된 순으로 읽고, 현재 부모를 함께
    # 돌려준다.
    tail = fetch_relink_tail(BASE, story["root"], BASE - timedelta(days=1))
    assert [(t.cluster_id, t.parent_id) for t in tail] == [
        (story["plain"], None),
        (story["other"], None),
        (story["target"], story["root"]),
        (story["twin"], story["target"]),
    ]
    assert all(t.embedding is not None and t.summary is None for t in tail)
    # relink_since 보다 먼저 시작한 클러스터는 빠진다. 시작 시각이 같으면 id 가 큰 쪽만 정렬상 뒤다.
    assert [
        t.cluster_id for t in fetch_relink_tail(BASE + timedelta(days=9), story["target"], BASE)
    ] == [story["twin"]]
    assert [
        t.cluster_id for t in fetch_relink_tail(BASE, story["root"], BASE + timedelta(days=4))
    ] == [story["other"], story["target"], story["twin"]]

    # 재판정은 판정된 클러스터를 덮어쓰고, story_root_id 는 새 부모에게서 물려받는다.
    assert record_link_decision(story["target"], story["other"], 0.8, "follow_up", relink=True)
    assert record_link_decision(story["twin"], story["target"], 1.0, "same_event", relink=True)
    target_row, twin_row = _row(story["target"]), _row(story["twin"])
    assert (target_row["parent_cluster_id"], target_row["story_root_id"]) == (
        story["other"],
        story["other"],
    )
    assert twin_row["story_root_id"] == story["other"]
    assert target_row["updated_at"] == STALE
    # 판정 전(또는 초기화된) 클러스터는 재판정이 되살리지 않는다.
    assert record_link_decision(story["stale"], None, None, None, relink=True) is False
    assert _row(story["stale"])["linked_at"] is None


def test_link_relation_is_checked(story):
    with pytest.raises(IntegrityError):
        record_link_decision(story["root"], None, None, "sequel")


def test_reset_links_clears_only_clusters_from_since(story):
    for key in ("root", "plain", "other", "target"):
        save_cluster_embedding(story[key], _unit(0))
    for key in ("root", "plain", "other"):
        record_link_decision(story[key], None, None, None)
    record_link_decision(story["target"], story["root"], 1.0, "follow_up")
    since = BASE + timedelta(days=3)

    assert count_resettable_clusters(since) == {"clusters": 2, "linked": 2}
    assert reset_cluster_links(since) == 2

    # 3/1 root·3/3 plain 은 그대로 남고, 3/5 other·3/10 target 은 임베딩·판정이 모두 지워진다.
    for key in ("root", "plain"):
        row = _row(story[key])
        assert not row["no_embedding"] and row["linked_at"] is not None
        assert row["story_root_id"] == story[key]
    for key in ("other", "target"):
        row = _row(story[key])
        assert row["no_embedding"]
        assert (
            row["parent_cluster_id"],
            row["story_root_id"],
            row["link_score"],
            row["link_relation"],
            row["linked_at"],
        ) == (None, None, None, None, None)
        assert row["updated_at"] == STALE
    assert count_resettable_clusters(since) == {"clusters": 0, "linked": 0}
