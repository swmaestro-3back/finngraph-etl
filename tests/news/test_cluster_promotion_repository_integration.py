"""news_clusters repository 통합 테스트 — 미판정 기사·승격 대상·후보 조회, 대표 설정, 제목 대상,
삼중항 미처리 대표 수.

실제 Postgres 에 news / news_clusters 행을 만든다. 마이그레이션이 적용된 DB 가 필요하다.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta

import pytest
from sqlalchemy import text

from pipelines.common.clients.postgres import session_scope
from pipelines.news.repositories.postgres.news_clusters import (
    count_pending_representatives,
    fetch_cluster_candidates,
    fetch_promotable_cluster_ids,
    fetch_unclustered_news,
    fetch_untitled_promoted,
    promote_cluster,
    update_cluster_title,
)
from pipelines.news.utils.date_utils import SEOUL_TIMEZONE

pytestmark = pytest.mark.integration

PUBLISHED = datetime(2026, 9, 2, 9, 0, tzinfo=SEOUL_TIMEZONE)
SINCE = datetime(2026, 1, 1, tzinfo=SEOUL_TIMEZONE)
BODY = "엘앤에프가 삼성SDI 에 양극재를 공급하는 계약을 체결했다고 밝혔다. " * 5


def _insert_cluster(session, *, member_count: int, original_size: int) -> int:
    return int(
        session.execute(
            text(
                """
                INSERT INTO news_clusters (
                    original_size, member_count, first_published_at, last_published_at
                )
                VALUES (:original_size, :member_count, :published, :published)
                RETURNING id;
                """
            ),
            {"original_size": original_size, "member_count": member_count, "published": PUBLISHED},
        ).scalar_one()
    )


def _insert_news(
    session,
    marker: str,
    index: int,
    cluster_id: int | None,
    *,
    candidate: bool = False,
    body: str | None = BODY,
    published_at: datetime | None = PUBLISHED,
) -> int:
    return int(
        session.execute(
            text(
                """
                INSERT INTO news (
                    title, text, link, originallink, published_at, cluster_id, cluster_terms
                )
                VALUES (
                    :title, :body, :link, :link, :published_at, :cluster_id,
                    CAST(:terms AS jsonb)
                )
                RETURNING id;
                """
            ),
            {
                "title": f"승격 {marker} {index}",
                "body": body,
                "link": f"https://example.com/cp/{marker}/{index}",
                # 뒤에 넣은 기사가 더 이르게 발행된 것으로 둔다 — 조회 순서 확인용
                "published_at": published_at - timedelta(hours=index) if published_at else None,
                "cluster_id": cluster_id,
                "terms": '{"엘앤에프": 2.0}' if candidate else None,
            },
        ).scalar_one()
    )


def _delete(marker: str, cluster_ids: list[int]) -> None:
    with session_scope() as session:
        session.execute(
            text("DELETE FROM news WHERE link LIKE :p"), {"p": f"https://example.com/cp/{marker}/%"}
        )
        if cluster_ids:
            session.execute(
                text("DELETE FROM news_clusters WHERE id = ANY(:ids)"), {"ids": cluster_ids}
            )


@pytest.fixture
def clusters():
    """full: 후보 2 + 승격 후 기사 1 (승격 기준 2), small: 후보 1."""
    marker = uuid.uuid4().hex
    with session_scope() as session:
        full = _insert_cluster(session, member_count=2, original_size=3)
        small = _insert_cluster(session, member_count=1, original_size=1)
        first = _insert_news(session, marker, 0, full, candidate=True)
        second = _insert_news(session, marker, 1, full, candidate=True)
        follower = _insert_news(session, marker, 2, full)
        lone = _insert_news(session, marker, 3, small, candidate=True)

    yield {
        "full": full,
        "small": small,
        "first": first,
        "second": second,
        "follower": follower,
        "lone": lone,
        "marker": marker,
    }

    _delete(marker, [full, small])


def _text_of(news_id: int):
    with session_scope() as session:
        return session.execute(
            text("SELECT text FROM news WHERE id = :id"), {"id": news_id}
        ).scalar_one()


def _representative_of(cluster_id: int):
    with session_scope() as session:
        return session.execute(
            text("SELECT representative_news_id FROM news_clusters WHERE id = :id"),
            {"id": cluster_id},
        ).scalar_one()


def test_unclustered_news_have_body_and_fall_back_to_collected_at():
    marker = uuid.uuid4().hex
    with session_scope() as session:
        dated = _insert_news(session, marker, 0, None)
        undated = _insert_news(session, marker, 1, None, published_at=None)
        without_body = _insert_news(session, marker, 2, None, body=None)
        blank_body = _insert_news(session, marker, 3, None, body="   ")

    try:
        rows = {row["_news_id"]: row for row in fetch_unclustered_news()}
        order = list(rows)

        assert dated in rows and undated in rows
        # 본문이 없는 옛 행은 판정 대상이 아니다
        assert without_body not in rows and blank_body not in rows
        assert rows[dated] == {
            "_news_id": dated,
            "title": f"승격 {marker} 0",
            "text": BODY,
            "published_at": PUBLISHED,
        }
        # 발행 시각이 없으면 수집 시각(지금)으로 본다 — 죽지 않고 발행 시각순의 뒤쪽에 선다
        assert rows[undated]["published_at"] is not None
        assert order.index(dated) < order.index(undated)
    finally:
        _delete(marker, [])


def test_clustered_news_are_not_unclustered(clusters):
    unclustered = {row["_news_id"] for row in fetch_unclustered_news()}

    assert clusters["first"] not in unclustered
    assert clusters["follower"] not in unclustered


def test_promotable_clusters_have_enough_candidates_and_no_representative(clusters):
    promotable = fetch_promotable_cluster_ids(2, SINCE)

    assert clusters["full"] in promotable
    assert clusters["small"] not in promotable
    # 조회 범위 밖(오래 갱신되지 않은 클러스터)은 다시 시도하지 않는다
    assert clusters["full"] not in fetch_promotable_cluster_ids(
        2, datetime.now(SEOUL_TIMEZONE) + timedelta(days=1)
    )


def test_candidates_exclude_followers_and_carry_body_in_published_order(clusters):
    candidates = fetch_cluster_candidates([clusters["full"], clusters["small"]])

    # second 가 first 보다 한 시간 먼저 발행됐다
    assert [item["_news_id"] for item in candidates[clusters["full"]]] == [
        clusters["second"],
        clusters["first"],
    ]
    first_item = candidates[clusters["full"]][1]
    assert first_item == {
        "_news_id": clusters["first"],
        "title": f"승격 {clusters['marker']} 0",
        "text": BODY,
        "published_at": PUBLISHED,
    }
    assert [item["_news_id"] for item in candidates[clusters["small"]]] == [clusters["lone"]]
    assert fetch_cluster_candidates([]) == {}


def test_promote_sets_representative_without_touching_bodies(clusters):
    assert promote_cluster(clusters["full"], clusters["first"]) is True

    assert _representative_of(clusters["full"]) == clusters["first"]
    assert _text_of(clusters["first"]) == BODY
    assert _text_of(clusters["second"]) == BODY
    assert clusters["full"] not in fetch_promotable_cluster_ids(2, SINCE)

    # 이미 대표가 있으면 덮어쓰지 않는다
    assert promote_cluster(clusters["full"], clusters["second"]) is False
    assert _representative_of(clusters["full"]) == clusters["first"]


def test_promote_with_news_outside_the_cluster_raises(clusters):
    with pytest.raises(ValueError):
        promote_cluster(clusters["full"], clusters["lone"])

    assert _representative_of(clusters["full"]) is None


def test_untitled_promoted_lists_cluster_until_titled(clusters):
    assert clusters["full"] not in fetch_untitled_promoted(2, SINCE)  # 아직 대표가 없다

    promote_cluster(clusters["full"], clusters["first"])
    # 옛 로직의 클러스터처럼 대표는 있지만 후보 수가 기준에 못 미치면 빠진다
    promote_cluster(clusters["small"], clusters["lone"])

    untitled = fetch_untitled_promoted(2, SINCE)
    assert clusters["full"] in untitled
    assert clusters["small"] not in untitled

    update_cluster_title(clusters["full"], "엘앤에프 삼성SDI 양극재 공급계약")
    assert clusters["full"] not in fetch_untitled_promoted(2, SINCE)


def test_pending_representatives_count_only_unextracted_representatives(clusters):
    before = count_pending_representatives(2)

    promote_cluster(clusters["full"], clusters["first"])
    # 옛 로직의 클러스터처럼 대표는 있지만 후보 수가 기준에 못 미치면 세지 않는다
    promote_cluster(clusters["small"], clusters["lone"])
    # 대표가 아닌 기사(second·follower)는 triple_extracted 가 NULL 이어도 세지 않는다
    assert count_pending_representatives(2) == before + 1

    with session_scope() as session:
        session.execute(
            text("UPDATE news SET triple_extracted = FALSE WHERE id = :id"),
            {"id": clusters["first"]},
        )
    assert count_pending_representatives(2) == before
