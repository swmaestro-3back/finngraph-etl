"""승격 저장소 통합 테스트 — 판정 대상·멤버 조회(news.py), 승격·제목·삼중항 대기(news_clusters.py).

실제 Postgres(V14 적용)가 필요하다.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta

import pytest
from sqlalchemy import text

from pipelines.common.clients.postgres import session_scope
from pipelines.news.repositories.postgres.news import (
    count_unclustered_news,
    fetch_cluster_member_articles,
    fetch_unclustered_news,
    fetch_unvectorized_news,
    save_term_weights,
)
from pipelines.news.repositories.postgres.news_clusters import (
    count_pending_representatives,
    fetch_promotable_cluster_ids,
    fetch_untitled_promoted,
    promote_cluster,
    update_cluster_title,
)
from pipelines.news.utils.date_utils import SEOUL_TIMEZONE

pytestmark = pytest.mark.integration

PUBLISHED = datetime(2026, 9, 2, 9, 0, tzinfo=SEOUL_TIMEZONE)
SINCE = datetime(2026, 1, 1, tzinfo=SEOUL_TIMEZONE)
BODY = "엘앤에프가 삼성SDI 에 양극재를 공급하는 계약을 체결했다고 밝혔다. " * 5
WEIGHTS = {"title": {"엘앤에프": 2.0}, "body": {"양극재": 1.0}, "entities": {"7": 1.0}}


def _insert_cluster(session, *, member_count: int) -> int:
    return int(
        session.execute(
            text(
                """
                INSERT INTO news_clusters (
                    member_count, first_published_at, mean_published_at, last_published_at
                )
                VALUES (:member_count, :p, :p, :p)
                RETURNING id;
                """
            ),
            {"member_count": member_count, "p": PUBLISHED},
        ).scalar_one()
    )


def _insert_news(
    session,
    marker: str,
    index: int,
    cluster_id: int | None,
    *,
    weights: dict | None = WEIGHTS,
    body: str | None = BODY,
    published_at: datetime | None = PUBLISHED,
) -> int:
    return int(
        session.execute(
            text(
                """
                INSERT INTO news (
                    title, text, link, originallink, published_at, cluster_id, term_weights
                )
                VALUES (:title, :body, :link, :link, :published_at, :cluster_id, CAST(:w AS jsonb))
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
                "w": json.dumps(weights) if weights is not None else None,
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
    """full: 멤버 3 (승격 기준 2), small: 멤버 1."""
    marker = uuid.uuid4().hex
    with session_scope() as session:
        full = _insert_cluster(session, member_count=3)
        small = _insert_cluster(session, member_count=1)
        first = _insert_news(session, marker, 0, full)
        second = _insert_news(session, marker, 1, full)
        third = _insert_news(session, marker, 2, full, body="짧은 본문")
        lone = _insert_news(session, marker, 3, small)

    yield {
        "full": full,
        "small": small,
        "first": first,
        "second": second,
        "third": third,
        "lone": lone,
        "marker": marker,
    }

    _delete(marker, [full, small])


def _representative_of(cluster_id: int):
    with session_scope() as session:
        return session.execute(
            text("SELECT representative_news_id FROM news_clusters WHERE id = :id"),
            {"id": cluster_id},
        ).scalar_one()


def test_unvectorized_news_have_body_and_company_ids():
    marker = uuid.uuid4().hex
    with session_scope() as session:
        target = _insert_news(session, marker, 0, None, weights=None)
        vectorized = _insert_news(session, marker, 1, None)
        without_body = _insert_news(session, marker, 2, None, weights=None, body=None)
        blank_body = _insert_news(session, marker, 3, None, weights=None, body="   ")

    try:
        rows = {row["_news_id"]: row for row in fetch_unvectorized_news()}

        assert target in rows
        assert rows[target] == {
            "_news_id": target,
            "title": f"승격 {marker} 0",
            "text": BODY,
            "company_ids": [],
        }
        # 이미 벡터화됐거나 본문이 없는(옛 구조) 행은 대상이 아니다
        assert vectorized not in rows and without_body not in rows and blank_body not in rows

        assert save_term_weights([(target, WEIGHTS)]) == {"saved": 1, "failed": 0}
        assert target not in {row["_news_id"] for row in fetch_unvectorized_news()}
    finally:
        _delete(marker, [])


def test_unclustered_news_fall_back_to_collected_at():
    """Review Focus 2: 발행 시각이 없으면 수집 시각(지금)으로 보고, 벡터화 전 기사는 대상이 아니다."""
    marker = uuid.uuid4().hex
    with session_scope() as session:
        before = count_unclustered_news()
        dated = _insert_news(session, marker, 0, None)
        undated = _insert_news(session, marker, 1, None, published_at=None)
        unvectorized = _insert_news(session, marker, 2, None, weights=None)
        _insert_news(session, marker, 3, None, weights=None, body=None)  # 옛 구조, 판정 안 함

    try:
        rows = {row["_news_id"]: row for row in fetch_unclustered_news()}
        order = list(rows)

        assert dated in rows and undated in rows and unvectorized not in rows
        # 백필 DAG 의 발행 판단: 벡터화 전이라도 본문이 있으면 곧 판정될 기사라 센다. 본문 없는 행은 안 센다
        assert count_unclustered_news() == before + 3
        assert rows[dated] == {
            "_news_id": dated,
            "title": f"승격 {marker} 0",
            "term_weights": WEIGHTS,
            "published_at": PUBLISHED,
        }
        assert rows[undated]["published_at"] is not None
        assert order.index(dated) < order.index(undated)
    finally:
        _delete(marker, [])


def test_clustered_news_are_not_unclustered(clusters):
    unclustered = {row["_news_id"] for row in fetch_unclustered_news()}

    assert clusters["first"] not in unclustered
    assert clusters["third"] not in unclustered


def test_member_articles_carry_term_weights_and_body_length_in_published_order(clusters):
    members = fetch_cluster_member_articles([clusters["full"], clusters["small"]])

    # third(−2h) → second(−1h) → first 순
    assert [m["_news_id"] for m in members[clusters["full"]]] == [
        clusters["third"],
        clusters["second"],
        clusters["first"],
    ]
    first_item = members[clusters["full"]][2]
    assert first_item == {
        "_news_id": clusters["first"],
        "title": f"승격 {clusters['marker']} 0",
        "term_weights": WEIGHTS,
        "body_length": len(BODY),
        "published_at": PUBLISHED,
    }
    assert members[clusters["full"]][0]["body_length"] == len("짧은 본문")
    assert [m["_news_id"] for m in members[clusters["small"]]] == [clusters["lone"]]
    assert fetch_cluster_member_articles([]) == {}


def test_promotable_clusters_have_enough_members_and_no_representative(clusters):
    promotable = fetch_promotable_cluster_ids(2, SINCE)

    assert clusters["full"] in promotable
    assert clusters["small"] not in promotable
    # 조회 범위 밖(오래 갱신되지 않은 클러스터)은 다시 시도하지 않는다
    assert clusters["full"] not in fetch_promotable_cluster_ids(
        2, datetime.now(SEOUL_TIMEZONE) + timedelta(days=1)
    )


def test_promote_sets_representative_once(clusters):
    assert promote_cluster(clusters["full"], clusters["first"]) is True
    assert _representative_of(clusters["full"]) == clusters["first"]
    assert clusters["full"] not in fetch_promotable_cluster_ids(2, SINCE)

    # 이미 대표가 있으면 덮어쓰지 않는다
    assert promote_cluster(clusters["full"], clusters["second"]) is False
    assert _representative_of(clusters["full"]) == clusters["first"]


def test_promote_with_news_outside_the_cluster_raises(clusters):
    with pytest.raises(ValueError):
        promote_cluster(clusters["full"], clusters["lone"])

    assert _representative_of(clusters["full"]) is None


def test_untitled_promoted_lists_cluster_until_titled(clusters):
    assert clusters["full"] not in fetch_untitled_promoted(2)  # 아직 대표가 없다

    promote_cluster(clusters["full"], clusters["first"])
    promote_cluster(clusters["small"], clusters["lone"])  # 기준 미만은 빠진다

    untitled = fetch_untitled_promoted(2)
    assert clusters["full"] in untitled
    assert clusters["small"] not in untitled

    update_cluster_title(clusters["full"], "엘앤에프 삼성SDI 양극재 공급계약")
    assert clusters["full"] not in fetch_untitled_promoted(2)


def test_pending_representatives_count_only_unextracted_representatives(clusters):
    before = count_pending_representatives(2)

    promote_cluster(clusters["full"], clusters["first"])
    promote_cluster(clusters["small"], clusters["lone"])  # 기준 미만은 세지 않는다
    assert count_pending_representatives(2) == before + 1

    with session_scope() as session:
        session.execute(
            text("UPDATE news SET triple_extracted = FALSE WHERE id = :id"),
            {"id": clusters["first"]},
        )
    assert count_pending_representatives(2) == before
