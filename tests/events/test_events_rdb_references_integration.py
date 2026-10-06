"""events RDB 참조 조회 통합 테스트.

실제 Postgres 에 news / news_clusters / companies / news_companies 행을 만들고 조회 함수를 직접
호출한다. 마이그레이션이 적용된 DB 가 필요하다.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta

import pytest
from sqlalchemy import text

from pipelines.common.clients.postgres import session_scope
from pipelines.events.repositories.postgres.news import fetch_cluster_company_ids
from pipelines.events.repositories.postgres.news_clusters import fetch_promoted_clusters
from pipelines.news.utils.date_utils import SEOUL_TIMEZONE

pytestmark = pytest.mark.integration

PUBLISHED = datetime(2026, 9, 2, 9, 0, tzinfo=SEOUL_TIMEZONE)
SINCE = datetime(2026, 1, 1, tzinfo=SEOUL_TIMEZONE)


def _insert_news(session, marker: str, index: int, *, candidate: bool) -> int:
    return int(
        session.execute(
            text(
                """
                INSERT INTO news (title, text, link, published_at, cluster_terms)
                VALUES (:title, '본문', :link, :published_at, CAST(:terms AS jsonb))
                RETURNING id;
                """
            ),
            {
                "title": f"기사 {marker} {index}",
                "link": f"https://example.com/ev/{marker}/{index}",
                "published_at": PUBLISHED,
                "terms": '{"엘앤에프": 2.0}' if candidate else None,
            },
        ).scalar_one()
    )


def _insert_cluster(
    session, news_ids: list[int], *, representative: int | None, title: str | None, members: int
) -> int:
    cluster_id = int(
        session.execute(
            text(
                """
                INSERT INTO news_clusters (
                    representative_news_id, title, original_size, member_count,
                    first_published_at, last_published_at
                )
                VALUES (:representative, :title, :members, :members, :published, :published)
                RETURNING id;
                """
            ),
            {
                "representative": representative,
                "title": title,
                "members": members,
                "published": PUBLISHED,
            },
        ).scalar_one()
    )
    session.execute(
        text("UPDATE news SET cluster_id = :c WHERE id = ANY(:ids)"),
        {"c": cluster_id, "ids": news_ids},
    )
    return cluster_id


def _insert_company(session, name: str) -> int:
    return int(
        session.execute(
            text(
                """
                INSERT INTO companies (name, is_listed, country)
                VALUES (:name, true, 'KR')
                RETURNING id;
                """
            ),
            {"name": name},
        ).scalar_one()
    )


@pytest.fixture
def fixture_rows():
    """promoted: 대표·제목 있고 후보 3건 + 승격 후 기사 1건(승격 기준 3).
    untitled: 제목 없음. collecting: 대표 없음. legacy: 옛 로직의 클러스터(후보 1건)."""
    marker = uuid.uuid4().hex
    with session_scope() as session:
        # news[0..2] 후보, news[3] 승격 후 기사, news[4..6] 다른 클러스터의 후보
        news = [_insert_news(session, marker, i, candidate=i != 3) for i in range(7)]
        promoted = _insert_cluster(
            session, news[0:4], representative=news[0], title="엘앤에프 공급계약", members=3
        )
        untitled = _insert_cluster(
            session, [news[4]], representative=news[4], title=None, members=3
        )
        collecting = _insert_cluster(session, [news[5]], representative=None, title=None, members=3)
        legacy = _insert_cluster(
            session, [news[6]], representative=news[6], title="옛 클러스터", members=1
        )
        a, b, c = (_insert_company(session, f"기업{marker[:8]}{suffix}") for suffix in "ABC")
        # A: 후보 3건, B: 후보 2건, C: 후보 1건 + 승격 후 기사 1건
        links = [
            (news[0], a), (news[1], a), (news[2], a),
            (news[0], b), (news[1], b),
            (news[2], c), (news[3], c),
        ]  # fmt: skip
        for news_id, company_id in links:
            session.execute(
                text("INSERT INTO news_companies (news_id, company_id) VALUES (:n, :c)"),
                {"n": news_id, "c": company_id},
            )

    yield {
        "promoted": promoted,
        "untitled": untitled,
        "collecting": collecting,
        "legacy": legacy,
        "companies": (a, b, c),
    }

    with session_scope() as session:
        session.execute(
            text("DELETE FROM news WHERE link LIKE :p"), {"p": f"https://example.com/ev/{marker}/%"}
        )
        session.execute(
            text("DELETE FROM news_clusters WHERE id = ANY(:ids)"),
            {"ids": [promoted, untitled, collecting, legacy]},
        )
        session.execute(text("DELETE FROM companies WHERE id = ANY(:ids)"), {"ids": [a, b, c]})


def test_fetch_promoted_clusters_needs_representative_title_and_enough_candidates(fixture_rows):
    by_id = {cluster.cluster_id: cluster for cluster in fetch_promoted_clusters(3, SINCE)}

    assert fixture_rows["promoted"] in by_id
    assert by_id[fixture_rows["promoted"]].title == "엘앤에프 공급계약"
    assert by_id[fixture_rows["promoted"]].first_published_at == PUBLISHED
    for excluded in ("untitled", "collecting", "legacy"):
        assert fixture_rows[excluded] not in by_id


def test_fetch_promoted_clusters_respects_scan_range(fixture_rows):
    future = datetime.now(SEOUL_TIMEZONE) + timedelta(days=1)

    assert fixture_rows["promoted"] not in {
        c.cluster_id for c in fetch_promoted_clusters(3, future)
    }


def test_cluster_companies_need_enough_candidate_articles(fixture_rows):
    a, b, c = fixture_rows["companies"]
    cluster_id = fixture_rows["promoted"]

    # 후보 3건 이상: A 만. C 는 승격 후 기사까지 세면 2건이지만 후보로는 1건이다.
    assert fetch_cluster_company_ids([cluster_id], 3) == {cluster_id: [a]}
    # 기준을 낮추면 많은 후보에 나온 기업이 먼저 온다
    assert fetch_cluster_company_ids([cluster_id], 2) == {cluster_id: [a, b]}
    assert fetch_cluster_company_ids([cluster_id], 1) == {cluster_id: [a, b, c]}


def test_cluster_without_qualifying_company_has_no_key(fixture_rows):
    assert fetch_cluster_company_ids([fixture_rows["collecting"]], 1) == {}
    assert fetch_cluster_company_ids([], 3) == {}
