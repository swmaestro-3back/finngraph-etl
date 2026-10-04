"""삼중항 추출 대상 조회와 news 상태 컬럼(triple_extracted) 규약 통합 테스트.

대상은 클러스터 대표 기사뿐이다. NULL=미시도(추출 대상), TRUE=삼중항 있음, FALSE=시도했으나 없음.

로컬 DB 필요: docker compose up -d db 후 마이그레이션 적용 상태.
"""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy import text

from pipelines.common.clients.postgres import session_scope
from pipelines.triples.repositories.postgres.news import (
    fetch_unprocessed_triple_news_items,
    mark_triple_extraction_result,
)

pytestmark = pytest.mark.integration


def _insert_news(session, marker: str, suffix: str) -> int:
    return int(
        session.execute(
            text(
                """
                INSERT INTO news (title, text, link)
                VALUES (:title, '본문 내용입니다.', :link)
                RETURNING id;
                """
            ),
            {"title": f"통합테스트 {marker}", "link": f"https://example.com/{marker}/{suffix}"},
        ).scalar_one()
    )


@pytest.fixture
def rows():
    """representative: 대표 기사(기업 2개 연결), member: 같은 클러스터의 대표 아닌 기사."""
    marker = uuid.uuid4().hex
    with session_scope() as session:
        representative = _insert_news(session, marker, "rep")
        member = _insert_news(session, marker, "member")
        cluster_id = int(
            session.execute(
                text(
                    """
                    INSERT INTO news_clusters (
                        representative_news_id, original_size, member_count,
                        first_published_at, last_published_at
                    )
                    VALUES (:representative, 2, 2, now(), now())
                    RETURNING id;
                    """
                ),
                {"representative": representative},
            ).scalar_one()
        )
        session.execute(
            text("UPDATE news SET cluster_id = :c WHERE id = ANY(:ids)"),
            {"c": cluster_id, "ids": [representative, member]},
        )
        company_ids = [
            int(
                session.execute(
                    text(
                        """
                        INSERT INTO companies (name, is_listed, country)
                        VALUES (:name, true, 'KR')
                        RETURNING id;
                        """
                    ),
                    {"name": f"기업{marker[:8]}{suffix}"},
                ).scalar_one()
            )
            for suffix in "AB"
        ]
        for news_id in (representative, member):
            for company_id in company_ids:
                session.execute(
                    text("INSERT INTO news_companies (news_id, company_id) VALUES (:n, :c)"),
                    {"n": news_id, "c": company_id},
                )

    yield {"representative": representative, "member": member, "company_ids": sorted(company_ids)}

    with session_scope() as session:
        session.execute(
            text("DELETE FROM news WHERE id = ANY(:ids)"), {"ids": [representative, member]}
        )
        session.execute(text("DELETE FROM news_clusters WHERE id = :id"), {"id": cluster_id})
        session.execute(text("DELETE FROM companies WHERE id = ANY(:ids)"), {"ids": company_ids})


def _targets(promote_size: int = 2) -> dict[int, dict]:
    return {item["news_id"]: item for item in fetch_unprocessed_triple_news_items(promote_size)}


def test_only_cluster_representative_is_fetched_with_its_linked_companies(rows):
    targets = _targets()

    assert rows["representative"] in targets
    # 본문이 있어도 대표가 아니면 추출 대상이 아니다
    assert rows["member"] not in targets
    assert targets[rows["representative"]]["company_ids"] == rows["company_ids"]
    assert targets[rows["representative"]]["text"] == "본문 내용입니다."


def test_representative_of_legacy_small_cluster_is_not_fetched(rows):
    # 옛 로직의 클러스터는 대표가 있어도 후보 수(2)가 승격 기준(3)에 못 미친다
    assert rows["representative"] not in _targets(promote_size=3)


def test_mark_with_triples_sets_true(rows):
    mark_triple_extraction_result([rows["representative"]], [])

    # TRUE는 "시도 완료"를 겸하므로 추출 대상에서 빠진다
    assert rows["representative"] not in _targets()

    with session_scope() as session:
        triple_extracted = session.execute(
            text("SELECT triple_extracted FROM news WHERE id = :id"),
            {"id": rows["representative"]},
        ).scalar_one()
    assert triple_extracted is True


def test_mark_without_triples_sets_false(rows):
    mark_triple_extraction_result([], [rows["representative"]])

    # FALSE도 "시도 완료"라 추출 대상에서 빠진다 — 미시도(NULL)와 구분되는 지점
    assert rows["representative"] not in _targets()

    with session_scope() as session:
        triple_extracted = session.execute(
            text("SELECT triple_extracted FROM news WHERE id = :id"),
            {"id": rows["representative"]},
        ).scalar_one()
    assert triple_extracted is False
