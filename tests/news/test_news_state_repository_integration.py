"""news 상태 컬럼(triple_extracted) 규약 통합 테스트.

NULL=미시도(추출 대상), TRUE=삼중항 있음, FALSE=시도했으나 없음.

로컬 DB 필요: docker compose up -d db 후 0000 베이스라인 적용 상태.
"""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy import text

from pipelines.common.clients.postgres import session_scope
from pipelines.news.loaders.postgres import (
    fetch_unprocessed_triple_news_items,
    fetch_unsummarized_news_items,
    mark_triple_extraction_result,
)

pytestmark = pytest.mark.integration


@pytest.fixture
def news_row():
    marker = uuid.uuid4().hex
    with session_scope() as session:
        news_id = session.execute(
            text(
                """
                INSERT INTO news (title, text, link)
                VALUES (:title, '본문 내용입니다.', :link)
                RETURNING id;
                """
            ),
            {"title": f"통합테스트 {marker}", "link": f"https://example.com/{marker}"},
        ).scalar_one()

    yield int(news_id)

    with session_scope() as session:
        session.execute(text("DELETE FROM news WHERE id = :id"), {"id": news_id})


def test_new_row_is_fetched_as_unprocessed(news_row):
    ids = [item["news_id"] for item in fetch_unprocessed_triple_news_items(limit=10000)]
    assert news_row in ids


def test_mark_with_triples_sets_true(news_row):
    mark_triple_extraction_result([news_row], [])

    # TRUE는 "시도 완료"를 겸하므로 추출 대상에서 빠진다
    ids = [item["news_id"] for item in fetch_unprocessed_triple_news_items(limit=10000)]
    assert news_row not in ids

    with session_scope() as session:
        triple_extracted = session.execute(
            text("SELECT triple_extracted FROM news WHERE id = :id"),
            {"id": news_row},
        ).scalar_one()
    assert triple_extracted is True


def test_mark_without_triples_sets_false(news_row):
    mark_triple_extraction_result([], [news_row])

    # FALSE도 "시도 완료"라 추출 대상에서 빠진다 — 미시도(NULL)와 구분되는 지점
    ids = [item["news_id"] for item in fetch_unprocessed_triple_news_items(limit=10000)]
    assert news_row not in ids

    with session_scope() as session:
        triple_extracted = session.execute(
            text("SELECT triple_extracted FROM news WHERE id = :id"),
            {"id": news_row},
        ).scalar_one()
    assert triple_extracted is False


def test_only_triple_extracted_news_is_summarize_target(news_row):
    # 미처리 상태에서는 요약 대상이 아니다
    assert news_row not in [i["_news_id"] for i in fetch_unsummarized_news_items(limit=10000)]

    # 삼중항 없음 → 여전히 대상 아님
    mark_triple_extraction_result([], [news_row])
    assert news_row not in [i["_news_id"] for i in fetch_unsummarized_news_items(limit=10000)]

    # 삼중항 있음 → 대상
    mark_triple_extraction_result([news_row], [])
    assert news_row in [i["_news_id"] for i in fetch_unsummarized_news_items(limit=10000)]


def test_assign_cluster_representatives():
    from pipelines.news.loaders.postgres import assign_cluster_representatives

    marker = uuid.uuid4().hex
    with session_scope() as session:
        ids = [
            session.execute(
                text(
                    """
                    INSERT INTO news (title, text, link)
                    VALUES (:title, '본문', :link)
                    RETURNING id;
                    """
                ),
                {"title": f"클러스터 {marker} {i}", "link": f"https://example.com/c/{marker}/{i}"},
            ).scalar_one()
            for i in range(3)
        ]

    try:
        # 그룹1: [대표=ids[0], 멤버=ids[1]], 그룹2: 단독 [ids[2]]
        updated = assign_cluster_representatives([[ids[0], ids[1]], [ids[2]]])
        assert updated == 3

        with session_scope() as session:
            rows = dict(
                session.execute(
                    text("SELECT id, cluster_rep_news_id FROM news WHERE id = ANY(:ids)"),
                    {"ids": ids},
                ).fetchall()
            )
        assert rows[ids[0]] == ids[0]  # 대표는 자기 자신
        assert rows[ids[1]] == ids[0]  # 멤버는 대표를 가리킴
        assert rows[ids[2]] == ids[2]  # 단독 기사도 자기 자신
    finally:
        with session_scope() as session:
            session.execute(text("DELETE FROM news WHERE id = ANY(:ids)"), {"ids": ids})
