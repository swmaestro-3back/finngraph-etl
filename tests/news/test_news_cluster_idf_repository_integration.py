"""IDF 사전 저장소 통합 테스트. 로컬 DB 의 news_cluster_idf_* 두 표를 실제로 다시 만든다(개발 DB 전용).

실데이터와 섞여도 단언이 깨지지 않게 다른 데이터에 없을 토큰(zz…)을 쓰고 df 는 그 토큰만 본다.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta

import pytest
from sqlalchemy import text

from pipelines.common.clients.postgres import session_scope
from pipelines.news.repositories.postgres.news_cluster_idf import (
    load_idf_tables,
    rebuild_idf_tables,
)
from pipelines.news.transformers.clustering.document import FIELDS
from pipelines.news.utils.date_utils import SEOUL_TIMEZONE

pytestmark = pytest.mark.integration

T0 = datetime(2001, 9, 1, 9, tzinfo=SEOUL_TIMEZONE)


@pytest.fixture
def marker():
    value = uuid.uuid4().hex
    yield value
    with session_scope() as session:
        session.execute(
            text("DELETE FROM news WHERE link LIKE :p"), {"p": f"https://example.com/idf/{value}/%"}
        )


def _news(marker: str, index: int, term_weights: dict | None, published_at=T0) -> int:
    with session_scope() as session:
        return int(
            session.execute(
                text(
                    """
                    INSERT INTO news (title, text, link, published_at, term_weights)
                    VALUES ('제목', '본문', :link, :p, CAST(:w AS jsonb)) RETURNING id;
                    """
                ),
                {
                    "link": f"https://example.com/idf/{marker}/{index}",
                    "p": published_at,
                    "w": json.dumps(term_weights) if term_weights is not None else None,
                },
            ).scalar_one()
        )


def test_rebuild_counts_documents_per_field_and_all_as_union(marker):
    z = marker[:6]
    _news(
        marker,
        1,
        {
            "title": {f"zz가{z}": 1.0, f"zz나{z}": 1.0},
            "body": {f"zz가{z}": 2.0},
            "entities": {"7": 1.0},
        },
    )
    _news(
        marker,
        2,
        {"title": {f"zz가{z}": 1.0}, "body": {f"zz다{z}": 1.0}, "entities": {"7": 1.0, "8": 1.0}},
    )
    _news(marker, 3, {"title": {}, "body": {}, "entities": {}})
    _news(marker, 4, None)  # 벡터화 전 — 코퍼스가 아니다
    _news(  # 기간 밖
        marker,
        5,
        {"title": {f"zz가{z}": 1.0}, "body": {}, "entities": {}},
        published_at=T0 - timedelta(days=30),
    )

    result = rebuild_idf_tables(since=T0 - timedelta(days=1))
    tables = load_idf_tables()

    assert set(tables) == set(FIELDS)
    assert tables["title"].document_frequency[f"zz가{z}"] == 2
    assert tables["title"].document_frequency[f"zz나{z}"] == 1
    assert tables["body"].document_frequency[f"zz가{z}"] == 1
    assert tables["body"].document_frequency[f"zz다{z}"] == 1
    # all 은 title∪body: zz가 는 1번 문서에 두 번 나와도 문서 수 1, 2번 문서까지 2
    assert tables["all"].document_frequency[f"zz가{z}"] == 2
    assert tables["all"].document_frequency[f"zz나{z}"] == 1
    assert tables["all"].document_frequency[f"zz다{z}"] == 1
    assert tables["entities"].document_frequency["7"] >= 2
    # 문서 수는 모든 field 공통이고 토큰이 없는 3번 문서도 센다
    assert tables["title"].article_count == tables["all"].article_count >= 3
    assert result["documents"] == tables["title"].article_count
    assert result["terms"] >= 8


def test_rebuild_replaces_the_table_wholesale(marker):
    z = marker[:6]
    with session_scope() as session:
        session.execute(
            text(
                "INSERT INTO news_cluster_idf_terms (field, token, df) VALUES ('title', :t, 99) "
                "ON CONFLICT DO NOTHING"
            ),
            {"t": f"zz옛토큰{z}"},
        )
    _news(marker, 1, {"title": {f"zz새토큰{z}": 1.0}, "body": {}, "entities": {}})

    rebuild_idf_tables(since=T0 - timedelta(days=1))
    with session_scope() as session:
        first = session.execute(text("SELECT count(*) FROM news_cluster_idf_terms")).scalar_one()
    rebuild_idf_tables(since=T0 - timedelta(days=1))
    with session_scope() as session:
        second = session.execute(text("SELECT count(*) FROM news_cluster_idf_terms")).scalar_one()
        meta = session.execute(
            text("SELECT id, corpus_since FROM news_cluster_idf_meta")
        ).fetchall()

    tables = load_idf_tables()
    assert f"zz옛토큰{z}" not in tables["title"].document_frequency
    assert tables["title"].document_frequency[f"zz새토큰{z}"] == 1
    assert first == second  # 두 번 돌려도 같은 표
    assert len(meta) == 1 and meta[0][1] == T0 - timedelta(days=1)


def test_load_returns_empty_tables_when_meta_missing():
    """Review Focus 3: 표가 없는 최초 런은 네 필드 모두 빈 표다."""
    with session_scope() as session:
        session.execute(text("DELETE FROM news_cluster_idf_terms"))
        session.execute(text("DELETE FROM news_cluster_idf_meta"))

    tables = load_idf_tables()

    assert set(tables) == set(FIELDS)
    assert all(t.article_count == 0 and t.document_frequency == {} for t in tables.values())
    assert tables["title"].idf("아무토큰") == 1.0  # 최초 런: 모든 토큰이 같은 무게
