"""news_cluster_judgments repository 통합 테스트.

실제 Postgres 에 판정 행을 넣고 읽어 본다. V6 마이그레이션이 적용된 로컬 DB 가 필요하다.
"""

from __future__ import annotations

import uuid
from datetime import datetime

import pytest
from sqlalchemy import text

from pipelines.common.clients.postgres import session_scope
from pipelines.news.repositories.news_cluster_judgments import insert_cluster_judgments
from pipelines.news.transformers.clustering import ClusterJudgment
from pipelines.news.utils.date_utils import SEOUL_TIMEZONE

pytestmark = pytest.mark.integration

RUN_AT = datetime(2026, 9, 10, 12, tzinfo=SEOUL_TIMEZONE)
PUBLISHED = datetime(2026, 9, 10, 9, tzinfo=SEOUL_TIMEZONE)


@pytest.fixture
def marker():
    """테스트가 넣은 판정 행을 link 접두어로 지운다."""
    value = uuid.uuid4().hex
    yield value
    with session_scope() as session:
        session.execute(
            text("DELETE FROM news_cluster_judgments WHERE link LIKE :prefix"),
            {"prefix": f"https://example.com/judgment/{value}/%"},
        )


def test_insert_cluster_judgments_writes_every_row(marker):
    judgments = [
        ClusterJudgment(
            run_at=RUN_AT,
            link=f"https://example.com/judgment/{marker}/0",
            title="삼성전자 유상증자 결정",
            description="이사회 결의",
            published_at=PUBLISHED,
            cluster_id=None,
            is_new_cluster=False,
            kept=False,
            seed_similarity=0.4123,
        ),
        ClusterJudgment(
            run_at=RUN_AT,
            link=f"https://example.com/judgment/{marker}/1",
            title="현대차 미국 리콜 확대",
            description=None,
            published_at=PUBLISHED,
            cluster_id=None,
            is_new_cluster=True,
            kept=False,
            seed_similarity=None,
        ),
    ]

    assert insert_cluster_judgments(judgments) == 2
    assert insert_cluster_judgments([]) == 0

    with session_scope() as session:
        rows = session.execute(
            text(
                """
                SELECT link, title, description, published_at, run_at, cluster_id,
                       is_new_cluster, kept, seed_similarity, created_at
                  FROM news_cluster_judgments
                 WHERE link LIKE :prefix
                 ORDER BY link;
                """
            ),
            {"prefix": f"https://example.com/judgment/{marker}/%"},
        ).fetchall()

    assert [(r.title, r.description, r.is_new_cluster, r.kept) for r in rows] == [
        ("삼성전자 유상증자 결정", "이사회 결의", False, False),
        ("현대차 미국 리콜 확대", None, True, False),
    ]
    assert all(r.run_at == RUN_AT and r.published_at == PUBLISHED for r in rows)
    assert all(r.cluster_id is None and r.created_at is not None for r in rows)
    assert float(rows[0].seed_similarity) == pytest.approx(0.4123)
    assert rows[1].seed_similarity is None
