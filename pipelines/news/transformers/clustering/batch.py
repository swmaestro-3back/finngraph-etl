"""클러스터 판정의 입력 변환 — 순수 함수만.

저장된 기사 행을 클러스터링 문서·발행 시각으로 바꾸고, 시드 창을 계산한다. DB 를 만지지
않으므로 transformer 다. 조회와 기록은 repositories/postgres/news_clusters.py.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from pipelines.news.transformers.clustering.online import Terms
from pipelines.news.transformers.clustering.preprocess import document_terms


def row_documents(
    rows: list[dict[str, Any]], lead_chars: int, description_weight: float
) -> tuple[list[Terms], list[datetime]]:
    """저장된 기사 행(title·text·published_at)을 클러스터링 문서와 발행 시각으로 바꾼다.

    문서는 제목 + 본문 앞 lead_chars 자다. 판정이 수집과 다른 DAG 에서 돌아 검색 스니펫을 쓸 수
    없으므로 본문 리드가 그 자리를 대신한다. 발행 시각은 조회 쿼리가 수집 시각으로 메워 준다.
    """

    documents = [
        document_terms(
            row.get("title", ""), (row.get("text") or "")[:lead_chars], description_weight
        )
        for row in rows
    ]
    published_ats = [row["published_at"] for row in rows]
    return documents, published_ats


def seed_window(
    published_ats: list[datetime], window_days: float, backward_days: float = 0
) -> tuple[datetime, datetime]:
    """클러스터 시드 조회 창 [start, end] — first_published_at 이 이 안인 클러스터만 후보다.

    기사는 클러스터 시작 backward_days 전부터 window_days 뒤 사이에 발행된 것만 붙는다. 그래서
    배치의 가장 이른 기사보다 window_days 먼저 시작한 시드부터, 가장 늦은 기사보다
    backward_days 늦게 시작한 시드까지가 붙을 수 있는 전부다.
    """

    return (
        min(published_ats) - timedelta(days=window_days),
        max(published_ats) + timedelta(days=backward_days),
    )
