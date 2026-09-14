"""배치 클러스터링의 입력 변환 — 순수 함수만.

기사 dict 목록을 클러스터링 문서·보도 시각으로 바꾸고, 시드 창의 시작을 계산한다. DB 를
만지지 않으므로 transformer 다. 쓰기(클러스터 기록)는 repositories/news_clusters.py.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any

from pipelines.news.transformers.clustering.incremental import Terms
from pipelines.news.transformers.clustering.preprocess import document_terms
from pipelines.news.utils.date_utils import parse_news_pub_date


def _published_at(pub_date: str) -> datetime | None:
    if not pub_date:
        return None

    parsed = parse_news_pub_date(pub_date)

    if parsed is None:
        logging.warning(f"pubDate 파싱 실패: {pub_date}")

    return parsed


def batch_documents(
    items: list[dict[str, Any]],
    description_weight: float,
    fallback_time: datetime,
) -> tuple[list[Terms], list[datetime]]:
    """기사 목록을 클러스터링 문서와 보도 시각으로 바꾼다.

    보도 시각을 모르는 기사는 fallback_time(실행 시각)으로 본다 — 윈도우와 cap 날짜 판정에
    쓰는 값이라 비워 둘 수 없다. news.published_at 저장값은 저장 시 따로 파싱해 NULL 을
    허용한다.
    """

    documents = [
        document_terms(item.get("title", ""), item.get("description", ""), description_weight)
        for item in items
    ]
    published_ats = [_published_at(item.get("pubDate", "")) or fallback_time for item in items]
    return documents, published_ats


def seed_window(published_ats: list[datetime], window_days: int) -> tuple[datetime, datetime]:
    """클러스터 시드 조회 창 [start, end] — first_published_at 이 이 안인 클러스터만 후보다.

    새 기사는 시드 시작 이후 window_days 안에 발행된 것만 합류하므로, 배치의 가장 이른
    기사보다 window_days 먼저 시작한 시드부터 가장 늦은 기사 시각에 시작한 시드까지가
    붙을 수 있는 전부다. 그 밖의 시드는 읽어도 어느 기사와도 합쳐질 수 없다.
    """

    return min(published_ats) - timedelta(days=window_days), max(published_ats)
