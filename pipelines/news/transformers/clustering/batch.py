"""클러스터 판정의 입력 변환 — 순수 함수만.

저장된 기사 행(term_weights·published_at)을 판정 문서로 바꾸고 후보 클러스터 조회 하한을 계산한다.
토큰화는 하지 않는다 — vectorize_articles 가 이미 term_weights 를 채웠다. DB 를 만지지 않으므로
transformer 다. 조회와 기록은 repositories/postgres/.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from pipelines.news.transformers.clustering.document import STORED_FIELDS, TermWeights


def row_documents(rows: list[dict[str, Any]]) -> tuple[list[TermWeights], list[datetime]]:
    """저장 행의 term_weights 를 저장 필드 세 개가 모두 있는 사본으로. 빠진 필드는 빈 dict 다."""
    documents = [
        {field: dict((row.get("term_weights") or {}).get(field) or {}) for field in STORED_FIELDS}
        for row in rows
    ]
    published_ats = [row["published_at"] for row in rows]
    return documents, published_ats


def candidate_since(published_ats: list[datetime], candidate_days: float) -> datetime:
    """후보 클러스터 조회 하한 — 배치의 가장 이른 기사 − candidate_days.

    last_published_at 하한(candidate_days)과 first_published_at 하한(max_span_days) 둘 다 이 식이다.
    배치의 가장 이른 기사보다 그만큼 먼저 끝난(또는 시작한) 클러스터는 어느 기사에도 후보가 아니다.
    """
    return min(published_ats) - timedelta(days=candidate_days)
