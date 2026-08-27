"""트리플 → 뉴스-기업(news_companies) 연결 해석.

삼중항의 엔드포인트는 전부 gazetteer로 정규화된 기업명이다. 기업명→ticker 해석
(references/graph.py)은 원장(relation_sources)의 code 백필과 공유해야 하므로 job이
한 번 조회해 넘긴다. 이 모듈은 그 매핑을 companies.id로 해석한다.
비상장사·미시드 기업은 매핑에 없어 연결에서 제외된다.
"""

from __future__ import annotations

from pipelines.triples.edges import edge_of
from pipelines.triples.models import Triplet
from pipelines.triples.references.rdb import fetch_company_ids_by_tickers


def collect_company_names(triplets: list[Triplet]) -> list[str]:
    """유효 엣지의 subject/object 기업명을 중복 제거·정렬해 돌려준다."""

    names: set[str] = set()

    for triplet in triplets:
        edge = edge_of(triplet)
        if edge is None:
            continue
        subject_name, _, object_name = edge
        names.add(subject_name)
        names.add(object_name)

    return sorted(names)


def resolve_company_ids(name_to_ticker: dict[str, str | None]) -> list[int]:
    """기업명→ticker 매핑을 companies.id 목록으로 해석한다 (정렬·중복 제거)."""

    tickers = sorted({ticker for ticker in name_to_ticker.values() if ticker})

    if not tickers:
        return []

    ticker_to_company_id = fetch_company_ids_by_tickers(tickers)

    return sorted(set(ticker_to_company_id.values()))
