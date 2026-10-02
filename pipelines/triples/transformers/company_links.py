"""트리플 → 뉴스-기업(news_companies) 연결.

삼중항의 엔드포인트는 전부 gazetteer 매치라 company_id 를 직접 들고 있다. 유효 엣지의
양 끝 기업을 모으기만 하면 된다.
"""

from __future__ import annotations

from pipelines.triples.edges import edge_of
from pipelines.triples.models import Triplet


def collect_company_ids(triplets: list[Triplet]) -> list[int]:
    """유효 엣지의 subject/object 기업 id를 중복 제거·정렬해 돌려준다."""

    company_ids: set[int] = set()

    for triplet in triplets:
        if edge_of(triplet) is None:
            continue
        company_ids.add(triplet.subject.company_id)
        company_ids.add(triplet.object.company_id)

    return sorted(company_ids)
