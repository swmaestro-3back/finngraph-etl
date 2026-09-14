"""
제목과 스니펫에서 후보 종목명 추출
"""

from __future__ import annotations

from functools import lru_cache
from typing import Any

from flashtext import KeywordProcessor

from pipelines.triples.ontology.gazetteers.krx_dict import KRX_COMPANY_DICT


@lru_cache
def _processor() -> KeywordProcessor:
    processor = KeywordProcessor(case_sensitive=True)
    processor.add_keywords_from_dict(KRX_COMPANY_DICT)
    return processor


def extract_company_candidates(text: str) -> list[str]:

    names: list[str] = []
    seen: set[str] = set()
    for name in _processor().extract_keywords(text):
        if name in seen:
            continue
        seen.add(name)
        names.append(name)
    return names


def attach_candidate_companies(items: list[dict[str, Any]]) -> None:

    for item in items:
        origin = [company["name"] for company in item.get("_query_companies", [])]
        found = extract_company_candidates(f"{item.get('title', '')} {item.get('description', '')}")
        item["_candidate_companies"] = list(dict.fromkeys(origin + found))
