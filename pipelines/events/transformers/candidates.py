"""
Cluster에서 간선으로 연결될 후보 기업 정규명 추출
gazetteer를 기반으로 flashtext를 활용하여 추출
"""

from __future__ import annotations

from typing import Any, Protocol


class CompanyExtractor(Protocol):
    def canonicalize(self, text: str) -> str: ...

    def extract(self, text: str) -> list[Any]: ...


def extract_company_candidates(
    texts: list[str], extractor: CompanyExtractor
) -> tuple[list[str], list[str]]:
    canonical_texts = [extractor.canonicalize(text) for text in texts]

    candidates: list[str] = []
    seen: set[str] = set()
    for text in texts:
        for entity in extractor.extract(text):
            name = entity.text
            if name in seen:
                continue
            seen.add(name)
            candidates.append(name)

    return canonical_texts, candidates
