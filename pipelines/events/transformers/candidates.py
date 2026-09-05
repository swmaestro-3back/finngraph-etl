"""후보 기업 정규명 추출.

EntityExtractor(triples)를 직접 import 하지 않고 같은 두 메서드를 가진 객체를 받는다 —
gazetteer 가 gitignore 라 CI 에 없기 때문이다. job 이 실제 EntityExtractor 를 넘긴다.

후보는 **원문**에서 뽑는다. 정규화된 텍스트에서 뽑으면 정규명이 별칭 목록에 없는 기업
('알파벳 A': ['구글'])을 놓친다. 정규화된 텍스트는 LLM 이 보는 본문과 후보 목록의 글자를
맞추기 위한 것이다.
"""

from __future__ import annotations

from typing import Any, Protocol


class CompanyExtractor(Protocol):
    def canonicalize(self, text: str) -> str: ...

    def extract(self, text: str) -> list[Any]: ...  # 항목은 .text(정규명)를 가진다


def extract_company_candidates(
    texts: list[str], extractor: CompanyExtractor
) -> tuple[list[str], list[str]]:
    """(정규화된 텍스트 목록, 후보 정규명 목록). 후보는 첫 등장 순으로 중복 제거한다."""

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
