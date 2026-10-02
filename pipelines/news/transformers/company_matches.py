"""제목 × 기업 개체 사전 — 검색 종목 언급 확인과 판정할 기업 목록 (LLM 없음).

제목만 본다. 제목에 나온 기업은 대개 기사의 주인공이라, 스니펫까지 보는 것보다 판정 대상의
오탐이 적다. 스니펫에만 검색 종목이 나오는 기사는 버린다.

검색 종목 언급은 문자열이 아니라 기업으로 본다 — 매치 중 검색 종목과 같은 company_id 가 있어야
남긴다. 약칭('LG엔솔')으로만 적힌 제목도 통과하고, 다른 상장사 이름의 일부('SK하이닉스' 안의
'SK')는 최장 일치라 언급으로 치지 않는다.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from pipelines.common.gazetteer import CompanyMatcher, get_company_matcher


@dataclass
class TitleMatchResult:
    kept: list[dict[str, Any]] = field(default_factory=list)
    removed: list[dict[str, Any]] = field(default_factory=list)
    companies: int = 0
    max_companies: int = 0


def match_title_companies(
    items: list[dict[str, Any]], matcher: CompanyMatcher | None = None
) -> TitleMatchResult:
    """제목에 검색 종목이 나온 기사만 남기고 `_title_companies` 를 붙인다.

    `_title_companies` 는 검색 종목이 첫 번째이고, 나머지는 제목 등장 순서다. 같은 기업은 처음
    나온 표기 하나로 합친다. 기사가 없으면 사전을 읽지 않는다.
    """

    result = TitleMatchResult()
    if not items:
        return result

    if matcher is None:
        matcher = get_company_matcher()

    for item in items:
        query_company = item.get("_query_company")
        if not query_company:
            result.removed.append(item)
            continue

        query_company_id = int(query_company["company_id"])
        companies: dict[int, dict[str, Any]] = {}
        for match in matcher.extract(item.get("title", "")):
            companies.setdefault(
                match.entry.company_id,
                {
                    "company_id": match.entry.company_id,
                    "name": match.canonical,
                    "surface": match.text,
                },
            )

        query = companies.pop(query_company_id, None)
        if query is None:
            result.removed.append(item)
            continue

        item["_title_companies"] = [query, *companies.values()]
        result.kept.append(item)
        result.companies += len(item["_title_companies"])
        result.max_companies = max(result.max_companies, len(item["_title_companies"]))

    return result
