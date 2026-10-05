"""제목·본문 × 기업 개체 사전 — 검색 종목 언급 확인과 판정할 기업 목록 (LLM 없음).

제목 매치(match_title_companies)는 제목만 본다. 제목에 나온 기업은 대개 기사의 주인공이라,
스니펫까지 보는 것보다 판정 대상의 오탐이 적다. 스니펫에만 검색 종목이 나오는 기사는 버린다.

검색 종목 언급은 문자열이 아니라 기업으로 본다 — 매치 중 검색 종목과 같은 company_id 가 있어야
남긴다. 약칭('LG엔솔')으로만 적힌 제목도 통과하고, 다른 상장사 이름의 일부('SK하이닉스' 안의
'SK')는 최장 일치라 언급으로 치지 않는다.

본문 매치(match_body_companies)는 관련성 필터와 본문 크롤링 뒤에 돈다. 제목에서 이미 판정받은
기업을 빼고 본문에만 나온 기업을 후보로 붙인다. 걸러내는 것은 LLM 엔티티 필터의 몫이다.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from pipelines.common.gazetteer import CompanyMatch, CompanyMatcher, get_company_matcher

# 제목에서 종목을 나열할 때 쓰는 구분자. 이것만 사이에 둔 기업들을 한 나열로 센다
LISTING_SEPARATOR_PATTERN = re.compile(r"\s*[·ㆍ・,/]\s*")


@dataclass
class TitleMatchResult:
    kept: list[dict[str, Any]] = field(default_factory=list)
    removed: list[dict[str, Any]] = field(default_factory=list)
    companies: int = 0
    max_companies: int = 0


def longest_listing(title: str, matches: list[CompanyMatch]) -> int:
    """제목에서 구분자(가운뎃점·쉼표·빗금)만 사이에 두고 이어진 기업 수의 최댓값.

    "삼성전자·LG전자·셀트리온 등"은 3, "두산밥캣, 3분기 실적"은 1 이다. 같은 기업의 다른 표기는
    한 기업으로 센다. 제목 필터가 이 수로 여러 종목을 모은 기사를 가린다.
    """

    longest = 0
    listed: set[int] = set()
    previous: CompanyMatch | None = None
    for match in matches:
        joined = previous is not None and LISTING_SEPARATOR_PATTERN.fullmatch(
            title[previous.end : match.start]
        )
        if not joined:
            listed = set()
        listed.add(match.entry.company_id)
        longest = max(longest, len(listed))
        previous = match

    return longest


def match_title_companies(
    items: list[dict[str, Any]], matcher: CompanyMatcher | None = None
) -> TitleMatchResult:
    """제목에 검색 종목이 나온 기사만 남기고 `_title_companies` 와 `_title_listing` 을 붙인다.

    `_title_companies` 는 검색 종목이 첫 번째이고, 나머지는 제목 등장 순서다. 같은 기업은 처음
    나온 표기 하나로 합친다. `_title_listing` 은 제목에 나열된 기업 수(longest_listing)다.
    기사가 없으면 사전을 읽지 않는다.
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
        title = item.get("title", "")
        matches = matcher.extract(title)
        companies: dict[int, dict[str, Any]] = {}
        for match in matches:
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
        item["_title_listing"] = longest_listing(title, matches)
        result.kept.append(item)
        result.companies += len(item["_title_companies"])
        result.max_companies = max(result.max_companies, len(item["_title_companies"]))

    return result


def match_body_companies(items: list[dict[str, Any]], matcher: CompanyMatcher | None = None) -> int:
    """본문에만 나온 기업을 `_body_companies` 로 붙인다. 붙인 표기 수의 합을 돌려준다.

    제목에서 판정받은 기업(`_title_companies`, 통과·탈락 모두)과 검색 종목은 뺀다 — 관련성 필터의
    판정이 그 기업들의 최종 판정이다. 표기 기준으로 중복을 제거하므로 같은 기업의 다른 표기는 따로
    남는다(엔티티 필터가 표기마다 판정한다). 본문 등장 순서다. 기사가 없으면 사전을 읽지 않는다.
    """

    if not items:
        return 0

    if matcher is None:
        matcher = get_company_matcher()

    total = 0
    for item in items:
        judged_ids = {int(company["company_id"]) for company in item.get("_title_companies") or []}
        query_company = item.get("_query_company")
        if query_company:
            judged_ids.add(int(query_company["company_id"]))

        seen: set[str] = set()
        companies: list[dict[str, Any]] = []
        for match in matcher.extract(item.get("_text") or ""):
            if match.entry.company_id in judged_ids or match.text in seen:
                continue
            seen.add(match.text)
            companies.append(
                {
                    "company_id": match.entry.company_id,
                    "name": match.canonical,
                    "surface": match.text,
                }
            )

        item["_body_companies"] = companies
        total += len(companies)

    return total
