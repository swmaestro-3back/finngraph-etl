"""이슈 연결 투표자가 읽는 이슈 자료형이다. job 이 DB 에서 값을 채운다.

Issue 는 제목, 첫·마지막 기사 시각, 멤버 기사(제목·발행 시각·연결 기업), 연결 기업(news_companies
합집합), 주요 기업(제목에 나온 연결 기업, issue_linker.primary_company_ids), 한 줄 요약
(issue_linker.node_summary), 대표 기사 요약 문단을 담는다. CompanyName 은 기업 하나의
companies.name, ticker, 개체 사전 별칭을 담는다.

멤버는 저장소가 돌려준 순서를 유지한다. 후보 기사(승격 전에 들어온 기사)가 먼저이고, 그 안에서는
발행 시각순이다. 이슈 요약문을 만드는 함수 일부가 이 순서에 기대므로 다시 정렬하지 않는다.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import datetime

from pipelines.common.utils.time import KST


@dataclass(frozen=True)
class Member:
    title: str
    published_at: datetime
    company_ids: frozenset[int] = field(default_factory=frozenset)


@dataclass(frozen=True)
class CompanyName:
    """기업 하나의 이름 정보다. name 은 companies.name, aliases 는 개체 사전 별칭이다."""

    name: str
    ticker: str | None = None
    aliases: tuple[str, ...] = ()


@dataclass(frozen=True)
class Issue:
    id: int
    title: str
    first_published_at: datetime
    last_published_at: datetime
    members: tuple[Member, ...] = ()
    # 연결 기업 id 별 companies.name 이며, id 오름차순으로 둔다.
    companies: dict[int, str] = field(default_factory=dict)
    # 주요 기업 id 를 오름차순으로 둔다.
    primary_company_ids: tuple[int, ...] = ()
    node_summary: str | None = None
    summary: str | None = None

    @property
    def member_count(self) -> int:
        return len(self.members)

    @property
    def linked(self) -> frozenset[int]:
        return frozenset(self.companies)

    @property
    def primary(self) -> frozenset[int]:
        return frozenset(self.primary_company_ids)

    def with_members(self, members: tuple[Member, ...], last: datetime) -> Issue:
        return replace(self, members=members, last_published_at=last)


def collapse(text: str | None) -> str:
    return " ".join((text or "").split())


def kst(value: datetime) -> datetime:
    return value.astimezone(KST)


def fmt_minutes(value: datetime) -> str:
    """KST 기준 'YYYY-MM-DD HH:MM' 문자열로 바꾼다."""

    return kst(value).strftime("%Y-%m-%d %H:%M")


def gap_hours(parent: Issue, child: Issue) -> float:
    return (child.first_published_at - parent.first_published_at).total_seconds() / 3600


def last_seen(issue: Issue) -> datetime:
    """이슈의 마지막 시각이다. last_published_at 과 멤버 발행 시각 중 가장 늦은 값을 쓴다."""

    return max([issue.last_published_at, *(m.published_at for m in issue.members)])


def issue_company_ids(issue: Issue) -> set[int]:
    ids = set(issue.companies)
    for member in issue.members:
        ids.update(member.company_ids)
    return ids


def company_aliases(company_ids, names: dict[int, CompanyName]) -> list[str]:
    """기업들의 이름과 별칭을 긴 것부터(길이가 같으면 사전순) 돌려준다. 텍스트에서 이름을 지울 때
    긴 이름을 먼저 지워야 짧은 별칭이 긴 이름의 일부만 지우지 않는다."""

    aliases: set[str] = set()
    for cid in company_ids:
        entry = names.get(int(cid))
        if not entry:
            continue
        for alias in [entry.name, *entry.aliases]:
            alias = collapse(alias)
            if alias:
                aliases.add(alias)
    return sorted(aliases, key=lambda a: (-len(a), a))
