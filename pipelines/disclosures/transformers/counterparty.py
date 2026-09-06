"""계약상대 이름을 법인 식별자로 역매칭한다.

공시 원문의 계약상대는 고유번호·종목코드 없는 자유 텍스트라, companies 마스터
(sync_dart_corp_codes 가 DART corpCode.xml 전량을 적재해 둔 것)에 이름으로 되짚는다.

두 규칙이 전부를 결정한다 — **틀린 식별자는 null 보다 나쁘다**, 그리고 "회사가 아니다"는
마스터에 없다는 사실로만 판정한다(공사·공단 같은 상호 접미사로 추측하지 않는다 —
한국도로공사처럼 실재하는 법인을 통째로 막기 때문이다).

과거 3년 표본 기준 매칭률은 corp_code 약 26%, ticker 약 8%다. null 대부분은 해외 법인과
익명 기재('국내 반도체 기업')이지 매처 실패가 아니다.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from typing import Any

from pipelines.disclosures.models import CorpMasterRow

# 법인격 표기와 기호는 회사마다 제각각이라 비교 전에 걷어낸다.
_LEGAL = (
    r"주식회사|\(주\)|㈜|\(유\)|유한회사|\(재\)|\(사\)"
    r"|Co\.,?\s*Ltd\.?|Corp(oration)?\.?|Inc\.?|LLC|Ltd\.?|Company"
)
_SYMBOLS = r"[\s·․.,'\"\-()\[\]]+"

# 괄호 안에 든 법인격 표기. 병기 분리 전에 먼저 떼어낸다.
_LEGAL_PAREN = re.compile(r"\((주|유|재|사)\)|㈜")
_TRAILING_PAREN = re.compile(r"^(.*?)\(([^)]+)\)\s*$")

# 상대를 감춘 기재 — 회사명이 아예 아니므로 조회하지 않는다.
_ANONYMOUS = re.compile(
    r"(국내|해외|국외|북미|미국|유럽|중국|일본|아시아|글로벌)\s*\S{0,8}\s*(기업|업체|회사|고객사|바이어)"
    r"|비공개|영업\s*기밀|공시\s*유보|익명"
)

EMPTY: dict[str, Any] = {
    "counterparty_corp_name": None,
    "counterparty_corp_code": None,
    "counterparty_ticker": None,
    "counterparty_company_id": None,
    "counterparty_match_rule": None,
}


def norm(name: str) -> str:
    """비교 키: 법인격 표기·공백·기호를 걷어내고 소문자로."""

    text = re.sub(_LEGAL, " ", str(name), flags=re.I)
    return re.sub(_SYMBOLS, "", text).lower()


def is_multi_company(name: str) -> bool:
    """한 칸에 여러 회사를 나열한 컨소시엄 기재는 하나의 코드로 갈 수 없다."""

    return name.count(",") >= 1 and len(re.findall(r"주식회사|\(주\)|㈜", name)) >= 2


def looks_anonymous(name: str) -> bool:
    """계약상대를 감춘 기재('국내 반도체 기업')만 True."""

    return bool(_ANONYMOUS.search(name))


def candidates(name: str) -> list[tuple[str, str]]:
    """(rule, 표기) 시도 순서.

    `한국가스공사(KOREA GAS CORPORATION)` 같은 국문·영문 병기는 통째로도, 단순 기호
    제거로도 마스터에 안 걸려서, 괄호를 갈라 두 표기를 각각 후보로 삼는다.
    """

    forms: list[tuple[str, str]] = [("original", name)]

    stripped = _LEGAL_PAREN.sub("", name).strip()
    match = _TRAILING_PAREN.match(stripped)
    if match:
        outer, inner = match.group(1).strip(), match.group(2).strip()
        if outer:
            forms.append(("paren_outer", outer))
        if inner:
            forms.append(("paren_inner", inner))
    return forms


class CorpResolver:
    """이름 -> 법인 식별자. 모호하거나 못 찾으면 전부 None.

    db_aliases 는 company_aliases(source DART·CURATED)에서 온 (별칭, company_id) 쌍이다.
    상장사의 companies.name 은 KIS 종목명("현대차")으로 매일 덮여 DART 법인명
    ("현대자동차")이 마스터에서 사라지고, 사명변경·통용표기(CURATED)는 현행 등록부
    어디에도 없으므로, 별칭으로 보존한 표기를 여기서 되살린다.
    """

    def __init__(
        self,
        rows: Iterable[CorpMasterRow],
        db_aliases: Iterable[tuple[str, int]] = (),
    ) -> None:
        rows = list(rows)
        listed_index: dict[str, list[CorpMasterRow]] = {}
        all_index: dict[str, list[CorpMasterRow]] = {}
        for row in rows:
            all_index.setdefault(norm(row.name), []).append(row)
            if row.ticker:
                listed_index.setdefault(norm(row.name), []).append(row)

        by_company_id = {row.company_id: row for row in rows}
        alias_index: dict[str, list[CorpMasterRow]] = {}
        for alias, company_id in db_aliases:
            row = by_company_id.get(company_id)
            if row is None:  # 마스터에 없는 법인(corp_code 미부여 등)의 별칭은 버린다
                continue
            alias_index.setdefault(norm(alias), []).append(row)

        self.listed = listed_index
        self.all = all_index
        self.db_alias = alias_index
        self._cache: dict[str, dict[str, Any]] = {}

    @staticmethod
    def _unique(rows: list[CorpMasterRow] | None) -> CorpMasterRow | None:
        if not rows:
            return None
        if len({row.corp_code for row in rows}) > 1:
            return None  # 같은 정규화 이름에 다른 법인 -> 추측하지 않는다
        return rows[0]

    def _lookup(self, key: str) -> tuple[str, CorpMasterRow] | None:
        # 모호성은 마스터명과 DB 별칭을 합친 키 공간으로 판정한다. 이름이 여러 법인에
        # 걸리는데 상장사(혹은 마스터명)를 우선하면, 틀린 추측이 자신 있어 보이게 될
        # 뿐이다. 같은 법인을 가리키는 중복은 _unique 가 corp_code 기준으로 허용한다.
        combined = (self.all.get(key) or []) + (self.db_alias.get(key) or [])
        if combined and self._unique(combined) is None:
            return None
        for source in ("listed", "all", "db_alias"):
            hit = self._unique(getattr(self, source).get(key))
            if hit:
                return source, hit
        return None

    def resolve(self, name: str | None) -> dict[str, Any]:
        if not name:
            return dict(EMPTY)
        if name in self._cache:  # 같은 계약상대가 공시마다 되풀이된다
            return dict(self._cache[name])

        result = self._resolve_uncached(name)
        self._cache[name] = result
        return dict(result)

    def _resolve_uncached(self, name: str) -> dict[str, Any]:
        if is_multi_company(name) or looks_anonymous(name):
            return dict(EMPTY)

        for rule, spelling in candidates(name):
            if looks_anonymous(spelling):
                continue
            found = self._lookup(norm(spelling))
            if found is None:
                continue
            source, row = found
            return {
                "counterparty_corp_name": row.name,
                "counterparty_corp_code": row.corp_code,
                "counterparty_ticker": row.ticker,
                "counterparty_company_id": row.company_id,
                "counterparty_match_rule": f"{rule}:{source}",
            }
        return dict(EMPTY)


class CorpMaster:
    """companies 마스터 1회 적재분. 계약상대 역매칭과 제출사 자기 식별을 함께 담당한다.

    job 시작 시 한 번 만들어 전체 공시에 재사용한다 — 공시마다 DB를 오가지 않는다.
    """

    def __init__(
        self,
        rows: Iterable[CorpMasterRow],
        db_aliases: Iterable[tuple[str, int]] = (),
    ) -> None:
        rows = list(rows)
        self.by_corp_code: dict[str, CorpMasterRow] = {row.corp_code: row for row in rows}
        self._resolver = CorpResolver(rows, db_aliases=db_aliases)

    def resolve_counterparty(self, name: str | None) -> dict[str, Any]:
        return self._resolver.resolve(name)

    def resolve_filer(self, corp_code: str) -> CorpMasterRow | None:
        return self.by_corp_code.get(corp_code)
