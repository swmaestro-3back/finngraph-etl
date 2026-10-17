"""별칭 후보 → 개체 사전 행. alias 당 기업 하나만 남긴다.

같은 표기가 여러 기업에 걸리면 매칭 결과가 어느 기업인지 알 수 없다. 우선순위가 가장 높은
기업이 **하나뿐일** 때만 그 기업에 주고, 아니면 사전에서 뺀다.

- CURATED 가 최우선이다. 사람이 고른 표기라 자동 출처보다 믿는다 — '구글'처럼 한 회사의
  두 종목(GOOGL·GOOG)에 다 해당하는 표기는 CURATED 로 한쪽을 지정해야 살아남는다.
- 다음은 정식명(NAME)이다. 어느 기업의 정식명이 다른 기업의 부가 별칭과 겹칠 때 정식명이
  사라지면 그 기업은 자기 이름으로도 매칭되지 않는다.
- 나머지(STOCK_NAME·KIS_MASTER·DART)는 동급이다.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import replace

from pipelines.companies.models import GazetteerAlias

# 한 글자 표기는 본문 어디에나 걸려 매칭의 의미가 없다.
MIN_ALIAS_LENGTH = 2

_PRIORITY = {"CURATED": 0, "NAME": 1}
_DEFAULT_PRIORITY = 2


def _priority(candidate: GazetteerAlias) -> int:
    return _PRIORITY.get(candidate.source, _DEFAULT_PRIORITY)


def build_entries(
    candidates: list[GazetteerAlias],
) -> tuple[list[GazetteerAlias], dict[str, tuple[str, ...]]]:
    """별칭 후보를 alias 당 한 줄로 정리한다.

    Returns:
        tuple[list[GazetteerAlias], dict[str, tuple[str, ...]]]: alias 순으로 정렬된 사전 행과,
            우선순위로도 가르지 못해 뺀 별칭 → 걸린 기업들의 정식명.
    """

    # alias → company_id → 그 기업의 가장 우선순위 높은 후보
    by_alias: dict[str, dict[int, GazetteerAlias]] = defaultdict(dict)
    for candidate in candidates:
        alias = (candidate.alias or "").strip()
        if len(alias) < MIN_ALIAS_LENGTH:
            continue
        if alias != candidate.alias:
            candidate = replace(candidate, alias=alias)

        current = by_alias[alias].get(candidate.company_id)
        if current is None or _priority(candidate) < _priority(current):
            by_alias[alias][candidate.company_id] = candidate

    entries: list[GazetteerAlias] = []
    collisions: dict[str, tuple[str, ...]] = {}
    for alias in sorted(by_alias):
        owners = list(by_alias[alias].values())
        best = min(_priority(owner) for owner in owners)
        winners = [owner for owner in owners if _priority(owner) == best]
        if len(winners) == 1:
            entries.append(winners[0])
            continue
        collisions[alias] = tuple(sorted(owner.canonical_name for owner in owners))

    return entries, collisions
