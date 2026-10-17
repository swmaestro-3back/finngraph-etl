"""대표 기사 선정 단위 테스트 (DB/외부 인프라 불필요, Kiwi 토큰화는 실제로 돈다)."""

from __future__ import annotations

import pytest

from pipelines.news.transformers.clustering.representative import pick_representative
from pipelines.news.transformers.clustering.vectorize import IdfTable

IDF = IdfTable()
TITLE = "엘앤에프, 삼성SDI에 양극재 공급 계약"

SUPPLY = "엘앤에프가 삼성SDI와 양극재 공급 계약을 체결했다. 계약 규모는 3조원이다. " * 6
MARKET = "코스피가 외국인 매도에 하락 마감했다. 반도체 업종이 약세를 보였다. " * 6


def test_no_body_returns_none():
    assert pick_representative([TITLE, TITLE], ["", "   "], IDF, min_chars=200) is None


def test_length_mismatch_raises():
    with pytest.raises(ValueError):
        pick_representative([TITLE], [], IDF, min_chars=200)


def test_single_candidate_with_body_is_representative():
    assert pick_representative([TITLE, TITLE], ["", SUPPLY], IDF, min_chars=200) == 1


def test_picks_the_candidate_closest_to_the_others():
    # 같은 사건을 다룬 두 본문이 서로 가깝다. 동떨어진 본문은 대표가 되지 않는다.
    picked = pick_representative(
        ["코스피 하락 마감", TITLE, TITLE], [MARKET, SUPPLY, SUPPLY], IDF, min_chars=200
    )

    # 두 본문이 같아 동점이면 앞선(먼저 발행된) 후보다
    assert picked == 1


def test_short_body_is_skipped_when_a_long_one_exists():
    assert pick_representative([TITLE, TITLE], ["짧은 본문", SUPPLY], IDF, min_chars=200) == 1


def test_short_bodies_are_used_when_nothing_is_long_enough():
    # 전부 짧으면 대표 없이 두는 것보다 짧은 본문이라도 고른다
    assert pick_representative([TITLE, TITLE], ["", "짧은 본문 하나"], IDF, min_chars=200) == 1
