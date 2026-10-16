"""대표 기사 선정 단위 테스트 (DB/외부 인프라 불필요). 입력은 저장된 term_weights 다."""

from __future__ import annotations

import pytest

from pipelines.news.transformers.clustering.representative import pick_representative
from pipelines.news.transformers.clustering.vectorize import IdfTable

IDF = IdfTable()
SUPPLY = {
    "title": {"엘앤에프": 3.0, "양극재": 1.0},
    "body": {"양극재": 2.0, "공급": 1.0, "계약": 1.0},
    "entities": {"1": 1.0},
}
MARKET = {
    "title": {"코스피": 1.0},
    "body": {"외국인": 1.0, "매도": 1.0, "하락": 1.0},
    "entities": {},
}
LONG = 300
SHORT = 10


def test_no_body_returns_none():
    assert pick_representative([SUPPLY, SUPPLY], [0, 0], IDF, min_chars=200) is None


def test_length_mismatch_raises():
    with pytest.raises(ValueError):
        pick_representative([SUPPLY], [], IDF, min_chars=200)


def test_single_candidate_with_body_is_representative():
    assert pick_representative([SUPPLY, SUPPLY], [0, LONG], IDF, min_chars=200) == 1


def test_picks_the_candidate_closest_to_the_others():
    # 같은 사건을 다룬 두 문서가 서로 가깝다. 동떨어진 문서는 대표가 되지 않는다.
    picked = pick_representative([MARKET, SUPPLY, SUPPLY], [LONG, LONG, LONG], IDF, min_chars=200)
    # 두 문서가 같아 동점이면 앞선(먼저 발행된) 후보다
    assert picked == 1


def test_uses_title_plus_body_for_similarity():
    # 제목만 겹치는 후보와 본문만 겹치는 후보: all 벡터로 비교하므로 둘 모두와 겹치는 SUPPLY 가 대표다
    title_only = {"title": {"엘앤에프": 3.0, "양극재": 1.0}, "body": {"기타": 1.0}, "entities": {}}
    body_only = {
        "title": {"무관": 1.0},
        "body": {"양극재": 2.0, "공급": 1.0, "계약": 1.0},
        "entities": {},
    }
    picked = pick_representative(
        [title_only, SUPPLY, body_only], [LONG, LONG, LONG], IDF, min_chars=200
    )
    assert picked == 1


def test_short_body_is_skipped_when_a_long_one_exists():
    assert pick_representative([SUPPLY, SUPPLY], [SHORT, LONG], IDF, min_chars=200) == 1


def test_short_bodies_are_used_when_nothing_is_long_enough():
    # 전부 짧으면 대표 없이 두는 것보다 짧은 본문이라도 고른다
    assert pick_representative([SUPPLY, SUPPLY], [0, SHORT], IDF, min_chars=200) == 1
