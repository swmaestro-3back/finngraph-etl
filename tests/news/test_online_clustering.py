"""온라인 클러스터 판정(online.py) 단위 테스트 (DB/외부 인프라 불필요).

문서는 손으로 만든 (토큰, 가중치) 목록이다. IdfTable() 은 모든 토큰의 IDF 가 1 이라 유사도를
손으로 따져 볼 수 있다.
"""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from pipelines.news.transformers.clustering.online import (
    ClusterSeed,
    assign_online,
    profile_vector,
    sum_terms,
    top_keywords,
)
from pipelines.news.transformers.clustering.vectorize import IdfTable
from pipelines.news.utils.date_utils import SEOUL_TIMEZONE

IDF = IdfTable()

SUPPLY = [("엘앤에프", 3.0), ("양극재", 1.0), ("공급", 1.0)]
EARNINGS = [("삼성전자", 3.0), ("실적", 1.0)]


def _at(day: int, hour: int = 9) -> datetime:
    return datetime(2026, 9, day, hour, tzinfo=SEOUL_TIMEZONE)


def _seed(terms=SUPPLY, **overrides) -> ClusterSeed:
    fields = {
        "cluster_id": 1,
        "term_weights": sum_terms(terms),
        "member_count": 1,
        "promoted": False,
        "first_published_at": _at(1),
    }
    fields.update(overrides)
    return ClusterSeed(**fields)


def _assign(documents, published_ats, seeds=(), **overrides):
    options = {"threshold": 0.35, "promote_size": 10, "window_days": 7, "backward_days": 1}
    options.update(overrides)
    return assign_online(documents, published_ats, list(seeds), IDF, **options)


# ── 헬퍼 ────────────────────────────────────────────────────────────────────


def test_sum_terms_and_top_keywords():
    summed = sum_terms([("삼성전자", 2.0), ("실적", 1.0)], [("삼성전자", 1.0)])

    assert summed == {"삼성전자": 3.0, "실적": 1.0}
    # 가중치가 같으면 토큰 사전순
    assert top_keywords({"b": 1.0, "a": 1.0, "c": 5.0}, 2) == ["c", "a"]


def test_profile_vector_scales_profile_to_average_article():
    # 같은 기사 4건의 합은 기사 1건과 같은 방향이어야 한다
    one = profile_vector(sum_terms(SUPPLY), 1, IDF)
    four = profile_vector(sum_terms(SUPPLY, SUPPLY, SUPPLY, SUPPLY), 4, IDF)

    assert four == pytest.approx(one)
    assert profile_vector({}, 0, IDF) == {}


# ── 편입 / 신규 ──────────────────────────────────────────────────────────────


def test_empty_input_returns_nothing():
    assert _assign([], []) == []


def test_length_mismatch_raises():
    with pytest.raises(ValueError):
        _assign([SUPPLY], [])


def test_similar_documents_form_one_cluster_and_others_get_their_own():
    assignments = _assign([SUPPLY, SUPPLY, EARNINGS], [_at(2), _at(2, 10), _at(2, 11)])

    assert len(assignments) == 2
    supply, earnings = assignments
    assert supply.seed is None
    assert (supply.candidates, supply.followers) == ([0, 1], [])
    assert supply.term_weights == {"엘앤에프": 6.0, "양극재": 2.0, "공급": 2.0}
    assert (supply.first_published_at, supply.last_published_at) == (_at(2), _at(2, 10))
    assert (earnings.candidates, earnings.term_weights) == ([2], {"삼성전자": 3.0, "실적": 1.0})


def test_document_joins_matching_seed_and_keeps_seed_anchor():
    seed = _seed()

    [assignment] = _assign([SUPPLY], [_at(2)], [seed])

    assert assignment.seed is seed
    assert (assignment.candidates, assignment.followers) == ([0], [])
    assert assignment.term_weights == {"엘앤에프": 6.0, "양극재": 2.0, "공급": 2.0}
    # 기준점은 시드의 것 그대로다
    assert assignment.first_published_at == _at(1)
    assert assignment.last_published_at == _at(2)
    # 시드 객체는 바뀌지 않는다
    assert seed.term_weights == sum_terms(SUPPLY)


def test_seed_without_new_documents_is_omitted():
    assignments = _assign([EARNINGS], [_at(2)], [_seed()])

    assert [assignment.seed for assignment in assignments] == [None]


def test_document_joins_the_most_similar_cluster():
    exact = _seed(cluster_id=1)
    partial = _seed(SUPPLY + [("lfp", 3.0), ("수주", 3.0)], cluster_id=2)

    [assignment] = _assign([SUPPLY], [_at(2)], [partial, exact])

    assert assignment.seed is exact


def test_later_document_sees_profile_updated_by_earlier_one():
    first = [("엘앤에프", 3.0), ("양극재", 1.0)]
    second = [("엘앤에프", 3.0), ("양극재", 1.0), ("삼성sdi", 3.0)]
    third = [("삼성sdi", 3.0), ("공급", 1.0)]

    # third 는 first 하고만 있으면 겹치는 토큰이 없어 따로 간다
    assert len(_assign([first, third], [_at(2), _at(3)])) == 2
    # second 가 프로필에 삼성sdi 를 더한 뒤에는 같은 클러스터에 붙는다
    [assignment] = _assign([first, second, third], [_at(2), _at(2, 10), _at(3)])
    assert assignment.candidates == [0, 1, 2]


def test_documents_are_processed_in_published_order():
    assignments = _assign([SUPPLY, SUPPLY], [_at(3), _at(2)])

    [assignment] = assignments
    assert assignment.candidates == [1, 0]
    assert assignment.first_published_at == _at(2)


def test_documents_without_tokens_never_match():
    assignments = _assign([[], [], SUPPLY], [_at(2), _at(2, 10), _at(2, 11)])

    assert [assignment.candidates for assignment in assignments] == [[0], [1], [2]]
    assert assignments[0].term_weights == {}


# ── 승격된 클러스터 / 후보 상한 ──────────────────────────────────────────────


def test_promoted_seed_only_counts_and_keeps_profile():
    seed = _seed(member_count=10, promoted=True)

    [assignment] = _assign([SUPPLY], [_at(2)], [seed])

    assert (assignment.candidates, assignment.followers) == ([], [0])
    assert assignment.term_weights == seed.term_weights


def test_full_seed_without_representative_only_counts():
    # 후보가 다 찼는데 크롤링 실패로 대표가 없는 클러스터 — 후보를 더 받지 않는다
    seed = _seed(sum_terms(*[SUPPLY] * 10).items(), member_count=10, promoted=False)

    [assignment] = _assign([SUPPLY], [_at(2)], [seed])

    assert (assignment.candidates, assignment.followers) == ([], [0])


def test_candidates_stop_at_promote_size_within_one_run():
    documents = [SUPPLY] * 12
    published_ats = [_at(2) + timedelta(minutes=minute) for minute in range(12)]

    [assignment] = _assign(documents, published_ats)

    assert assignment.candidates == list(range(10))
    assert assignment.followers == [10, 11]
    assert assignment.term_weights["엘앤에프"] == 30.0


# ── 시간 창 ──────────────────────────────────────────────────────────────────


def test_document_joins_within_window_after_cluster_start():
    seed = _seed(first_published_at=_at(1))

    assert _assign([SUPPLY], [_at(8)], [seed])[0].seed is seed  # 정확히 7일 뒤
    assert _assign([SUPPLY], [_at(8, 10)], [seed])[0].seed is None  # 7일 + 1시간


def test_document_published_shortly_before_cluster_start_joins():
    seed = _seed(first_published_at=_at(5))

    [joined] = _assign([SUPPLY], [_at(4)], [seed])  # 정확히 1일 전
    assert joined.seed is seed
    assert joined.first_published_at == _at(5)  # 기준점은 앞으로 옮겨지지 않는다

    assert _assign([SUPPLY], [_at(4, 8)], [seed])[0].seed is None  # 25시간 전


def test_new_cluster_window_starts_at_its_first_document():
    # 2일에 시작한 클러스터는 9일까지만 자란다. 10일 기사는 새 클러스터다.
    assignments = _assign([SUPPLY, SUPPLY, SUPPLY], [_at(2), _at(9), _at(10)])

    assert [assignment.candidates for assignment in assignments] == [[0, 1], [2]]
