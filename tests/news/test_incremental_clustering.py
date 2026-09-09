"""배치 간 클러스터 판정(incremental.py) 단위 테스트 (DB/외부 인프라 불필요)."""

from __future__ import annotations

from datetime import UTC, date, datetime

import pytest

from pipelines.news.transformers.clustering import (
    ClusterSeed,
    assign_batch,
    build_clusters,
    build_tfidf,
    document_terms,
    merge_term_weights,
    pick_representative,
    sum_terms,
    top_keywords,
)
from pipelines.news.transformers.clustering.incremental import (
    local_date,
    profile_terms,
    select_within_cap,
)
from pipelines.news.utils.date_utils import SEOUL_TIMEZONE

KST = SEOUL_TIMEZONE


def _at(day: int, hour: int = 9) -> datetime:
    return datetime(2026, 9, day, hour, tzinfo=KST)


def _seed(titles: list[str], member_count: int | None = None, **overrides) -> ClusterSeed:
    """제목 목록을 판정한 기존 클러스터. 프로필은 제목 토큰의 합이다."""
    term_weights = sum_terms(*(document_terms(title) for title in titles))
    fields = {
        "cluster_id": 1,
        "term_weights": term_weights,
        "original_size": len(titles),
        "member_count": len(titles) if member_count is None else member_count,
        "last_stored_date": date(2026, 9, 1),
    }
    fields.update(overrides)
    return ClusterSeed(**fields)


# ── 프로필 / 헬퍼 ────────────────────────────────────────────────────────────


def test_profile_terms_scales_to_average_article():
    seed = ClusterSeed(
        cluster_id=1,
        term_weights={"삼성전자": 9.0, "유상증자": 3.0},
        original_size=3,
        member_count=3,
        last_stored_date=None,
    )

    assert dict(profile_terms(seed)) == {"삼성전자": 3.0, "유상증자": 1.0}


def test_profile_terms_empty_profile_is_empty_vector():
    seed = ClusterSeed(
        cluster_id=1, term_weights={}, original_size=2, member_count=2, last_stored_date=None
    )

    assert profile_terms(seed) == []


def test_sum_and_merge_term_weights():
    summed = sum_terms([("a", 1.0), ("b", 2.0)], [("a", 0.5)])
    assert summed == {"a": 1.5, "b": 2.0}

    base = {"a": 1.0}
    merged = merge_term_weights(base, {"a": 1.0, "c": 3.0})
    assert merged == {"a": 2.0, "c": 3.0}
    assert base == {"a": 1.0}  # 원본은 그대로


def test_top_keywords_orders_by_weight_then_token():
    keywords = top_keywords({"b": 2.0, "a": 2.0, "c": 5.0, "d": 0.1}, count=3)

    assert keywords == ["c", "a", "b"]


def test_local_date_uses_kst():
    # UTC 자정 직전 = KST 다음날 아침
    moment = datetime(2026, 9, 1, 23, 30, tzinfo=UTC)

    assert local_date(moment) == date(2026, 9, 2)


def test_pick_representative_single_member():
    assert pick_representative([[("a", 1.0)]]) == (0, 1.0)


def test_pick_representative_chooses_medoid():
    terms = [
        document_terms("삼성전자 대규모 유상증자 확정"),
        document_terms("삼성전자 유상증자 결정"),
        document_terms("삼성전자 유상증자 발표"),
    ]

    index, cohesion = pick_representative(terms)

    similarity = build_tfidf(terms).cosine_similarity()
    [cluster] = build_clusters(similarity, threshold=0.0)
    assert index == cluster.representative
    assert cohesion == pytest.approx(cluster.cohesion)


# ── cap 정책 ─────────────────────────────────────────────────────────────────


def test_select_within_cap_fills_room_by_similarity():
    day = date(2026, 9, 2)
    candidates = [(0, day, 0.4), (1, day, 0.9), (2, day, 0.6)]

    kept, dropped = select_within_cap(candidates, member_count=1, last_stored_date=None, cap=3)

    assert kept == [1, 2]  # 남은 자리 2개를 유사도 높은 순으로
    assert dropped == [0]


def test_select_within_cap_allows_one_extra_per_new_date():
    day2, day3 = date(2026, 9, 2), date(2026, 9, 3)
    candidates = [(0, day2, 0.6), (1, day2, 0.8), (2, day3, 0.5), (3, day3, 0.7)]

    kept, dropped = select_within_cap(
        candidates, member_count=3, last_stored_date=date(2026, 9, 1), cap=3
    )

    assert kept == [1, 3]  # 날짜마다 유사도 최고 1개
    assert dropped == [0, 2]


def test_select_within_cap_same_date_as_last_stored_gets_nothing():
    day = date(2026, 9, 1)

    kept, dropped = select_within_cap(
        [(0, day, 0.9), (1, day, 0.8)], member_count=3, last_stored_date=day, cap=3
    )

    assert kept == []
    assert dropped == [0, 1]


def test_select_within_cap_room_is_filled_by_similarity_across_dates():
    day1, day2 = date(2026, 9, 1), date(2026, 9, 2)
    # 자리 2개: 날짜가 빠르다고 유사도 낮은 기사가 자리를 차지하면 안 된다.
    candidates = [(0, day1, 0.3), (1, day1, 0.5), (2, day2, 0.95)]

    kept, dropped = select_within_cap(
        candidates, member_count=1, last_stored_date=date(2026, 8, 31), cap=3
    )

    assert kept == [2, 1]
    assert dropped == [0]  # 자리를 채운 뒤 마지막 저장일(day2)보다 늦지 않아 예외도 없다


def test_select_within_cap_older_than_last_stored_gets_no_exception():
    kept, dropped = select_within_cap(
        [(0, date(2026, 9, 2), 0.9)], member_count=3, last_stored_date=date(2026, 9, 3), cap=3
    )

    assert kept == []
    assert dropped == [0]


def test_select_within_cap_room_then_exception_across_dates():
    day2, day3 = date(2026, 9, 2), date(2026, 9, 3)
    # 자리 1개: day2 에서 유사도 높은 것이 자리를 채우고, day3 은 날짜 예외로 1개 더
    candidates = [(0, day2, 0.4), (1, day2, 0.9), (2, day3, 0.9), (3, day3, 0.5)]

    kept, dropped = select_within_cap(
        candidates, member_count=2, last_stored_date=date(2026, 9, 1), cap=3
    )

    assert kept == [1, 2]
    assert dropped == [0, 3]


# ── assign_batch ─────────────────────────────────────────────────────────────


def test_assign_batch_without_seeds_caps_each_cluster():
    """시드가 없으면 기존 단일 배치 클러스터링과 같다: 군집당 cap, 대표가 첫 번째."""
    titles = [
        "삼성전자 유상증자 결정",
        "삼성전자 유상증자 발표",
        "삼성전자 유상증자 공시",
        "삼성전자 유상증자 단행",
        "현대차 미국 리콜 확대",
    ]
    documents = [document_terms(title) for title in titles]

    assignments = assign_batch(documents, [_at(2)] * 5, seeds=[], threshold=0.35, cap=3)

    [samsung, hyundai] = assignments
    assert samsung.seed is None and hyundai.seed is None
    assert sorted(samsung.members) == [0, 1, 2, 3]
    assert len(samsung.kept) == 3
    assert len(samsung.dropped) == 1
    assert "삼성전자" in titles[samsung.kept[0]]
    assert hyundai.members == hyundai.kept == [4]
    assert samsung.cohesion is not None and hyundai.cohesion == 1.0
    assert samsung.term_weights == sum_terms(*(documents[i] for i in samsung.members))


def test_assign_batch_first_kept_is_medoid_when_under_cap():
    titles = [
        "삼성전자 대규모 유상증자 확정",
        "삼성전자 유상증자 결정",
        "삼성전자 유상증자 발표",
    ]
    documents = [document_terms(title) for title in titles]

    [assignment] = assign_batch(documents, [_at(2)] * 3, seeds=[], threshold=0.35, cap=3)

    similarity = build_tfidf(documents).cosine_similarity()
    [cluster] = build_clusters(similarity, threshold=0.35)
    assert cluster.representative != 0  # 대표가 입력상 첫 번째가 아님을 전제로 검증한다
    assert assignment.kept[0] == cluster.representative
    assert len(assignment.kept) == 3 and assignment.dropped == []


def test_assign_batch_handles_empty():
    assert assign_batch([], [], seeds=[], threshold=0.35, cap=3) == []


def test_assign_batch_joins_matching_seed_and_creates_new_for_others():
    seed = _seed(["삼성전자 유상증자 결정", "삼성전자 유상증자 발표"], member_count=2)
    documents = [
        document_terms("삼성전자 유상증자 공시"),  # 시드에 합류
        document_terms("현대차 미국 리콜 확대"),  # 새 클러스터
    ]

    assignments = assign_batch(documents, [_at(2), _at(2)], seeds=[seed], threshold=0.35, cap=3)

    joined = next(a for a in assignments if a.seed is seed)
    fresh = next(a for a in assignments if a.seed is None)
    assert joined.members == joined.kept == [0]  # member_count 2 → 자리 1개
    assert joined.term_weights == sum_terms(documents[0])
    assert fresh.members == fresh.kept == [1]


def test_assign_batch_seed_without_new_members_is_omitted():
    seed = _seed(["삼성전자 유상증자 결정"])
    documents = [document_terms("현대차 미국 리콜 확대")]

    [assignment] = assign_batch(documents, [_at(2)], seeds=[seed], threshold=0.35, cap=3)

    assert assignment.seed is None


def test_assign_batch_two_seeds_never_merge_even_if_similar():
    seed_a = _seed(["삼성전자 유상증자 결정"], cluster_id=1)
    seed_b = _seed(["삼성전자 유상증자 발표"], cluster_id=2)
    documents = [document_terms("삼성전자 유상증자 공시")]

    assignments = assign_batch(documents, [_at(2)], seeds=[seed_a, seed_b], threshold=0.35, cap=3)

    assert len(assignments) == 1  # 새 기사는 두 시드 중 하나에만 붙는다
    assert assignments[0].seed in (seed_a, seed_b)


def test_assign_batch_applies_cumulative_cap_with_date_exception():
    seed = _seed(
        ["삼성전자 유상증자 결정", "삼성전자 유상증자 발표", "삼성전자 유상증자 공시"],
        last_stored_date=date(2026, 9, 1),
    )
    documents = [
        document_terms("삼성전자 유상증자 단행"),  # 9/1: cap 이 찼고 같은 날 → 버림
        document_terms("삼성전자 유상증자 마무리"),  # 9/2: 날짜 예외로 1개 저장
        document_terms("삼성전자 유상증자 청약 시작"),  # 9/2: 같은 날 두 번째 → 버림
    ]
    published = [_at(1, 18), _at(2, 9), _at(2, 15)]

    [assignment] = assign_batch(documents, published, seeds=[seed], threshold=0.35, cap=3)

    assert assignment.seed is seed
    assert sorted(assignment.members) == [0, 1, 2]
    assert len(assignment.kept) == 1 and assignment.kept[0] in (1, 2)
    assert len(assignment.dropped) == 2
    assert assignment.first_published_at == _at(1, 18)
    assert assignment.last_published_at == _at(2, 15)


def test_assign_batch_empty_profile_seed_never_matches():
    seed = _seed(["삼성전자 유상증자 결정"], term_weights={})
    documents = [document_terms("삼성전자 유상증자 발표")]

    [assignment] = assign_batch(documents, [_at(2)], seeds=[seed], threshold=0.35, cap=3)

    assert assignment.seed is None


def test_assign_batch_rejects_length_mismatch():
    with pytest.raises(ValueError):
        assign_batch([document_terms("삼성전자")], [], seeds=[], threshold=0.35, cap=3)


# ── 시간 감쇠 ────────────────────────────────────────────────────────────────


def test_time_decay_halves_per_half_life_and_ignores_negative_gap():
    import numpy as np

    from pipelines.news.transformers.clustering.incremental import time_decay

    factors = time_decay(np.array([0.0, 7.0, 14.0, -3.0]), half_life_days=7.0)

    assert factors.tolist() == pytest.approx([1.0, 0.5, 0.25, 1.0])


def test_time_decay_disabled_when_half_life_is_zero():
    import numpy as np

    from pipelines.news.transformers.clustering.incremental import time_decay

    assert time_decay(np.array([0.0, 30.0]), half_life_days=0.0).tolist() == [1.0, 1.0]


def test_assign_batch_decay_prefers_recent_seed_and_blocks_stale_one():
    # 같은 제목의 시드 둘. 최근 시드(9/1)에는 붙고, 오래된 시드(8/1)만 있으면 감쇠 때문에 못 붙는다.
    recent = _seed(["삼성전자 유상증자 결정"], cluster_id=1, last_published_at=_at(1))
    stale = _seed(
        ["삼성전자 유상증자 결정"],
        cluster_id=2,
        last_published_at=datetime(2026, 8, 1, 9, tzinfo=KST),
    )
    documents = [document_terms("삼성전자 유상증자 발표")]

    [with_both] = assign_batch(
        documents, [_at(2)], seeds=[stale, recent], threshold=0.35, cap=3, decay_half_life_days=7.0
    )
    assert with_both.seed is recent

    [stale_only] = assign_batch(
        documents, [_at(2)], seeds=[stale], threshold=0.35, cap=3, decay_half_life_days=7.0
    )
    assert stale_only.seed is None  # 한 달 경과 → 0.5**(32/7) ≈ 0.04 배

    [no_decay] = assign_batch(documents, [_at(2)], seeds=[stale], threshold=0.35, cap=3)
    assert no_decay.seed is stale  # 감쇠를 끄면 그대로 붙는다


def test_assign_batch_seed_without_last_published_at_is_not_decayed():
    seed = _seed(["삼성전자 유상증자 결정"], last_published_at=None)
    documents = [document_terms("삼성전자 유상증자 발표")]

    [assignment] = assign_batch(
        documents, [_at(30)], seeds=[seed], threshold=0.35, cap=3, decay_half_life_days=1.0
    )

    assert assignment.seed is seed
