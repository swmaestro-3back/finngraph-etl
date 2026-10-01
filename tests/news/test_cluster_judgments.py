"""클러스터 판정 기록(judgments.py) 단위 테스트 (DB/외부 인프라 불필요)."""

from __future__ import annotations

from datetime import date, datetime

import pytest

from pipelines.news.transformers.clustering import (
    ClusterAssignment,
    ClusterJudgment,
    ClusterSeed,
    assign_batch,
    build_cluster_judgments,
    document_terms,
    is_stored_member,
    sum_terms,
)
from pipelines.news.utils.date_utils import SEOUL_TIMEZONE

RUN_AT = datetime(2026, 9, 10, 12, tzinfo=SEOUL_TIMEZONE)


def _at(day: int, hour: int = 9) -> datetime:
    return datetime(2026, 9, day, hour, tzinfo=SEOUL_TIMEZONE)


def _item(n: int, news_id: int | None = None, action: str = "inserted", **extra) -> dict:
    """n 번째 기사. news_id 가 있으면 저장된 기사, 없으면 본문 실패 등으로 저장 안 된 기사."""
    item = {"title": f"기사 {n}", "description": f"요약 {n}", "link": f"https://a/{n}", **extra}
    if news_id is not None:
        item["_news_id"] = news_id
        item["_save_action"] = action
    return item


def _seed(cluster_id: int = 7) -> ClusterSeed:
    return ClusterSeed(
        cluster_id=cluster_id,
        term_weights={"삼성전자": 2.0},
        original_size=2,
        member_count=2,
        last_stored_date=date(2026, 9, 1),
        first_published_at=_at(1),
    )


def _assignment(
    members: list[int],
    kept: list[int],
    seed: ClusterSeed | None = None,
    seed_similarities: dict[int, float] | None = None,
) -> ClusterAssignment:
    return ClusterAssignment(
        seed=seed,
        members=members,
        kept=kept,
        dropped=[index for index in members if index not in kept],
        term_weights={},
        first_published_at=_at(2),
        last_published_at=_at(2),
        cohesion=None if seed else 0.8,
        seed_similarities=seed_similarities or {},
    )


def _by_link(judgments: list[ClusterJudgment]) -> dict[str, ClusterJudgment]:
    return {judgment.link: judgment for judgment in judgments}


def test_is_stored_member_requires_new_news_row():
    assert is_stored_member(_item(0, news_id=1))
    assert is_stored_member(_item(0, news_id=1, action="updated"))
    assert not is_stored_member(_item(0))  # 본문 실패 → 저장 안 됨
    assert not is_stored_member(_item(0, news_id=1, action="skipped_existing"))


def test_seed_join_records_kept_and_cap_dropped_with_seed_similarity():
    seed = _seed(cluster_id=7)
    items = [_item(0, news_id=900), _item(1)]
    assignment = _assignment([0, 1], kept=[0], seed=seed, seed_similarities={0: 0.62, 1: 0.41})

    judgments = build_cluster_judgments(items, [assignment], [_at(2), _at(3)], [7], RUN_AT)

    assert judgments == [
        ClusterJudgment(
            run_at=RUN_AT,
            link="https://a/0",
            title="기사 0",
            description="요약 0",
            published_at=_at(2),
            cluster_id=7,
            is_new_cluster=False,
            kept=True,
            seed_similarity=0.62,
        ),
        # cap 탈락도 같은 클러스터로 판정된 기록이 남는다
        ClusterJudgment(
            run_at=RUN_AT,
            link="https://a/1",
            title="기사 1",
            description="요약 1",
            published_at=_at(3),
            cluster_id=7,
            is_new_cluster=False,
            kept=False,
            seed_similarity=0.41,
        ),
    ]


def test_new_cluster_marks_only_stored_kept_articles_as_members():
    # 0: cap 안·저장, 1: cap 안·본문 실패, 2: cap 탈락, 3: cap 안·기존 행 스킵
    items = [
        _item(0, news_id=900),
        _item(1),
        _item(2),
        _item(3, news_id=901, action="skipped_existing"),
    ]
    assignment = _assignment([0, 1, 2, 3], kept=[0, 1, 3])

    judgments = _by_link(build_cluster_judgments(items, [assignment], [_at(2)] * 4, [501], RUN_AT))

    assert {link: j.kept for link, j in judgments.items()} == {
        "https://a/0": True,
        "https://a/1": False,
        "https://a/2": False,
        "https://a/3": False,
    }
    # 본문 실패·cap 탈락도 판정된 클러스터 id 를 그대로 가진다
    assert {j.cluster_id for j in judgments.values()} == {501}
    assert all(j.is_new_cluster and j.seed_similarity is None for j in judgments.values())


def test_new_cluster_not_created_has_no_cluster_id():
    # 저장된 기사가 없어 클러스터를 만들지 않았다
    items = [_item(0), _item(1)]
    assignment = _assignment([0, 1], kept=[0])

    judgments = build_cluster_judgments(items, [assignment], [_at(2)] * 2, [None], RUN_AT)

    assert [(j.cluster_id, j.is_new_cluster, j.kept) for j in judgments] == [
        (None, True, False),
        (None, True, False),
    ]


def test_failed_seed_record_keeps_seed_id_but_no_members():
    items = [_item(0, news_id=900)]
    assignment = _assignment([0], kept=[0], seed=_seed(cluster_id=7), seed_similarities={0: 0.5})

    [judgment] = build_cluster_judgments(items, [assignment], [_at(2)], [None], RUN_AT)

    assert (judgment.cluster_id, judgment.is_new_cluster, judgment.kept) == (7, False, False)


def test_rows_cover_every_member_across_assignments_and_blank_description_is_null():
    seed = _seed(cluster_id=7)
    items = [_item(0, news_id=900), _item(1, news_id=901, description=""), _item(2)]
    assignments = [
        _assignment([0, 2], kept=[0], seed=seed, seed_similarities={0: 0.7, 2: 0.4}),
        _assignment([1], kept=[1]),
    ]

    judgments = _by_link(
        build_cluster_judgments(items, assignments, [_at(2)] * 3, [7, 502], RUN_AT)
    )

    assert sorted(judgments) == ["https://a/0", "https://a/1", "https://a/2"]
    assert (judgments["https://a/1"].cluster_id, judgments["https://a/1"].kept) == (502, True)
    assert judgments["https://a/1"].description is None


def test_length_mismatch_raises():
    items = [_item(0, news_id=900)]
    assignment = _assignment([0], kept=[0])

    with pytest.raises(ValueError):
        build_cluster_judgments(items, [assignment], [_at(2)], [], RUN_AT)
    with pytest.raises(ValueError):
        build_cluster_judgments(items, [assignment], [], [501], RUN_AT)


def test_assign_batch_exposes_seed_similarity_for_joins_only():
    seed = ClusterSeed(
        cluster_id=7,
        term_weights=sum_terms(
            document_terms("삼성전자 유상증자 결정"), document_terms("삼성전자 유상증자 발표")
        ),
        original_size=2,
        member_count=2,
        last_stored_date=date(2026, 9, 1),
        first_published_at=_at(1),
    )
    documents = [
        document_terms("삼성전자 유상증자 공시"),  # 시드에 합류
        document_terms("현대차 미국 리콜 확대"),  # 새 클러스터
    ]

    assignments = assign_batch(documents, [_at(2), _at(2)], seeds=[seed], threshold=0.35, cap=3)

    joined = next(a for a in assignments if a.seed is seed)
    fresh = next(a for a in assignments if a.seed is None)
    assert set(joined.seed_similarities) == {0}
    assert 0.35 <= joined.seed_similarities[0] <= 1.0
    assert fresh.seed_similarities == {}
