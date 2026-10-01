"""이슈 타임라인 연결 판정 단위 테스트. DB·Bedrock 은 부르지 않는다."""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from pipelines.news.transformers.issue_linker import (
    LinkCandidate,
    LinkDecision,
    build_embedding_text,
    choose_parent,
    to_vector_literal,
)
from pipelines.news.utils.date_utils import SEOUL_TIMEZONE

NOW = datetime(2026, 10, 1, 9, tzinfo=SEOUL_TIMEZONE)
MICRON = 100
SAMSUNG = 200


def _candidate(cluster_id: int, days_ago: int, score: float, *companies: int) -> LinkCandidate:
    return LinkCandidate(
        cluster_id=cluster_id,
        first_published_at=NOW - timedelta(days=days_ago),
        score=score,
        company_ids=frozenset(companies),
    )


def _choose(candidates, *companies, threshold=0.6, no_company_threshold=0.75):
    return choose_parent(
        candidates,
        first_published_at=NOW,
        company_ids=companies,
        threshold=threshold,
        no_company_threshold=no_company_threshold,
    )


def test_build_embedding_text_joins_title_summary_and_recent_titles():
    text = build_embedding_text(
        " 마이크론  실적 ",
        "마이크론 4분기 매출이 전년 대비 46% 늘었어요.",
        ["마이크론 4분기 호실적", "  ", "HBM 매출 급증", "가이던스 상향"],
    )

    # 최신 3개까지, 빈 제목은 뺀다
    assert text == (
        "마이크론 실적\n"
        "마이크론 4분기 매출이 전년 대비 46% 늘었어요.\n"
        "마이크론 4분기 호실적\n"
        "HBM 매출 급증"
    )


def test_build_embedding_text_without_summary_or_titles():
    assert build_embedding_text("미국 건설지출", None, []) == "미국 건설지출"
    assert build_embedding_text("미국 건설지출", "  ", ["8월 건설지출 0.9%↑"]) == (
        "미국 건설지출\n8월 건설지출 0.9%↑"
    )


def test_to_vector_literal_is_pgvector_text():
    assert to_vector_literal([0.5, -1, 2.25e-05]) == "[0.5,-1.0,2.25e-05]"
    assert to_vector_literal([]) == "[]"


def test_choose_parent_empty_candidates_starts_new_story():
    assert _choose([], MICRON) == LinkDecision(parent_id=None, score=None)


def test_choose_parent_picks_max_score_among_company_overlap():
    decision = _choose(
        [
            _candidate(1, 30, 0.70, MICRON),
            _candidate(2, 3, 0.82, MICRON, SAMSUNG),
            _candidate(3, 1, 0.95, SAMSUNG),  # 기업이 안 겹친다
        ],
        MICRON,
    )

    assert decision == LinkDecision(parent_id=2, score=0.82)


def test_choose_parent_threshold_is_inclusive():
    assert _choose([_candidate(1, 5, 0.6, MICRON)], MICRON).parent_id == 1
    assert _choose([_candidate(1, 5, 0.5999, MICRON)], MICRON).parent_id is None


def test_choose_parent_tie_prefers_later_then_larger_id():
    decision = _choose(
        [
            _candidate(1, 30, 0.8, MICRON),
            _candidate(2, 2, 0.8, MICRON),  # 같은 점수 중 가장 늦게 시작 → 바로 앞 노드
            _candidate(3, 10, 0.8, MICRON),
        ],
        MICRON,
    )
    assert decision.parent_id == 2

    same_time = _choose([_candidate(4, 2, 0.8, MICRON), _candidate(5, 2, 0.8, MICRON)], MICRON)
    assert same_time.parent_id == 5


def test_choose_parent_ignores_candidates_not_earlier():
    decision = _choose(
        [
            _candidate(1, 0, 0.99, MICRON),
            _candidate(2, -1, 0.99, MICRON),
            _candidate(3, 4, 0.7, MICRON),
        ],
        MICRON,
    )

    assert decision.parent_id == 3


def test_no_company_cluster_links_only_to_no_company_with_stricter_threshold():
    candidates = [
        _candidate(1, 5, 0.95, MICRON),  # 기업 있는 클러스터에는 잇지 않는다
        _candidate(
            2,
            30,
            0.74,
        ),  # 기업 없음이지만 엄격한 임계값 미달
        _candidate(3, 31, 0.80),
    ]

    assert _choose(candidates) == LinkDecision(parent_id=3, score=0.80)
    assert _choose(candidates[:2]).parent_id is None


def test_company_cluster_never_links_to_no_company_cluster():
    assert _choose([_candidate(1, 5, 0.99)], MICRON).parent_id is None


@pytest.mark.parametrize("score", [0.6, 0.7])
def test_no_company_threshold_does_not_apply_to_company_clusters(score):
    # 기업이 겹치면 일반 임계값(0.6)만 본다
    assert _choose([_candidate(1, 5, score, MICRON)], MICRON).parent_id == 1
