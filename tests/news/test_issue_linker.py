"""이슈 타임라인 연결 판정을 검증하는 단위 테스트다. DB·Bedrock 은 부르지 않는다."""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from pipelines.common.gazetteer import CompanyMatcher, GazetteerEntry
from pipelines.news.transformers.issue_linker import (
    FOLLOW_UP,
    SAME_EVENT,
    ClusterMember,
    LinkCandidate,
    LinkDecision,
    build_embedding_text,
    choose_parent,
    classify_relation,
    decide_link,
    node_summary,
    primary_company_ids,
    to_vector_literal,
)
from pipelines.news.utils.date_utils import SEOUL_TIMEZONE

NOW = datetime(2026, 10, 1, 9, tzinfo=SEOUL_TIMEZONE)
TARGET_ID = 1000
MICRON = 100
SAMSUNG = 200
SK_HYNIX = 300

MATCHER = CompanyMatcher(
    {
        "마이크론": GazetteerEntry(MICRON, 1, "MU", "마이크론테크놀로지"),
        "삼성전자": GazetteerEntry(SAMSUNG, 2, "005930", "삼성전자"),
        "삼전": GazetteerEntry(SAMSUNG, 2, "005930", "삼성전자"),
        "SK하이닉스": GazetteerEntry(SK_HYNIX, 3, "000660", "SK하이닉스"),
    }
)


def _candidate(
    cluster_id: int, days_ago: float, score: float, *companies: int, hours_ago: float = 0
) -> LinkCandidate:
    return LinkCandidate(
        cluster_id=cluster_id,
        first_published_at=NOW - timedelta(days=days_ago, hours=hours_ago),
        score=score,
        company_ids=frozenset(companies),
    )


def _choose(candidates, *companies, threshold=0.45, no_company_threshold=0.75):
    return choose_parent(
        candidates,
        cluster_id=TARGET_ID,
        first_published_at=NOW,
        company_ids=companies,
        threshold=threshold,
        no_company_threshold=no_company_threshold,
    )


# ── 노드 요약 ────────────────────────────────────────────────────────────────


def test_node_summary_prefers_change_point():
    points = [
        {"kind": "AFFECTED", "text": "메모리 3사가 영향을 받아요."},
        {"kind": "CHANGE", "text": "  마이크론 4분기 매출이  46% 늘었어요. "},
    ]

    assert node_summary(points, "첫 문장이에요. 둘째 문장이에요.") == (
        "마이크론 4분기 매출이 46% 늘었어요."
    )


@pytest.mark.parametrize(
    "points",
    [
        None,
        [],
        [{"kind": "SCALE", "text": "규모는 1조원이에요."}],
        [{"kind": "CHANGE", "text": "   "}],
        # JSONB 를 읽은 값이라 형식을 믿지 않는다.
        "CHANGE",
        [{"kind": "CHANGE"}, "깨진 항목"],
    ],
)
def test_node_summary_falls_back_to_first_sentence(points):
    summary = "마이크론 매출이 7.5% 늘었어요. HBM 이 끌었어요! 다음 분기도 늘어요?"

    assert node_summary(points, summary) == "마이크론 매출이 7.5% 늘었어요."


def test_node_summary_single_sentence_without_terminal_space():
    assert node_summary(None, " 마이크론이 실적을 발표해요. ") == "마이크론이 실적을 발표해요."
    assert node_summary(None, "마침표 없는 요약") == "마침표 없는 요약"


@pytest.mark.parametrize("summary", [None, "", "   "])
def test_node_summary_none_without_points_or_summary(summary):
    assert node_summary([], summary) is None


# ── 임베딩 텍스트 ─────────────────────────────────────────────────────────────


def test_build_embedding_text_joins_title_summary_and_first_titles():
    text = build_embedding_text(
        " 마이크론  실적 ",
        "마이크론 4분기 매출이 전년 대비 46% 늘었어요.",
        ["마이크론 4분기 호실적", "  ", "HBM 매출 급증", "가이던스 상향", "넷째 제목"],
    )

    # 빈 제목은 빼고, 주어진 순서대로 앞 3개까지만 넣는다. 저장소는 후보 기사(승격 전에 들어온
    # 기사)를 먼저, 발행 시각순으로 준다.
    assert text == (
        "마이크론 실적\n"
        "마이크론 4분기 매출이 전년 대비 46% 늘었어요.\n"
        "마이크론 4분기 호실적\n"
        "HBM 매출 급증\n"
        "가이던스 상향"
    )


def test_build_embedding_text_without_summary_or_titles():
    assert build_embedding_text("미국 건설지출", None, []) == "미국 건설지출"
    assert build_embedding_text("미국 건설지출", "  ", ["8월 건설지출 0.9%↑"]) == (
        "미국 건설지출\n8월 건설지출 0.9%↑"
    )


def test_to_vector_literal():
    assert to_vector_literal([1, 0.5, -0.25]) == "[1.0,0.5,-0.25]"


# ── 주요 기업 ────────────────────────────────────────────────────────────────


def test_primary_companies_keep_only_title_mentioned():
    members = [
        # 본문에만 나온 거래처(SK하이닉스)는 news_companies 에 있어도 주요 기업이 아니다.
        ClusterMember("마이크론, 4분기 실적 발표", frozenset({MICRON, SK_HYNIX})),
        # 약칭도 같은 기업으로 잡는다.
        ClusterMember("삼전도 HBM 공급 기대", frozenset({SAMSUNG})),
    ]

    assert primary_company_ids(members, MATCHER) == {MICRON, SAMSUNG}


def test_primary_companies_ignore_title_companies_not_linked():
    # 제목에 나왔어도 news_companies 판정을 통과하지 못한 기업은 넣지 않는다.
    members = [ClusterMember("마이크론·SK하이닉스 실적 비교", frozenset({MICRON}))]

    assert primary_company_ids(members, MATCHER) == {MICRON}


def test_primary_companies_fall_back_to_all_when_titles_match_nothing():
    members = [
        ClusterMember("메모리 업황 반등", frozenset({MICRON})),
        ClusterMember("HBM 공급 부족", frozenset({SK_HYNIX})),
    ]

    assert primary_company_ids(members, MATCHER) == {MICRON, SK_HYNIX}


def test_primary_companies_empty_without_companies():
    members = [ClusterMember("마이크론 실적", frozenset())]

    assert primary_company_ids(members, MATCHER) == frozenset()
    assert primary_company_ids([], MATCHER) == frozenset()


# ── 부모 선택 ────────────────────────────────────────────────────────────────


def test_choose_parent_picks_highest_score_sharing_company():
    parent = _choose(
        [
            _candidate(1, 10, 0.50, MICRON),
            _candidate(2, 3, 0.70, MICRON, SAMSUNG),
            # 코사인은 높지만 기업이 겹치지 않는다.
            _candidate(3, 2, 0.95, SK_HYNIX),
        ],
        MICRON,
    )

    assert parent is not None and parent.cluster_id == 2


def test_choose_parent_threshold_is_inclusive():
    assert _choose([_candidate(1, 5, 0.45, MICRON)], MICRON).cluster_id == 1
    assert _choose([_candidate(1, 5, 0.449, MICRON)], MICRON) is None


def test_choose_parent_requires_earlier_start():
    later = _candidate(1, -1, 0.9, MICRON)
    same_time_larger_id = LinkCandidate(TARGET_ID + 1, NOW, 0.9, frozenset({MICRON}))
    itself = LinkCandidate(TARGET_ID, NOW, 1.0, frozenset({MICRON}))

    assert _choose([later, same_time_larger_id, itself], MICRON) is None


def test_choose_parent_same_start_smaller_id_counts_as_earlier():
    # 같은 시각에 시작한 클러스터 둘(같은 사건이 나뉜 경우)은 id 가 작은 쪽이 먼저다.
    twin = LinkCandidate(TARGET_ID - 1, NOW, 0.9, frozenset({MICRON}))

    assert _choose([twin], MICRON) == twin


def test_choose_parent_tie_breaks_by_later_start_then_larger_id():
    older = _candidate(1, 10, 0.8, MICRON)
    newer = _candidate(2, 2, 0.8, MICRON)
    newer_larger_id = _candidate(3, 2, 0.8, MICRON)

    assert _choose([older, newer], MICRON) == newer
    assert _choose([newer_larger_id, older, newer], MICRON) == newer_larger_id


def test_choose_parent_without_companies_links_only_to_company_less_with_strict_threshold():
    candidates = [
        _candidate(1, 30, 0.74),
        _candidate(2, 20, 0.95, MICRON),
        _candidate(3, 25, 0.80),
    ]

    assert _choose(candidates).cluster_id == 3
    assert _choose([_candidate(1, 30, 0.74)]) is None


def test_choose_parent_with_companies_never_links_to_company_less():
    assert _choose([_candidate(1, 5, 0.99)], MICRON) is None


def test_choose_parent_without_candidates():
    assert _choose([], MICRON) is None


# ── 관계 ─────────────────────────────────────────────────────────────────────

GAP = timedelta(hours=24)
SCORE_GAP = timedelta(hours=168)


@pytest.mark.parametrize(
    ("gap", "score", "expected"),
    [
        # 첫 기사 시각이 가까우면 점수와 무관하게 같은 사건이다.
        (timedelta(0), 0.1, SAME_EVENT),
        (timedelta(hours=3), 0.5, SAME_EVENT),
        (timedelta(hours=24), 0.5, SAME_EVENT),
        (timedelta(hours=24, seconds=1), 0.5, FOLLOW_UP),
        # 며칠 떨어졌어도 코사인이 같은 사건 기준(0.75) 이상이면 같은 사건이 나뉜 것이다.
        (timedelta(days=3), 0.78, SAME_EVENT),
        (timedelta(days=3), 0.75, SAME_EVENT),
        (timedelta(days=3), 0.749, FOLLOW_UP),
        # 코사인 기준은 간격 상한까지만 본다(경계 포함).
        (timedelta(hours=168), 0.8, SAME_EVENT),
        (timedelta(hours=168, seconds=1), 0.8, FOLLOW_UP),
        # 시리즈 지표의 다음 회차(7월 → 8월 건설지출)는 코사인이 높아도 후속이다.
        (timedelta(days=30), 0.80, FOLLOW_UP),
        (timedelta(days=30), 0.5, FOLLOW_UP),
    ],
)
def test_classify_relation(gap, score, expected):
    relation = classify_relation(
        gap,
        score,
        same_event_max_gap=GAP,
        same_event_score=0.75,
        same_event_score_max_gap=SCORE_GAP,
    )

    assert relation == expected


def _decide(candidates, *companies):
    return decide_link(
        candidates,
        cluster_id=TARGET_ID,
        first_published_at=NOW,
        company_ids=companies,
        threshold=0.45,
        no_company_threshold=0.75,
        same_event_max_gap=GAP,
        same_event_score=0.75,
        same_event_score_max_gap=SCORE_GAP,
    )


def test_decide_link_marks_follow_up_and_same_event():
    assert _decide([_candidate(1, 5, 0.6, MICRON)], MICRON) == LinkDecision(1, 0.6, FOLLOW_UP)
    # 같은 날 시작한 중복 클러스터는 같은 사건이다.
    assert _decide([_candidate(2, 0, 0.5, MICRON, hours_ago=6)], MICRON) == LinkDecision(
        2, 0.5, SAME_EVENT
    )
    # 며칠 떨어졌어도 코사인이 같은 사건 기준 이상이면 같은 사건이다.
    assert _decide([_candidate(3, 4, 0.8, MICRON)], MICRON) == LinkDecision(3, 0.8, SAME_EVENT)
    # 기업 없는 연결은 하한(0.75)부터 같은 사건 기준을 넘지만, 한 달 뒤의 다음 회차는 후속이다.
    assert _decide([_candidate(4, 30, 0.9)]) == LinkDecision(4, 0.9, FOLLOW_UP)


def test_decide_link_root_without_parent():
    assert _decide([_candidate(1, 5, 0.3, MICRON)], MICRON) == LinkDecision()
    assert LinkDecision().relation is None
