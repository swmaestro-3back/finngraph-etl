"""link_issues job 단위 테스트. DB·Bedrock·개체 사전은 메모리 가짜로 갈아끼우고 흐름만 본다.

가짜 저장소는 SQL 과 같은 규칙(이름·대표 있음, 요약 대기, 먼저 시작, lookback, 판정 완료, 대상 주요
기업이 연결된 후보, min_score, 페이지 경계, 꼬리 창)으로 대상·후보·꼬리를 고르고 판정을 바로
반영한다 — 같은 런의 앞선 이슈가 뒤 이슈의 후보가 되는지를 본다.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass, field
from datetime import datetime, timedelta

import pytest

from pipelines.common.gazetteer import CompanyMatcher, GazetteerEntry
from pipelines.common.utils.time import now_kst
from pipelines.news.repositories.postgres.news_clusters import LinkTarget
from pipelines.news.transformers.issue_linker import ClusterMember, LinkCandidate

MICRON = 100
SK_HYNIX = 300
ZERO = {
    "scanned": 0,
    "embedded": 0,
    "linked": 0,
    "same_event": 0,
    "follow_up": 0,
    "roots": 0,
    "relinked": 0,
    "relink_changed": 0,
    "failed": 0,
}

MATCHER = CompanyMatcher(
    {
        "마이크론": GazetteerEntry(MICRON, 1, "MU", "마이크론테크놀로지"),
        "SK하이닉스": GazetteerEntry(SK_HYNIX, 3, "000660", "SK하이닉스"),
    }
)

# 임베딩 가짜: 텍스트에 먼저 걸리는 주제어로 벡터를 고른다. HBM 은 마이크론과 코사인 0.6 이다.
TOPIC_VECTORS = {
    "HBM": [0.6, 0.0, 0.8, 0.0],
    "마이크론": [1.0, 0.0, 0.0, 0.0],
    "건설지출": [0.0, 1.0, 0.0, 0.0],
}
OTHER_VECTOR = [0.0, 0.0, 0.0, 1.0]
MICRON_VECTOR = "[1.0,0.0,0.0,0.0]"
CONSTRUCTION_VECTOR = "[0.0,1.0,0.0,0.0]"


def _stats(**counts: int) -> dict[str, int]:
    return {**ZERO, **counts}


@dataclass
class FakeCluster:
    title: str | None
    first_published_at: datetime
    summary: str | None = None
    summary_points: list | None = None
    members: list[ClusterMember] = field(default_factory=list)
    representative: bool = True
    updated_at: datetime | None = None
    embedding: str | None = None
    linked: bool = False
    parent: int | None = None
    root: int | None = None
    score: float | None = None
    relation: str | None = None

    @property
    def companies(self) -> frozenset[int]:
        return frozenset(c for member in self.members for c in member.company_ids)


def _cosine(a: str, b: str) -> float:
    x, y = json.loads(a), json.loads(b)
    dot = sum(p * q for p, q in zip(x, y, strict=True))
    return dot / (math.sqrt(sum(p * p for p in x)) * math.sqrt(sum(q * q for q in y)))


@pytest.fixture
def fake(monkeypatch):
    from pipelines.news import config
    from pipelines.news.jobs import link_issues as job

    monkeypatch.setenv("NEWS_ISSUE_LINK_ENABLED", "true")
    monkeypatch.setenv("NEWS_ISSUE_LINK_THRESHOLD", "0.45")
    monkeypatch.setenv("NEWS_ISSUE_LINK_NO_COMPANY_THRESHOLD", "0.75")
    monkeypatch.setenv("NEWS_ISSUE_LINK_LOOKBACK_DAYS", "90")
    monkeypatch.setenv("NEWS_ISSUE_LINK_MAX_PER_RUN", "200")
    monkeypatch.setenv("NEWS_ISSUE_SAME_EVENT_MAX_GAP_HOURS", "24")
    monkeypatch.setenv("NEWS_ISSUE_SAME_EVENT_SCORE", "0.75")
    monkeypatch.setenv("NEWS_ISSUE_SAME_EVENT_SCORE_MAX_GAP_HOURS", "168")
    monkeypatch.setenv("NEWS_ISSUE_RELINK_WINDOW_HOURS", "72")
    monkeypatch.setenv("NEWS_ISSUE_EMBEDDING_MODEL", "titan-test")
    config.get_news_settings.cache_clear()

    clusters: dict[int, FakeCluster] = {}
    calls: dict = {
        "embed": [],
        "targets": [],
        "members": [],
        "candidates": [],
        "tails": [],
        "fail_record": set(),
        "matcher": 0,
    }

    def fetch_link_targets(since, limit, summary_deadline):
        calls["targets"].append((since, limit, summary_deadline))

        def ready(c: FakeCluster) -> bool:
            return bool(c.summary_points is not None or (c.summary or "").strip()) or (
                c.updated_at is not None and c.updated_at < summary_deadline
            )

        rows = sorted(
            (
                (cid, c)
                for cid, c in clusters.items()
                if c.title
                and c.representative
                and not c.linked
                and c.first_published_at >= since
                and ready(c)
            ),
            key=lambda pair: (pair[1].first_published_at, pair[0]),
        )[:limit]
        return [
            LinkTarget(cid, c.title, c.first_published_at, c.summary, c.summary_points, c.embedding)
            for cid, c in rows
        ]

    def fetch_cluster_members(cluster_ids):
        calls["members"].append(list(cluster_ids))
        return {cid: list(clusters[cid].members) for cid in cluster_ids if clusters[cid].members}

    def embed_texts(texts, dim, model=None):
        calls["embed"].append((list(texts), dim, model))
        if any("임베딩장애" in text for text in texts):
            raise RuntimeError("bedrock down")
        return [
            next((v for word, v in TOPIC_VECTORS.items() if word in text), OTHER_VECTOR)
            for text in texts
        ]

    def save_cluster_embedding(cluster_id, embedding):
        clusters[cluster_id].embedding = embedding

    def fetch_link_candidates(
        *,
        cluster_id,
        embedding,
        first_published_at,
        company_ids,
        window_start,
        min_score,
        limit,
        after=None,
    ):
        calls["candidates"].append((cluster_id, company_ids, window_start, min_score))
        found = []
        for cid, c in clusters.items():
            if not c.linked or c.embedding is None:
                continue
            if (c.first_published_at, cid) >= (first_published_at, cluster_id):
                continue
            if c.first_published_at < window_start:
                continue
            if company_ids and not (c.companies & company_ids):
                continue
            if not company_ids and c.companies:
                continue
            score = _cosine(c.embedding, embedding)
            if score >= min_score:
                found.append(LinkCandidate(cid, c.first_published_at, score))

        def key(x: LinkCandidate) -> tuple:
            return (x.score, x.first_published_at, x.cluster_id)

        found.sort(key=key, reverse=True)
        if after is not None:
            found = [x for x in found if key(x) < key(after)]
        return found[:limit]

    def fetch_relink_tail(after_published_at, after_id, relink_since):
        calls["tails"].append((after_published_at, after_id, relink_since))
        rows = sorted(
            (
                (cid, c)
                for cid, c in clusters.items()
                if c.linked
                and c.embedding is not None
                and (c.first_published_at, cid) > (after_published_at, after_id)
                and c.first_published_at >= relink_since
            ),
            key=lambda pair: (pair[1].first_published_at, pair[0]),
        )
        return [
            LinkTarget(
                cid, c.title, c.first_published_at, embedding=c.embedding, parent_id=c.parent
            )
            for cid, c in rows
        ]

    def record_link_decision(cluster_id, parent_id, score, relation, *, relink=False):
        if cluster_id in calls["fail_record"]:
            raise RuntimeError("db hiccup")
        c = clusters[cluster_id]
        if c.linked != relink:
            return False
        c.linked, c.parent, c.score, c.relation = True, parent_id, score, relation
        c.root = (clusters[parent_id].root or parent_id) if parent_id else cluster_id
        return True

    def get_company_matcher():
        calls["matcher"] += 1
        return MATCHER

    for name, fn in {
        "fetch_link_targets": fetch_link_targets,
        "fetch_cluster_members": fetch_cluster_members,
        "embed_texts": embed_texts,
        "save_cluster_embedding": save_cluster_embedding,
        "fetch_link_candidates": fetch_link_candidates,
        "fetch_relink_tail": fetch_relink_tail,
        "record_link_decision": record_link_decision,
        "get_company_matcher": get_company_matcher,
    }.items():
        monkeypatch.setattr(job, name, fn)

    yield job, clusters, calls
    config.get_news_settings.cache_clear()


def _members(*titles: str, companies: tuple[int, ...] = ()) -> list[ClusterMember]:
    return [ClusterMember(title, frozenset(companies)) for title in titles]


def test_run_embeds_missing_once_and_links_oldest_first(fake):
    job, clusters, calls = fake
    now = now_kst()
    # 이미 판정된 7월 건설지출 루트 (기업 없음, 임베딩 있음)
    clusters[1] = FakeCluster(
        "미국 7월 건설지출",
        now - timedelta(days=30),
        embedding=CONSTRUCTION_VECTOR,
        linked=True,
        root=1,
    )
    # 이번 런 대상: 마이크론 프리뷰(가장 먼저) → 8월 건설지출 → 마이크론 HBM 실적
    clusters[10] = FakeCluster(
        "마이크론 실적 발표 일정",
        now - timedelta(days=3),
        summary="첫 문장이에요. 둘째 문장이에요.",
        summary_points=[
            {"kind": "AFFECTED", "text": "메모리 업체가 영향을 받아요."},
            {"kind": "CHANGE", "text": "마이크론이 4분기 실적을 발표해요."},
        ],
        members=_members(
            "마이크론, 29일 실적 발표",
            "마이크론 실적 D-7",
            "실적 시즌",
            "옛 기사",
            companies=(MICRON,),
        ),
    )
    clusters[11] = FakeCluster(
        "미국 8월 건설지출", now - timedelta(days=2), summary="건설지출이 늘었어요. 둘째."
    )
    clusters[12] = FakeCluster(
        "마이크론 HBM 실적",
        now - timedelta(days=1),
        summary_points=[],
        summary="마이크론 4분기 매출이 46% 늘었어요.",
        members=_members("마이크론 4분기 호실적", companies=(MICRON,)),
    )

    stats = job.run()

    assert stats == _stats(scanned=3, embedded=3, linked=2, follow_up=2, roots=1)
    # 임베딩은 한 번에, 뉴스 전용 모델·1024 차원으로
    assert len(calls["embed"]) == 1
    texts, dim, model = calls["embed"][0]
    assert (dim, model) == (1024, "titan-test")
    # 제목 / 대표 기사 CHANGE 포인트 / 멤버 기사 제목 앞 3개(저장소가 후보 기사 먼저 발행순으로 준다)
    assert texts[0] == (
        "마이크론 실적 발표 일정\n"
        "마이크론이 4분기 실적을 발표해요.\n"
        "마이크론, 29일 실적 발표\n"
        "마이크론 실적 D-7\n"
        "실적 시즌"
    )
    # CHANGE 가 없으면 요약 첫 문장
    assert texts[1] == "미국 8월 건설지출\n건설지출이 늘었어요."
    # 연결 대상 창은 지금부터 lookback 90일, 요약 대기는 24시간
    since, limit, deadline = calls["targets"][0]
    assert abs(since - (now - timedelta(days=90))) < timedelta(minutes=1)
    assert abs(deadline - (now - timedelta(hours=24))) < timedelta(minutes=1)
    assert limit == 200
    # 개체 사전은 런당 한 번
    assert calls["matcher"] == 1

    # 프리뷰는 새 이야기의 루트, 실적은 같은 런에 먼저 판정된 프리뷰에 후속으로 이어진다
    # (이틀 떨어졌고 코사인 0.6 은 같은 사건 기준 0.75 미만)
    assert (clusters[10].parent, clusters[10].root, clusters[10].relation) == (None, 10, None)
    assert (clusters[12].parent, clusters[12].root, clusters[12].relation) == (10, 10, "follow_up")
    assert clusters[12].score == pytest.approx(0.6)
    # 기업 없는 8월 건설지출은 기업 없는 7월 건설지출에 잇는다. 코사인은 같은 사건 기준을 넘지만
    # 4주 떨어져 코사인 기준의 간격 상한(168시간) 밖이라 후속이다.
    assert (clusters[11].parent, clusters[11].root, clusters[11].relation) == (1, 1, "follow_up")


def test_candidate_filter_uses_target_primary_companies_and_thresholds(fake):
    job, clusters, calls = fake
    now = now_kst()
    clusters[1] = FakeCluster(
        "마이크론 실적",
        now - timedelta(days=1),
        summary="요약이에요.",
        # SK하이닉스는 본문에만 나온 거래처라 주요 기업이 아니다
        members=_members("마이크론 실적 발표", companies=(MICRON, SK_HYNIX)),
    )
    clusters[2] = FakeCluster("건설지출", now - timedelta(hours=1), summary="요약이에요.")

    job.run()

    (first_id, first_companies, first_window, first_min), (_, second_companies, _, second_min) = (
        calls["candidates"]
    )
    assert (first_id, first_companies, first_min) == (1, frozenset({MICRON}), 0.45)
    assert abs(first_window - (clusters[1].first_published_at - timedelta(days=90))) < timedelta(
        seconds=1
    )
    # 기업 없는 대상은 기업 없는 후보만, 더 높은 하한으로
    assert (second_companies, second_min) == (frozenset(), 0.75)


def test_primary_companies_are_judged_per_cluster(fake):
    job, clusters, calls = fake
    now = now_kst()
    # 후보: 마이크론은 제목에 나온 기사가 아니라 다른 멤버 기사에 연결돼 있다. 주요 기업은 클러스터
    # 단위(연결 기업 중 멤버 제목 하나 이상에 나온 기업)라 마이크론도 주요 기업이다.
    clusters[1] = FakeCluster(
        "SK하이닉스 HBM 증설",
        now - timedelta(days=5),
        members=_members("SK하이닉스, 마이크론 겨냥 증설", companies=(SK_HYNIX,))
        + _members("SK하이닉스 증설 효과", companies=(SK_HYNIX, MICRON)),
        embedding=MICRON_VECTOR,
        linked=True,
        root=1,
    )
    clusters[2] = FakeCluster(
        "마이크론 실적",
        now - timedelta(days=1),
        summary="마이크론 실적이에요.",
        members=_members("마이크론 4분기 실적", companies=(MICRON,)),
    )

    stats = job.run()

    assert stats == _stats(scanned=1, embedded=1, linked=1, same_event=1)
    assert clusters[2].parent == 1


def test_candidate_without_primary_overlap_is_rejected(fake):
    job, clusters, calls = fake
    now = now_kst()
    clusters[1] = FakeCluster(
        "SK하이닉스 HBM 증설",
        now - timedelta(days=5),
        # 마이크론은 연결돼 있지만 어느 제목에도 없다
        members=_members("SK하이닉스 HBM 증설", companies=(SK_HYNIX, MICRON)),
        embedding=MICRON_VECTOR,
        linked=True,
        root=1,
    )
    clusters[2] = FakeCluster(
        "마이크론 실적",
        now - timedelta(days=1),
        summary="마이크론 실적이에요.",
        members=_members("마이크론 4분기 실적", companies=(MICRON,)),
    )

    stats = job.run()

    assert stats == _stats(scanned=1, embedded=1, roots=1)
    assert clusters[2].parent is None and clusters[2].root == 2
    # 후보의 멤버는 런 중에 한 번만 읽는다
    assert calls["members"] == [[2], [1]]


def test_close_start_is_same_event_even_with_low_score(fake):
    job, clusters, calls = fake
    now = now_kst()
    clusters[1] = FakeCluster(
        "마이크론 실적",
        now - timedelta(hours=10),
        members=_members("마이크론 실적", companies=(MICRON,)),
        embedding=MICRON_VECTOR,
        linked=True,
        root=1,
    )
    # 같은 발표를 다른 표현으로 묶은 클러스터 — 6시간 차, 코사인 0.6
    clusters[2] = FakeCluster(
        "HBM 매출 급증",
        now - timedelta(hours=4),
        summary="요약이에요.",
        members=_members("마이크론 HBM 매출 급증", companies=(MICRON,)),
    )

    assert job.run() == _stats(scanned=1, embedded=1, linked=1, same_event=1)
    assert (clusters[2].parent, clusters[2].relation) == (1, "same_event")
    assert clusters[2].score == pytest.approx(0.6)


def test_target_without_summary_waits_until_stale(fake):
    job, clusters, calls = fake
    now = now_kst()
    # 대표 요약이 아직 없고 방금 승격·제목이 붙었다 — 요약을 기다린다
    clusters[1] = FakeCluster("마이크론 실적", now - timedelta(days=1), updated_at=now)
    # 요약 없이 하루 넘게 지났다 — 요약 없이 잇는다
    clusters[2] = FakeCluster(
        "미국 건설지출", now - timedelta(days=3), updated_at=now - timedelta(hours=25)
    )
    # 이름이나 대표가 없으면 대상이 아니다
    clusters[3] = FakeCluster(None, now - timedelta(days=2), summary="요약이에요.")
    clusters[4] = FakeCluster(
        "대표 없음", now - timedelta(days=2), summary="요약이에요.", representative=False
    )

    stats = job.run()

    assert stats == _stats(scanned=1, embedded=1, roots=1)
    assert calls["embed"][0][0] == ["미국 건설지출"]
    assert not clusters[1].linked and clusters[2].linked


def test_run_reuses_stored_embedding(fake):
    job, clusters, calls = fake
    now = now_kst()
    clusters[1] = FakeCluster(
        "마이크론 실적",
        now - timedelta(days=5),
        members=_members("마이크론 실적", companies=(MICRON,)),
        embedding=MICRON_VECTOR,
        linked=True,
        root=1,
    )
    # 지난 런에 임베딩은 저장됐지만 연결 기록에 실패한 대상
    clusters[2] = FakeCluster(
        "마이크론 HBM 공급",
        now - timedelta(days=1),
        summary="요약이에요.",
        members=_members("마이크론 HBM 공급", companies=(MICRON,)),
        embedding=MICRON_VECTOR,
    )

    assert job.run() == _stats(scanned=1, linked=1, same_event=1)
    assert calls["embed"] == []
    assert clusters[2].parent == 1


def test_candidates_are_paged_until_one_passes_primary_company_rule(fake):
    job, clusters, calls = fake
    now = now_kst()
    # 맞는 부모: 마이크론이 제목에 나온다. 코사인 0.6
    clusters[1] = FakeCluster(
        "마이크론 HBM 공급",
        now - timedelta(days=10),
        members=_members("마이크론 HBM 공급 확대", companies=(MICRON,)),
        embedding="[0.6,0.0,0.8,0.0]",
        linked=True,
        root=1,
    )
    # 한 페이지를 넘는 고득점 후보. 마이크론은 본문에만 스쳐(제목은 SK하이닉스) SQL 의 기업 조건은
    # 넘지만 주요 기업 규칙에서 떨어진다
    for offset in range(job.CANDIDATE_PAGE_SIZE + 2):
        clusters[100 + offset] = FakeCluster(
            "SK하이닉스 HBM 증설",
            now - timedelta(days=5, minutes=offset),
            members=_members("SK하이닉스 HBM 증설", companies=(SK_HYNIX, MICRON)),
            embedding=MICRON_VECTOR,
            linked=True,
            root=100 + offset,
        )
    clusters[200] = FakeCluster(
        "마이크론 실적",
        now - timedelta(days=1),
        summary="마이크론 실적이에요.",
        members=_members("마이크론 4분기 실적", companies=(MICRON,)),
    )

    assert job.run() == _stats(scanned=1, embedded=1, linked=1, follow_up=1)
    assert (clusters[200].parent, clusters[200].relation) == (1, "follow_up")
    assert clusters[200].score == pytest.approx(0.6)
    # 첫 페이지는 전부 탈락이라 다음 페이지를 읽었다
    assert [cluster_id for cluster_id, *_ in calls["candidates"]] == [200, 200]


def test_root_without_eligible_candidate_reads_only_short_page(fake):
    job, clusters, calls = fake
    now = now_kst()
    clusters[1] = FakeCluster(
        "SK하이닉스 HBM 증설",
        now - timedelta(days=5),
        members=_members("SK하이닉스 HBM 증설", companies=(SK_HYNIX, MICRON)),
        embedding=MICRON_VECTOR,
        linked=True,
        root=1,
    )
    clusters[2] = FakeCluster(
        "마이크론 실적",
        now - timedelta(days=1),
        summary="요약이에요.",
        members=_members("마이크론 실적", companies=(MICRON,)),
    )

    assert job.run() == _stats(scanned=1, embedded=1, roots=1)
    # 후보가 한 페이지에 못 미치면 더 읽지 않는다
    assert len(calls["candidates"]) == 1


def test_earlier_cluster_judged_late_relinks_tail(fake):
    """같은 사건이 둘로 갈렸는데 늦게 시작한 쪽이 먼저 승격·판정된 경우.

    백필처럼 시작 순서로 한 번에 이으면 B·C 는 A 에 같은 사건으로 붙는다. 스케줄 연결은 A 가 들어올
    때 A 보다 뒤에 시작한 판정(꼬리)을 다시 만들어 같은 결과에 이른다.
    """

    job, clusters, calls = fake
    now = now_kst()
    a_start = now - timedelta(hours=24)
    # A 는 먼저 시작했지만 아직 후보가 덜 차 대표가 없다
    clusters[1] = FakeCluster(
        "마이크론 실적",
        a_start,
        summary="마이크론 실적이에요.",
        members=_members("마이크론 4분기 실적", companies=(MICRON,)),
        representative=False,
    )
    clusters[2] = FakeCluster(
        "마이크론 실적 예상 상회",
        now - timedelta(hours=23),
        summary="마이크론 실적이 예상을 넘었어요.",
        members=_members("마이크론 실적 예상 상회", companies=(MICRON,)),
    )
    clusters[3] = FakeCluster(
        "마이크론 실적 반응",
        now - timedelta(hours=10),
        summary="마이크론 주가가 올랐어요.",
        members=_members("마이크론 실적에 주가 급등", companies=(MICRON,)),
    )

    # 첫 런: A 는 대상이 아니다. B 는 루트, C 는 B 에 같은 사건으로 붙는다
    assert job.run() == _stats(scanned=2, embedded=2, linked=1, same_event=1, roots=1)
    assert (clusters[2].parent, clusters[3].parent, clusters[3].root) == (None, 2, 2)

    # 둘째 런: A 가 승격돼 들어온다. A 는 앞선 이슈가 없어 루트이고, A 보다 뒤에 시작한 B·C 를 다시
    # 판정한다
    clusters[1].representative = True
    stats = job.run()

    assert stats == _stats(scanned=1, embedded=1, roots=1, relinked=2, relink_changed=1)
    assert (clusters[1].parent, clusters[1].root) == (None, 1)
    assert (clusters[2].parent, clusters[2].root, clusters[2].relation) == (1, 1, "same_event")
    assert clusters[2].score == pytest.approx(1.0)
    # C 는 코사인이 같은 A·B 중 늦게 시작한 B 를 그대로 부모로 두고, 루트는 새로 물려받는다
    assert (clusters[3].parent, clusters[3].root, clusters[3].relation) == (2, 1, "same_event")
    # 꼬리는 A 뒤, 지금부터 72시간 안에 시작한 판정이다
    after_published_at, after_id, relink_since = calls["tails"][-1]
    assert (after_published_at, after_id) == (a_start, 1)
    assert abs(relink_since - (now - timedelta(hours=72))) < timedelta(minutes=1)


@pytest.mark.parametrize("window_hours", ["72", "0"])
def test_tail_outside_relink_window_is_kept(fake, monkeypatch, window_hours):
    from pipelines.news import config

    job, clusters, calls = fake
    monkeypatch.setenv("NEWS_ISSUE_RELINK_WINDOW_HOURS", window_hours)
    config.get_news_settings.cache_clear()
    now = now_kst()
    # B 는 나흘 전에 시작해 루트로 판정됐다. 하루 먼저 시작한 A 가 이제야 들어온다
    clusters[1] = FakeCluster(
        "마이크론 실적",
        now - timedelta(days=5),
        summary="마이크론 실적이에요.",
        members=_members("마이크론 4분기 실적", companies=(MICRON,)),
    )
    clusters[2] = FakeCluster(
        "마이크론 실적 예상 상회",
        now - timedelta(days=4),
        members=_members("마이크론 실적 예상 상회", companies=(MICRON,)),
        embedding=MICRON_VECTOR,
        linked=True,
        root=2,
    )

    assert job.run() == _stats(scanned=1, embedded=1, roots=1)
    # 창 밖(또는 창 0)이라 B 는 다시 판정하지 않는다 — 기간을 정해 백필 스크립트로 다시 만든다
    assert (clusters[2].parent, clusters[2].root) == (None, 2)
    assert len(calls["tails"]) == (1 if window_hours == "72" else 0)


def test_run_isolates_one_failure(fake):
    job, clusters, calls = fake
    now = now_kst()
    clusters[1] = FakeCluster(
        "마이크론 실적",
        now - timedelta(days=2),
        summary="요약이에요.",
        members=_members("마이크론 실적", companies=(MICRON,)),
    )
    clusters[2] = FakeCluster("건설지출", now - timedelta(days=1), summary="요약이에요.")
    calls["fail_record"].add(1)

    stats = job.run()

    assert stats == _stats(scanned=2, embedded=2, roots=1, failed=1)
    # 실패한 대상은 판정 전으로 남아 다음 런에 다시 잡힌다
    assert not clusters[1].linked
    assert clusters[2].root == 2


def test_run_keeps_embedded_targets_when_embedding_batch_fails(fake):
    job, clusters, calls = fake
    now = now_kst()
    clusters[1] = FakeCluster("임베딩장애 이슈", now - timedelta(days=2), summary="요약이에요.")
    clusters[2] = FakeCluster(
        "건설지출", now - timedelta(days=1), summary="요약이에요.", embedding=CONSTRUCTION_VECTOR
    )

    stats = job.run()

    assert stats == _stats(scanned=2, roots=1, failed=1)
    assert clusters[1].embedding is None and not clusters[1].linked
    # 꼬리는 이번에 실제로 잇는 대상 기준이다 — 임베딩에 실패한 대상은 다음 런에 다시 고른다
    assert [cluster_id for _, cluster_id, _ in calls["tails"]] == [2]


def test_run_raises_when_all_fail(fake):
    job, clusters, calls = fake
    clusters[1] = FakeCluster(
        "임베딩장애 이슈", now_kst() - timedelta(days=1), summary="요약이에요."
    )

    with pytest.raises(RuntimeError, match="전건 실패"):
        job.run()


def test_run_counts_nothing_for_target_linked_by_another_run(fake, monkeypatch):
    job, clusters, calls = fake
    clusters[1] = FakeCluster("건설지출", now_kst() - timedelta(days=1), summary="요약이에요.")
    monkeypatch.setattr(job, "record_link_decision", lambda *args, **kwargs: False)

    assert job.run() == _stats(scanned=1, embedded=1)


def test_run_without_targets_skips_embedding_and_gazetteer(fake):
    job, clusters, calls = fake
    clusters[1] = FakeCluster("이미 판정됨", now_kst(), summary="요약이에요.", linked=True, root=1)

    assert job.run() == ZERO
    assert calls["embed"] == [] and calls["matcher"] == 0


def test_run_skips_everything_when_disabled(fake, monkeypatch):
    from pipelines.news import config

    job, clusters, calls = fake
    monkeypatch.delenv("NEWS_ISSUE_LINK_ENABLED")
    config.get_news_settings.cache_clear()
    clusters[1] = FakeCluster("마이크론 실적", now_kst() - timedelta(days=1), summary="요약.")

    # 기본값은 꺼짐
    assert job.run() == ZERO
    # 대상 조회도 임베딩도 하지 않는다
    assert calls["targets"] == [] and calls["embed"] == []
    assert not clusters[1].linked


def test_link_pending_uses_given_window_and_limit(fake, monkeypatch):
    from pipelines.news import config

    job, clusters, calls = fake
    # 백필 경로는 스위치와 무관하게 돈다
    monkeypatch.setenv("NEWS_ISSUE_LINK_ENABLED", "false")
    config.get_news_settings.cache_clear()
    now = now_kst()
    for cid in range(1, 4):
        clusters[cid] = FakeCluster(f"이슈 {cid}", now - timedelta(days=400 - cid), summary="요약.")

    since = now - timedelta(days=1000)
    stats = job.link_pending(since=since, limit=2)

    assert calls["targets"][0][:2] == (since, 2)
    assert stats["scanned"] == 2
    assert [cid for cid, c in sorted(clusters.items()) if c.linked] == [1, 2]
