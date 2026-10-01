"""link_issues job 단위 테스트. DB·Bedrock 은 메모리 가짜로 갈아끼우고 흐름만 본다.

가짜 저장소는 SQL 과 같은 규칙(먼저 시작, lookback, 판정 완료, 기업 규칙, min_score)으로 후보를
고르고, 판정을 바로 반영한다 — 같은 런의 앞선 대상이 뒤 대상의 후보가 되는지를 본다.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass, field
from datetime import datetime, timedelta

import pytest

from pipelines.common.utils.time import now_kst
from pipelines.news.repositories.issue_timeline import LinkTarget
from pipelines.news.transformers.issue_linker import LinkCandidate

MICRON = 100

# 임베딩 가짜: 텍스트의 주제어로 단위 벡터를 고른다
TOPIC_VECTORS = {"마이크론": [1.0, 0.0, 0.0], "건설지출": [0.0, 1.0, 0.0]}
OTHER_VECTOR = [0.0, 0.0, 1.0]


@dataclass
class FakeCluster:
    title: str
    first_published_at: datetime
    summary: str | None = None
    companies: frozenset[int] = frozenset()
    embedding: str | None = None
    linked: bool = False
    parent: int | None = None
    root: int | None = None
    score: float | None = None
    member_titles: list[str] = field(default_factory=list)


def _cosine(a: str, b: str) -> float:
    x, y = json.loads(a), json.loads(b)
    dot = sum(p * q for p, q in zip(x, y, strict=True))
    return dot / (math.sqrt(sum(p * p for p in x)) * math.sqrt(sum(q * q for q in y)))


@pytest.fixture
def fake(monkeypatch):
    from pipelines.news import config
    from pipelines.news.jobs import link_issues as job

    monkeypatch.setenv("NEWS_ISSUE_LINK_ENABLED", "true")
    monkeypatch.setenv("NEWS_ISSUE_LINK_THRESHOLD", "0.6")
    monkeypatch.setenv("NEWS_ISSUE_LINK_NO_COMPANY_THRESHOLD", "0.75")
    monkeypatch.setenv("NEWS_ISSUE_LINK_LOOKBACK_DAYS", "90")
    monkeypatch.setenv("NEWS_ISSUE_LINK_MAX_PER_RUN", "200")
    monkeypatch.setenv("NEWS_ISSUE_EMBEDDING_MODEL", "titan-test")
    config.get_news_settings.cache_clear()

    clusters: dict[int, FakeCluster] = {}
    calls: dict[str, list] = {"embed": [], "since": [], "fail_record": set()}

    def fetch_link_targets(since, limit):
        calls["since"].append(since)
        rows = sorted(
            (
                (cid, c)
                for cid, c in clusters.items()
                if c.title and not c.linked and c.first_published_at >= since
            ),
            key=lambda pair: (pair[1].first_published_at, pair[0]),
        )[:limit]
        return [
            LinkTarget(cid, c.title, c.summary, c.first_published_at, c.embedding)
            for cid, c in rows
        ]

    def fetch_recent_member_titles(cluster_ids, per_cluster):
        return {cid: clusters[cid].member_titles[:per_cluster] for cid in cluster_ids}

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

    def fetch_cluster_company_ids(cluster_ids):
        return {cid: clusters[cid].companies for cid in cluster_ids if clusters[cid].companies}

    def fetch_link_candidates(
        *, cluster_id, embedding, first_published_at, company_ids, lookback_days, min_score, limit
    ):
        found = []
        for cid, c in clusters.items():
            if cid == cluster_id or not c.linked or c.embedding is None:
                continue
            if not (
                first_published_at - timedelta(days=lookback_days)
                <= c.first_published_at
                < first_published_at
            ):
                continue
            if company_ids and not (c.companies & company_ids):
                continue
            if not company_ids and c.companies:
                continue
            score = _cosine(c.embedding, embedding)
            if score >= min_score:
                found.append(LinkCandidate(cid, c.first_published_at, score, c.companies))
        found.sort(key=lambda x: (x.score, x.first_published_at, x.cluster_id), reverse=True)
        return found[:limit]

    def record_link_decision(cluster_id, parent_id, score):
        if cluster_id in calls["fail_record"]:
            raise RuntimeError("db hiccup")
        c = clusters[cluster_id]
        c.linked, c.parent, c.score = True, parent_id, score
        c.root = (clusters[parent_id].root or parent_id) if parent_id else cluster_id
        return True

    for name, fn in {
        "fetch_link_targets": fetch_link_targets,
        "fetch_recent_member_titles": fetch_recent_member_titles,
        "embed_texts": embed_texts,
        "save_cluster_embedding": save_cluster_embedding,
        "fetch_cluster_company_ids": fetch_cluster_company_ids,
        "fetch_link_candidates": fetch_link_candidates,
        "record_link_decision": record_link_decision,
    }.items():
        monkeypatch.setattr(job, name, fn)

    yield job, clusters, calls
    config.get_news_settings.cache_clear()


def test_run_embeds_missing_once_and_links_oldest_first(fake):
    job, clusters, calls = fake
    now = now_kst()
    # 이미 판정된 7월 건설지출 루트 (기업 없음, 임베딩 있음)
    clusters[1] = FakeCluster(
        "미국 7월 건설지출",
        now - timedelta(days=30),
        embedding="[0.0,1.0,0.0]",
        linked=True,
        root=1,
    )
    # 이번 런 대상: 마이크론 프리뷰(가장 먼저) → 8월 건설지출 → 마이크론 실적
    clusters[10] = FakeCluster(
        "마이크론 실적 발표 일정",
        now - timedelta(days=3),
        summary="마이크론이 4분기 실적을 발표해요.",
        companies=frozenset({MICRON}),
        member_titles=["마이크론, 29일 실적 발표"],
    )
    clusters[11] = FakeCluster("미국 8월 건설지출", now - timedelta(days=2), summary=None)
    clusters[12] = FakeCluster(
        "마이크론 4분기 실적",
        now - timedelta(days=1),
        summary="마이크론 4분기 매출이 46% 늘었어요.",
        companies=frozenset({MICRON}),
    )

    stats = job.run()

    assert stats == {"scanned": 3, "embedded": 3, "linked": 2, "roots": 1, "failed": 0}
    # 임베딩은 한 번에, 뉴스 전용 모델·1024 차원으로
    assert len(calls["embed"]) == 1
    texts, dim, model = calls["embed"][0]
    assert (dim, model) == (1024, "titan-test")
    assert (
        texts[0]
        == "마이크론 실적 발표 일정\n마이크론이 4분기 실적을 발표해요.\n마이크론, 29일 실적 발표"
    )
    # 연결 대상 창은 지금부터 lookback 90일
    assert (
        now - timedelta(days=90, minutes=1)
        < calls["since"][0]
        <= now - timedelta(days=90) + timedelta(minutes=1)
    )

    # 프리뷰는 새 이야기의 루트, 실적은 같은 런에 먼저 판정된 프리뷰에 이어진다
    assert (clusters[10].parent, clusters[10].root, clusters[10].score) == (None, 10, None)
    assert (clusters[12].parent, clusters[12].root) == (10, 10)
    assert clusters[12].score == pytest.approx(1.0)
    # 기업 없는 8월 건설지출은 기업 없는 7월 건설지출에 이어진다
    assert (clusters[11].parent, clusters[11].root) == (1, 1)


def test_run_reuses_stored_embedding(fake):
    job, clusters, calls = fake
    now = now_kst()
    clusters[1] = FakeCluster(
        "마이크론 실적",
        now - timedelta(days=5),
        companies=frozenset({MICRON}),
        embedding="[1.0,0.0,0.0]",
        linked=True,
        root=1,
    )
    # 지난 런에 임베딩은 저장됐지만 연결 기록에 실패한 대상
    clusters[2] = FakeCluster(
        "마이크론 HBM 공급",
        now - timedelta(days=1),
        companies=frozenset({MICRON}),
        embedding="[1.0,0.0,0.0]",
    )

    assert job.run() == {"scanned": 1, "embedded": 0, "linked": 1, "roots": 0, "failed": 0}
    assert calls["embed"] == []
    assert clusters[2].parent == 1


def test_run_isolates_one_failure(fake):
    job, clusters, calls = fake
    now = now_kst()
    clusters[1] = FakeCluster(
        "마이크론 실적", now - timedelta(days=2), companies=frozenset({MICRON})
    )
    clusters[2] = FakeCluster("건설지출", now - timedelta(days=1))
    calls["fail_record"].add(1)

    stats = job.run()

    assert stats == {"scanned": 2, "embedded": 2, "linked": 0, "roots": 1, "failed": 1}
    # 실패한 대상은 판정 전으로 남아 다음 런에 다시 잡힌다
    assert not clusters[1].linked
    assert clusters[2].root == 2


def test_run_keeps_embedded_targets_when_embedding_batch_fails(fake):
    job, clusters, calls = fake
    now = now_kst()
    clusters[1] = FakeCluster("임베딩장애 이슈", now - timedelta(days=2))
    clusters[2] = FakeCluster("건설지출", now - timedelta(days=1), embedding="[0.0,1.0,0.0]")

    stats = job.run()

    assert stats == {"scanned": 2, "embedded": 0, "linked": 0, "roots": 1, "failed": 1}
    assert clusters[1].embedding is None and not clusters[1].linked


def test_run_raises_when_all_fail(fake):
    job, clusters, calls = fake
    clusters[1] = FakeCluster("임베딩장애 이슈", now_kst() - timedelta(days=1))

    with pytest.raises(RuntimeError, match="전건 실패"):
        job.run()


def test_run_without_targets_skips_embedding(fake):
    job, clusters, calls = fake
    clusters[1] = FakeCluster("이미 판정됨", now_kst(), linked=True, root=1)

    assert job.run() == {"scanned": 0, "embedded": 0, "linked": 0, "roots": 0, "failed": 0}
    assert calls["embed"] == []


def test_run_skips_everything_when_disabled(fake, monkeypatch):
    from pipelines.news import config

    job, clusters, calls = fake
    monkeypatch.setenv("NEWS_ISSUE_LINK_ENABLED", "false")
    config.get_news_settings.cache_clear()
    clusters[1] = FakeCluster("마이크론 실적", now_kst() - timedelta(days=1))

    assert job.run() == {"scanned": 0, "embedded": 0, "linked": 0, "roots": 0, "failed": 0}
    # 대상 조회도 임베딩도 하지 않는다
    assert calls["since"] == [] and calls["embed"] == []
    assert not clusters[1].linked


def test_link_pending_uses_given_window_and_limit(fake, monkeypatch):
    from pipelines.news import config

    job, clusters, calls = fake
    # 백필 경로는 스위치와 무관하게 돈다
    monkeypatch.setenv("NEWS_ISSUE_LINK_ENABLED", "false")
    config.get_news_settings.cache_clear()
    now = now_kst()
    for cid in range(1, 4):
        clusters[cid] = FakeCluster(f"이슈 {cid}", now - timedelta(days=400 - cid))

    since = now - timedelta(days=1000)
    stats = job.link_pending(since=since, limit=2)

    assert calls["since"] == [since]
    assert stats["scanned"] == 2
    assert [cid for cid, c in sorted(clusters.items()) if c.linked] == [1, 2]
