"""cluster_articles job 단위 테스트. 저장소와 LLM 은 메모리 대역으로 갈아끼운다."""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from pipelines.news.transformers.clustering.online import ClusterSeed
from pipelines.news.transformers.clustering.vectorize import IdfTable
from pipelines.news.utils.date_utils import SEOUL_TIMEZONE

T0 = datetime(2026, 9, 9, 10, tzinfo=SEOUL_TIMEZONE)
TITLE = "엘앤에프, 삼성SDI에 양극재 공급 계약"
BODY = "엘앤에프가 삼성SDI에 양극재를 공급한다. 원료는 포스코퓨처엠이 댄다. " * 10
RESULT_ZERO = {"articles": 0, "created": 0, "updated": 0, "failed": 0}


class FakeStore:
    """news·news_clusters 저장소의 메모리 대역. 런 사이에 상태가 이어진다."""

    def __init__(self):
        self.clusters: dict[int, dict] = {}
        self.news: dict[int, dict] = {}

    def add(self, title: str = TITLE, body: str = BODY, published_at: datetime = T0) -> int:
        news_id = 1000 + len(self.news)
        self.news[news_id] = {
            "title": title,
            "text": body,
            "published_at": published_at,
            "cluster_id": None,
            "candidate": False,
        }
        return news_id

    def _row(self, news_id: int) -> dict:
        row = self.news[news_id]
        return {
            "_news_id": news_id,
            "title": row["title"],
            "text": row["text"],
            "published_at": row["published_at"],
        }

    def unclustered(self):
        ids = [news_id for news_id, row in self.news.items() if row["cluster_id"] is None]
        return [self._row(i) for i in sorted(ids, key=lambda i: (self.news[i]["published_at"], i))]

    def seeds(self, window_start, window_end):
        return [
            ClusterSeed(
                cluster_id=cluster_id,
                term_weights=dict(cluster["term_weights"]),
                member_count=cluster["member_count"],
                promoted=cluster["representative"] is not None,
                first_published_at=cluster["first"],
            )
            for cluster_id, cluster in self.clusters.items()
        ]

    def record(self, assignments, items, documents, keyword_count):
        created = updated = 0
        for assignment in assignments:
            size = len(assignment.candidates) + len(assignment.followers)
            if assignment.seed is None:
                cluster_id = len(self.clusters) + 1
                self.clusters[cluster_id] = {
                    "term_weights": assignment.term_weights,
                    "member_count": len(assignment.candidates),
                    "original_size": size,
                    "representative": None,
                    "title": None,
                    "first": assignment.first_published_at,
                }
                created += 1
            else:
                cluster_id = assignment.seed.cluster_id
                cluster = self.clusters[cluster_id]
                cluster["term_weights"] = assignment.term_weights
                cluster["member_count"] += len(assignment.candidates)
                cluster["original_size"] += size
                updated += 1
            for index in assignment.candidates + assignment.followers:
                row = self.news[items[index]["_news_id"]]
                row["cluster_id"] = cluster_id
                row["candidate"] = index in assignment.candidates
        return {"created": created, "updated": updated, "failed": 0}

    def promotable(self, promote_size, since):
        return [
            cluster_id
            for cluster_id, cluster in self.clusters.items()
            if cluster["representative"] is None and cluster["member_count"] >= promote_size
        ]

    def candidates(self, cluster_ids):
        return {
            cluster_id: [
                self._row(news_id)
                for news_id, row in sorted(self.news.items())
                if row["cluster_id"] == cluster_id and row["candidate"]
            ]
            for cluster_id in cluster_ids
        }

    def promote(self, cluster_id, news_id):
        self.clusters[cluster_id]["representative"] = news_id
        return True

    def untitled(self, promote_size, since):
        return [
            cluster_id
            for cluster_id, cluster in self.clusters.items()
            if cluster["representative"] is not None and cluster["title"] is None
        ]

    def set_title(self, cluster_id, title):
        self.clusters[cluster_id]["title"] = title

    def pending(self, promote_size):
        return sum(
            1
            for cluster in self.clusters.values()
            if cluster["representative"] and cluster["member_count"] >= promote_size
        )


@pytest.fixture
def wired(monkeypatch):
    """잡의 저장소 호출을 FakeStore 로, 제목 LLM 을 가짜로 갈아끼운다."""
    from pipelines.news import config
    from pipelines.news.jobs import cluster_articles as job

    monkeypatch.setenv("NEWS_CLUSTER_WINDOW_DAYS", "7")
    monkeypatch.setenv("NEWS_CLUSTER_BACKWARD_DAYS", "1")
    monkeypatch.setenv("NEWS_CLUSTER_THRESHOLD", "0.35")
    monkeypatch.setenv("NEWS_CLUSTER_DESCRIPTION_WEIGHT", "0.4")
    monkeypatch.setenv("NEWS_CLUSTER_LEAD_CHARS", "200")
    monkeypatch.setenv("NEWS_CLUSTER_PROMOTE_SIZE", "10")
    monkeypatch.setenv("NEWS_CLUSTER_PROMOTE_RETRY_DAYS", "3")
    monkeypatch.setenv("NEWS_CLUSTER_IDF_DAYS", "90")
    monkeypatch.setenv("NEWS_CLUSTER_REPRESENTATIVE_MIN_CHARS", "200")
    monkeypatch.setenv("NEWS_LLM_MAX_CONCURRENCY", "2")
    config.get_news_settings.cache_clear()

    store = FakeStore()
    calls: dict[str, list] = {"title_input": []}

    monkeypatch.setattr(job, "fetch_unclustered_news", store.unclustered)
    monkeypatch.setattr(job, "fetch_cluster_seeds", store.seeds)
    monkeypatch.setattr(job, "fetch_idf_table", lambda since: IdfTable())
    monkeypatch.setattr(job, "record_assignments", store.record)
    monkeypatch.setattr(job, "fetch_promotable_cluster_ids", store.promotable)
    monkeypatch.setattr(job, "fetch_cluster_candidates", store.candidates)
    monkeypatch.setattr(job, "promote_cluster", store.promote)
    monkeypatch.setattr(job, "fetch_untitled_promoted", store.untitled)
    monkeypatch.setattr(job, "update_cluster_title", store.set_title)
    monkeypatch.setattr(job, "count_pending_representatives", store.pending)

    def fake_title_clusters(clusters, max_concurrency, max_chars):
        calls["title_input"].append(
            {cluster_id: [h.title for h in headlines] for cluster_id, headlines in clusters.items()}
        )
        return {cluster_id: "양극재 공급계약" for cluster_id in clusters}

    monkeypatch.setattr(job, "title_clusters", fake_title_clusters)

    yield job, store, calls
    config.get_news_settings.cache_clear()


def _add_same_event(store: FakeStore, count: int, start: int = 0) -> None:
    for index in range(start, start + count):
        store.add(published_at=T0 + timedelta(minutes=index))


def test_cluster_is_promoted_at_tenth_article_with_ten_headlines(wired):
    job, store, calls = wired

    # 런 1: 9건 — 수집 중. 대표도 제목도 없다.
    _add_same_event(store, 9)
    assert job.assign() == {"articles": 9, "created": 1, "updated": 0, "failed": 0}
    result = job.promote()

    [cluster] = store.clusters.values()
    assert (cluster["member_count"], cluster["original_size"]) == (9, 9)
    assert cluster["representative"] is None
    assert result == {"promoted": 0, "titled": 0, "pending_triples": 0}

    # 런 2: 10번째 — 저장된 본문으로 대표를 정하고, 후보 10건의 제목으로 이름을 짓는다.
    _add_same_event(store, 1, start=9)
    assert job.assign() == {"articles": 1, "created": 0, "updated": 1, "failed": 0}
    result = job.promote()

    assert (cluster["member_count"], cluster["original_size"]) == (10, 10)
    assert cluster["representative"] is not None
    assert cluster["title"] == "양극재 공급계약"
    assert calls["title_input"] == [{1: [TITLE] * 10}]
    assert result == {"promoted": 1, "titled": 1, "pending_triples": 1}

    # 런 3: 11번째 — count 만 오른다. 후보는 늘지 않고 제목도 다시 짓지 않는다.
    _add_same_event(store, 1, start=10)
    job.assign()
    result = job.promote()

    assert (cluster["member_count"], cluster["original_size"]) == (10, 11)
    assert [row["candidate"] for row in store.news.values()].count(True) == 10
    assert (result["promoted"], result["titled"]) == (0, 0)
    assert len(calls["title_input"]) == 1


def test_backlog_larger_than_promote_size_stops_candidates_at_ten(wired):
    job, store, calls = wired

    # 백필: 한 런에 같은 사건 기사 25건이 밀려 있다
    _add_same_event(store, 25)
    assert job.assign() == {"articles": 25, "created": 1, "updated": 0, "failed": 0}

    [cluster] = store.clusters.values()
    assert (cluster["member_count"], cluster["original_size"]) == (10, 25)
    assert job.promote()["promoted"] == 1
    assert calls["title_input"] == [{1: [TITLE] * 10}]


def test_assign_without_unclustered_news_reads_nothing_else(wired, monkeypatch):
    job, store, _ = wired

    def fail(*args):
        raise AssertionError("판정 대상이 없는데 시드를 읽음")

    monkeypatch.setattr(job, "fetch_cluster_seeds", fail)

    assert job.assign() == RESULT_ZERO


def test_article_left_unclustered_by_failed_record_is_judged_next_run(wired, monkeypatch):
    job, store, _ = wired
    attempts: list[int] = []

    def flaky(assignments, items, documents, keyword_count):
        attempts.append(len(items))
        if len(attempts) == 1:
            return {"created": 0, "updated": 0, "failed": len(assignments)}
        return store.record(assignments, items, documents, keyword_count)

    monkeypatch.setattr(job, "record_assignments", flaky)
    store.add()

    assert job.assign() == {"articles": 1, "created": 0, "updated": 0, "failed": 1}
    assert len(store.unclustered()) == 1

    # 다음 런이 같은 기사를 다시 읽어 판정한다
    assert job.assign() == {"articles": 1, "created": 1, "updated": 0, "failed": 0}
    assert store.unclustered() == []


def test_contract_and_cancellation_within_window_share_a_cluster(wired):
    job, store, _ = wired
    store.add("엘앤에프, 삼성SDI와 양극재 공급 계약 체결", published_at=T0)
    store.add("엘앤에프, 삼성SDI와 양극재 공급 계약 취소", published_at=T0 + timedelta(days=3))
    store.add("현대차, 미국 조지아 공장 증설 발표", "현대차가 공장을 증설한다. " * 20, T0)

    job.assign()

    sizes = sorted(cluster["original_size"] for cluster in store.clusters.values())
    assert sizes == [1, 2]


def _candidate(news_id: int, body: str = BODY) -> dict:
    return {"_news_id": news_id, "title": TITLE, "text": body, "published_at": T0}


def test_promote_isolates_failures_and_skips_clusters_without_body(wired, monkeypatch):
    job, store, _ = wired
    promoted: list[tuple[int, int]] = []

    # 1: 승격 기록이 DB 오류로 실패, 2: 후보 전부에 본문이 없음(옛 행), 3: 후보 행이 없음, 4: 정상
    monkeypatch.setattr(job, "fetch_promotable_cluster_ids", lambda size, since: [1, 2, 3, 4])
    monkeypatch.setattr(
        job,
        "fetch_cluster_candidates",
        lambda ids: {
            1: [_candidate(11)],
            2: [_candidate(21, body="")],
            4: [_candidate(41), _candidate(42)],
        },
    )

    def fake_promote(cluster_id, news_id):
        if cluster_id == 1:
            raise RuntimeError("db down")
        promoted.append((cluster_id, news_id))
        return True

    monkeypatch.setattr(job, "promote_cluster", fake_promote)

    result = job.promote()

    # 나머지가 실패해도 4번은 승격된다. 본문이 같으면 먼저 발행된 후보가 대표다.
    assert promoted == [(4, 41)]
    assert result["promoted"] == 1


def test_promote_does_not_count_cluster_already_promoted_by_another_run(wired, monkeypatch):
    job, store, _ = wired
    monkeypatch.setattr(job, "fetch_promotable_cluster_ids", lambda size, since: [4])
    monkeypatch.setattr(job, "fetch_cluster_candidates", lambda ids: {4: [_candidate(41)]})
    monkeypatch.setattr(job, "promote_cluster", lambda cluster_id, news_id: False)

    assert job.promote()["promoted"] == 0


def test_promote_leaves_title_null_when_llm_fails_and_reports_pending(wired, monkeypatch):
    job, store, _ = wired
    _add_same_event(store, 10)
    job.assign()
    monkeypatch.setattr(job, "title_clusters", lambda clusters, max_concurrency, max_chars: {})

    result = job.promote()

    [cluster] = store.clusters.values()
    assert cluster["title"] is None
    # 제목이 없어도 대표는 정해졌으므로 삼중항 DAG 는 깨운다
    assert result == {"promoted": 1, "titled": 0, "pending_triples": 1}


def _capture_idf(monkeypatch, job) -> list[IdfTable]:
    """assign_online 이 받은 IDF 표를 모은다. 판정은 원래 함수가 한다."""
    seen: list[IdfTable] = []
    original = job.assign_online

    def spy(documents, published_ats, seeds, idf, **kwargs):
        seen.append(idf)
        return original(documents, published_ats, seeds, idf, **kwargs)

    monkeypatch.setattr(job, "assign_online", spy)
    return seen


def test_assign_uses_stored_idf_table_by_default(wired, monkeypatch):
    job, store, _ = wired
    seen = _capture_idf(monkeypatch, job)

    _add_same_event(store, 3)
    job.assign()

    assert seen[0].document_count == 0


def test_assign_adds_batch_frequency_when_requested(wired, monkeypatch):
    job, store, _ = wired
    seen = _capture_idf(monkeypatch, job)

    # 초기 백필: 저장된 IDF 표가 비어 있어도 배치에서 센 문서 수로 판정한다
    _add_same_event(store, 3)
    assert job.assign(include_batch_idf=True) == {
        "articles": 3,
        "created": 1,
        "updated": 0,
        "failed": 0,
    }

    assert seen[0].document_count == 3
    assert seen[0].document_frequency["엘앤에프"] == 3


def test_count_unclustered_reads_repository(wired, monkeypatch):
    job, store, _ = wired
    monkeypatch.setattr(job, "count_unclustered_news", lambda: len(store.unclustered()))

    _add_same_event(store, 4)
    assert job.count_unclustered() == 4

    job.assign()
    assert job.count_unclustered() == 0
