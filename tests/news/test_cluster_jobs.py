"""클러스터 잡 단위 테스트. 저장소는 메모리 대역, LLM 은 가짜다. Kiwi 토큰화는 실제로 돈다."""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest

import pipelines.news.jobs.assign_clusters as assign_job
import pipelines.news.jobs.promote_clusters as promote_job
import pipelines.news.jobs.rebuild_idf as rebuild_job
import pipelines.news.jobs.vectorize_articles as vectorize_job
from pipelines.news import config
from pipelines.news.transformers.clustering.document import FIELDS
from pipelines.news.transformers.clustering.online import ClusterSeed
from pipelines.news.transformers.clustering.vectorize import IdfTable
from pipelines.news.utils.date_utils import SEOUL_TIMEZONE

T0 = datetime(2026, 9, 9, 10, tzinfo=SEOUL_TIMEZONE)
TITLE = "엘앤에프, 삼성SDI에 양극재 공급 계약"
BODY = "엘앤에프가 삼성SDI에 양극재를 공급한다. 원료는 포스코퓨처엠이 댄다. " * 10


class FakeStore:
    """news·news_clusters·IDF 저장소의 메모리 대역. 런 사이에 상태가 이어진다."""

    def __init__(self):
        self.news: dict[int, dict] = {}
        self.clusters: dict[int, dict] = {}
        self.fail_vectorize_ids: set[int] = set()
        self.fail_assign_ids: set[int] = set()
        self.rebuilt_since: datetime | None = None

    def add(self, title=TITLE, body=BODY, published_at=T0, company_ids=(1,)) -> int:
        news_id = 1000 + len(self.news)
        self.news[news_id] = {
            "title": title,
            "text": body,
            "company_ids": list(company_ids),
            "published_at": published_at,
            "term_weights": None,
            "cluster_id": None,
            "score": None,
            "features": None,
        }
        return news_id

    # news.py
    def fetch_unvectorized_news(self):
        return [
            {"_news_id": i, "title": n["title"], "text": n["text"], "company_ids": n["company_ids"]}
            for i, n in sorted(self.news.items())
            if n["term_weights"] is None
        ]

    def save_term_weights(self, rows):
        counts = {"saved": 0, "failed": 0}
        for news_id, term_weights in rows:
            if news_id in self.fail_vectorize_ids:
                counts["failed"] += 1
                continue
            self.news[news_id]["term_weights"] = term_weights
            counts["saved"] += 1
        return counts

    def fetch_unclustered_news(self):
        ids = sorted(
            (
                i
                for i, n in self.news.items()
                if n["cluster_id"] is None and n["term_weights"] is not None
            ),
            key=lambda i: (self.news[i]["published_at"], i),
        )
        return [
            {
                "_news_id": i,
                "title": self.news[i]["title"],
                "term_weights": self.news[i]["term_weights"],
                "published_at": self.news[i]["published_at"],
            }
            for i in ids
        ]

    def count_unclustered_news(self):
        return len(self.fetch_unclustered_news())

    def fetch_cluster_member_articles(self, cluster_ids):
        return {
            c: [
                {
                    "_news_id": i,
                    "title": n["title"],
                    "term_weights": n["term_weights"],
                    "body_length": len(n["text"]),
                    "published_at": n["published_at"],
                }
                for i, n in sorted(self.news.items(), key=lambda kv: (kv[1]["published_at"], kv[0]))
                if n["cluster_id"] == c
            ]
            for c in cluster_ids
        }

    # news_cluster_idf.py
    def load_idf_tables(self):
        return {field: IdfTable() for field in FIELDS}  # 실제 계약: 네 필드 전부, 비어 있음

    def rebuild_idf_tables(self, since):
        self.rebuilt_since = since
        return {"terms": 0, "documents": 0}

    # news_clusters.py
    def fetch_cluster_seeds(self, since, first_since):
        return [
            ClusterSeed(
                cid,
                {f: dict(t) for f, t in c["centroids"].items()},
                c["member_count"],
                c["first"],
                c["mean"],
                c["last"],
            )
            for cid, c in self.clusters.items()
            if c["last"] >= since and c["first"] >= first_since
        ]

    def record_assignments(self, assignments, items, keyword_count):
        from pipelines.news.transformers.clustering.document import with_all
        from pipelines.news.transformers.clustering.online import top_keywords

        created = updated = failed = 0
        for a in assignments:
            news_ids = [items[i]["_news_id"] for i in a.members]
            if set(news_ids) & self.fail_assign_ids:
                failed += 1
                continue
            keywords = top_keywords(with_all(a.centroids)["all"], keyword_count)
            if a.seed is None:
                cid = len(self.clusters) + 1
                self.clusters[cid] = {
                    "centroids": a.centroids,
                    "member_count": a.article_count,
                    "keywords": keywords,
                    "first": a.first_published_at,
                    "mean": a.mean_published_at,
                    "last": a.last_published_at,
                    "representative": None,
                    "title": None,
                }
                created += 1
            else:
                cid = a.seed.cluster_id
                c = self.clusters[cid]
                c.update(
                    centroids=a.centroids,
                    member_count=a.article_count,
                    keywords=keywords,
                    first=min(c["first"], a.first_published_at),
                    mean=a.mean_published_at,
                    last=max(c["last"], a.last_published_at),
                )
                updated += 1
            for news_id, score, features in zip(news_ids, a.scores, a.features, strict=True):
                self.news[news_id].update(cluster_id=cid, score=score, features=features)
        return {"created": created, "updated": updated, "failed": failed}

    def fetch_promotable_cluster_ids(self, promote_size, since):
        return [
            c
            for c, v in self.clusters.items()
            if v["representative"] is None and v["member_count"] >= promote_size
        ]

    def promote_cluster(self, cid, news_id):
        if self.news[news_id]["cluster_id"] != cid:
            raise ValueError("not a member")
        if self.clusters[cid]["representative"] is not None:
            return False
        self.clusters[cid]["representative"] = news_id
        return True

    def fetch_untitled_promoted(self, promote_size):
        return [
            c
            for c, v in self.clusters.items()
            if v["representative"] is not None
            and v["title"] is None
            and v["member_count"] >= promote_size
        ]

    def update_cluster_title(self, cid, title):
        self.clusters[cid]["title"] = title

    def count_pending_representatives(self, promote_size):
        return sum(
            1
            for v in self.clusters.values()
            if v["representative"] is not None and v["member_count"] >= promote_size
        )


@pytest.fixture
def store(monkeypatch):
    monkeypatch.setenv("NEWS_CLUSTER_PROMOTE_SIZE", "3")
    monkeypatch.setenv("NEWS_CLUSTER_THRESHOLD", "1.0")
    monkeypatch.setenv("NEWS_CLUSTER_TEXT_WEIGHTS", "1,1,1,1")
    monkeypatch.setenv("NEWS_CLUSTER_TIME_WEIGHTS", "0.1,0.1,0.1")
    config.get_news_settings.cache_clear()
    store = FakeStore()
    for module in (vectorize_job, rebuild_job, assign_job, promote_job):
        for name in dir(store):
            if hasattr(module, name) and not name.startswith("_"):
                monkeypatch.setattr(module, name, getattr(store, name))
    yield store
    config.get_news_settings.cache_clear()


def test_vectorize_fills_term_weights_and_isolates_failures(store):
    good = store.add(company_ids=(7, 3))
    bad = store.add(title="삼성전자 실적 발표", body="삼성전자가 실적을 발표했다. " * 10)
    store.fail_vectorize_ids.add(bad)

    result = vectorize_job.run()

    assert result == {"vectorized": 1, "failed": 1}
    weights = store.news[good]["term_weights"]
    assert set(weights) == {"title", "body", "entities"}
    # Kiwi 는 문맥에 따라 '삼성SDI' 를 NNP 하나 또는 복합명사로 보므로 가중치 값은 고정하지 않는다
    assert weights["title"]["삼성sdi"] > 0 and weights["body"]["삼성sdi"] > 0
    assert weights["entities"] == {"3": 1.0, "7": 1.0}
    assert store.news[bad]["term_weights"] is None  # 다음 런에 다시 시도된다
    assert vectorize_job.run() == {"vectorized": 0, "failed": 1}  # 실패분만 다시 본다


def test_vectorize_with_nothing_to_do(store):
    assert vectorize_job.run() == {"vectorized": 0, "failed": 0}


def test_rebuild_idf_uses_idf_days(store, monkeypatch):
    monkeypatch.setenv("NEWS_CLUSTER_IDF_DAYS", "30")
    config.get_news_settings.cache_clear()

    assert rebuild_job.run() == {"terms": 0, "documents": 0}

    age = datetime.now(SEOUL_TIMEZONE) - store.rebuilt_since
    assert timedelta(days=29, hours=23) < age < timedelta(days=30, minutes=5)


def test_assign_skips_articles_that_are_not_vectorized(store):
    store.add()
    assert assign_job.run() == {"articles": 0, "created": 0, "updated": 0, "failed": 0}
    assert assign_job.count_unclustered() == 0


def test_assign_creates_and_joins_clusters_across_runs(store):
    store.add()
    store.add(published_at=T0 + timedelta(hours=1))
    store.add(
        title="삼성전자 실적 발표", body="삼성전자가 실적을 발표했다. " * 10, company_ids=(2,)
    )
    vectorize_job.run()
    assert assign_job.count_unclustered() == 3
    first = assign_job.run()
    late = store.add(published_at=T0 + timedelta(days=1))
    vectorize_job.run()
    second = assign_job.run()

    assert (first["articles"], first["created"], first["updated"], first["failed"]) == (3, 2, 0, 0)
    assert (second["articles"], second["created"], second["updated"]) == (1, 0, 1)
    assert store.clusters[1]["member_count"] == 3 and store.news[late]["cluster_id"] == 1
    # 점수 Γ 와 피처 7개가 기록된다. 첫 기사는 둘 다 None
    assert store.news[1000]["score"] is None and store.news[1000]["features"] is None
    assert store.news[late]["score"] > 1.0
    assert set(store.news[late]["features"]) == {
        "title", "body", "all", "entities", "last", "mean", "first",
    }  # fmt: skip
    # centroids 는 서브벡터 3개의 합, keywords 는 title+body 상위 토큰
    assert set(store.clusters[1]["centroids"]) == {"title", "body", "entities"}
    assert store.clusters[1]["centroids"]["entities"] == {"1": 3.0}
    assert "삼성sdi" in store.clusters[1]["keywords"] and len(store.clusters[1]["keywords"]) <= 6
    assert store.clusters[1]["last"] == T0 + timedelta(days=1)


def test_assign_clusters_articles_without_companies_by_text(store):
    """Review Focus 1: news_companies 가 없는 기사도 title·body·all 코사인으로 같은 사건에 붙는다."""
    store.add(company_ids=())
    store.add(published_at=T0 + timedelta(hours=1), company_ids=())
    vectorize_job.run()

    result = assign_job.run()

    assert (result["created"], result["failed"]) == (1, 0)
    assert store.clusters[1]["member_count"] == 2
    assert store.clusters[1]["centroids"]["entities"] == {}
    assert store.news[1001]["features"]["entities"] == 0.0


def test_assign_isolates_cluster_failures(store):
    a = store.add()
    store.add(
        title="삼성전자 실적 발표", body="삼성전자가 실적을 발표했다. " * 10, company_ids=(2,)
    )
    store.fail_assign_ids.add(a)
    vectorize_job.run()

    result = assign_job.run()

    assert result["failed"] == 1 and result["created"] == 1
    assert store.news[a]["cluster_id"] is None  # 다음 런에 다시 판정된다


def _three_member_cluster(store):
    for hour in range(3):
        store.add(published_at=T0 + timedelta(hours=hour))
    vectorize_job.run()
    assign_job.run()


def test_promote_picks_representative_titles_and_counts_pending(store, monkeypatch):
    _three_member_cluster(store)
    titles_seen = []

    def fake_title(clusters, max_concurrency, max_chars):
        titles_seen.append({c: len(h) for c, h in clusters.items()})
        return {c: "양극재 공급 계약" for c in clusters}

    monkeypatch.setattr(promote_job, "title_clusters", fake_title)

    result = promote_job.run()

    assert result == {"promoted": 1, "titled": 1, "pending_triples": 1}
    assert store.clusters[1]["representative"] in store.news
    assert store.clusters[1]["title"] == "양극재 공급 계약"
    assert titles_seen == [{1: 3}]


def test_promote_caps_headlines(store, monkeypatch):
    monkeypatch.setenv("NEWS_CLUSTER_TITLE_HEADLINES", "2")
    config.get_news_settings.cache_clear()
    _three_member_cluster(store)
    seen = []
    monkeypatch.setattr(
        promote_job,
        "title_clusters",
        lambda clusters, max_concurrency, max_chars: (
            seen.append({c: len(h) for c, h in clusters.items()}) or {}
        ),
    )

    promote_job.run()

    assert seen == [{1: 2}]


def test_promote_leaves_title_null_when_llm_fails(store, monkeypatch):
    _three_member_cluster(store)
    monkeypatch.setattr(
        promote_job, "title_clusters", lambda clusters, max_concurrency, max_chars: {}
    )

    assert promote_job.run()["titled"] == 0
    assert store.clusters[1]["title"] is None
    assert promote_job.run()["promoted"] == 0  # 이미 승격됨
