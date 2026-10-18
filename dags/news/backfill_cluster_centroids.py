"""기존 클러스터를 보존한 채 V14 가 추가한 컬럼 채우기 — 수동 1회.

    vectorize_articles ──► rebuild_idf ──► compute_cluster_centroids

`scripts/recluster_news.py --apply` 와 달리 클러스터를 지우지 않는다. news.cluster_id·Event·트리플·
요약·제목·이슈 연결을 모두 보존하고, term_weights 없는 기사 전량을 벡터화한 뒤 IDF 사전을 다시
만들고, 멤버 term_weights 합으로 news_clusters 의 centroids·mean_published_at·member_count 를
채운다. 옛 모델이 만든 클러스터 구성은 바뀌지 않는다 — 목적은 다음 판정 런부터 새 기사가 옛
클러스터와도 정상적으로 비교되게 하는 것이다. LLM 을 부르지 않는다.

멱등이라 여러 번 돌려도 같은 값이다. `news_cluster_articles` 와 겹쳐 돌아도 깨지지는 않지만, 판정
런이 읽은 시드와 이 DAG 가 쓰는 값이 어긋날 수 있으니 판정 DAG 를 잠시 끄고 돌린다.
"""

from __future__ import annotations

from datetime import datetime, timedelta

try:
    from airflow.sdk import dag, task
except ImportError:
    dag = None
    task = None


if dag and task:

    @dag(
        dag_id="news_backfill_cluster_centroids",
        start_date=datetime(2026, 1, 1),
        schedule=None,
        catchup=False,
        max_active_runs=1,
        tags=["news", "backfill", "manual"],
        doc_md=__doc__,
    )
    def news_backfill_cluster_centroids():
        @task(retries=1, retry_delay=timedelta(minutes=5))
        def vectorize_articles() -> dict[str, int]:
            from pipelines.news.jobs.vectorize_articles import run

            return run()

        @task(retries=1, retry_delay=timedelta(minutes=10))
        def rebuild_idf() -> dict[str, int]:
            from pipelines.news.jobs.rebuild_idf import run

            return run()

        @task(retries=1, retry_delay=timedelta(minutes=5))
        def compute_cluster_centroids() -> dict[str, int]:
            from pipelines.news.jobs.compute_cluster_centroids import run

            return run()

        vectorize_articles() >> rebuild_idf() >> compute_cluster_centroids()

    news_backfill_cluster_centroids()
