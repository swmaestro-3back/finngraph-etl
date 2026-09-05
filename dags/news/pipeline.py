from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

try:
    from airflow.sdk import dag, task
except ImportError:
    dag = None
    task = None


if dag and task:

    @dag(
        dag_id="news_pipeline",
        start_date=datetime(2026, 1, 1),
        # 뉴스가 뜸한 심야(22~05시)는 건너뛴다. 06~21시 매 정각, KST 기준.
        schedule="0 6-21 * * *",
        catchup=False,
        max_active_runs=1,
        tags=["news", "triples", "events"],
    )
    def news_pipeline():
        @task(retries=2, retry_delay=timedelta(minutes=5))
        def collect_articles() -> dict[str, Any]:
            from pipelines.news.jobs.collect_articles import run

            return run()

        @task(retries=1, retry_delay=timedelta(minutes=10))
        def extract_triples() -> dict[str, int]:
            from pipelines.triples.jobs.extract_triples import run

            return run()

        @task(retries=1, retry_delay=timedelta(minutes=10))
        def summarize_articles() -> dict[str, Any]:
            from pipelines.news.jobs.summarize_articles import run

            result = run()
            return {"fetched": result["fetched"], "saved": result["saved"]}

        @task(retries=1, retry_delay=timedelta(minutes=10))
        def promote_events() -> dict[str, int]:
            """news_clusters 를 Neo4j Event 로 승격·갱신한다.

            전제: 재생성된 0002 와 0003 이 neo4j-init 으로 적용돼 있고,
            companies_sync_master.seed_graph 가 한 번은 성공해 KRX is_listed 가 채워져 있을 것.
            그렇지 않아도 갱신이 나중에 간선을 붙이지만 첫 며칠의 created_without_edges 가 높다.
            """
            from pipelines.events.jobs.promote_events import run

            return run()

        collect_articles() >> extract_triples() >> summarize_articles() >> promote_events()

    news_pipeline()
