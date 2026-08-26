from __future__ import annotations

from datetime import timedelta
from typing import Any

try:
    import pendulum
    from airflow.sdk import dag, task
except ImportError:
    pendulum = None
    dag = None
    task = None


if dag and task:

    @dag(
        dag_id="news_pipeline",
        start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
        schedule="0 * * * *",
        catchup=False,
        max_active_runs=1,
        tags=["news", "triples"],
    )
    def news_pipeline():
        @task(retries=2, retry_delay=timedelta(minutes=5))
        def collect_and_cluster() -> dict[str, Any]:
            from pipelines.news.jobs.collect_and_cluster import run

            return run()

        @task(retries=1, retry_delay=timedelta(minutes=10))
        def extract_triples() -> dict[str, int]:
            from pipelines.triples.jobs.extract import run

            return run()

        @task(retries=1, retry_delay=timedelta(minutes=10))
        def summarize() -> dict[str, Any]:
            from pipelines.news.jobs.summarize import run

            result = run()
            return {"fetched": result["fetched"], "saved": result["saved"]}

        collect_and_cluster() >> extract_triples() >> summarize()

    news_pipeline()
