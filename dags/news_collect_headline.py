from __future__ import annotations

from datetime import datetime, timedelta

try:
    from airflow.decorators import dag, task
except ImportError:
    dag = None
    task = None


if dag and task:

    @dag(
        dag_id="news_collect_headline",
        start_date=datetime(2026, 1, 1),
        schedule="*/30 * * * *",
        catchup=False,
        max_active_runs=1,
        tags=["news", "headline"],
    )
    def news_collect_headline():
        @task(retries=2, retry_delay=timedelta(minutes=5))
        def collect_headline() -> None:
            from pipelines.news.jobs.collect_headline import run

            run()

        collect_headline()

    news_collect_headline()
