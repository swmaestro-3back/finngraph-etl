from __future__ import annotations

from datetime import timedelta

try:
    import pendulum
    from airflow.sdk import Asset, dag, task
except ImportError:
    pendulum = None
    Asset = None
    dag = None
    task = None


if dag and task:
    news_collected = Asset("etl://news/collected")

    @dag(
        dag_id="news_collect_headline",
        start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
        schedule="*/30 * * * *",
        catchup=False,
        max_active_runs=1,
        tags=["news"],
    )
    def news_collect_headline():
        @task(retries=2, retry_delay=timedelta(minutes=5), outlets=[news_collected])
        def collect_headline() -> None:
            from pipelines.news.jobs.collect_headline import run

            run()

        collect_headline()

    news_collect_headline()
