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
        dag_id="news_collect_keyword_search",
        start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
        schedule="0 * * * *",
        catchup=False,
        max_active_runs=1,
        tags=["news"],
    )
    def news_collect_keyword_search():
        @task(retries=2, retry_delay=timedelta(minutes=5), outlets=[news_collected])
        def collect_keyword_search() -> None:
            from pipelines.news.jobs.collect_keyword_search import run

            run()

        collect_keyword_search()

    news_collect_keyword_search()
