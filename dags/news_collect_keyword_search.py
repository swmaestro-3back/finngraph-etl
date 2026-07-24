from __future__ import annotations

from datetime import timedelta

try:
    import pendulum
    from airflow.sdk import dag, task
except ImportError:
    pendulum = None
    dag = None
    task = None


if dag and task:

    @dag(
        dag_id="news_collect_keyword_search",
        start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
        schedule="0 * * * *",
        catchup=False,
        max_active_runs=1,
        tags=["news", "keyword_search"],
    )
    def news_collect_keyword_search():
        @task(retries=2, retry_delay=timedelta(minutes=5))
        def collect_keyword_search() -> None:
            from pipelines.news.jobs.collect_keyword_search import run

            run()

        collect_keyword_search()

    news_collect_keyword_search()
