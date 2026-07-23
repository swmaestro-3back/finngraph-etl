from __future__ import annotations

from datetime import timedelta
from typing import Any

try:
    import pendulum
    from airflow.decorators import dag, task
except ImportError:
    pendulum = None
    dag = None
    task = None


if dag and task:

    @dag(
        dag_id="news_filter_meaningless",
        start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
        schedule="0 * * * *",
        catchup=False,
        max_active_runs=1,
        tags=["news", "filter", "material-event"],
    )
    def news_filter_meaningless():

        @task(retries=1, retry_delay=timedelta(minutes=10))
        def judge() -> dict[str, Any]:
            from pipelines.news.jobs.filter_meaningless_news import judge_unchecked_news

            result = judge_unchecked_news()

            return {
                "kept_ids": result["kept_ids"],
                "dropped_ids": result["dropped_ids"],
                "fetched": result["fetched"],
                "kept": result["kept"],
                "dropped": result["dropped"],
            }

        @task(retries=3, retry_delay=timedelta(minutes=2))
        def mark(judged: dict[str, Any]) -> None:
            from pipelines.news.jobs.filter_meaningless_news import mark_material_results

            mark_material_results(judged["kept_ids"], judged["dropped_ids"])

        mark(judge())

    news_filter_meaningless()
