from __future__ import annotations

from datetime import timedelta
from typing import Any

try:
    import pendulum
    from airflow.sdk import Asset, dag, task
except ImportError:
    pendulum = None
    Asset = None
    dag = None
    task = None


if dag and task:
    news_filtered = Asset("etl://news/filtered")

    @dag(
        dag_id="news_filter_meaningless",
        start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
        schedule=Asset("etl://news/collected"),
        catchup=False,
        max_active_runs=1,
        tags=["news"],
    )
    def news_filter_meaningless():

        @task(retries=1, retry_delay=timedelta(minutes=10))
        def judge() -> dict[str, Any]:
            from pipelines.news.jobs.filter_meaningless import judge_unchecked_news

            result = judge_unchecked_news(mode="async")

            return {
                "kept_ids": result["kept_ids"],
                "dropped_ids": result["dropped_ids"],
                "fetched": result["fetched"],
                "kept": result["kept"],
                "dropped": result["dropped"],
            }

        @task(retries=3, retry_delay=timedelta(minutes=2), outlets=[news_filtered])
        def mark(judged: dict[str, Any]) -> None:
            from pipelines.news.jobs.filter_meaningless import mark_material_results

            mark_material_results(judged["kept_ids"], judged["dropped_ids"])

        mark(judge())

    news_filter_meaningless()
