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

    @dag(
        dag_id="news_summarize",
        start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
        schedule=Asset("etl://news/relations"),
        catchup=False,
        max_active_runs=1,
        tags=["news"],
    )
    def news_summarize():

        @task(retries=1, retry_delay=timedelta(minutes=10))
        def summarize() -> dict[str, Any]:
            from pipelines.news.jobs.summarize import summarize_unsummarized_news

            result = summarize_unsummarized_news(mode="async")

            return {
                "rows": result["rows"],
                "fetched": result["fetched"],
                "summarized": result["summarized"],
            }

        @task(retries=3, retry_delay=timedelta(minutes=2))
        def save(summarized: dict[str, Any]) -> None:
            from pipelines.news.jobs.summarize import save_summaries

            save_summaries(summarized["rows"])

        save(summarize())

    news_summarize()
