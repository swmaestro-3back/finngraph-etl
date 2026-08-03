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
        dag_id="news_summarize",
        start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
        schedule="0 * * * *",
        catchup=False,
        max_active_runs=1,
        tags=["news", "summarize"],
    )
    def news_summarize():

        @task(retries=1, retry_delay=timedelta(minutes=10))
        def summarize() -> dict[str, Any]:
            from pipelines.news.jobs.summarize_news import summarize_unsummarized_news

            # provider는 NEWS_SUMMARY_PROVIDER로 선택 (기본 bedrock, vllm 선택 가능).
            # async 모드: vLLM은 httpx, Bedrock은 boto3+to_thread — 모두 Semaphore 동시 4요청.
            result = summarize_unsummarized_news(mode="async", max_concurrency=4)

            return {
                "rows": result["rows"],
                "fetched": result["fetched"],
                "summarized": result["summarized"],
            }

        @task(retries=3, retry_delay=timedelta(minutes=2))
        def save(summarized: dict[str, Any]) -> None:
            from pipelines.news.jobs.summarize_news import save_summaries

            save_summaries(summarized["rows"])

        save(summarize())

    news_summarize()
