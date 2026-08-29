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
        # 뉴스가 뜸한 심야(22~05시)는 건너뛴다. 06~21시 매 정각, KST 기준.
        schedule="0 6-21 * * *",
        catchup=False,
        max_active_runs=1,
        tags=["news", "triples"],
    )
    def news_pipeline():
        @task(retries=2, retry_delay=timedelta(minutes=5))
        def collect_news() -> dict[str, Any]:
            from pipelines.news.jobs.collect_news import run

            return run()

        @task(retries=1, retry_delay=timedelta(minutes=10))
        def filter_meaningless() -> dict[str, Any]:
            from pipelines.news.jobs.filter_meaningless_news import run

            result = run()
            # removed_items에는 기사 원문이 들어 있어 XCom에는 카운트만 남긴다
            return {
                "fetched": result["fetched"],
                "kept": result["kept"],
                "dropped": result["dropped"],
            }

        @task(retries=1, retry_delay=timedelta(minutes=10))
        def extract_triples() -> dict[str, int]:
            from pipelines.triples.jobs.extract_triples import run

            return run()

        @task(retries=1, retry_delay=timedelta(minutes=10))
        def summarize_articles() -> dict[str, Any]:
            from pipelines.news.jobs.summarize_articles import run

            result = run()
            return {"fetched": result["fetched"], "saved": result["saved"]}

        (collect_news() >> filter_meaningless() >> extract_triples() >> summarize_articles())

    news_pipeline()
