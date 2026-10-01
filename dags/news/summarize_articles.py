"""뉴스 요약 — 삼중항 추출을 마친 미요약 기사 전량.

요약 대상이 `news.triple_extracted = TRUE` 인 기사라 수집이 아니라 삼중항 추출 뒤에 걸려 있다 —
`triples_extract_triples` 가 발행하는 `etl://triples/extracted` Asset 으로 깨어난다.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

try:
    from airflow.sdk import Asset, dag, task
except ImportError:
    Asset = None
    dag = None
    task = None


if dag and task:

    @dag(
        dag_id="news_summarize_articles",
        start_date=datetime(2026, 1, 1),
        schedule=Asset("etl://triples/extracted"),
        catchup=False,
        max_active_runs=1,
        tags=["news"],
        doc_md=__doc__,
    )
    def news_summarize_articles():
        @task(retries=1, retry_delay=timedelta(minutes=10))
        def summarize_articles() -> dict[str, Any]:
            from pipelines.news.jobs.summarize_articles import run

            result = run()
            return {"fetched": result["fetched"], "saved": result["saved"]}

        summarize_articles()

    news_summarize_articles()
