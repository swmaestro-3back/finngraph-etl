from __future__ import annotations

import asyncio
from datetime import datetime, timedelta

try:
    from airflow.sdk import dag, task
except ImportError:
    dag = None
    task = None


if dag and task:

    @dag(
        dag_id="triplets_etl",
        start_date=datetime(2026, 1, 1),
        schedule="0 * * * *",
        catchup=False,
        max_active_runs=1,
        tags=["triplets", "neo4j", "etl"],
    )
    def triplets_etl():

        # 뉴스별로 성공 즉시 마킹되므로 task 재시도는 미처리분만 다시 처리
        @task(retries=1, retry_delay=timedelta(minutes=10))
        def extract_and_load() -> dict[str, int]:
            from pipelines.triplets.jobs.extract_triplets import extract_unprocessed_triplets

            # 배치 크기 기본값(100개)은 job 모듈의 DEFAULT_LIMIT에서 관리한다
            return asyncio.run(extract_unprocessed_triplets())

        extract_and_load()

    triplets_etl()
