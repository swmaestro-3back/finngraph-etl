"""미국 상장사(NYSE·NASDAQ) 적재 — Postgres → Neo4j.

`companies_crawl_us`가 발행하는 `etl://companies/us_crawled` Asset으로 깨어난다. 크롤링과
적재를 별도 DAG로 나눈 이유·경로를 XCom으로 넘기지 않는 이유는 `crawl_us.py` 참고. 이
DAG는 `resolve_paths`로 최신 날짜 폴더의 us.json·us_overview.json을 스스로 찾는다.
"""

from __future__ import annotations

from datetime import datetime, timedelta

try:
    from airflow.sdk import Asset, dag, task
except ImportError:
    Asset = None
    dag = None
    task = None


if dag and task:

    @dag(
        dag_id="companies_load_us",
        start_date=datetime(2026, 1, 1),
        schedule=Asset("etl://companies/us_crawled"),
        catchup=False,
        max_active_runs=1,
        tags=["companies"],
        default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    )
    def companies_load_us():
        @task(multiple_outputs=True)
        def resolve_paths() -> dict[str, str]:
            from pipelines.companies.jobs.resolve_us_paths import run

            return run()

        @task
        def load_postgres(index_path: str, overview_path: str) -> int:
            from pipelines.companies.jobs.load_us_postgres import run

            return run(index_path, overview_path)

        @task
        def load_neo4j(index_path: str) -> int:
            from pipelines.companies.jobs.load_us_neo4j import run

            return run(index_path)

        paths = resolve_paths()
        loaded = load_postgres(paths["index_path"], paths["overview_path"])
        neo4j = load_neo4j(paths["index_path"])
        loaded >> neo4j

    companies_load_us()
