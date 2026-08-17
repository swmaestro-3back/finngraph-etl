from __future__ import annotations

from datetime import datetime

try:
    from airflow.sdk import Asset, dag, task
except ImportError:
    Asset = None
    dag = None
    task = None


if dag and task:

    @dag(
        dag_id="companies_sync_master",
        start_date=datetime(2026, 1, 1),
        # stocks를 읽어 법인 축으로 옮기는 작업이라 종목 마스터 확정 직후 기동한다.
        schedule=Asset("etl://stocks/master"),
        catchup=False,
        max_active_runs=1,
        tags=["companies"],
    )
    def companies_sync_master():
        @task(retries=2)
        def sync_master() -> None:
            from pipelines.companies.jobs.sync_master import run

            run()

        # 마스터 확정 직후 Neo4j에 상장사(name↔ticker)를 시드한다.
        # 테마 검증·적재와 삼중항 종목코드 채움이 이 시드를 전제로 동작한다.
        @task(retries=2)
        def seed_graph() -> None:
            from pipelines.companies.jobs.seed_graph import run

            run()

        sync_master() >> seed_graph()

    companies_sync_master()
