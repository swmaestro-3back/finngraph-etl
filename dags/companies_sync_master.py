from __future__ import annotations

from datetime import datetime

try:
    from airflow.decorators import dag, task
except ImportError:
    dag = None
    task = None


if dag and task:

    @dag(
        dag_id="companies_sync_master",
        start_date=datetime(2026, 1, 1),
        # 종목 마스터 동기화(stocks_sync_master, 평일 08:00) 이후 실행.
        # stocks를 읽어 법인 축으로 옮기는 작업이라 선행 DAG가 끝난 뒤여야 한다.
        schedule="30 8 * * 1-5",
        catchup=False,
        max_active_runs=1,
        tags=["companies", "master"],
    )
    def companies_sync_master():
        @task(retries=2)
        def sync_master() -> None:
            from pipelines.companies.jobs.sync_company_master import run

            run()

        sync_master()

    companies_sync_master()
