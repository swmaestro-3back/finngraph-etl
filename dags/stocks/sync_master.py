from __future__ import annotations

from datetime import datetime

try:
    from airflow.sdk import Asset, dag, task
except ImportError:
    Asset = None
    dag = None
    task = None


if dag and task:
    # 종목 마스터 확정 신호. companies_sync_master가 구독한다.
    stocks_master_synced = Asset("etl://stocks/master")

    @dag(
        dag_id="stocks_sync_master",
        start_date=datetime(2026, 1, 1),
        schedule="0 8 * * 1-5",
        catchup=False,
        max_active_runs=1,
        tags=["stocks"],
    )
    def stocks_sync_master():
        @task(retries=2, outlets=[stocks_master_synced])
        def sync_master() -> None:
            from pipelines.stocks.jobs.sync_master import run

            run()

        sync_master()

    stocks_sync_master()
