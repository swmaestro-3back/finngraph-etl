from __future__ import annotations

from datetime import datetime

try:
    from airflow.decorators import dag, task
except ImportError:
    dag = None
    task = None


if dag and task:

    @dag(
        dag_id="stocks_sync_master",
        start_date=datetime(2026, 1, 1),
        schedule="0 8 * * 1-5",
        catchup=False,
        max_active_runs=1,
        tags=["stocks", "master"],
    )
    def stocks_sync_master():
        @task(retries=2)
        def sync_master() -> None:
            from pipelines.stocks.jobs.sync_stock_master import run

            run()

        sync_master()

    stocks_sync_master()
