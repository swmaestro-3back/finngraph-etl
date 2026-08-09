from __future__ import annotations

from datetime import datetime

try:
    from airflow.sdk import dag, task
except ImportError:
    dag = None
    task = None


if dag and task:

    @dag(
        dag_id="stocks_daily_backfill",
        start_date=datetime(2026, 1, 1),
        schedule=None,
        catchup=False,
        max_active_runs=1,
        tags=["stocks"],
    )
    def stocks_daily_backfill():
        @task
        def run_backfill_daily() -> None:
            from pipelines.stocks.jobs.backfill_daily import run

            run()

        run_backfill_daily()

    stocks_daily_backfill()
