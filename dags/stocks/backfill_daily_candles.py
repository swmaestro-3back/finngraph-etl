from __future__ import annotations

from datetime import datetime

try:
    from airflow.sdk import dag, task
except ImportError:
    dag = None
    task = None


if dag and task:

    @dag(
        dag_id="stocks_backfill_daily_candles",
        start_date=datetime(2026, 1, 1),
        schedule=None,
        catchup=False,
        max_active_runs=1,
        tags=["stocks"],
    )
    def stocks_backfill_daily_candles():
        @task
        def backfill_daily_candles() -> None:
            from pipelines.stocks.jobs.backfill_daily_candles import run

            run()

        backfill_daily_candles()

    stocks_backfill_daily_candles()
