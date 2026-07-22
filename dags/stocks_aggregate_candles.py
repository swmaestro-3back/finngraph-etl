from __future__ import annotations

from datetime import datetime

try:
    from airflow.decorators import dag, task
except ImportError:
    dag = None
    task = None


if dag and task:

    @dag(
        dag_id="stocks_aggregate_candles",
        start_date=datetime(2026, 1, 1),
        schedule="*/5 9-16 * * 1-5",
        catchup=False,
        max_active_runs=1,
        tags=["stocks", "candles", "aggregate"],
    )
    def stocks_aggregate_candles():
        @task(retries=2)
        def aggregate_candles() -> None:
            from pipelines.stocks.jobs.aggregate_candles import run

            run()

        aggregate_candles()

    stocks_aggregate_candles()
