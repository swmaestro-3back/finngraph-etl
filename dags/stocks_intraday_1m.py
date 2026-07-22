from __future__ import annotations

from datetime import datetime

try:
    from airflow.decorators import dag, task
except ImportError:
    dag = None
    task = None


if dag and task:

    @dag(
        dag_id="stocks_intraday_1m",
        start_date=datetime(2026, 1, 1),
        schedule="*/5 9-15 * * 1-5",
        catchup=False,
        max_active_runs=1,
        tags=["stocks", "intraday", "1m"],
    )
    def stocks_intraday_1m():
        @task(retries=2)
        def collect_intraday_1m() -> None:
            from pipelines.stocks.jobs.collect_intraday_1m import run

            run()

        collect_intraday_1m()

    stocks_intraday_1m()
