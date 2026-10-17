from __future__ import annotations

from datetime import datetime

try:
    from airflow.sdk import Param, dag, task
except ImportError:
    Param = None
    dag = None
    task = None


if dag and task:

    @dag(
        dag_id="market_calendar_backfill_market_days",
        start_date=datetime(2026, 1, 1),
        schedule=None,
        catchup=False,
        max_active_runs=1,
        tags=["market_calendar", "backfill", "manual"],
        params={
            "days_back": Param(
                default=365,
                type="integer",
                minimum=1,
                title="되짚을 일수",
                description="오늘부터 과거로 채울 일수",
            ),
        },
    )
    def market_calendar_backfill_market_days():
        @task(retries=0)
        def backfill_market_days(params: dict) -> int:
            from pipelines.market_calendar.jobs.backfill_market_days import run

            return run(days_back=int(params["days_back"]))

        backfill_market_days()

    market_calendar_backfill_market_days()
