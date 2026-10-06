from __future__ import annotations

from datetime import datetime, timedelta
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from airflow.sdk import dag, task
else:
    try:
        from airflow.sdk import dag, task
    except ImportError:
        dag = None
        task = None


if dag and task:

    @dag(
        dag_id="stocks_weekly_period_candles",
        start_date=datetime(2026, 1, 1),
        schedule="0 9 * * 6",
        catchup=False,
        max_active_runs=1,
        tags=["stocks"],
    )
    def stocks_weekly_period_candles():
        @task(retries=2)
        def collect_period_candles() -> None:
            from pipelines.stocks.jobs.collect_daily_candles import run_period

            run_period()

        @task(retries=2)
        def calculate_period_change_rates() -> int:
            from pipelines.common.config import get_settings
            from pipelines.common.utils.time import now_kst
            from pipelines.stocks.jobs.calculate_change_rates import run_period

            since = now_kst().date() - timedelta(days=get_settings().stock_period_lookback_days)
            return run_period(since=since)

        collect_period_candles() >> calculate_period_change_rates()

    stocks_weekly_period_candles()
