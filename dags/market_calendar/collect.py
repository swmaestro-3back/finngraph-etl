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
        dag_id="market_calendar_collect",
        start_date=datetime(2026, 1, 1),
        schedule="0 7 * * 1-5",
        catchup=False,
        max_active_runs=1,
        tags=["market_calendar"],
        default_args={"execution_timeout": timedelta(minutes=30)},
    )
    def market_calendar_collect():
        @task(retries=1)
        def sync_market_days() -> int:
            from pipelines.market_calendar.jobs.sync_market_days import run

            return run()

        @task(retries=1, trigger_rule="all_done")
        def collect_dividends() -> int:
            from pipelines.market_calendar.jobs.collect_events import run

            return run("dividends")

        @task(retries=1, trigger_rule="all_done")
        def collect_bonus() -> int:
            from pipelines.market_calendar.jobs.collect_events import run

            return run("bonus")

        @task(retries=1, trigger_rule="all_done")
        def collect_rights() -> int:
            from pipelines.market_calendar.jobs.collect_events import run

            return run("rights")

        @task(retries=1, trigger_rule="all_done")
        def collect_agm() -> int:
            from pipelines.market_calendar.jobs.collect_events import run

            return run("agm")

        @task(retries=1, trigger_rule="all_done")
        def collect_ipos() -> int:
            from pipelines.market_calendar.jobs.collect_ipos import run

            return run()

        @task(retries=1, trigger_rule="all_done")
        def collect_ipo_filings() -> int:
            from pipelines.market_calendar.jobs.collect_ipo_filings import run

            return run()

        # 각 단계는 all_done으로 끝까지 돌리고, 하나라도 실패하면 여기서 DAG run을 실패로 남긴다.
        @task
        def finish() -> None:
            return None

        steps = [
            sync_market_days(),
            collect_dividends(),
            collect_bonus(),
            collect_rights(),
            collect_agm(),
            collect_ipos(),
            collect_ipo_filings(),
        ]
        for upstream, downstream in zip(steps, steps[1:], strict=False):
            upstream >> downstream
        steps >> finish()

    market_calendar_collect()
