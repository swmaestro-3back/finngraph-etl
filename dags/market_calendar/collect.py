from __future__ import annotations

from datetime import datetime
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
        schedule="30 7 * * *",
        catchup=False,
        max_active_runs=1,
        tags=["market_calendar"],
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
        ]
        for upstream, downstream in zip(steps, steps[1:], strict=False):
            upstream >> downstream
        steps >> finish()

    market_calendar_collect()
