from __future__ import annotations

from datetime import datetime, timedelta

try:
    from airflow.sdk import Asset, dag, task
except ImportError:
    Asset = None
    dag = None
    task = None


if dag and task:

    @dag(
        dag_id="themes_link_news",
        start_date=datetime(2026, 1, 1),
        schedule=Asset("etl://news/relations"),
        catchup=False,
        max_active_runs=1,
        tags=["themes"],
        default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    )
    def themes_link_news():
        @task
        def link_news() -> None:
            from pipelines.themes.jobs.link_news import run

            run()

        link_news()

    themes_link_news()
