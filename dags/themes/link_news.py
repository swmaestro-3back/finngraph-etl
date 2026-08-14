from __future__ import annotations

from datetime import datetime

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
    )
    def themes_link_news():
        @task(retries=2)
        def link_news_themes() -> None:
            from pipelines.themes.jobs.link_news_themes import run

            run()

        link_news_themes()

    themes_link_news()
