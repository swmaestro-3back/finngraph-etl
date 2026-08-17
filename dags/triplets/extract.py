from __future__ import annotations

from datetime import datetime, timedelta

try:
    from airflow.sdk import Asset, dag, task
except ImportError:
    Asset = None
    dag = None
    task = None

if dag and task:
    news_relations_updated = Asset("etl://news/relations")

    @dag(
        dag_id="triplets_extract",
        start_date=datetime(2026, 1, 1),
        schedule=Asset("etl://news/filtered"),
        catchup=False,
        max_active_runs=1,
        tags=["triplets"],
    )
    def triplets_extract():

        @task(retries=1, retry_delay=timedelta(minutes=10), outlets=[news_relations_updated])
        def extract_and_load() -> dict[str, int]:
            from pipelines.triplets.jobs.extract import run

            return run()

        extract_and_load()

    triplets_extract()
