from __future__ import annotations

from datetime import datetime

try:
    from airflow.sdk import Asset, AssetAny, dag, task
except ImportError:
    Asset = None
    AssetAny = None
    dag = None
    task = None


if dag and task:

    @dag(
        dag_id="companies_sync_service_companies",
        start_date=datetime(2026, 1, 1),
        schedule=AssetAny(
            Asset("etl://themes/stocks"),
            Asset("etl://companies/linked"),
        ),
        catchup=False,
        max_active_runs=1,
        tags=["companies"],
    )
    def companies_sync_service_companies():
        @task(retries=2)
        def sync_service_companies() -> None:
            from pipelines.companies.jobs.sync_service_companies import run

            run()

        sync_service_companies()

    companies_sync_service_companies()
