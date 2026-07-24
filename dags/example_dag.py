from __future__ import annotations

from datetime import datetime

try:
    from airflow.sdk import dag, task
except ImportError:  # Allows syntax checks without Airflow installed.
    dag = None
    task = None


if dag and task:

    @dag(
        dag_id="example_etl_healthcheck",
        start_date=datetime(2026, 1, 1),
        schedule=None,
        catchup=False,
        tags=["example"],
    )
    def example_etl_healthcheck():
        @task
        def ping() -> str:
            return "ok"

        ping()

    example_etl_healthcheck()
