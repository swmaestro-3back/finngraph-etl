from __future__ import annotations

from datetime import datetime

try:
    from airflow.decorators import dag, task
except ImportError:
    dag = None
    task = None


if dag and task:

    @dag(
        dag_id="themes_pipeline",
        start_date=datetime(2026, 1, 1),
        schedule="0 0 * * *",
        catchup=False,
        max_active_runs=1,
        tags=["themes"],
    )
    def themes_pipeline():
        @task(retries=2)
        def run_theme_pipeline() -> None:
            from pipelines.themes.jobs.run_pipeline import run

            run()

        run_theme_pipeline()

    themes_pipeline()
