from __future__ import annotations

from datetime import datetime

try:
    from airflow.decorators import dag, task
except ImportError:
    dag = None
    task = None


if dag and task:

    @dag(
        dag_id="themes_mirror_to_rdb",
        start_date=datetime(2026, 1, 1),
        schedule="0 1 * * *",  # 테마 크롤러(themes_pipeline, 자정) 이후 실행
        catchup=False,
        max_active_runs=1,
        tags=["themes"],
    )
    def themes_mirror_to_rdb():
        @task(retries=2)
        def mirror_to_rdb() -> None:
            from pipelines.themes.jobs.mirror_to_rdb import run

            run()

        mirror_to_rdb()

    themes_mirror_to_rdb()
