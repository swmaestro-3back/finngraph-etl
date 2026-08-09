from __future__ import annotations

import asyncio
from datetime import datetime

try:
    from airflow.sdk import dag, task
except ImportError:
    dag = None
    task = None

SOURCES = ["judal", "naver"]

if dag and task:

    @dag(
        dag_id="themes_etl",
        start_date=datetime(2026, 1, 1),
        schedule="0 0 * * *",
        catchup=False,
        max_active_runs=1,
        tags=["themes"],
    )
    def themes_pipeline():

        @task
        def reset_themes() -> None:
            from pipelines.themes.jobs.steps import reset

            asyncio.run(reset())

        @task
        def extract_themes(source_name: str) -> str:
            from pipelines.themes.jobs.steps import extract_source

            return asyncio.run(extract_source(source_name))

        @task
        def validate_themes(source_paths: list[str]) -> str:
            from pipelines.themes.jobs.steps import validate_themes as _validate

            return asyncio.run(_validate(source_paths))

        @task
        def load_themes(validated_path: str) -> None:
            from pipelines.themes.jobs.steps import load_themes as _load

            asyncio.run(_load(validated_path))

        reset = reset_themes()

        # SOURCE별 task를 생성하고 reset 이후 병렬 실행되도록 fan-out
        # extracted 리스트는 SOURCES 순서를 유지하므로 XCom 집계 순서도 보장됨
        extracted = [
            extract_themes.override(task_id=f"extract_{source}")(source) for source in SOURCES
        ]
        reset >> extracted

        load_themes(validate_themes(extracted))

    # 최종 실행 흐름: reset -> extract(judal -> naver -> antwinner) -> validate -> load
    themes_pipeline()
