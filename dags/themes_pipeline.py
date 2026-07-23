from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Any

try:
    from airflow.decorators import dag, task
except ImportError:
    dag = None
    task = None

SOURCES = ["judal", "naver", "antwinner"]

if dag and task:

    @dag(
        dag_id="themes_pipeline",
        start_date=datetime(2026, 1, 1),
        schedule="0 0 * * *",
        catchup=False,
        max_active_runs=1,
        tags=["etl", "themes"],
    )
    def themes_pipeline():

        @task
        def reset_themes() -> None:
            from pipelines.themes.jobs.steps import reset

            asyncio.run(reset())

        @task
        def extract_themes(source_name: str) -> list[dict[str, Any]]:
            from pipelines.themes.jobs.steps import extract_source

            return asyncio.run(extract_source(source_name))

        @task
        def validate_themes(
            raw_themes_nested: list[list[dict[str, Any]]],
        ) -> list[dict[str, Any]]:
            from pipelines.themes.jobs.steps import validate_themes as _validate

            return asyncio.run(_validate(raw_themes_nested))

        @task
        def load_themes(validated_themes: list[dict[str, Any]]) -> None:
            from pipelines.themes.jobs.steps import load_themes as _load

            asyncio.run(_load(validated_themes))

        reset = reset_themes()

        # SOURCE별 task를 생성하고 SOURCES 순서대로 순차 의존성으로 연결
        extracted = [
            extract_themes.override(task_id=f"extract_{source}")(source) for source in SOURCES
        ]
        chain = reset
        for step in extracted:
            chain >> step
            chain = step

        load_themes(validate_themes(extracted))

    # 최종 실행 흐름: reset -> extract(judal -> naver -> antwinner) -> validate -> load
    themes_pipeline()
