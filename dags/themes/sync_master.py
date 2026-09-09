from __future__ import annotations

from datetime import datetime, timedelta

try:
    from airflow.sdk import Asset, dag, task
except ImportError:
    Asset = None
    dag = None
    task = None

SOURCES: tuple[str, ...] = ("judal", "naver")

if dag and task:
    theme_stocks_loaded = Asset("etl://themes/stocks")

    @dag(
        dag_id="themes_sync_master",
        start_date=datetime(2026, 1, 1),
        schedule="0 23 * * 0",  # 매주 일요일 저녁 11시 스케줄링
        catchup=False,
        max_active_runs=1,
        tags=["themes"],
        default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    )
    def themes_sync_master():
        @task
        def extract_source(source_name: str) -> str:
            from pipelines.themes.jobs.extract_source import run

            return run(source_name)

        @task
        def merge_themes(source_paths: list[str]) -> str:
            from pipelines.themes.jobs.merge_themes import run

            return run(source_paths)

        @task
        def load_neo4j(merged_path: str) -> None:
            from pipelines.themes.jobs.load_neo4j import run

            run(merged_path)

        @task(outlets=[theme_stocks_loaded])
        def load_postgres(merged_path: str) -> None:
            from pipelines.themes.jobs.load_postgres import run

            run(merged_path)

        @task
        def embed_themes() -> None:
            from pipelines.themes.jobs.embed_themes import run

            run()

        # SOURCE별 task를 생성해 병렬 실행 fan-out
        extracted = [
            extract_source.override(task_id=f"extract_{source}")(source) for source in SOURCES
        ]

        merged = merge_themes(extracted)

        neo4j_loaded = load_neo4j(merged)
        load_postgres(merged)

        neo4j_loaded >> embed_themes()

    themes_sync_master()
