from __future__ import annotations

from datetime import datetime, timedelta

try:
    from airflow.sdk import Asset, dag, task
except ImportError:
    Asset = None
    dag = None
    task = None

# SOURCES: tuple[str, ...] = ("naver", "judal",)
SOURCES: tuple[str, ...] = ("naver",)

if dag and task:
    # 테마 편입 확정 신호. 수집 대상 파생(companies_sync_service_companies)이 구독한다.
    theme_stocks_loaded = Asset("etl://themes/stocks")

    @dag(
        dag_id="themes_pipeline",
        start_date=datetime(2026, 1, 1),
        schedule=None,
        catchup=False,
        max_active_runs=1,
        tags=["themes"],
        default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    )
    def themes_pipeline():
        @task
        def extract_source(source_name: str) -> str:
            from pipelines.themes.jobs.extract_source import run

            return run(source_name)

        @task
        def validate_themes(source_paths: list[str]) -> str:
            from pipelines.themes.jobs.validate_themes import run

            return run(source_paths)

        @task
        def load_graph(validated_path: str) -> None:
            from pipelines.themes.jobs.load_graph import run

            run(validated_path)

        @task(outlets=[theme_stocks_loaded])
        def load_rdb(validated_path: str) -> None:
            from pipelines.themes.jobs.load_rdb import run

            run(validated_path)

        @task
        def embed_themes() -> None:
            from pipelines.themes.jobs.embed_themes import run

            run()

        # SOURCE별 task를 생성해 병렬 실행되도록 fan-out
        # extracted 리스트는 SOURCES 순서를 유지
        extracted = [
            extract_source.override(task_id=f"extract_{source}")(source) for source in SOURCES
        ]

        validated = validate_themes(extracted)

        graph_loaded = load_graph(validated)
        load_rdb(validated)

        graph_loaded >> embed_themes()

    # 최종 흐름: extract(소스 병렬) -> validate -> (load_graph ∥ load_rdb) -> embed
    # 전량 삭제는 각 로더(load_graph/load_rdb) 안에서 적재 직전에 일어난다.
    themes_pipeline()
