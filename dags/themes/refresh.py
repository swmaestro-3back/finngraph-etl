from __future__ import annotations

from datetime import datetime, timedelta

try:
    from airflow.sdk import dag, task
except ImportError:
    dag = None
    task = None

SOURCES: tuple[str, ...] = ("naver", "judal")

if dag and task:

    @dag(
        dag_id="themes_refresh",
        start_date=datetime(2026, 1, 1),
        schedule="0 0 * * *",
        catchup=False,
        max_active_runs=1,
        tags=["themes"],
        default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    )
    def themes_refresh():
        @task(retries=0)
        def reset_graph() -> None:
            # DETACH DELETE라 재시도해도 얻을 게 없다. 실패하면 실행을 멈추는 편이 낫다.
            from pipelines.themes.jobs.reset_graph import run

            run()

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

        @task
        def load_rdb(validated_path: str) -> None:
            from pipelines.themes.jobs.load_rdb import run

            run(validated_path)

        @task
        def sync_service_companies() -> None:
            from pipelines.companies.jobs.sync_service_companies import run

            run()

        reset = reset_graph()

        # SOURCE별 task를 생성하고 reset 이후 병렬 실행되도록 fan-out
        # extracted 리스트는 SOURCES 순서를 유지
        extracted = [
            extract_source.override(task_id=f"extract_{source}")(source) for source in SOURCES
        ]
        reset >> extracted

        validated = validate_themes(extracted)

        # 저장소가 달라 실패 특성도 다르다. 한쪽이 죽어도 다른 쪽은 성공해야 하므로
        # 하나의 태스크로 합치지 않는다.
        load_graph(validated)
        # 테마 편입이 확정된 뒤에 수집 대상을 넓힌다. 그래프 적재와는 무관하다.
        load_rdb(validated) >> sync_service_companies()

    # 최종 실행 흐름: reset -> extract(소스 병렬) -> validate
    #                        -> load_graph ∥ (load_rdb -> sync_service_companies)
    themes_refresh()
