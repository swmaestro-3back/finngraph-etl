from __future__ import annotations

from datetime import datetime, timedelta

try:
    from airflow.sdk import Asset, dag, task
except ImportError:
    Asset = None
    dag = None
    task = None

SOURCES: tuple[str, ...] = ("naver",)

if dag and task:
    # 테마 편입 확정 신호. 수집 대상 파생(companies_service_companies)이 구독한다.
    theme_stocks_loaded = Asset("etl://themes/stocks")

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

        @task(outlets=[theme_stocks_loaded])
        def load_rdb(validated_path: str) -> None:
            from pipelines.themes.jobs.load_rdb import run

            run(validated_path)

        @task
        def embed_themes() -> None:
            from pipelines.themes.jobs.embed_themes import run

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
        graph_loaded = load_graph(validated)
        rdb_loaded = load_rdb(validated)

        # 적재가 전량 삭제-재적재라 임베딩도 매 회차 전량 재생성된다.
        # pg 에 쓰고 Neo4j Theme 노드에도 복사하므로 양쪽 적재를 모두 기다린다.
        # 청크 커밋 + 해시 비교로 재개 가능하므로 실패 시 재시도만 하면 된다.
        [graph_loaded, rdb_loaded] >> embed_themes()

    # 최종 흐름: reset -> extract(소스 병렬) -> validate -> (load_graph ∥ load_rdb) -> embed
    themes_refresh()
