from __future__ import annotations

from datetime import datetime, timedelta

try:
    from airflow.sdk import Asset, dag, task
except ImportError:
    Asset = None
    dag = None
    task = None


if dag and task:

    @dag(
        dag_id="events_promote_clusters",
        start_date=datetime(2026, 1, 1),
        schedule=Asset("etl://news/clusters"),
        catchup=False,
        max_active_runs=1,
        tags=["events"],
    )
    def events_promote_clusters():
        # 배치 하나가 통째로 성공/실패하고 멱등이라 재시도가 싸다.
        @task(retries=2)
        def sync_events() -> dict[str, int]:
            from pipelines.events.jobs.sync_events import run

            return run()

        # 클러스터별로 실패가 격리돼 있고 실패한 클러스터는 다음 런에 자연히 재시도된다.
        @task(retries=1, retry_delay=timedelta(minutes=10))
        def generate_events() -> dict[str, int]:
            from pipelines.events.jobs.generate_events import run

            return run()

        # 의존 없음 — 병렬 실행. 각자 기존/신규 절반만 처리한다.
        sync_events()
        generate_events()

    events_promote_clusters()
