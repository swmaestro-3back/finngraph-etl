"""뉴스 클러스터 → Neo4j Event 승격 (events_pipeline).

시각이 아니라 news_pipeline 의 collect_articles 가 발행하는 Asset 을 구독한다. 클러스터가
생성·갱신된 시간에만 발행되므로, 조용한 시간에는 이 DAG 도 돌지 않는다.

    news_pipeline.collect_articles ──► etl://news/clusters ──► events_pipeline

sync_events(기존 Event 갱신, LLM 없음)와 generate_events(신규 Event 생성, LLM)는 서로
의존하지 않는다 — 둘 다 후보 클러스터를 스캔해 Neo4j 존재 여부로 자기 몫만 고른다. 둘 다
MERGE 라 겹쳐도 데이터는 깨지지 않지만, 같은 클러스터에 LLM 을 두 번 부르지 않도록
max_active_runs=1 로 런끼리는 직렬화한다.

전제: 재생성된 0002 와 0003 이 neo4j-init 으로 적용돼 있고, companies_sync_master.seed_graph
가 한 번은 성공해 KRX is_listed 가 채워져 있을 것. 그렇지 않아도 갱신이 나중에 간선을 붙이지만
첫 며칠의 created_without_edges 가 높다.
"""

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
        dag_id="events_pipeline",
        start_date=datetime(2026, 1, 1),
        schedule=Asset("etl://news/clusters"),
        catchup=False,
        max_active_runs=1,
        tags=["events"],
    )
    def events_pipeline():
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

    events_pipeline()
