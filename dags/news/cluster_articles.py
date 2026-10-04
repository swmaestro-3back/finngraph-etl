"""뉴스 클러스터 판정 → 대표 선정 → Event 생성·요약.

`news_collect_articles`·`news_backfill_krx100` 이 발행하는 `etl://news/articles` Asset 으로
깨어난다. 이벤트 내용은 쓰지 않고 DB 를 폴링한다 — `assign_clusters` 는 cluster_id 가 없는 기사
전량을, 나머지 task 는 각자의 미처리분 전량을 읽는다. 런이 도는 동안 쌓인 이벤트는 다음 런 하나가
한꺼번에 처리한다.

클러스터를 쓰는 곳은 이 DAG 뿐이고 `max_active_runs=1` 이라, 수집과 백필이 동시에 돌아도 같은
사건에 클러스터가 둘 생기지 않는다.

`promote_clusters` 는 삼중항 미처리 대표 기사가 남아 있을 때만 `etl://news/clusters` 를 발행해
`triples_extract_triples` 를 깨운다. 이번 런에 승격이 없어도 지난 런에 추출이 실패한 대표가 있으면
다시 깨운다. 발행을 건너뛰어도(skip) Event 생성과 요약은 돈다(`trigger_rule="none_failed"`).
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

try:
    from airflow.sdk import Asset, dag, task
    from airflow.sdk.exceptions import AirflowSkipException
except ImportError:
    AirflowSkipException = None
    Asset = None
    dag = None
    task = None


if dag and task:
    news_clusters_updated = Asset("etl://news/clusters")

    @dag(
        dag_id="news_cluster_articles",
        start_date=datetime(2026, 1, 1),
        schedule=Asset("etl://news/articles"),
        catchup=False,
        max_active_runs=1,
        tags=["news"],
        doc_md=__doc__,
    )
    def news_cluster_articles():
        @task(retries=1, retry_delay=timedelta(minutes=5))
        def assign_clusters() -> dict[str, int]:
            from pipelines.news.jobs.cluster_articles import assign

            return assign()

        @task(retries=1, retry_delay=timedelta(minutes=5), outlets=[news_clusters_updated])
        def promote_clusters() -> dict[str, int]:
            from pipelines.news.jobs.cluster_articles import promote

            result = promote()
            if result["pending_triples"] == 0:
                # 스킵하면 outlets 를 발행하지 않는다. 실패가 아니라 "삼중항이 할 일이 없다"다.
                raise AirflowSkipException("삼중항 미처리 대표 0건 — 삼중항 DAG 를 깨우지 않는다")
            return result

        # 클러스터별로 실패가 격리돼 있고 실패한 클러스터는 다음 런에 자연히 재시도된다.
        # promote_clusters 가 skip 이어도 돈다 — 제목은 지난 런에 붙었을 수 있다.
        @task(retries=1, retry_delay=timedelta(minutes=10), trigger_rule="none_failed")
        def generate_events() -> dict[str, int]:
            from pipelines.events.jobs.generate_events import run

            return run()

        @task(retries=1, retry_delay=timedelta(minutes=10), trigger_rule="none_failed")
        def summarize_articles() -> dict[str, Any]:
            from pipelines.news.jobs.summarize_articles import run

            result = run()
            return {"fetched": result["fetched"], "saved": result["saved"]}

        promoted = promote_clusters()
        assign_clusters() >> promoted
        promoted >> [generate_events(), summarize_articles()]

    news_cluster_articles()
