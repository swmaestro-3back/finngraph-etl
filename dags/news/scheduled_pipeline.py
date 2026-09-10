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
        dag_id="news_scheduled_pipeline",
        start_date=datetime(2026, 1, 1),
        # 06~18시 매 정각, KST 기준.
        schedule="0 6-18 * * *",
        catchup=False,
        max_active_runs=1,
        tags=["news", "triples"],
    )
    def news_scheduled_pipeline():
        @task(retries=2, retry_delay=timedelta(minutes=5), outlets=[news_clusters_updated])
        def collect_articles() -> dict[str, Any]:
            from pipelines.news.jobs.collect_articles import run

            result = run()
            clusters = result["clusters"]
            if clusters["created"] + clusters["updated"] == 0:
                # 스킵하면 outlets 를 발행하지 않는다. 실패가 아니라 "할 일이 없었다"다.
                # 갱신할 카운터도 없는 시간이라 events_promote_clusters 를 깨울 이유가 없다.
                raise AirflowSkipException("클러스터 생성·갱신 0건 — 하류를 깨우지 않는다")
            return result

        # all_done 인 이유는 위가 스킵돼도 돌아야 하기 때문이다 — 삼중항 추출은 news 를 폴링하는
        # 배치라 이전 런에서 못 처리한 기사가 밀려 있을 수 있다(companies_sync_master.seed_graph
        # 와 같은 이유).
        @task(retries=1, retry_delay=timedelta(minutes=10), trigger_rule="all_done")
        def extract_triples() -> dict[str, int]:
            from pipelines.triples.jobs.extract_triples import run

            return run()

        @task(retries=1, retry_delay=timedelta(minutes=10))
        def summarize_articles() -> dict[str, Any]:
            from pipelines.news.jobs.summarize_articles import run

            result = run()
            return {"fetched": result["fetched"], "saved": result["saved"]}

        collect_articles() >> extract_triples() >> summarize_articles()

    news_scheduled_pipeline()
