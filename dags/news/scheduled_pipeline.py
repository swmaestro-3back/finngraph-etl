from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

try:
    from airflow.sdk import Asset, AssetOrTimeSchedule, MultipleCronTriggerTimetable, dag, task
    from airflow.sdk.exceptions import AirflowSkipException
except ImportError:
    AirflowSkipException = None
    Asset = None
    AssetOrTimeSchedule = None
    MultipleCronTriggerTimetable = None
    dag = None
    task = None


if dag and task:
    news_clusters_updated = Asset("etl://news/clusters")

    @dag(
        dag_id="news_scheduled_pipeline",
        start_date=datetime(2026, 1, 1),
        # 평일 장중은 핫테마 발행 직후에만 돈다 — 검색 종목이 그때 바뀐다. 발행이 없는 시간은
        # cron 이 메운다: 평일 장 시작 전(밤사이 기사)·마감 후(시황·공시 기사), 주말.
        schedule=AssetOrTimeSchedule(
            timetable=MultipleCronTriggerTimetable(
                "30 7 * * 1-5",
                "0 18,21 * * 1-5",
                "0 9,15,21 * * 0,6",
                timezone="Asia/Seoul",
            ),
            assets=Asset("etl://themes/hot"),
        ),
        catchup=False,
        max_active_runs=1,
        tags=["news", "triples"],
        # 검색할 테마 ID 를 수동으로 지정하려면 conf 로 넘긴다 (예: {"theme_ids": [12, 34]}).
        # 비어 있으면 select_themes 가 최신 일봉 기준 급등락 테마를 고른다.
        params={"theme_ids": []},
    )
    def news_scheduled_pipeline():
        @task(retries=1, retry_delay=timedelta(minutes=5))
        def select_themes(params: dict[str, Any] | None = None) -> dict[str, list]:
            from pipelines.news.jobs.select_themes import run

            selection = run([int(theme_id) for theme_id in (params or {}).get("theme_ids") or []])
            if not selection.theme_ids:
                raise AirflowSkipException("선정된 테마가 없다 — 일봉이 아직 없거나 전부 보합")
            return {"theme_ids": selection.theme_ids, "tickers": selection.tickers}

        @task(retries=2, retry_delay=timedelta(minutes=5), outlets=[news_clusters_updated])
        def collect_articles(selection: dict[str, list]) -> dict[str, Any]:
            from pipelines.news.jobs.collect_articles import run

            result = run(theme_ids=selection["theme_ids"], tickers=selection["tickers"])
            if result["created"] + result["updated"] == 0:
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

        collect_articles(select_themes()) >> extract_triples() >> summarize_articles()

    news_scheduled_pipeline()
