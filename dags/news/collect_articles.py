"""핫테마 편입 기업 뉴스 수집 (핫테마 발행 직후 + 장외 시간 cron).

수집·군집화까지만 한다. 삼중항 추출과 요약은 이 DAG 이 발행하는 `etl://news/clusters` Asset 을
따라 `triples_extract_triples` → `news_summarize_articles` 가 이어서 돈다 — LLM 처리가 길어져도
다음 수집을 막지 않고, 백필(`news_backfill_krx100`)과 하류를 공유하기 위해서다.
"""

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
        dag_id="news_collect_articles",
        start_date=datetime(2026, 1, 1),
        # 평일 장중은 핫테마 발행 직후에만 돈다 — 검색 종목이 그때 바뀐다. 발행이 없는 시간은
        # cron 이 메운다: 평일 장 시작 전(밤사이 기사)·마감 후(시황·공시 기사), 주말.
        schedule=AssetOrTimeSchedule(
            timetable=MultipleCronTriggerTimetable(
                "30 7 * * 1-5",
                "0 21 * * 1-5",
                "0 9,15,21 * * 0,6",
                timezone="Asia/Seoul",
            ),
            assets=Asset("etl://themes/hot"),
        ),
        catchup=False,
        max_active_runs=1,
        tags=["news"],
        doc_md=__doc__,
        # 검색할 테마 ID 를 수동으로 지정하려면 conf 로 넘긴다 (예: {"theme_ids": [12, 34]}).
        # 비어 있으면 select_themes 가 백엔드가 Redis 에 발행한 핫테마를 쓴다.
        params={"theme_ids": []},
    )
    def news_collect_articles():
        @task(retries=1, retry_delay=timedelta(minutes=5))
        def select_themes(params: dict[str, Any] | None = None) -> dict[str, list]:
            from pipelines.news.jobs.select_themes import run

            selection = run([int(theme_id) for theme_id in (params or {}).get("theme_ids") or []])
            return {"theme_ids": selection.theme_ids, "tickers": selection.tickers}

        @task(retries=2, retry_delay=timedelta(minutes=5), outlets=[news_clusters_updated])
        def collect_articles(selection: dict[str, list]) -> dict[str, Any]:
            from pipelines.news.jobs.collect_articles import run

            result = run(theme_ids=selection["theme_ids"], tickers=selection["tickers"])
            if result["created"] + result["updated"] == 0:
                # 스킵하면 outlets 를 발행하지 않는다. 실패가 아니라 "할 일이 없었다"다.
                # 새 기사도 갱신할 카운터도 없는 시간이라 하류(삼중항 추출·Event 승격)를 깨울
                # 이유가 없다.
                raise AirflowSkipException("클러스터 생성·갱신 0건 — 하류를 깨우지 않는다")
            return result

        collect_articles(select_themes())

    news_collect_articles()
