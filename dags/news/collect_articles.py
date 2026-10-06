"""핫테마 편입 기업 뉴스 수집 (핫테마 발행 직후 + 장외 시간 cron).

수집·필터·본문 크롤링·기업 판정·저장까지만 한다. 클러스터 판정은 이 DAG 이 발행하는
`etl://news/articles` Asset 을 따라 `news_cluster_articles` 가, 삼중항 추출은 그 뒤
`triples_extract_triples` 가 이어서 돈다 — LLM 처리가 길어져도 다음 수집을 막지 않기 위해서다.
백필(`news_backfill_krx100`)은 클러스터 DAG 를 따로 둔다(`news_backfill_cluster_articles`).
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
    news_articles_saved = Asset("etl://news/articles")

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

        @task(retries=2, retry_delay=timedelta(minutes=5), outlets=[news_articles_saved])
        def collect_articles(selection: dict[str, list]) -> dict[str, Any]:
            from pipelines.news.jobs.collect_articles import run

            result = run(theme_ids=selection["theme_ids"], tickers=selection["tickers"])
            if result["saved"] == 0:
                # 스킵하면 outlets 를 발행하지 않는다. 실패가 아니라 "할 일이 없었다"다.
                # 클러스터 DAG 가 할 일은 새로 저장된 기사가 있을 때 생긴다.
                raise AirflowSkipException("신규 저장 0건 — 클러스터 DAG 를 깨우지 않는다")
            return result

        collect_articles(select_themes())

    news_collect_articles()
