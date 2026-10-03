"""뉴스 삼중항 추출 — 미처리 기사 전량.

`news_collect_articles`·`news_backfill_krx100` 이 발행하는 `etl://news/clusters` Asset 으로
깨어난다. 이벤트 내용은 쓰지 않고 `news.triple_extracted IS NULL` 인 기사를 전량 폴링하므로,
런이 도는 동안 쌓인 이벤트는 다음 런 하나가 한꺼번에 처리한다. 폴링에 락이 없어
`max_active_runs=1` 이 같은 기사의 중복 추출을 막는다.

추출에 실패한 기사는 미처리로 남아 다음 런(= 다음에 새 기사가 수집된 시점)에 다시 시도한다.
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
    triples_extracted = Asset("etl://triples/extracted")

    @dag(
        dag_id="triples_extract_triples",
        start_date=datetime(2026, 1, 1),
        schedule=Asset("etl://news/clusters"),
        catchup=False,
        max_active_runs=1,
        tags=["triples"],
        doc_md=__doc__,
    )
    def triples_extract_triples():
        @task(retries=1, retry_delay=timedelta(minutes=10), outlets=[triples_extracted])
        def extract_triples() -> dict[str, int]:
            from pipelines.triples.jobs.extract_triples import run

            return run()

        extract_triples()

    triples_extract_triples()
