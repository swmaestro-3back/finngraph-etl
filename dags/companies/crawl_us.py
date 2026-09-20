"""미국 상장사(NYSE·NASDAQ) 크롤링 — Wikipedia·한경 인덱스 → 네이버 overview.

크롤링과 적재를 별도 DAG로 나눈 이유는 재시도 단위가 다르기 때문이다. 크롤링은 외부
사이트 응답에 좌우돼 실패·재시도가 잦고, 적재는 로컬 DB 작업이라 크롤링과 묶어 같이
재시도할 필요가 없다 — 적재만 실패했을 때 크롤링을 다시 돌릴 이유가 없고, 마지막 크롤링
산출물에 대고 적재를 손으로 다시 돌릴 수도 있어야 한다.

크롤링 산출물은 pipelines/companies/data/{YYYYMMDD}/에 남는다. 경로는 XCom이 아니라
`companies_load_us` DAG가 `resolve_us_paths`로 최신 날짜 폴더를 스스로 찾아 읽는다 — 두
DAG는 서로 다른 실행 단위라 XCom으로 경로를 넘길 수 없다.

주 1회면 충분하다. 지수 편입·편출은 분기 리밸런싱 단위고 기업 개요는 그보다 느리게 바뀐다.
themes 주간 DAG(일요일 23시)보다 한 시간 앞서 돈다.
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
    companies_us_crawled = Asset("etl://companies/us_crawled")

    @dag(
        dag_id="companies_crawl_us",
        start_date=datetime(2026, 1, 1),
        schedule="0 22 * * 0",
        catchup=False,
        max_active_runs=1,
        tags=["companies"],
        default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    )
    def companies_crawl_us():
        @task
        def crawl_index() -> str:
            from pipelines.companies.jobs.crawl_us_index import run

            return run()

        @task(outlets=[companies_us_crawled])
        def crawl_overview(index_path: str) -> str:
            from pipelines.companies.jobs.crawl_us_overview import run

            return run(index_path)

        crawl_overview(crawl_index())

    companies_crawl_us()
