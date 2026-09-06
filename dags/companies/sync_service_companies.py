"""수집 대상 파생 — theme_stocks 에 편입된 종목의 법인을 수집 대상에 올린다.

입력이 둘이고 **어느 쪽이 바뀌어도 다시 계산해야 한다.**

    theme_stocks           크롤링으로 테마 구성이 바뀜
    stocks.company_id      corpCode 로 새 법인이 생겨 종목에 붙음

그래서 AND 가 아니라 AssetAny 다. 예전에는 테마 DAG(현 themes_init) 안에 있어서
크롤링이 실패한 날에는 새로 연결된 법인이 반영되지 않았다.

DB 안에서 끝나는 INSERT ... SELECT 하나라 자주 돌아도 비용이 없다.
"""

from __future__ import annotations

from datetime import datetime

try:
    from airflow.sdk import Asset, AssetAny, dag, task
except ImportError:
    Asset = None
    AssetAny = None
    dag = None
    task = None


if dag and task:

    @dag(
        dag_id="companies_sync_service_companies",
        start_date=datetime(2026, 1, 1),
        schedule=AssetAny(
            Asset("etl://themes/stocks"),
            Asset("etl://companies/linked"),
        ),
        catchup=False,
        max_active_runs=1,
        tags=["companies"],
    )
    def companies_sync_service_companies():
        @task(retries=2)
        def sync_service_companies() -> None:
            from pipelines.companies.jobs.sync_service_companies import run

            run()

        sync_service_companies()

    companies_sync_service_companies()
