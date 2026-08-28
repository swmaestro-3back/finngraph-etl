"""DART 고유번호 동기화 — 법인 행의 원천.

corpCode.xml 118,760건을 받아 법인을 만들고 corp_code 를 붙인다. **stocks 를 읽지 않아**
종목 마스터와 순서가 얽히지 않는다 — 둘은 독립이고 companies_sync_master 가 교차점이다.

일간 DART 파이프라인에서 떼어낸 이유는 대상 선정이다. 개요·재무는 service_companies 를
거르는데 그 목록은 이 잡이 만든 법인 위에서 정해진다. 한 DAG 에 있으면 최초 실행에서
개요·재무가 대상 0건으로 끝나고, 같은 DAG 를 두 번 돌려야 채워진다.
"""

from __future__ import annotations

from datetime import datetime

try:
    from airflow.sdk import Asset, dag, task
except ImportError:
    Asset = None
    dag = None
    task = None


if dag and task:
    # 법인 목록 확정 신호. companies_sync_master 가 구독한다.
    companies_corp_codes = Asset("etl://companies/corp_codes")

    @dag(
        dag_id="companies_sync_dart_corp_codes",
        start_date=datetime(2026, 1, 1),
        schedule="0 3 * * *",
        catchup=False,
        max_active_runs=1,
        tags=["companies"],
    )
    def companies_sync_dart_corp_codes():
        @task(retries=2, outlets=[companies_corp_codes])
        def sync_dart_corp_codes() -> None:
            from pipelines.companies.jobs.sync_dart_corp_codes import run

            run()

        sync_dart_corp_codes()

    companies_sync_dart_corp_codes()
