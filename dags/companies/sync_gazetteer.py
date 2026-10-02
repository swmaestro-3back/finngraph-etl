"""상장 기업 개체 사전(entity_gazetteer) 재생성.

사전의 원천은 국내 마스터(companies_sync_master)와 미국 적재(companies_load_us) 두 갈래다.
**둘 중 하나만 끝나도** 기업명·종목·별칭이 바뀌었을 수 있으므로 AssetAny 다.

`etl://companies/linked` 를 구독하지 않는 이유: 그 Asset 은 종목 연결 수가 바뀐 날에만
발행된다. 사명 변경·상폐·DART 별칭은 연결 수를 바꾸지 않아 사전이 갱신되지 않는다. 그래서
마스터 체인이 끝날 때마다 발행되는 `etl://companies/master_synced` 를 따로 둔다.
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
        dag_id="companies_sync_gazetteer",
        start_date=datetime(2026, 1, 1),
        schedule=AssetAny(
            Asset("etl://companies/master_synced"),
            Asset("etl://companies/us_loaded"),
        ),
        catchup=False,
        max_active_runs=1,
        tags=["companies"],
    )
    def companies_sync_gazetteer():
        @task(retries=2)
        def sync_gazetteer() -> None:
            from pipelines.companies.jobs.sync_gazetteer import run

            run()

        sync_gazetteer()

    companies_sync_gazetteer()
