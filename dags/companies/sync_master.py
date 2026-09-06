"""기업 마스터 동기화 — 상장 여부 토글과 종목 연결.

종목과 법인 **둘 중 하나만 바뀌어도** 할 일이 생긴다. 종목이 바뀌면 상장 여부를 다시
판정해야 하고, corpCode 로 법인이 새로 생기면 아직 연결되지 않은 종목에 붙일 수 있다.
그래서 AND 가 아니라 AssetAny 다.

네 쿼리 모두 `IS DISTINCT FROM` · `ON CONFLICT` 가드가 있어 두 번 돌아도 0건으로 끝난다.
"""

from __future__ import annotations

from datetime import datetime

try:
    from airflow.sdk import Asset, AssetAny, dag, task
    from airflow.sdk.exceptions import AirflowSkipException
except ImportError:
    AirflowSkipException = None
    Asset = None
    AssetAny = None
    dag = None
    task = None


if dag and task:
    # 연결이 바뀐 날에만 발행한다. 하류(service_companies)가 stocks.company_id 를 읽으므로
    # 아무것도 안 바뀐 날 깨우면 DART 호출만 두 배가 된다.
    companies_linked = Asset("etl://companies/linked")

    @dag(
        dag_id="companies_sync_master",
        start_date=datetime(2026, 1, 1),
        schedule=AssetAny(
            Asset("etl://stocks/master"),
            Asset("etl://companies/corp_codes"),
        ),
        catchup=False,
        max_active_runs=1,
        tags=["companies"],
    )
    def companies_sync_master():
        @task(retries=2, outlets=[companies_linked])
        def sync_master() -> None:
            from pipelines.companies.jobs.sync_master import run

            linked = run()
            if not linked:
                # 스킵하면 outlets 를 발행하지 않는다. 실패가 아니라 "할 일이 없었다"다.
                raise AirflowSkipException("연결 변경 0건 — 하류를 깨우지 않는다")

        # 마스터 확정 직후 Neo4j에 상장사(name↔ticker)를 시드한다.
        # 테마 검증·적재와 트리플 종목코드 채움이 이 시드를 전제로 동작한다.
        #
        # all_done 인 이유는 위가 스킵돼도 시드는 돌아야 하기 때문이다 — 그래프가 비어 있는
        # 채로 Postgres 만 최신인 상태가 생길 수 있다. upsert 라 반복 실행이 안전하다.
        @task(retries=2, trigger_rule="all_done")
        def seed_graph() -> None:
            from pipelines.companies.jobs.seed_graph import run

            run()

        sync_master() >> seed_graph()

    companies_sync_master()
