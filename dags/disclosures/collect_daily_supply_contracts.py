"""단일판매ㆍ공급계약체결 공시 일일 수집(전일분 + lookback 되짚음) 후 그래프 연결.

04시인 이유: 제출사 company_id 와 계약상대 역매칭이 companies 테이블에 기대는데,
corp_code 를 매일 갱신하는 companies_dart_pipeline 이 03시에 돈다. 그 직후로 두어
법인 마스터가 그날 최신인 상태로 매칭한다(하드 의존은 아니다 — 하루 늦어도 다음 날
lookback 겹침이 자연히 메운다).

그래프 간선 반영까지 job 안에서 이어서 돈다 — 태스크는 하나다.
"""

from __future__ import annotations

from datetime import datetime

try:
    from airflow.sdk import dag, task
except ImportError:
    dag = None
    task = None


if dag and task:

    @dag(
        dag_id="disclosures_collect_daily_supply_contracts",
        start_date=datetime(2026, 1, 1),
        schedule="0 4 * * *",
        catchup=False,
        max_active_runs=1,
        tags=["disclosures"],
    )
    def disclosures_collect_daily_supply_contracts():
        @task(retries=2)
        def collect_daily_supply_contracts() -> None:
            from pipelines.disclosures.jobs.collect_daily_supply_contracts import run

            run()

        collect_daily_supply_contracts()

    disclosures_collect_daily_supply_contracts()
