"""단일판매ㆍ공급계약체결 공시 3개년 백필. 수동 실행 전용.

일 API 한도(20,000건)나 회차 상한(DISCLOSURE_FETCH_BATCH_SIZE)에 걸리면 그 회차는
정상 종료된다 — 이미 적재된 접수번호는 원문을 다시 받지 않으므로, 재트리거하면 남은
구간을 이어서 채운다.

제출사·계약상대 매칭이 companies 테이블에 기대므로 corp_code 동기화
(companies_dart_pipeline)가 한 번은 돈 뒤에 실행해야 매칭이 채워진다.

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
        dag_id="disclosures_backfill_supply_contracts",
        start_date=datetime(2026, 1, 1),
        schedule=None,
        catchup=False,
        max_active_runs=1,
        tags=["disclosures"],
    )
    def disclosures_backfill_supply_contracts():
        @task
        def run_backfill() -> None:
            from pipelines.disclosures.jobs.backfill_supply_contracts import run

            run()

        run_backfill()

    disclosures_backfill_supply_contracts()
