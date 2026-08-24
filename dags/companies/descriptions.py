"""기업 설명 생성 — 주 1회, 새 정기공시가 올라온 법인만.

일간 DART 파이프라인에서 떼어냈다. corpCode·개요·재무는 매일 볼 값이지만 사업 설명은
분기마다 바뀐다. 공시 접수를 감지해 돌리는 편이 낫지만 공시 수집이 아직 없어 주 1회
폴링으로 대신하며, 접수 감지가 붙으면 schedule을 Asset으로 바꾼다.

토요일 04:00은 장이 없는 날이라 KIS 계열 배치와 겹치지 않는다.
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
        dag_id="companies_descriptions",
        start_date=datetime(2026, 1, 1),
        schedule="0 4 * * 6",
        catchup=False,
        max_active_runs=1,
        tags=["companies"],
    )
    def companies_descriptions():
        @task(retries=1)
        def generate_descriptions() -> None:
            from pipelines.companies.jobs.generate_descriptions import run

            run()

        generate_descriptions()

    companies_descriptions()
