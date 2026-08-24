"""OpenDART 파이프라인.

순서가 고정이다.

    고유번호 동기화 → 기업개황 → 재무 → 설명 생성

기업개황이 재무보다 먼저인 이유는 결산월(acc_mt) 때문이다. DART 재무의 회계연도 표기
(fiscal_yymm)를 만들 때 결산월이 필요하고, 없으면 12월로 가정해 3월·6월 결산 법인이
어긋난다.

주기는 매일 03시다. 근거는 schedule 옆 주석에 적어 뒀다.
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    # 타입 체커에는 항상 진짜 심볼을 보여준다. 아래 fallback 의 None 이 타입에 섞이면
    # 중첩 함수 안(@task 데코레이터 자리)에서 `if dag and task:` narrowing 이 풀려
    # "Object of type None cannot be called" 로 잡힌다.
    from airflow.sdk import dag, task
else:
    # airflow 는 optional 의존성이라 설치 안 된 환경에서도 import 자체는 통과해야 한다.
    try:
        from airflow.sdk import dag, task
    except ImportError:
        dag = None
        task = None


if dag and task:

    @dag(
        dag_id="companies_dart_pipeline",
        start_date=datetime(2026, 1, 1),
        # 법인 행을 만드는 곳이 이 DAG뿐이다. 주 1회면 신규 상장이 최대 7일간
        # company_id 없이 떠 있는다.
        schedule="0 3 * * *",
        catchup=False,
        max_active_runs=1,
        tags=["companies"],
    )
    def companies_dart_pipeline():
        @task(retries=2)
        def sync_corp_codes() -> None:
            from pipelines.companies.jobs.sync_dart_corp_codes import run

            run()

        @task(retries=2)
        def collect_profiles() -> None:
            from pipelines.companies.jobs.collect_dart_profiles import run

            run()

        @task(retries=2)
        def collect_financials() -> None:
            from pipelines.companies.jobs.collect_dart_financials import run

            run()

        sync_corp_codes() >> collect_profiles() >> collect_financials()

    companies_dart_pipeline()
