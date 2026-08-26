"""OpenDART 개요·재무 수집.

기업개황이 재무보다 먼저인 이유는 결산월(acc_mt) 때문이다. DART 재무의 회계연도 표기
(fiscal_yymm)를 만들 때 결산월이 필요하고, 없으면 12월로 가정해 3월·6월 결산 법인이
어긋난다.

법인 행 생성(sync_corp_codes)은 companies_corp_codes 로 떼어냈다. 두 태스크가 대상을
service_companies 로 거르는데, 그 목록이 법인 위에서 정해지므로 한 DAG 에 있으면
최초 실행에서 대상 0건으로 끝난다.
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
        # 09시. 앞선 사슬(03시 corpCode → 08시 종목 마스터 → 연결 → 수집 대상)이
        # 끝난 뒤에 돈다. Asset 으로 걸지 않은 이유는 이 잡이 DART 일 한도의 큰 몫을
        # 쓰기 때문이다 — 하루 몇 번 도는지가 예측 가능해야 한다.
        schedule="0 9 * * *",
        catchup=False,
        max_active_runs=1,
        tags=["companies"],
    )
    def companies_dart_pipeline():
        @task(retries=2)
        def collect_profiles() -> None:
            from pipelines.companies.jobs.collect_dart_profiles import run

            run()

        @task(retries=2)
        def collect_financials() -> None:
            from pipelines.companies.jobs.collect_dart_financials import run

            run()

        collect_profiles() >> collect_financials()

    companies_dart_pipeline()
