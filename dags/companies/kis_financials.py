"""KIS 상장사 재무 수집.

종목당 6회 호출(재무제표 3종 × 연간·분기)이라 전 종목을 매일 돌 수 없다. job이 갱신
오래된 순으로 배치 크기만큼만 처리하므로, 매일 돌리면 회차를 거듭하며 전체가 채워진다.

19:00은 종목 파이프라인(18:00)이 끝난 뒤다. 같은 KIS 호출 한도를 나눠 쓰므로 겹치지
않게 둔다.
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    # 타입 체커에는 항상 진짜 심볼을 보여준다. 아래 fallback 의 None 이 타입에 섞이면
    # 중첩 함수 안(@task 데코레이터 자리)에서 `if dag and task:` narrowing 이 풀려
    # "Object of type None cannot be called" 로 잡힌다.
    from airflow.sdk import Asset, dag, task
else:
    # airflow 는 optional 의존성이라 설치 안 된 환경에서도 import 자체는 통과해야 한다.
    try:
        from airflow.sdk import Asset, dag, task
    except ImportError:
        Asset = None
        dag = None
        task = None


if dag and task:
    # 재무 확정 신호. stocks_compute_derived가 구독한다.
    companies_financials_updated = Asset("etl://companies/financials")

    @dag(
        dag_id="companies_kis_financials",
        start_date=datetime(2026, 1, 1),
        schedule="0 19 * * 1-5",
        catchup=False,
        max_active_runs=1,
        tags=["companies"],
    )
    def companies_kis_financials():
        @task(retries=2, outlets=[companies_financials_updated])
        def collect_kis_financials() -> None:
            from pipelines.companies.jobs.collect_kis_financials import run

            run()

        collect_kis_financials()

    companies_kis_financials()
