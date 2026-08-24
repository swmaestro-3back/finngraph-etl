"""배당 수집.

배당은 분기에 한 번 바뀌므로 주 1회면 충분하다. 종목당 1회 호출이라 평일 예산과 겹치지
않도록 토요일 새벽에 돌린다.
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
        dag_id="stocks_weekly_dividends",
        start_date=datetime(2026, 1, 1),
        schedule="0 6 * * 6",
        catchup=False,
        max_active_runs=1,
        tags=["stocks"],
    )
    def stocks_weekly_dividends():
        @task(retries=2)
        def collect_dividends() -> None:
            from pipelines.stocks.jobs.collect_dividends import run

            run()

        collect_dividends()

    stocks_weekly_dividends()
