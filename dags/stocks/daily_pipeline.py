"""장 마감 후 종목 데이터 파이프라인.

순서가 의미를 갖는다.

    일봉 → 기간봉(주·월)

같은 KIS 호출 예산을 쓰는 작업은 한 DAG에 묶어 동시 실행을 피한다 — 여러 DAG가 겹치면
초당 호출 한도에 걸린다.

18:00에 도는 이유는 장 마감(15:30)과 정산 시차 때문이다. 마감 직후에는 당일 일봉이
확정되지 않는다.
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
        dag_id="stocks_daily_pipeline",
        start_date=datetime(2026, 1, 1),
        schedule="0 18 * * 1-5",
        catchup=False,
        max_active_runs=1,
        tags=["stocks"],
    )
    def stocks_daily_pipeline():
        @task(retries=2)
        def collect_daily_candles() -> None:
            from pipelines.stocks.jobs.collect_daily import run

            run()

        @task(retries=2)
        def collect_period_candles() -> None:
            from pipelines.stocks.jobs.collect_daily import run_period

            run_period()

        collect_daily_candles() >> collect_period_candles()

    stocks_daily_pipeline()
