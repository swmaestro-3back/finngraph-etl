"""장중 일봉 갱신.

09~16시 매 정각 일봉을 받고, 그 일봉으로 이번 주·이번 달 봉을 계산한다. 주·월봉의 확정값은
18시 stocks_daily_pipeline이 KIS에서 받아 같은 키에 덮어쓴다. Asset은 발행하지 않는다 —
파생 지표 계산은 마감 후 한 번이면 된다.
"""

from __future__ import annotations

from datetime import datetime

try:
    from airflow.sdk import dag, task
    from airflow.sdk.exceptions import AirflowSkipException
except ImportError:
    AirflowSkipException = None
    dag = None
    task = None


if dag and task:

    @dag(
        dag_id="stocks_intraday_candles",
        start_date=datetime(2026, 1, 1),
        schedule="0 9-17 * * 1-5",
        catchup=False,
        max_active_runs=1,
        tags=["stocks"],
    )
    def stocks_intraday_candles():
        @task(retries=1)
        def check_market_open() -> None:
            from pipelines.common.utils.time import now_kst
            from pipelines.stocks.extractors.kis import is_market_open

            today = now_kst().date()
            if not is_market_open(today):
                raise AirflowSkipException(f"{today} 휴장일 — 수집을 건너뛴다")

        @task(retries=2)
        def collect_daily_candles() -> None:
            from pipelines.stocks.jobs.collect_daily_candles import run

            run()

        @task(retries=1)
        def aggregate_period_candles() -> int:
            from pipelines.stocks.jobs.aggregate_period_candles import run

            return run()

        check_market_open() >> collect_daily_candles() >> aggregate_period_candles()

    stocks_intraday_candles()
