"""장중 일봉 갱신.

09~17시 매 정각 일봉을 받고, 그 일봉으로 이번 주·이번 달 봉을 계산한다. 주·월봉의 확정값은
18시 stocks_daily_pipeline이 KIS에서 받아 같은 키에 덮어쓴다. Asset은 발행하지 않는다 —
파생 지표 계산은 마감 후 한 번이면 된다.

테마 지수는 매시간 최근 lookback(기본 10일) 구간의 테마 일봉을 다시 계산한다. 종목 일봉
수집도 같은 창을 다시 받으므로 창을 맞춘다. 실제로 값이 바뀌는 행은 대개 당일뿐이다. 그 일봉으로
테마 주·월봉도 갱신한다. 테마 계산은 KIS 를 호출하지 않아 종목 기간봉 합성과 병렬로 둔다.

등락률(change_rate)은 봉 적재와 분리된 태스크가 각 갈래 끝에서 다시 계산한다.
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

        @task(retries=1)
        def calculate_theme_daily() -> int:
            from pipelines.themes.jobs.calculate_theme_candles import run_daily

            return run_daily()

        @task(retries=1)
        def aggregate_theme_period() -> int:
            from datetime import timedelta

            from pipelines.common.config import get_settings
            from pipelines.common.utils.time import now_kst
            from pipelines.themes.jobs.calculate_theme_candles import run_period

            since = now_kst().date() - timedelta(days=get_settings().theme_daily_lookback_days)
            return run_period(since=since)

        @task(retries=1)
        def calculate_stock_change_rates() -> int:
            from pipelines.stocks.jobs.calculate_change_rates import run_daily, run_period

            return run_daily() + run_period()

        @task(retries=1)
        def calculate_theme_change_rates() -> int:
            from pipelines.themes.jobs.calculate_theme_candles import run_change_rates

            return run_change_rates()

        candles = check_market_open() >> collect_daily_candles()
        candles >> aggregate_period_candles() >> calculate_stock_change_rates()
        (
            candles
            >> calculate_theme_daily()
            >> aggregate_theme_period()
            >> calculate_theme_change_rates()
        )

    stocks_intraday_candles()
