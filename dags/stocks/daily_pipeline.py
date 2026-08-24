"""장 마감 후 종목 데이터 파이프라인.

순서가 의미를 갖는다.

    일봉 → 기간봉(주·월) → 투자자 수급

같은 KIS 호출 예산을 쓰는 작업은 한 DAG에 묶어 동시 실행을 피한다 — 여러 DAG가 겹치면
초당 호출 한도에 걸린다. 수급도 마감 후 KIS 배치라 여기에 함께 둔다.

끝나면 Asset을 발행한다. 파생 지표(PER·PBR·수익률)는 시세와 재무가 둘 다 있어야
계산되므로, 시각이 아니라 두 Asset이 모두 갱신된 시점에 기동한다(stocks_compute_derived).

18:00에 도는 이유는 장 마감(15:30)과 정산 시차 때문이다. 마감 직후에는 당일 일봉이
확정되지 않는다.
"""

from __future__ import annotations

from datetime import datetime

try:
    from airflow.sdk import Asset, dag, task
except ImportError:
    Asset = None
    dag = None
    task = None


if dag and task:
    # 시세·수급 확정 신호. stocks_compute_derived가 구독한다.
    stocks_daily_collected = Asset("etl://stocks/daily")

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

        @task(retries=2, outlets=[stocks_daily_collected])
        def collect_investor_flows() -> None:
            from pipelines.stocks.jobs.collect_investor_flows import run

            run()

        collect_daily_candles() >> collect_period_candles() >> collect_investor_flows()

    stocks_daily_pipeline()
