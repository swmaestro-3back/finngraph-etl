"""장 마감 후 종목 데이터 파이프라인.

순서가 의미를 갖는다.

    일봉 ─┬─ 기간봉(주·월) ──────────────┐
          └─ 테마 일봉 → 테마 기간봉 ──┴─ 투자자 수급

테마 봉은 종목 일봉으로 계산하는 시총 가중 지수라 일봉 뒤에 두고, KIS 를 쓰지 않아 기간봉과
병렬이다. 수급이 두 갈래 뒤에 있으므로 Asset(etl://stocks/daily)은 테마 봉까지 있는 상태에서
발행되고, 그 뒤의 핫테마 발행이 테마 봉을 읽을 수 있다.

같은 KIS 호출 예산을 쓰는 작업은 한 DAG에 묶어 동시 실행을 피한다 — 여러 DAG가 겹치면
초당 호출 한도에 걸린다. 수급은 네이버 API 라 KIS 예산과 무관하지만, 마감 후 확정값을
받는 일배치라 여기에 함께 둔다.

끝나면 Asset을 발행한다. 파생 지표(PER·PBR·수익률)는 시세와 재무가 둘 다 있어야
계산되므로, 시각이 아니라 두 Asset이 모두 갱신된 시점에 기동한다(stocks_compute_derived).
핫테마 발행과 브리핑 생성도 그 뒤에 붙는다 — 백엔드가 기준일을 캔들·밸류에이션이 둘 다
있는 날로 잡기 때문에, 일봉 직후에 발행하면 전날 값이 나간다.

18:00에 도는 이유는 장 마감(15:30)과 정산 시차 때문이다. 마감 직후에는 당일 일봉이
확정되지 않는다.
"""

from __future__ import annotations

from datetime import datetime, timedelta

try:
    from airflow.sdk import Asset, dag, task
    from airflow.sdk.exceptions import AirflowSkipException
except ImportError:
    AirflowSkipException = None
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

        @task(retries=2)
        def collect_period_candles() -> None:
            from pipelines.stocks.jobs.collect_daily_candles import run_period

            run_period()

        @task(retries=2)
        def calculate_theme_daily() -> int:
            from pipelines.themes.jobs.calculate_theme_candles import run_daily

            return run_daily()

        @task(retries=2)
        def calculate_theme_period() -> int:
            from pipelines.common.config import get_settings
            from pipelines.common.utils.time import now_kst
            from pipelines.themes.jobs.calculate_theme_candles import run_period

            since = now_kst().date() - timedelta(days=get_settings().theme_daily_lookback_days)
            return run_period(since=since)

        # 수급은 네이버 비공식 API 라 차단 의심 시 즉시 실패한다. 기본 5분 재시도는
        # 차단이 풀리기에 짧아 간격을 늘려 둔다.
        @task(retries=2, retry_delay=timedelta(minutes=15), outlets=[stocks_daily_collected])
        def collect_investor_flows() -> None:
            from pipelines.stocks.jobs.collect_investor_flows import run

            run()

        candles = check_market_open() >> collect_daily_candles()
        flows = collect_investor_flows()
        candles >> collect_period_candles() >> flows
        candles >> calculate_theme_daily() >> calculate_theme_period() >> flows

    stocks_daily_pipeline()
