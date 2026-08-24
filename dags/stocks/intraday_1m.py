from __future__ import annotations

from datetime import datetime

try:
    from airflow.sdk import dag, task
except ImportError:
    dag = None
    task = None


if dag and task:

    @dag(
        dag_id="stocks_intraday_1m",
        start_date=datetime(2026, 1, 1),
        # 분봉은 수집 범위에서 제외했다. 종목당 장중 5분마다 1회라 호출이 일봉의 수십 배인데,
        # 화면 요구사항이 일봉 기준이라 값을 쓰지 않는다. 코드는 남겨 두고 스케줄만 끈다 —
        # 필요해지면 cron만 되돌리면 된다. 수동 실행은 scripts/run_job.py로 가능하다.
        schedule=None,
        catchup=False,
        max_active_runs=1,
        tags=["stocks"],
    )
    def stocks_intraday_1m():
        @task(retries=2)
        def collect_intraday_1m() -> None:
            from pipelines.stocks.jobs.collect_intraday_1m import run

            run()

        collect_intraday_1m()

    stocks_intraday_1m()
