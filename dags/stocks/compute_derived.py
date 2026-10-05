"""파생 지표 계산 (PER·PBR·수익률).

시각이 아니라 시세 Asset 하나를 구독한다. 계산에 시세와 재무가 모두 필요한데, 재무는 같은 날
아침에 먼저 채워진다.

    companies_collect_kis_financials (06:00) → etl://companies/financials
    stocks_daily_pipeline    (20:30) → etl://stocks/daily
                                              ↓ 시세가 갱신되면
                                       stocks_compute_derived

cron으로 "22:00"처럼 못 박으면 앞 단계가 늦어지거나 실패한 날에도 그대로 돌아, 어제 시세와
오늘 재무를 섞은 값이 나온다. 지표가 틀렸다는 사실이 화면에 드러나지 않아 더 나쁘다.

PER은 분기 EPS 4개를 더한 TTM으로 계산하므로 재무가 먼저 확정돼야 한다.
"""

from __future__ import annotations

from datetime import datetime, timedelta

try:
    from airflow.sdk import Asset, dag, task
except ImportError:
    Asset = None
    dag = None
    task = None


if dag and task:

    @dag(
        dag_id="stocks_compute_derived",
        start_date=datetime(2026, 1, 1),
        schedule=Asset("etl://stocks/daily"),
        catchup=False,
        max_active_runs=1,
        tags=["stocks"],
    )
    def stocks_compute_derived():
        @task(retries=2)
        def compute_derived() -> None:
            from pipelines.stocks.jobs.compute_derived import run

            run()

        @task(retries=2, retry_delay=timedelta(minutes=2))
        def publish_hot_themes() -> dict:
            from pipelines.themes.jobs.publish_hot_themes import run

            return run()

        @task(retries=2, retry_delay=timedelta(minutes=2))
        def publish_briefing() -> dict:
            from pipelines.briefings.jobs.publish_briefing import run

            return run()

        compute_derived() >> publish_hot_themes() >> publish_briefing()

    stocks_compute_derived()
