"""파생 지표 계산 (PER·PBR·수익률).

시각이 아니라 Asset 두 개를 구독한다. 계산에 시세와 재무가 모두 필요한데, 둘은 서로
다른 DAG가 서로 다른 시각에 채운다.

    stocks_daily_pipeline    (18:00) → etl://stocks/daily
    companies_collect_kis_financials (19:00) → etl://companies/financials
                                              ↓ 둘 다 갱신되면
                                       stocks_compute_derived

cron으로 "20:00"처럼 못 박으면 앞 단계가 늦어지거나 실패한 날에도 그대로 돌아, 어제 시세와
오늘 재무를 섞은 값이 나온다. 지표가 틀렸다는 사실이 화면에 드러나지 않아 더 나쁘다.

PER은 분기 EPS 4개를 더한 TTM으로 계산하므로 재무가 먼저 확정돼야 한다.
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

    @dag(
        dag_id="stocks_compute_derived",
        start_date=datetime(2026, 1, 1),
        # 리스트는 AND다 — 두 Asset이 모두 갱신돼야 기동한다.
        schedule=[Asset("etl://stocks/daily"), Asset("etl://companies/financials")],
        catchup=False,
        max_active_runs=1,
        tags=["stocks"],
    )
    def stocks_compute_derived():
        @task(retries=2)
        def compute_derived() -> None:
            from pipelines.stocks.jobs.compute_derived import run

            run()

        compute_derived()

    stocks_compute_derived()
