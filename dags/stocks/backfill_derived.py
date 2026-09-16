"""파생 지표 소급 계산 (수동 실행).

정기 계산은 stocks_compute_derived 가 Asset 트리거로 최근 30일만 다시 계산한다. 일봉을
과거까지 백필한 뒤에는 그 구간의 PER·PBR·수익률이 비어 있으므로 여기서 소급한다.

계산은 SQL 두 문장이라 구간을 넓혀도 부담이 작고, 재실행하면 같은 결과로 덮어쓴다.
"""

from __future__ import annotations

from datetime import datetime

try:
    from airflow.sdk import Param, dag, task
except ImportError:
    Param = None
    dag = None
    task = None


if dag and task:

    @dag(
        dag_id="stocks_backfill_derived",
        start_date=datetime(2026, 1, 1),
        schedule=None,
        catchup=False,
        max_active_runs=1,
        tags=["stocks", "backfill", "manual"],
        doc_md=__doc__,
        params={
            "lookback_days": Param(
                default=1825,
                type="integer",
                minimum=1,
                title="소급 일수",
                description="오늘부터 며칠 전까지 다시 계산할지. 1825 = 5년",
            ),
        },
    )
    def stocks_backfill_derived():
        @task(retries=2)
        def compute_derived(params: dict) -> None:
            from pipelines.stocks.jobs.compute_derived import run

            run(lookback_days=params["lookback_days"])

        compute_derived()

    stocks_backfill_derived()
