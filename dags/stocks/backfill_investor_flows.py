"""투자자 수급 백필. 수동 실행 전용.

네이버 trend API의 startIdx 페이징으로 과거를 채운다. 1페이지가 365건(≈1년치)이고,
UI 폼에서 페이지 수와 종목을 지정한다. upsert 라 재실행해도 안전하다.

차단 의심(BlockSuspectedError)이면 job 이 즉시 실패하고, 재시도는 Airflow 에 맡긴다 —
10분 → 20분 → 40분 지수 백오프로 세 번 다시 들어간다. job 의 재개 필터가 이미 채워진
종목을 건너뛰므로 재시도는 멈춘 지점부터 이어하는 셈이 된다.
"""

from __future__ import annotations

from datetime import datetime, timedelta

try:
    from airflow.sdk import Param, dag, task
except ImportError:
    Param = None
    dag = None
    task = None


if dag and task:

    @dag(
        dag_id="stocks_backfill_investor_flows",
        start_date=datetime(2026, 1, 1),
        schedule=None,
        catchup=False,
        max_active_runs=1,
        tags=["stocks", "backfill", "manual"],
        doc_md=__doc__,
        params={
            "pages": Param(
                default=1,
                type="integer",
                minimum=1,
                maximum=10,
                title="페이지 수",
                description="1페이지 = 365건 ≈ 1년치",
            ),
            "tickers": Param(
                default=[],
                type="array",
                items={"type": "string", "pattern": "^[0-9A-Z]{6}$"},
                title="종목 단축코드",
                description="비우면 서비스 대상 전체 (예: 005930)",
            ),
        },
    )
    def stocks_backfill_investor_flows():
        @task(
            retries=3,
            retry_delay=timedelta(minutes=10),
            retry_exponential_backoff=True,
            max_retry_delay=timedelta(hours=1),
        )
        def backfill_investor_flows(params: dict) -> dict:
            from pipelines.stocks.jobs.backfill_investor_flows import run

            return run(pages=params["pages"], tickers=params["tickers"] or None)

        backfill_investor_flows()

    stocks_backfill_investor_flows()
