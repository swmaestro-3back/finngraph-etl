"""과거 주봉·월봉 백필 (수동 실행).

일별 갱신은 STOCK_PERIOD_LOOKBACK_DAYS(기본 120일)만 되짚으므로 그보다 과거는 이 DAG 로만
채워진다. UI 폼에서 종목·기간·봉 종류를 좁혀 부분 백필도 할 수 있다.
"""

from __future__ import annotations

from datetime import date, datetime

try:
    from airflow.sdk import Param, dag, task
except ImportError:
    Param = None
    dag = None
    task = None


if dag and task:

    @dag(
        dag_id="stocks_backfill_period_candles",
        start_date=datetime(2026, 1, 1),
        schedule=None,
        catchup=False,
        max_active_runs=1,
        tags=["stocks", "backfill", "manual"],
        doc_md=__doc__,
        params={
            "tickers": Param(
                default=[],
                type="array",
                items={"type": "string", "pattern": "^[0-9A-Z]{6}$"},
                title="종목 단축코드",
                description="비우면 서비스 대상 전체 (예: 005930)",
            ),
            "start_date": Param(
                default=None,
                type=["string", "null"],
                format="date",
                title="시작일",
                description="비우면 STOCK_DAILY_BACKFILL_YEARS(기본 5년) 전부터",
            ),
            "end_date": Param(
                default=None,
                type=["string", "null"],
                format="date",
                title="종료일",
                description="비우면 오늘",
            ),
            "periods": Param(
                default=["W", "M"],
                type="array",
                items={"type": "string", "enum": ["W", "M"]},
                title="봉 종류",
                description="W=주봉, M=월봉",
            ),
        },
    )
    def stocks_backfill_period_candles():
        @task
        def backfill_period_candles(params: dict) -> dict:
            from pipelines.stocks.jobs.backfill_period_candles import run

            def as_date(value: str | None) -> date | None:
                return date.fromisoformat(value) if value else None

            return run(
                tickers=params["tickers"] or None,
                start=as_date(params["start_date"]),
                end=as_date(params["end_date"]),
                periods=params["periods"] or None,
            )

        backfill_period_candles()

    stocks_backfill_period_candles()
