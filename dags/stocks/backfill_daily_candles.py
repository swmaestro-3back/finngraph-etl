"""과거 일봉 백필 (수동 실행).

기본 트리거는 서비스 대상 전 종목을 설정 연수(STOCK_DAILY_BACKFILL_YEARS)만큼 채운다.
Airflow UI 의 "Trigger DAG w/ config" 폼에서 종목·기간을 좁혀 부분 백필도 할 수 있다 —
특정 종목의 특정 연도만 다시 받아야 할 때 쓴다.

params 를 date 로 바꾸는 것까지가 이 층의 일이다. job 은 순수 파이썬 인자만 받는다.
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
        dag_id="stocks_backfill_daily_candles",
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
                description="비우면 서비스 대상 전체. 지정하면 그 종목만 (예: 005930)",
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
        },
    )
    def stocks_backfill_daily_candles():
        @task
        def backfill_daily_candles(params: dict) -> dict:
            from pipelines.stocks.jobs.backfill_daily_candles import run

            def as_date(value: str | None) -> date | None:
                return date.fromisoformat(value) if value else None

            return run(
                tickers=params["tickers"] or None,
                start=as_date(params["start_date"]),
                end=as_date(params["end_date"]),
            )

        backfill_daily_candles()

    stocks_backfill_daily_candles()
