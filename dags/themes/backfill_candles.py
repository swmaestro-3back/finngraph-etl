"""테마 지수 일봉·기간봉 백필 (수동 실행).

일별 갱신은 THEME_DAILY_LOOKBACK_DAYS(기본 10일)만 되짚으므로 그보다 과거는 이 DAG 로만 채워진다.
최초 배포 뒤 한 번 돌려 지수 이력을 만든다.

**주의 — 체인 지수.** 각 날의 지수는 전날 지수에 하루 수익률을 곱한 값이라, 중간 구간만
다시 계산하면 `end_date` 다음 날부터의 기존 행과 값이 이어지지 않는다. `end_date` 는 비워서
오늘까지 계산하는 것을 권장한다.

**알려진 한계.** theme_stocks 와 stocks.listed_shares 는 현재 값뿐이라 과거를 "지금의 구성·
지금의 주식수"로 재구성한다(생존 편향).

**실행 시각.** 장중(09~18시)에는 stocks_intraday_candles 가 같은 행을 갱신하므로 장외 시간에
실행한다.
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
        dag_id="themes_backfill_candles",
        start_date=datetime(2026, 1, 1),
        schedule=None,
        catchup=False,
        max_active_runs=1,
        tags=["themes", "backfill", "manual"],
        doc_md=__doc__,
        params={
            "start_date": Param(
                default=None,
                type=["string", "null"],
                format="date",
                title="시작일",
                description="비우면 stock_candles_daily 의 가장 이른 거래일부터",
            ),
            "end_date": Param(
                default=None,
                type=["string", "null"],
                format="date",
                title="종료일",
                description="비우면 오늘. 체인 지수라 오늘까지 계산하는 것을 권장",
            ),
            "theme_ids": Param(
                default=[],
                type="array",
                items={"type": "integer"},
                title="테마 id",
                description="비우면 편입 종목이 있는 전체 테마",
            ),
        },
    )
    def themes_backfill_candles():
        def _window(params: dict) -> tuple[date, date | None]:
            from pipelines.common.clients.postgres import session_scope
            from pipelines.stocks.repositories.postgres.stock_candles import fetch_first_trade_date

            def as_date(value: str | None) -> date | None:
                return date.fromisoformat(value) if value else None

            start = as_date(params["start_date"])
            if start is None:
                with session_scope() as session:
                    start = fetch_first_trade_date(session)
            if start is None:
                raise ValueError("종목 일봉이 없어 백필 시작일을 정할 수 없다")
            return start, as_date(params["end_date"])

        @task(retries=1)
        def backfill_theme_daily(params: dict) -> int:
            from pipelines.themes.jobs.calculate_theme_candles import run_daily

            start, end = _window(params)
            return run_daily(start=start, end=end, theme_ids=params["theme_ids"] or None)

        @task(retries=1)
        def backfill_theme_period(params: dict) -> int:
            from pipelines.themes.jobs.calculate_theme_candles import run_period

            start, _ = _window(params)
            return run_period(since=start, theme_ids=params["theme_ids"] or None)

        @task(retries=1)
        def calculate_change_rates(params: dict) -> int:
            from pipelines.themes.jobs.calculate_theme_candles import run_change_rates

            start, _ = _window(params)
            return run_change_rates(since=start, theme_ids=params["theme_ids"] or None)

        backfill_theme_daily() >> backfill_theme_period() >> calculate_change_rates()

    themes_backfill_candles()
