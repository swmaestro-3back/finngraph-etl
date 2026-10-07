"""이슈 타임라인 연결을 기존 이슈 전체에 만들거나 처음부터 다시 만든다.

수동 실행 전용이다. scripts/backfill_issue_timeline.py 와 같은 작업
(pipelines/news/jobs/backfill_issue_timeline.py)을 서버에 접속하지 않고 실행하려고 둔다.

- 처음 배포한 뒤: `links=true, apply=true` 로 전체 기간을 잇고, 끝나면
  NEWS_ISSUE_LINK_ENABLED 를 켠다.
- 판정 기준(임계값·임베딩 모델·판정 방식)을 바꾼 뒤: `reset=true, links=true, apply=true`.
- 옛 기사 백필(news_backfill_krx100) 뒤:
  `reset=true, links=true, since_days=<백필 기간>, apply=true`.

apply 가 false 이면 쓰지 않고 대상 수와 예시만 로그에 남긴다. 실행하는 동안 스케줄 연결과 판정이
겹치지 않게 NEWS_ISSUE_LINK_ENABLED 를 끈다.
"""

from __future__ import annotations

from datetime import datetime, timedelta

try:
    from airflow.sdk import Param, dag, task
except ImportError:
    dag = None
    task = None
    Param = None


if dag and task:

    @dag(
        dag_id="news_backfill_issue_timeline",
        start_date=datetime(2026, 1, 1),
        schedule=None,
        catchup=False,
        max_active_runs=1,
        tags=["news", "backfill", "manual"],
        doc_md=__doc__,
        params={
            "reset": Param(
                default=False,
                type="boolean",
                title="연결 초기화",
                description="기간 안 이슈의 임베딩과 연결 판정을 지우고 처음부터 다시 만든다",
            ),
            "links": Param(
                default=True,
                type="boolean",
                title="연결",
                description="판정할 이슈가 없어질 때까지 오래된 것부터 잇는다",
            ),
            "since_days": Param(
                default=None,
                type=["null", "integer"],
                minimum=1,
                title="기간(일)",
                description="지금부터 이 일수 안에 시작한 이슈만 다룬다. 비우면 전체 기간",
            ),
            "limit": Param(
                default=None,
                type=["null", "integer"],
                minimum=1,
                title="연결 수 상한",
                description="이번 실행에서 판정할 이슈 수 상한. 비우면 전부",
            ),
            "apply": Param(
                default=False,
                type="boolean",
                title="실제로 쓰기",
                description="false 이면 대상 수와 예시만 로그에 남긴다",
            ),
        },
    )
    def news_backfill_issue_timeline():
        # 판정마다 커밋하므로 task 가 중간에 끊겨도 다시 실행하면 남은 이슈만 이어서 판정한다.
        @task(retries=2, retry_delay=timedelta(minutes=5))
        def backfill_issue_timeline(params: dict | None = None) -> dict[str, int]:
            from pipelines.news.jobs.backfill_issue_timeline import run

            return run(
                reset=params["reset"],
                links=params["links"],
                since_days=params["since_days"],
                limit=params["limit"],
                apply=params["apply"],
            )

        backfill_issue_timeline()

    news_backfill_issue_timeline()
