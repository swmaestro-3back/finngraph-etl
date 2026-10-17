"""이슈 타임라인 연결을 기존 이슈 전체에 만들거나 처음부터 다시 만든다.

수동 실행 전용이며, pipelines/news/jobs/backfill_issue_timeline.py 를 실행한다.

- 처음 배포한 뒤: `links=true, apply=true` 로 전체 기간을 잇는다.
- 판정 기준(임계값·임베딩 모델·판정 방식)을 바꾼 뒤: `reset=true, links=true, apply=true`.
  초기화는 성격 분류도 지운다. 프롬프트와 입력이 같으면 LLM 응답 캐시를 써서 다시 분류한다.
- 옛 기사 백필(news_backfill_krx100) 뒤:
  `reset=true, links=true, since_days=<백필 기간>, apply=true`.

apply 가 false 이면 쓰지 않고 대상 수와 예시만 로그에 남긴다. 실행하는 동안 스케줄 연결과 판정이
겹치지 않게 news_cluster_articles DAG 를 멈춰 둔다. 멈춘 동안 쌓인 기사는 다시 켠 뒤 첫 실행이
클러스터에 넣는다.

판정 방식이 vote(기본)이면 이슈마다 LLM 을 여러 번 부른다. `max_llm_calls` 를 주면 새 호출 수
합계가 이 값에 닿을 때 남은 이슈를 두고 멈춘다. 받은 응답은 캐시에 남으므로 다시 실행하면 이어서
진행한다.
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
                description=(
                    "기간 안 이슈의 임베딩, 연결 판정, 성격 분류를 지우고 처음부터 다시 만든다"
                ),
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
            "max_llm_calls": Param(
                default=None,
                type=["null", "integer"],
                minimum=1,
                title="LLM 새 호출 수 상한",
                description="투표 판정의 LLM 새 호출 수 합계 상한. 닿으면 남은 이슈를 두고 멈춘다. "
                "비우면 상한 없음",
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
        def backfill_issue_timeline(params: dict | None = None) -> dict:
            from pipelines.news.jobs.backfill_issue_timeline import run

            return run(
                reset=params["reset"],
                links=params["links"],
                since_days=params["since_days"],
                limit=params["limit"],
                apply=params["apply"],
                max_llm_calls=params["max_llm_calls"],
            )

        backfill_issue_timeline()

    news_backfill_issue_timeline()
