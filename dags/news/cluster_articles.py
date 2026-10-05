"""뉴스 클러스터 판정 → 대표 선정 → Event 생성·요약 → 이슈 타임라인 연결.

`news_collect_articles` 가 발행하는 `etl://news/articles` Asset 으로 깨어난다. 이벤트 내용은 쓰지
않고 DB 를 폴링한다 — `assign_clusters` 는 cluster_id 가 없는 기사 전량을, 나머지 task 는 각자의
미처리분 전량을 읽는다. 런이 도는 동안 쌓인 이벤트는 다음 런 하나가 한꺼번에 처리한다.

`max_active_runs=1` 이라 수집이 겹쳐도 같은 사건에 클러스터가 둘 생기지 않는다. 클러스터를 쓰는
DAG 는 이것과 백필 전용 `news_backfill_cluster_articles` 둘이고 서로는 막지 않는다 —
`news_backfill_krx100` 을 돌리는 동안에는 이 DAG 를 꺼 둔다. 켜 두면 수집 중인 백필 기사를 이
DAG 가 먼저 판정한다.

`promote_clusters` 는 삼중항 미처리 대표 기사가 남아 있을 때만 `etl://news/clusters` 를 발행해
`triples_extract_triples` 를 깨운다. 이번 런에 승격이 없어도 지난 런에 추출이 실패한 대표가 있으면
다시 깨운다. 발행을 건너뛰어도(skip) Event 생성과 요약은 돈다(`trigger_rule="none_failed"`).

`link_issues` 는 요약 뒤에 이슈를 같은 이야기의 앞선 이슈에 잇는다. 요약이 실패해도 돌고
(`trigger_rule="all_done"`), `NEWS_ISSUE_LINK_ENABLED` 가 꺼져 있으면 아무것도 하지 않는다.
요약 실패가 성공 런에 묻히지 않게 요약 뒤에 말단 `finish` 를 둔다.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

try:
    from airflow.sdk import Asset, dag, task
    from airflow.sdk.exceptions import AirflowSkipException
except ImportError:
    AirflowSkipException = None
    Asset = None
    dag = None
    task = None


if dag and task:
    news_clusters_updated = Asset("etl://news/clusters")

    @dag(
        dag_id="news_cluster_articles",
        start_date=datetime(2026, 1, 1),
        schedule=Asset("etl://news/articles"),
        catchup=False,
        max_active_runs=1,
        tags=["news"],
        doc_md=__doc__,
    )
    def news_cluster_articles():
        @task(retries=1, retry_delay=timedelta(minutes=5))
        def assign_clusters() -> dict[str, int]:
            from pipelines.news.jobs.cluster_articles import assign

            return assign()

        @task(retries=1, retry_delay=timedelta(minutes=5), outlets=[news_clusters_updated])
        def promote_clusters() -> dict[str, int]:
            from pipelines.news.jobs.cluster_articles import promote

            result = promote()
            if result["pending_triples"] == 0:
                # 스킵하면 outlets 를 발행하지 않는다. 실패가 아니라 "삼중항이 할 일이 없다"다.
                raise AirflowSkipException("삼중항 미처리 대표 0건 — 삼중항 DAG 를 깨우지 않는다")
            return result

        # 클러스터별로 실패가 격리돼 있고 실패한 클러스터는 다음 런에 자연히 재시도된다.
        # promote_clusters 가 skip 이어도 돈다 — 제목은 지난 런에 붙었을 수 있다.
        @task(retries=1, retry_delay=timedelta(minutes=10), trigger_rule="none_failed")
        def generate_events() -> dict[str, int]:
            from pipelines.events.jobs.generate_events import run

            return run()

        @task(retries=1, retry_delay=timedelta(minutes=10), trigger_rule="none_failed")
        def summarize_articles() -> dict[str, Any]:
            from pipelines.news.jobs.summarize_articles import run

            result = run()
            return {"fetched": result["fetched"], "saved": result["saved"]}

        # 요약 뒤에 돌아야 이번 런에 승격된 이슈가 한 줄 요약을 넣고 임베딩된다. 요약이 실패해도
        # 돈다(all_done) — 요약 없는 이슈는 승격·제목 생성 24시간 뒤에 요약 없이 잇는다.
        @task(retries=1, retry_delay=timedelta(minutes=10), trigger_rule="all_done")
        def link_issues() -> dict[str, int]:
            from pipelines.news.jobs.link_issues import run

            return run()

        # DAG run 상태는 말단 task 로만 정해진다. 요약 뒤에 all_done 인 link_issues 가 붙으면 요약
        # 실패가 성공 런에 묻히므로, 요약 성공을 요구하는 말단을 하나 둬 실패를 런에 남긴다.
        @task
        def finish() -> None:
            return None

        promoted = promote_clusters()
        summarized = summarize_articles()
        assign_clusters() >> promoted
        promoted >> [generate_events(), summarized]
        summarized >> [link_issues(), finish()]

    news_cluster_articles()
