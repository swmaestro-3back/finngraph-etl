"""백필 뉴스 클러스터 판정 → 대표 선정 → Event 생성·요약 → 이슈 타임라인 연결.

`news_backfill_krx100` 이 모든 청크의 수집을 끝낸 뒤 한 번 발행하는 `etl://news/backfill-articles`
Asset 으로 깨어난다. task 구성은 `news_cluster_articles` 와 같고, 다른 점은 둘이다.

- **한 번에 판정한다.** 청크마다 판정하면 뒤 청크 기업의 기사가 앞 청크가 만든 클러스터의 시간
  창(기준점 1일 전 ~ 7일 뒤)보다 이르게 도착해 같은 사건에 클러스터가 하나 더 생긴다. 수집이 다
  끝난 뒤 cluster_id 가 없는 기사 전량을 발행 시각순으로 한 번에 본다.
- **IDF 에 배치 문서 수를 더한다.** 초기 백필에는 저장된 IDF 표가 비어 있어, 그대로 쓰면 기업명
  같은 흔한 토큰이 눌리지 않는다(`assign(include_batch_idf=True)`).

**백필 중에는 `news_cluster_articles` 를 꺼 둔다.** 두 DAG 모두 출처를 가리지 않고 cluster_id 가
없는 기사 전량을 읽는다. 켜 두면 정시 수집이 깨운 `news_cluster_articles` 가 수집 중인 백필 기사를
먼저 판정하고, 이 DAG 와 겹쳐 돌면 같은 사건에 클러스터가 둘 생긴다. 이 DAG 가 끝난 뒤 다시 켠다 —
그동안 정시 수집이 저장한 기사는 이 DAG 가 함께 판정한다.

삼중항 추출은 `news_cluster_articles` 와 같은 `etl://news/clusters` 를 발행해
`triples_extract_triples` 가 처리한다.

`link_issues` 는 스케줄 연결과 같은 제약(실행당 상한, lookback, 재판정 기간)으로 실행되므로, 백필로
들어온 옛 이슈를 모두 올바른 부모에 잇지는 못한다. 이 DAG 와 요약이 끝나면
`news_backfill_issue_timeline` 을 `reset=true, links=true, since_days=<백필 기간>, apply=true` 로
실행해 그 기간의 연결을 다시 만든다(pipelines/news/README.md).
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
        dag_id="news_backfill_cluster_articles",
        start_date=datetime(2026, 1, 1),
        schedule=Asset("etl://news/backfill-articles"),
        catchup=False,
        max_active_runs=1,
        tags=["news", "backfill"],
        doc_md=__doc__,
    )
    def news_backfill_cluster_articles():
        @task(retries=1, retry_delay=timedelta(minutes=5))
        def assign_clusters() -> dict[str, int]:
            from pipelines.news.jobs.cluster_articles import assign

            return assign(include_batch_idf=True)

        @task(retries=1, retry_delay=timedelta(minutes=5), outlets=[news_clusters_updated])
        def promote_clusters() -> dict[str, int]:
            from pipelines.news.jobs.cluster_articles import promote

            result = promote()
            if result["pending_triples"] == 0:
                # 스킵하면 outlets 를 발행하지 않는다. 실패가 아니라 "삼중항이 할 일이 없다"다.
                raise AirflowSkipException("삼중항 미처리 대표 0건 — 삼중항 DAG 를 깨우지 않는다")
            return result

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

        # 이번 실행에 승격된 이슈가 한 줄 요약까지 넣어 임베딩되도록 요약 뒤에 실행한다. 요약이
        # 실패해도 실행되며(all_done), 요약이 끝내 없는 이슈는 승격·제목 생성 24시간 뒤에 요약 없이
        # 잇는다.
        @task(retries=1, retry_delay=timedelta(minutes=10), trigger_rule="all_done")
        def link_issues() -> dict[str, int]:
            from pipelines.news.jobs.link_issues import run

            return run()

        # DAG run 상태는 뒤에 이어지는 task 가 없는 마지막 task 들로만 정해진다. all_done 인
        # link_issues 만 마지막 task 로 두면 요약이 실패해도 실행이 성공으로 끝나므로, 요약 성공을
        # 요구하는 마지막 task 를 하나 더 둔다.
        @task
        def finish() -> None:
            return None

        promoted = promote_clusters()
        summarized = summarize_articles()
        assign_clusters() >> promoted
        promoted >> [generate_events(), summarized]
        summarized >> [link_issues(), finish()]

    news_backfill_cluster_articles()
