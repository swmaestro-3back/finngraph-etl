"""KRX100 기업 뉴스 백필 (수동 실행).

초기 데이터가 없을 때 KRX100 편입 기업의 과거 뉴스를 미리 채운다. 수집 이후 단계(필터·본문
크롤링·기업 판정·저장)는 news_collect_articles 와 같다. 클러스터 판정은 청크마다 하지 않는다 —
모든 청크가 끝난 뒤 publish_backfill 이 etl://news/backfill-articles Asset 을 한 번 발행하고,
news_backfill_cluster_articles → triples_extract_triples 가 저장된 기사 전량을 한 번에 처리한다.

**트리거 전에 news_cluster_articles 를 꺼 둔다.** 켜 두면 정시 수집이 깨운 그 DAG 가 수집 중인
백필 기사를 먼저 판정한다. news_backfill_cluster_articles 가 끝난 뒤 다시 켠다.

**대상.** stocks.krx100 활성 종목의 기업 중 search_history 기준 재검색 시점이 된 기업만 —
스케줄 런과 같은 워터마크 규칙이라, 중간에 실패해도 다시 트리거하면 끝난 청크는 건너뛰고
기록이 있는 기업은 그 이후만 읽는다.

**청크.** 기업을 CHUNK_SIZE 개씩 나눠 하나씩 순서대로 돈다(네이버 API 요청 간격 유지). 청크마다
search_history 를 마킹하므로 실패해도 그 청크만 재시도한다.

**발행.** 실패한 청크가 있으면 발행하지 않는다 — 일부만 모인 채로 판정하지 않기 위해서다. 다시
트리거하면 남은 기업만 수집한 뒤 발행한다. 대상 기업이 없어 수집을 건너뛴 런도 판정을 기다리는
기사가 남아 있으면 발행한다.

**비용.** 처음 검색하는 기업은 lookback 전체를 읽고 새 기사를 전부 LLM 관련성 필터에 보낸다.
통과한 기사는 전부 본문을 크롤링하고, 본문에 다른 상장사가 나오면 LLM 엔티티 필터를 한 번 더 탄다.
처음엔 lookback_days 를 작게 줘서 청크당 기사 수·소요 시간을 확인하는 것을 권장한다.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

try:
    from airflow.sdk import Asset, Param, dag, task
    from airflow.sdk.exceptions import AirflowSkipException
except ImportError:
    AirflowSkipException = None
    Asset = None
    Param = None
    dag = None
    task = None

# 청크 하나 = 기업 20개 × 검색어 6개(NEWS_SEARCH_QUERY_TEMPLATES) × 최대 8페이지
# = 네이버 API 최대 960회
CHUNK_SIZE = 20


if dag and task:
    news_backfill_articles_saved = Asset("etl://news/backfill-articles")

    @dag(
        dag_id="news_backfill_krx100",
        start_date=datetime(2026, 1, 1),
        schedule=None,
        catchup=False,
        max_active_runs=1,
        tags=["news", "backfill", "manual"],
        doc_md=__doc__,
        params={
            "lookback_days": Param(
                default=180,
                type="integer",
                minimum=1,
                maximum=180,
                title="수집 기간(일)",
                description="처음 검색하는 기업이 거슬러 갈 일수. 최대 6개월",
            ),
            "max_pages": Param(
                default=8,
                type="integer",
                minimum=1,
                maximum=10,
                title="검색어당 최대 페이지",
                description="페이지당 100건. 네이버 검색 API 는 start 1000 까지라 10 이 상한",
            ),
        },
    )
    def news_backfill_krx100():
        @task(retries=1, retry_delay=timedelta(minutes=5))
        def select_companies() -> list[list[int]]:
            from pipelines.news.jobs.select_krx100_companies import run

            chunks = run(CHUNK_SIZE)
            if not chunks:
                raise AirflowSkipException("재검색 시점이 된 KRX100 기업이 없다")
            return chunks

        # 청크를 동시에 돌리면 네이버 API 요청 간격(REQUEST_DELAY)이 깨진다
        @task(retries=2, retry_delay=timedelta(minutes=5), max_active_tis_per_dagrun=1)
        def collect_articles(company_ids: list[int], params: dict | None = None) -> dict[str, Any]:
            from pipelines.news.jobs.collect_articles import run_krx100

            return run_krx100(
                company_ids,
                lookback_days=params["lookback_days"],
                max_pages=params["max_pages"],
            )

        # none_failed: 청크가 하나라도 실패하면 돌지 않고, select_companies 가 스킵해 수집이 통째로
        # 건너뛰어진 런에는 돈다(지난 런이 남긴 미판정 기사를 처리한다).
        @task(trigger_rule="none_failed", outlets=[news_backfill_articles_saved])
        def publish_backfill() -> dict[str, int]:
            from pipelines.news.jobs.cluster_articles import count_unclustered

            unclustered = count_unclustered()
            if unclustered == 0:
                # 스킵하면 outlets 를 발행하지 않는다
                raise AirflowSkipException("미판정 기사 0건 — 백필 클러스터 DAG 를 깨우지 않는다")
            return {"unclustered": unclustered}

        collect_articles.expand(company_ids=select_companies()) >> publish_backfill()

    news_backfill_krx100()
