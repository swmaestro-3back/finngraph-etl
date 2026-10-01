"""KRX300 기업 뉴스 백필 (수동 실행).

초기 데이터가 없을 때 KRX300 편입 기업의 과거 뉴스를 미리 채운다. 수집 이후 단계(필터·클러스터·
저장)는 news_collect_articles 와 같고, 삼중항 추출·요약은 청크가 끝날 때마다 발행하는
etl://news/clusters Asset 을 따라 triples_extract_triples → news_summarize_articles 가 수집과
나란히 처리한다.

**대상.** stocks.krx300 활성 종목의 기업 중 search_history 기준 재검색 시점이 된 기업만 —
스케줄 런과 같은 워터마크 규칙이라, 중간에 실패해도 다시 트리거하면 끝난 청크는 건너뛰고
기록이 있는 기업은 그 이후만 읽는다.

**청크.** 기업을 CHUNK_SIZE 개씩 나눠 하나씩 순서대로 돈다(네이버 API 요청 간격 유지). 청크마다
search_history 를 마킹하므로 실패해도 그 청크만 재시도한다.

**비용.** 처음 검색하는 기업은 lookback 전체를 읽고 새 기사를 전부 LLM 관련성 필터에 보낸다.
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
    news_clusters_updated = Asset("etl://news/clusters")

    @dag(
        dag_id="news_backfill_krx300",
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
    def news_backfill_krx300():
        @task(retries=1, retry_delay=timedelta(minutes=5))
        def select_companies() -> list[list[int]]:
            from pipelines.news.jobs.select_krx300_companies import run

            chunks = run(CHUNK_SIZE)
            if not chunks:
                raise AirflowSkipException("재검색 시점이 된 KRX300 기업이 없다")
            return chunks

        # 청크를 동시에 돌리면 네이버 API 요청 간격(REQUEST_DELAY)이 깨진다
        @task(
            retries=2,
            retry_delay=timedelta(minutes=5),
            max_active_tis_per_dagrun=1,
            outlets=[news_clusters_updated],
        )
        def collect_articles(company_ids: list[int], params: dict | None = None) -> dict[str, Any]:
            from pipelines.news.jobs.collect_articles import run_krx300

            result = run_krx300(
                company_ids,
                lookback_days=params["lookback_days"],
                max_pages=params["max_pages"],
            )
            if result["created"] + result["updated"] == 0:
                raise AirflowSkipException("클러스터 생성·갱신 0건 — 하류를 깨우지 않는다")
            return result

        collect_articles.expand(company_ids=select_companies())

    news_backfill_krx300()
