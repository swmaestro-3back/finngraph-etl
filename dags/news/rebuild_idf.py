"""클러스터 판정용 IDF 사전 재구축 — 매일 03:00.

    rebuild_idf

최근 NEWS_CLUSTER_IDF_DAYS 일의 벡터화된 뉴스 전체를 서브벡터 필드별로 집계해
news_cluster_idf_terms·news_cluster_idf_meta 를 통째로 교체한다. 판정(news_cluster_articles)은
이 표를 런 시작 때 읽어 런 내내 고정하므로, 그날 새 기사는 다음 재구축까지 표에 없고 처음 보는
토큰은 최대 IDF 를 받는다 — 논문의 고정 IDF 사전 동작이다. 백필 DAG 는 같은 job 을 벡터화 뒤·판정
앞에 한 번 직접 돈다. 수집 DAG 가 도는 시간대(07:30 이후)를 피한다.
"""

from __future__ import annotations

from datetime import datetime, timedelta

try:
    from airflow.sdk import dag, task
except ImportError:
    dag = None
    task = None


if dag and task:

    @dag(
        dag_id="news_rebuild_idf",
        start_date=datetime(2026, 1, 1),
        schedule="0 3 * * *",
        catchup=False,
        max_active_runs=1,
        tags=["news"],
        doc_md=__doc__,
    )
    def news_rebuild_idf():
        @task(retries=1, retry_delay=timedelta(minutes=10))
        def rebuild_idf() -> dict[str, int]:
            from pipelines.news.jobs.rebuild_idf import run

            return run()

        rebuild_idf()

    news_rebuild_idf()
