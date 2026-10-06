from __future__ import annotations

from datetime import datetime

from pipelines.common.logging import get_logger
from pipelines.news.config import get_news_settings
from pipelines.news.extractors.search_collector import (
    collect_company_news,
    validate_search_settings,
)
from pipelines.news.extractors.text_fetcher import fetch_article_body
from pipelines.news.repositories.postgres.news import (
    has_article_body,
    remove_stored_by_url,
    save_news,
)
from pipelines.news.repositories.postgres.search_history import (
    CompanyQuery,
    fetch_due_company_queries,
    fetch_due_krx100_queries,
    fetch_due_ticker_queries,
    mark_companies_searched,
)
from pipelines.news.transformers.company_matches import (
    match_body_companies,
    match_title_companies,
)
from pipelines.news.transformers.filters.duplicate_filter import remove_duplicate_by_url
from pipelines.news.transformers.filters.entity_filter import filter_body_entities
from pipelines.news.transformers.filters.relevance_filter import filter_relevant_news
from pipelines.news.transformers.filters.title_filter import filter_titles
from pipelines.news.utils.date_utils import SEOUL_TIMEZONE

logger = get_logger(__name__)


def run(theme_ids: list[int], tickers: list[str] | None = None) -> dict[str, int]:
    """theme_ids 의 편입 기업 중 재검색 시점이 된 기업의 뉴스 수집"""

    validate_search_settings()
    settings = get_news_settings()
    run_started_at = datetime.now(SEOUL_TIMEZONE)

    # 1. 테마 편입 기업 중 search_history 기준 재검색 시점이 된 기업 조회
    if tickers:
        batch = fetch_due_ticker_queries(
            theme_ids, tickers, settings.search_interval_hours, run_started_at
        )
    else:
        batch = fetch_due_company_queries(theme_ids, settings.search_interval_hours, run_started_at)
    logger.info(
        "[collect_articles] 대상: 테마 %d개(%s) → 검색 기업 %d개 "
        "(간격 미도래 %d, company_id 없음 %d, 첫 검색 %d)",
        len(batch.theme_ids),
        f"핫테마 종목 {len(tickers)}개" if tickers else "편입 종목 전체",
        len(batch.queries),
        batch.skipped_not_due,
        batch.skipped_no_company,
        sum(1 for q in batch.queries if q.watermark is None),
    )

    return collect(batch.queries, run_started_at)


def run_krx100(
    company_ids: list[int],
    lookback_days: int | None = None,
    max_pages: int | None = None,
    query_templates: list[str] | None = None,
) -> dict[str, int]:
    """KRX100 기업 중 company_ids 에 든, 재검색 시점이 된 기업의 뉴스 수집 (news_backfill_krx100).

    lookback_days·max_pages 로 수집 창을 넓히고, query_templates 로 검색어 서식을 줄인다. None 이면
    스케줄 런과 같은 설정값이다. 서식마다 {name} 이 있어야 한다 — 없으면 모든 기업이 같은 검색어로
    검색된다.
    """

    if query_templates is not None and (
        not query_templates or any("{name}" not in template for template in query_templates)
    ):
        raise ValueError(f"검색어 서식마다 {{name}} 이 있어야 한다: {query_templates}")

    validate_search_settings()
    settings = get_news_settings()
    run_started_at = datetime.now(SEOUL_TIMEZONE)

    # 1. KRX100 기업 중 search_history 기준 재검색 시점이 된 기업 조회
    batch = fetch_due_krx100_queries(settings.search_interval_hours, run_started_at, company_ids)
    logger.info(
        "[collect_articles] 대상: KRX100 기업 %d개 → 검색 기업 %d개 (간격 미도래 %d, 첫 검색 %d)",
        len(company_ids),
        len(batch.queries),
        batch.skipped_not_due,
        sum(1 for q in batch.queries if q.watermark is None),
    )

    return collect(batch.queries, run_started_at, lookback_days, max_pages, query_templates)


def collect(
    queries: list[CompanyQuery],
    run_started_at: datetime,
    lookback_days: int | None = None,
    max_pages: int | None = None,
    query_templates: list[str] | None = None,
) -> dict[str, int]:
    """검색 대상 기업의 기사를 수집해 필터·본문 크롤링·기업 판정을 거쳐 저장하고 search_history 를
    마킹한다.

    클러스터 판정은 하지 않는다 — news_cluster_articles DAG 가 cluster_id 없는 기사를 읽어
    판정한다(jobs/cluster_articles.py).
    """

    if not queries:
        return {"saved": 0, "linked": 0}

    # 1. 네이버 API 호출 후 수동 필터링 + 연관성 필터링
    passed, failed_company_ids = _collect_and_filter(
        queries, run_started_at, lookback_days, max_pages, query_templates
    )

    # 8. 본문 크롤링 — 본문을 못 가져온 기사는 저장하지 않음
    with_body = _fetch_bodies(passed)

    # 9~10. 본문에만 나온 기업을 찾아 LLM 으로 판정한다. 통과한 기업이 _linked_companies 에 붙는다.
    # 후보가 너무 많은 기사(여러 종목을 모은 기사)는 여기서 버린다
    judged = _judge_body_companies(with_body)

    # 11~12. 본문과 함께 저장하고, 같은 트랜잭션에서 제목 판정을 통과한 기업과 본문 판정을 통과한
    # 기업 전부를 news_companies 에 연결한다
    save_result = save_news(judged)
    logger.info(
        "[collect_articles] 저장: 신규 %d / 기존 스킵 %d / 실패 %d",
        save_result["inserted_count"],
        save_result["skipped_existing_count"],
        save_result["failed_count"],
    )

    # 13. 기업 최신 검색 기록 갱신
    searched = [q.company_id for q in queries if q.company_id not in failed_company_ids]
    marked = mark_companies_searched(searched, run_started_at)

    logger.info(
        "[collect_articles] 완료: 저장 %d건, 기업 연결 %d행, 검색 기록 %d개 기업",
        save_result["inserted_count"],
        save_result["linked_count"],
        marked,
    )

    return {"saved": save_result["inserted_count"], "linked": save_result["linked_count"]}


def _collect_and_filter(
    queries: list[CompanyQuery],
    run_started_at: datetime,
    lookback_days: int | None,
    max_pages: int | None,
    query_templates: list[str] | None,
) -> tuple[list[dict], list[int]]:
    """네이버 수집부터 LLM 관련성 필터까지. (통과 기사, 수집에 실패한 company_id)를 돌려준다."""

    settings = get_news_settings()

    # 2. 네이버 기사 수집
    collected, failed_company_ids = collect_company_news(
        queries, run_started_at, lookback_days, max_pages, query_templates
    )
    logger.info(
        "[collect_articles] 수집: 기사 %d건 (기업 %d개 중 실패 %d개)",
        len(collected),
        len(queries),
        len(failed_company_ids),
    )
    if failed_company_ids:
        logger.warning(
            "[collect_articles] 수집 실패 기업 %d개: company_id=%s",
            len(failed_company_ids),
            failed_company_ids,
        )

    # 3. 제목 기업 매치 — 제목에 검색 대상 종목(별칭 포함)이 없는 기사 제거, 제목의 상장사는 판정
    # 대상으로 붙인다. URL 중복 제거보다 먼저다 — 같은 기사가 여러 종목 검색에 걸리면 중복 제거는
    # 먼저 걸린 사본만 남기는데, 그 검색 종목이 제목에 없으면 제목의 주인공 기업 사본까지 함께
    # 사라진다. 매치가 먼저 거르면 살아남는 사본은 검색 종목이 제목에 있는 것뿐이다.
    matched = match_title_companies(collected)

    # 4. 배치 내 URL 중복 제거
    unique_items, _ = remove_duplicate_by_url(matched.kept)

    # 5. 제목 필터 — 제외 패턴이나 종목 나열이 걸린 기사는 탈락, 통과 기사는 제목 선두 브라켓 제거
    titled_items, _ = filter_titles(unique_items)

    # 6. 이미 저장된 URL 제거 — DB 조회라 메모리 필터를 다 거친 뒤 한 번만 한다
    new_items = remove_stored_by_url(titled_items)
    logger.info(
        "[collect_articles] 필터: 제목에 검색 종목 %d (판정 기업 %d개, 기사당 최대 %d) "
        "→ URL 중복 제거 %d → 제목 필터 %d → DB 기존 제거 %d",
        len(matched.kept),
        matched.companies,
        matched.max_companies,
        len(unique_items),
        len(titled_items),
        len(new_items),
    )

    # 7. LLM 관련성 필터 — 제목만 본다. 본문 크롤링보다 먼저라 탈락 기사는 크롤링하지 않는다
    relevance = filter_relevant_news(new_items, max_concurrency=settings.news_llm_max_concurrency)
    if new_items and len(relevance.failed) == len(new_items):
        raise RuntimeError(
            f"관련성 판정 전건 실패 ({len(new_items)}건) — LLM 장애로 보고 재시도한다"
        )
    logger.info(
        "[collect_articles] LLM 관련성: 통과 %d (연결 기업 %d개) / 무효 %d / 실패 %d",
        len(relevance.passed),
        sum(len(item.get("_linked_companies") or []) for item in relevance.passed),
        len(relevance.invalid),
        len(relevance.failed),
    )

    return relevance.passed, failed_company_ids


def _fetch_bodies(items: list[dict]) -> list[dict]:
    """본문을 크롤링해 본문이 있는 기사만 돌려준다.

    본문을 못 가져온 기사는 버린다 — 행이 남지 않으므로 다른 종목 검색으로 다시 들어오면 관련성
    판정과 크롤링을 한 번 더 탄다.
    """

    fetched = fetch_article_body(items)
    with_body = [item for item in fetched if has_article_body(item)]
    logger.info(
        "[collect_articles] 본문: 대상 %d → 확보 %d / 실패 %d(버림)",
        len(fetched),
        len(with_body),
        len(fetched) - len(with_body),
    )

    return with_body


def _judge_body_companies(items: list[dict]) -> list[dict]:
    """본문에만 나온 기업을 사전으로 찾고 LLM 엔티티 필터로 거른다. 저장할 기사를 돌려준다.

    제목에서 판정받은 기업은 다시 판정하지 않는다. 호출이 실패한 기사는 제목 기업만 연결된다.
    호출한 기사가 전부 실패하면 LLM 장애로 보고 저장 전에 예외를 올린다.

    본문 후보가 NEWS_BODY_CANDIDATE_MAX 를 넘는 기사는 버린다 — 여러 종목을 모은 기사(공시·시황
    모음, 다이제스트)라 문단마다 다른 기업의 사건이 실려 있고, 저장하면 제목과 무관한 기업이
    연결된다. LLM 은 부르지 않는다.
    """

    settings = get_news_settings()

    match_body_companies(items)
    kept = [
        item for item in items if len(item["_body_companies"]) <= settings.news_body_candidate_max
    ]
    outcome = filter_body_entities(
        kept,
        max_concurrency=settings.news_llm_max_concurrency,
        body_limit=settings.news_llm_body_limit,
    )
    if outcome.judged and outcome.failed == outcome.judged:
        raise RuntimeError(
            f"엔티티 판정 전건 실패 ({outcome.judged}건) — LLM 장애로 보고 재시도한다"
        )
    logger.info(
        "[collect_articles] 본문 기업: 후보 %d개 초과 %d건(버림) / 후보 표기 %d개 (기사 %d건) "
        "→ 통과 %d개 / 호출 실패 %d건",
        settings.news_body_candidate_max,
        len(items) - len(kept),
        sum(len(item["_body_companies"]) for item in kept),
        outcome.judged,
        outcome.kept,
        outcome.failed,
    )

    return kept
