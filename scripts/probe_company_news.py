"""기업명 몇 개로 뉴스 수집 → 필터 → LLM 관련성 판정까지만 돌려 결과를 눈으로 본다.

collect_articles.run 의 2~8 단계(네이버 검색, URL 중복 제거, 유형 필터, 제목 폴리싱, 후보 부착,
관련성 판정)를 같은 함수로 이어 붙인 것이다. DB 는 전혀 건드리지 않는다 — 테마·재검색 시점
조회, 저장된 URL 제거, 클러스터링, 저장, search_history 마킹은 모두 뺐다. 프롬프트를 바꾼 뒤
무엇이 통과하고 무엇이 왜 떨어지는지 확인하는 용도다.

네이버 검색 API 와 Bedrock 을 실제로 부르므로 pytest 가 잡지 않는 스크립트로 둔다.

검색 창은 collect_articles 의 첫 검색과 같다 — watermark 없이 NEWS_SEARCH_LOOKBACK_DAYS 전체.

실행: uv run python scripts/probe_company_news.py 엘앤에프 에코프로
"""

from __future__ import annotations

import argparse
import logging
from datetime import datetime
from typing import Any

from pipelines.news.config import get_news_settings
from pipelines.news.extractors.search_collector import (
    collect_company_news,
    validate_search_settings,
)
from pipelines.news.repositories.search_history import CompanyQuery
from pipelines.news.transformers.company_candidates import attach_candidate_companies
from pipelines.news.transformers.filters.duplicate_filter import remove_duplicate_by_url
from pipelines.news.transformers.filters.news_type_filter import filter_official_source_news
from pipelines.news.transformers.filters.relevance_filter import (
    RelevanceResult,
    filter_relevant_news,
)
from pipelines.news.utils.date_utils import SEOUL_TIMEZONE
from pipelines.news.utils.text_utils import remove_leading_title_brackets

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logging.getLogger("langchain_aws").setLevel(logging.WARNING)
log = logging.getLogger("probe_company_news")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("names", nargs="+", help="검색할 상장사 이름 (gazetteer 정식명)")
    return parser.parse_args()


def build_queries(names: list[str]) -> list[CompanyQuery]:
    # company_id 는 DB 저장 단계에서만 쓰여 여기서는 의미가 없다.
    return [CompanyQuery(company_id=0, name=name) for name in names]


def collect_and_filter(names: list[str]) -> tuple[RelevanceResult, dict[str, int]]:
    validate_search_settings()
    settings = get_news_settings()
    run_started_at = datetime.now(SEOUL_TIMEZONE)
    # 1. CompanyQuery 생성
    queries = build_queries(names)

    # 2. 네이버 기사 수집
    collected, failed_company_ids = collect_company_news(queries, run_started_at)
    if failed_company_ids:
        log.warning("수집 실패 기업 %d개", len(failed_company_ids))

    # 3. 배치 내 URL 중복 제거
    unique_items, _ = remove_duplicate_by_url(collected)

    # 4. 기사 유형 필터로 제거
    typed_items, _ = filter_official_source_news(
        unique_items,
        pipeline_input={},
        official_source_threshold=settings.official_source_threshold,
    )

    # 5. 제목 폴리싱 (선두 브라켓 제거)
    for item in typed_items:
        item["title"] = remove_leading_title_brackets(item.get("title", ""))

    # 7. 후보 상장사 부착 (gazetteer 활용)
    attach_candidate_companies(typed_items)

    # 8. LLM 관련성 필터
    relevance = filter_relevant_news(typed_items, max_concurrency=settings.news_llm_max_concurrency)

    counts = {
        "collected": len(collected),
        "unique": len(unique_items),
        "typed": len(typed_items),
    }
    return relevance, counts


def print_bucket(name: str, items: list[dict[str, Any]], with_subjects: bool) -> None:
    print(f"\n=== {name} ({len(items)}건) ===")
    for item in items:
        print(f"- {item.get('title', '')}")
        print(f"  {item.get('link') or item.get('originallink', '')}")
        if with_subjects:
            subjects = item.get("_subject_names", [])
            candidates = item.get("_candidate_companies", [])
            print(f"  종목: {subjects}  (후보: {candidates})")


def main() -> None:
    args = parse_args()
    relevance, counts = collect_and_filter(args.names)

    print(
        f"\n[필터] 수집 {counts['collected']} → URL 중복 제거 {counts['unique']} "
        f"→ 유형 필터 {counts['typed']}"
    )
    print(
        f"[LLM] 통과 {len(relevance.passed)} / 무효 {len(relevance.invalid)} "
        f"/ 무관 {len(relevance.irrelevant)} / 실패 {len(relevance.failed)}"
    )

    print_bucket("통과 (passed)", relevance.passed, with_subjects=True)
    print_bucket("무효 (invalid: 기업 자체 사건·관계 아님)", relevance.invalid, False)
    print_bucket("무관 (irrelevant: 후보 없음 또는 후보 밖 이름)", relevance.irrelevant, False)
    print_bucket("실패 (failed)", relevance.failed, False)


if __name__ == "__main__":
    main()
