"""뉴스 통합 수집 job (news_pipeline DAG의 collect_news task).

헤드라인·검색 수집을 동시에 진행하고, 병합 배치에 통합 필터 → 전체
클러스터링(클러스터당 최대 N개 선별) → 본문 크롤링 → 저장 → 클러스터 대표
기록 순으로 진행한다. 클러스터링을 본문 수집보다 앞에 두어, 버려질 기사의
본문은 크롤링하지 않는다.

한 소스 수집이 실패하면 경고만 남기고 나머지 소스로 계속 진행한다(부분 진행).
두 소스가 모두 실패했을 때만 예외를 올려 Airflow retry에 맡긴다.
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from pipelines.news.config import get_news_settings
from pipelines.news.extractors.headline_collector import (
    validate_anchor_headline_settings,
)
from pipelines.news.extractors.search_collector import validate_search_settings
from pipelines.news.jobs.collect_headline import collect_category_new_headlines
from pipelines.news.jobs.collect_search import collect_search_news
from pipelines.news.loaders.postgres import fetch_search_keywords


def collect_headline_items() -> list[dict[str, Any]]:
    """전 카테고리 헤드라인을 수집하고 _source_type을 태깅한다.

    페이지별 필터(DB 워터마크 중단·공식출처·목표 개수)는 수집기 내부의 크롤링
    제어 장치로 그대로 동작하며, 병합 배치에서 같은 필터가 한 번 더 적용된다.
    """

    validate_anchor_headline_settings()
    settings = get_news_settings()

    items: list[dict[str, Any]] = []

    for category_id in settings.anchor_categories:
        category_items, category_stats = collect_category_new_headlines(
            category_id=category_id,
            target_count=settings.max_total_collected_items,
            max_more_calls=settings.headline_more_count,
        )
        items.extend(category_items)
        logging.info(
            "헤드라인 수집 %s: 선정 %d개 (raw %d)",
            category_stats.get("category_name", category_id),
            category_stats.get("selected", len(category_items)),
            category_stats.get("raw", 0),
        )

    for item in items:
        item["_source_type"] = "headline"

    return items


def collect_search_items() -> tuple[list[dict[str, Any]], list[int]]:
    """search_keywords 기반 검색 수집. (기사 목록, 키워드 id 목록)을 반환한다."""

    validate_search_settings()
    keywords = fetch_search_keywords()

    if not keywords:
        logging.info("search_keywords 테이블에 검색 쿼리가 없습니다.")
        return [], []

    items, _stats = collect_search_news([keyword["keyword"] for keyword in keywords])

    for item in items:
        item["_source_type"] = "search"

    return items, [keyword["id"] for keyword in keywords]


def collect_all_sources() -> tuple[list[dict[str, Any]], list[int], dict[str, Any]]:
    """두 소스를 동시에 수집한다. 한쪽 실패는 건너뛰고, 둘 다 실패하면 예외."""

    with ThreadPoolExecutor(max_workers=2) as executor:
        headline_future = executor.submit(collect_headline_items)
        search_future = executor.submit(collect_search_items)

    errors: dict[str, Exception] = {}
    headline_items: list[dict[str, Any]] = []
    search_items: list[dict[str, Any]] = []
    keyword_ids: list[int] = []

    try:
        headline_items = headline_future.result()
    except Exception as e:  # noqa: BLE001 - 소스 단위 격리가 목적
        errors["headline"] = e
        logging.warning("헤드라인 수집 실패, 다른 소스로 진행: %s", e)

    try:
        search_items, keyword_ids = search_future.result()
    except Exception as e:  # noqa: BLE001 - 소스 단위 격리가 목적
        errors["search"] = e
        logging.warning("검색 수집 실패, 다른 소스로 진행: %s", e)

    if len(errors) == 2:
        raise RuntimeError(f"헤드라인·검색 수집이 모두 실패했습니다: {errors}")

    stats = {
        "headline_collected": len(headline_items),
        "search_collected": len(search_items),
        "failed_sources": sorted(errors),
    }

    return headline_items + search_items, keyword_ids, stats
