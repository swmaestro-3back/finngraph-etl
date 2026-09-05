from __future__ import annotations

import re

from pipelines.common.logging import get_logger
from pipelines.themes.models import Company, Theme
from pipelines.themes.references.rdb import fetch_stock_names_by_tickers

logger = get_logger(__name__)

CONTAINMENT_OVERLAP_THRESHOLD = 0.9
MIN_STOCKS_FOR_CONTAINMENT = 5
MIN_CONTAINMENT_NAME_LENGTH = 2

# 중복 테마의 편입 사유를 이어붙일 때 쓰는 구분자. 임베딩 입력(theme_text)과 같은 관례다.
REASON_SEPARATOR = "\n"


def validate_company(themes: list[Theme]) -> list[Theme]:
    """
    크롤링한 COMPANY 데이터를 RDB stocks 테이블과 대조하여 검증한다.
    (RDB의 데이터를 기준으로 검증)

    1. ticker의 종목이 stocks에 없으면 해당 종목 제외
    2. ticker는 있으나 크롤링한 name과 stocks의 name이 다르면 해당 종목 제외
    """
    tickers = {c.ticker for theme in themes for c in theme.companies}
    ticker_to_name = fetch_stock_names_by_tickers(list(tickers))

    for theme in themes:
        resolved = []
        for c in theme.companies:
            name = ticker_to_name.get(c.ticker)
            if name is None:
                logger.warning("[%s] '%s' stocks에 존재하지 않아 제외합니다.", theme.name, c.ticker)
                continue
            if c.name != name:
                logger.warning(
                    "[%s] '%s' 기업명 불일치 (크롤링=%s, RDB=%s) 제외합니다.",
                    theme.name,
                    c.ticker,
                    c.name,
                    name,
                )
                continue
            resolved.append(c)
        theme.companies = resolved

    return themes


def _normalize_name(name: str) -> str:
    """표기 차이로 인한 미매칭을 막기 위해 공백과 특수문자('(', '/', '-' 등)를
    모두 제거하고 문자·숫자만 남긴다.
    예: "2차 전지" == "2차전지", "IT(소프트웨어/SI)" == "IT 소프트웨어 SI"."""
    return re.sub(r"[\W_]+", "", name)


def _overlap_coefficient(a: set[str], b: set[str]) -> float:
    """교집합 / 두 집합 중 작은 쪽 크기. 이름이 포함 관계일 때(크기 차가 큰 두 집합) 쓰는 지표."""
    if not a or not b:
        return 0.0
    return len(a & b) / min(len(a), len(b))


def _is_name_contained(normalized_a: str, normalized_b: str) -> bool:
    shorter, longer = sorted([normalized_a, normalized_b], key=len)
    if len(shorter) < MIN_CONTAINMENT_NAME_LENGTH:
        return False
    return shorter in longer


def _find_duplicate_name(
    theme: Theme, candidate_stocks: set[str], name_to_stocks: dict[str, set[str]]
) -> str | None:
    """이름이 완전히 같거나, 이름이 포함 관계이면서
    종목이 THRESHOLD 이상 겹치는 기존 테마명을 찾는다."""
    normalized_theme_name = _normalize_name(theme.name)

    for existing_name in name_to_stocks:
        if _normalize_name(existing_name) == normalized_theme_name:
            return existing_name

    if len(candidate_stocks) < MIN_STOCKS_FOR_CONTAINMENT:
        return None

    for existing_name, existing_stocks in name_to_stocks.items():
        if len(existing_stocks) < MIN_STOCKS_FOR_CONTAINMENT:
            continue
        if not _is_name_contained(normalized_theme_name, _normalize_name(existing_name)):
            continue
        if _overlap_coefficient(candidate_stocks, existing_stocks) >= CONTAINMENT_OVERLAP_THRESHOLD:
            return existing_name

    return None


def _merge_reason(kept: str | None, incoming: str | None) -> str | None:
    """두 편입 사유를 이어붙인다. 한쪽이 비어 있으면 다른 쪽을 그대로 쓴다.

    같은 종목을 서로 다른 소스가 설명한 문장이라 표현이 겹쳐도 그대로 남긴다. 이 값은
    BELONGS_TO.reason_embedding 의 입력이고, 사유가 없는 간선은 임베딩 대상에서 빠지므로
    (loaders/neo4j.py 의 fetch_reason_embedding_targets) 한쪽만 있어도 채워두는 편이 낫다.
    """
    kept_text = (kept or "").strip()
    incoming_text = (incoming or "").strip()

    if not incoming_text:
        return kept_text or None

    if not kept_text:
        return incoming_text

    return f"{kept_text}{REASON_SEPARATOR}{incoming_text}"


def _merge_company_reasons(kept: Theme, dropped: Theme) -> int:
    """버려지는 테마의 편입 사유를 채택된 테마의 같은 종목에 이어붙인다.

    양쪽에 다 있는 종목만 대상이라 종목 구성은 바뀌지 않는다. 버려지는 테마에만 있던
    종목은 소스마다 편입 기준이 달라 그대로 버린다. 보강된 종목 수를 반환한다.
    """
    incoming_reasons = {c.ticker: c.reason for c in dropped.companies}
    enriched = 0

    for company in kept.companies:
        if company.ticker not in incoming_reasons:
            continue

        merged = _merge_reason(company.reason, incoming_reasons[company.ticker])

        if merged != company.reason:
            company.reason = merged
            enriched += 1

    return enriched


def validate_theme(themes: list[Theme]) -> list[Theme]:
    """
    같은 배치(소스별 JSON을 모두 합친 것) 안에서 중복 테마를 걸러낸다.

    이름이 동일하거나, 이름이 포함 관계(예: "반도체" ⊂ "반도체소재")이면서
    종목이 CONTAINMENT_OVERLAP_THRESHOLD 이상 겹치면 중복으로 보고,
    먼저 발견된 테마만 남기고 나중 테마는 버린다. 버리기 전에 버려지는 테마의 소스를
    채택된 테마의 sources 에 쌓고, 양쪽에 다 있는 종목의 편입 사유는 채택된 테마 쪽에
    이어붙여 문장을 보강한다.
    """
    accepted: dict[str, Theme] = {}
    accepted_stocks: dict[str, set[str]] = {}

    for theme in themes:
        candidate_stocks = {c.ticker for c in theme.companies if isinstance(c, Company)}

        dup_name = _find_duplicate_name(theme, candidate_stocks, accepted_stocks)

        if dup_name is None:
            accepted[theme.name] = theme
            accepted_stocks[theme.name] = candidate_stocks
            continue

        kept = accepted[dup_name]

        # 소스 목록을 쌓는다. 순회 순서가 SOURCES 순서라 우선순위 순서가 그대로 유지된다.
        for source in theme.sources:
            if source not in kept.sources:
                kept.sources.append(source)

        enriched = _merge_company_reasons(kept, theme)

        logger.info(
            "[%s] 배치 내 테마 [%s]와 중복이라 버립니다 (종목 %d개, 편입사유 %d개 보강)",
            theme.name,
            dup_name,
            len(candidate_stocks),
            enriched,
        )

    result = list(accepted.values())
    logger.info("%d개 테마 -> %d개로 중복 제거 완료", len(themes), len(result))
    return result


def validate(themes: list[Theme]) -> list[Theme]:
    """extract와 load 사이에서 사용. company 검증 후 theme 중복 제거 순으로 수행한다."""
    themes = validate_company(themes)
    themes = validate_theme(themes)
    return themes
