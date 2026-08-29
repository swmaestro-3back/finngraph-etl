from __future__ import annotations

import re

from pipelines.common.logging import get_logger
from pipelines.themes.models import Company, Theme
from pipelines.themes.references.rdb import fetch_stock_names_by_tickers

logger = get_logger(__name__)

CONTAINMENT_OVERLAP_THRESHOLD = 0.9
MIN_STOCKS_FOR_CONTAINMENT = 5
MIN_CONTAINMENT_NAME_LENGTH = 2


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


def validate_theme(themes: list[Theme]) -> list[Theme]:
    """
    같은 배치(소스별 JSON을 모두 합친 것) 안에서 중복 테마를 걸러낸다.

    이름이 동일하거나, 이름이 포함 관계(예: "반도체" ⊂ "반도체소재")이면서
    종목이 CONTAINMENT_OVERLAP_THRESHOLD 이상 겹치면 중복으로 보고,
    먼저 발견된 테마만 남기고 나중 테마는 버린다.
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

        logger.info(
            "[%s] 배치 내 테마 [%s]와 중복이라 버립니다 (종목 %d개)",
            theme.name,
            dup_name,
            len(candidate_stocks),
        )

    result = list(accepted.values())
    logger.info("%d개 테마 -> %d개로 중복 제거 완료", len(themes), len(result))
    return result


def validate(themes: list[Theme]) -> list[Theme]:
    """extract와 load 사이에서 사용. company 검증 후 theme 중복 제거 순으로 수행한다."""
    themes = validate_company(themes)
    themes = validate_theme(themes)
    return themes
