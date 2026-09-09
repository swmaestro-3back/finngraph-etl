"""같은 테마로 판정된 것들을 하나로 합친다.

DB 에 의존하지 않는다. 기존 테마 목록은 호출자(jobs/merge_themes.py)가 넘긴다.
"""

from __future__ import annotations

from pipelines.common.logging import get_logger
from pipelines.themes.models import Theme
from pipelines.themes.transformers.duplicate_matcher import find_duplicate_name

logger = get_logger(__name__)

# 중복 테마의 편입 사유를 이어붙일 때 쓰는 구분자. 임베딩 입력(theme_text)과 같은 관례다.
REASON_SEPARATOR = "\n"


def _merge_reason(kept: str | None, incoming: str | None) -> str | None:

    kept_text = (kept or "").strip()
    incoming_text = (incoming or "").strip()

    if not incoming_text:
        return kept_text or None

    if not kept_text:
        return incoming_text

    return f"{kept_text}{REASON_SEPARATOR}{incoming_text}"


def _merge_companies(kept: Theme, dropped: Theme) -> tuple[int, int]:

    kept_by_ticker = {c.ticker: c for c in kept.companies}
    enriched = 0
    added = 0

    for incoming in dropped.companies:
        company = kept_by_ticker.get(incoming.ticker)

        if company is None:
            kept.companies.append(incoming)
            kept_by_ticker[incoming.ticker] = incoming
            added += 1
            continue

        merged = _merge_reason(company.reason, incoming.reason)

        if merged != company.reason:
            company.reason = merged
            enriched += 1

    return enriched, added


def _merge_sources(kept: Theme, dropped: Theme) -> None:

    for source in dropped.sources:
        if source not in kept.sources:
            kept.sources.append(source)


def merge_batch(themes: list[Theme]) -> list[Theme]:

    accepted: dict[str, Theme] = {}
    accepted_stocks: dict[str, set[str]] = {}

    for theme in themes:
        candidate_stocks = {c.ticker for c in theme.companies}

        dup_name = find_duplicate_name(theme, candidate_stocks, accepted_stocks)

        if dup_name is None:
            accepted[theme.name] = theme
            accepted_stocks[theme.name] = candidate_stocks
            continue

        kept = accepted[dup_name]
        _merge_sources(kept, theme)
        enriched, added = _merge_companies(kept, theme)
        accepted_stocks[dup_name] |= candidate_stocks

        logger.info(
            "[%s] 배치 내 테마 [%s]와 중복이라 합칩니다 (종목 %d개 추가, 편입사유 %d개 보강)",
            theme.name,
            dup_name,
            added,
            enriched,
        )

    result = list(accepted.values())
    logger.info("%d개 테마 -> %d개로 중복 제거 완료", len(themes), len(result))
    return result


def merge_existing(themes: list[Theme], existing: dict[str, set[str]]) -> list[Theme]:

    aligned: dict[str, Theme] = {}
    renamed = 0

    for theme in themes:
        candidate_stocks = {c.ticker for c in theme.companies}
        dup_name = find_duplicate_name(theme, candidate_stocks, existing)

        if dup_name is not None and dup_name != theme.name:
            logger.info("[%s] 기존 테마 [%s]로 이름을 맞춥니다", theme.name, dup_name)
            theme.name = dup_name
            renamed += 1

        already = aligned.get(theme.name)
        if already is None:
            aligned[theme.name] = theme
            continue

        _merge_sources(already, theme)
        enriched, added = _merge_companies(already, theme)
        logger.info(
            "[%s] 같은 기존 테마로 맞춰진 테마와 합칩니다 (종목 %d개 추가, 편입사유 %d개 보강)",
            theme.name,
            added,
            enriched,
        )

    result = list(aligned.values())
    logger.info(
        "%d개 테마 중 기존 테마로 맞춘 것 %d개, 최종 %d개 (기존 DB 테마 %d개 대조)",
        len(themes),
        renamed,
        len(result),
        len(existing),
    )
    return result
