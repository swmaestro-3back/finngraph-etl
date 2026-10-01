from __future__ import annotations

import logging
from dataclasses import dataclass, field

from pipelines.common.utils.time import now_kst
from pipelines.news.repositories.hot_themes import HotThemesUnavailableError, fetch_hot_themes
from pipelines.news.repositories.trade_dates import fetch_trade_dates


@dataclass(frozen=True)
class ThemeSelection:
    theme_ids: list[int]
    # 백엔드 핫테마가 고른 종목. 비어 있으면 collect_articles 가 테마 편입 종목 전체를 검색한다.
    tickers: list[str] = field(default_factory=list)


def run(theme_ids: list[int] | None = None) -> ThemeSelection:
    if theme_ids:
        logging.info("[select_themes] 수동 지정 테마 %d개 사용", len(theme_ids))
        return ThemeSelection([int(theme_id) for theme_id in theme_ids])

    hot = fetch_hot_themes()
    dates = fetch_trade_dates(as_of=now_kst().date())
    if not dates.covers(hot.trade_date):
        raise HotThemesUnavailableError(
            f"핫테마 기준일이 DB 범위 밖 (페이로드 {hot.trade_date}, "
            f"마감 {dates.settled} ~ 최신 {dates.latest})"
        )

    logging.info(
        "[select_themes] 핫테마 %d개·종목 %d개 사용 (기준일 %s)",
        len(hot.theme_ids),
        len(hot.tickers),
        hot.trade_date,
    )
    return ThemeSelection(hot.theme_ids, hot.tickers)
