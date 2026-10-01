from __future__ import annotations

import logging
from dataclasses import dataclass, field

from pipelines.common.utils.time import now_kst
from pipelines.news.config import get_news_settings
from pipelines.news.repositories.hot_themes import fetch_hot_themes
from pipelines.news.repositories.theme_changes import (
    fetch_theme_changes,
    fetch_trade_dates,
)
from pipelines.news.transformers.theme_momentum import select_momentum_themes


@dataclass(frozen=True)
class ThemeSelection:
    theme_ids: list[int]
    tickers: list[str] = field(default_factory=list)


def run(theme_ids: list[int] | None = None) -> ThemeSelection:
    if theme_ids:
        return ThemeSelection([int(theme_id) for theme_id in theme_ids])

    as_of = now_kst().date()

    hot = fetch_hot_themes()
    if hot is not None:
        dates = fetch_trade_dates(as_of=as_of)
        if dates.covers(hot.trade_date):
            logging.info(
                "백엔드 핫테마 %d개·종목 %d개 사용 (기준일 %s): %s",
                len(hot.theme_ids),
                len(hot.tickers),
                hot.trade_date,
                hot.theme_ids,
            )
            return ThemeSelection(hot.theme_ids, hot.tickers)
        logging.warning(
            "핫테마 기준일이 DB 범위 밖 (페이로드 %s, 마감 %s ~ 최신 %s) — 자체 선정으로 폴백한다",
            hot.trade_date,
            dates.settled,
            dates.latest,
        )

    changes = fetch_theme_changes(as_of=as_of)
    selected = select_momentum_themes(changes, get_news_settings().theme_count)

    by_id = {c.theme_id: c for c in changes}
    logging.info(
        "급등락 테마 %d개 자체 선정 (기준일 %s, 후보 %d개): %s",
        len(selected),
        changes[0].trade_date if changes else None,
        len(changes),
        [f"{by_id[i].name} {by_id[i].change:+.1f}%" for i in selected],
    )

    return ThemeSelection(selected)
