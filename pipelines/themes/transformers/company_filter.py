"""크롤링한 편입 종목을 RDB stocks 테이블과 대조해 걸러낸다.

DB 에 의존하지 않는다. 티커→종목명 대조표는 호출자(jobs/merge_themes.py)가 넘긴다 — merger 와
같은 규칙이라 로직을 DB 없이 테스트할 수 있다.
"""

from __future__ import annotations

from pipelines.common.logging import get_logger
from pipelines.themes.models import Theme

logger = get_logger(__name__)


def filter_companies(themes: list[Theme], ticker_to_name: dict[str, str]) -> list[Theme]:
    """stocks 에 없거나 종목명이 다른 편입 종목을 제외한다.

    ticker_to_name: 활성 종목의 티커→종목명. 여기 없는 티커는 상장폐지·오타·비활성으로 본다.
    """

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
