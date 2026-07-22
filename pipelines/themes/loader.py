from __future__ import annotations

from pipelines.common.logging import get_logger
from pipelines.themes.crud import upsert_themes
from pipelines.themes.models import Theme

logger = get_logger(__name__)


async def load(themes: list[Theme]) -> None:
    theme_batch = [
        {
            "name": theme.name,
            "source_theme_id": theme.source_theme_id,
            "description": theme.description,
            "source": theme.source,
            "companies": [{"ticker": c.ticker, "reason": c.reason} for c in theme.companies],
        }
        for theme in themes
    ]

    await upsert_themes(theme_batch)

    logger.info("%d개 Theme 및 BELONGS_TO 관계 적재 완료", len(themes))
