from __future__ import annotations

from pipelines.common.config import get_settings
from pipelines.common.logging import get_logger


logger = get_logger(__name__)


def run() -> None:
    settings = get_settings()
    logger.info(
        "stocks intraday 1m collection job is not implemented yet "
        "rate_limit_per_second=%s target_delay_minutes=%s",
        settings.kis_rate_limit_per_second,
        settings.stock_intraday_target_delay_minutes,
    )

