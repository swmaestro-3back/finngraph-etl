from __future__ import annotations

import logging

from pipelines.common.config import get_settings


def get_logger(name: str) -> logging.Logger:

    level = get_settings().log_level.upper()

    if level not in logging._nameToLevel:
        level = "INFO"

    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    return logging.getLogger(name)
