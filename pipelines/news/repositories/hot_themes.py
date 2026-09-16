from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import date

import redis

from pipelines.news.config import get_news_settings

HOT_THEMES_KEY = "etl:hot-themes"

_SOCKET_TIMEOUT_SECONDS = 2.0


@dataclass(frozen=True)
class HotThemes:
    trade_date: date | None
    theme_ids: list[int]


def fetch_hot_themes() -> HotThemes | None:
    try:
        client = redis.Redis.from_url(
            get_news_settings().hot_themes_redis_url,
            socket_timeout=_SOCKET_TIMEOUT_SECONDS,
            socket_connect_timeout=_SOCKET_TIMEOUT_SECONDS,
            decode_responses=True,
        )
        try:
            raw = client.get(HOT_THEMES_KEY)
        finally:
            client.close()
    except redis.RedisError as exc:
        logging.warning("핫테마 Redis 조회 실패 — 자체 선정으로 폴백한다: %s", exc)
        return None

    if raw is None:
        logging.info("핫테마 키 없음(%s) — 자체 선정으로 폴백한다", HOT_THEMES_KEY)
        return None

    parsed = parse_hot_themes(raw)
    if parsed is None:
        logging.warning("핫테마 페이로드 파싱 실패 — 자체 선정으로 폴백한다")
    return parsed


def parse_hot_themes(raw: str) -> HotThemes | None:
    try:
        payload = json.loads(raw)
        trade_date = date.fromisoformat(payload["tradeDate"]) if payload.get("tradeDate") else None
        theme_ids = [int(theme["id"]) for theme in payload["themes"]]
    except (ValueError, KeyError, TypeError):
        return None

    if not theme_ids:
        return None

    return HotThemes(trade_date=trade_date, theme_ids=theme_ids)
