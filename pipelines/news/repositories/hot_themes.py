from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import date

import redis

from pipelines.news.config import get_news_settings

HOT_THEMES_KEY = "etl:hot-themes"

_SOCKET_TIMEOUT_SECONDS = 2.0


class HotThemesUnavailableError(RuntimeError):
    """백엔드 핫테마를 쓸 수 없다 — 키 없음, 계약 위반, 기준일 불일치."""


@dataclass(frozen=True)
class HotThemes:
    trade_date: date | None
    theme_ids: list[int]
    tickers: list[str] = field(default_factory=list)


def fetch_hot_themes() -> HotThemes:
    """백엔드가 발행한 핫테마. Redis 장애(redis.RedisError)는 그대로 올린다."""

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

    if raw is None:
        raise HotThemesUnavailableError(f"핫테마 키 없음: {HOT_THEMES_KEY}")

    parsed = parse_hot_themes(raw)
    if parsed is None:
        raise HotThemesUnavailableError(f"핫테마 페이로드 파싱 실패: {HOT_THEMES_KEY}")
    return parsed


def parse_hot_themes(raw: str) -> HotThemes | None:
    try:
        payload = json.loads(raw)
        trade_date = date.fromisoformat(payload["tradeDate"]) if payload.get("tradeDate") else None
        theme_ids = [int(theme["id"]) for theme in payload["themes"]]
        tickers = [
            str(stock["ticker"]) for theme in payload["themes"] for stock in theme.get("stocks", [])
        ]
    except (ValueError, KeyError, TypeError, AttributeError):
        return None

    if not theme_ids:
        return None

    return HotThemes(
        trade_date=trade_date, theme_ids=theme_ids, tickers=list(dict.fromkeys(tickers))
    )
