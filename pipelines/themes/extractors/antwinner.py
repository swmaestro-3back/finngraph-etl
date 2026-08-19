from __future__ import annotations

import asyncio

from pipelines.common.clients.http import http_client
from pipelines.common.logging import get_logger
from pipelines.themes.extractors.base import BaseExtractor
from pipelines.themes.models import Company, Theme

logger = get_logger(__name__)


class AntWinnerExtractor(BaseExtractor):
    source_name: str = "antwinner"
    blacklist = ["하락장"]

    async def fetch_themes(self) -> list[Theme]:
        url = "https://antwinner.com/api/proxy/theme-keywords"

        session = http_client.get_session()
        async with session.get(url, headers=self.HEADERS, timeout=self.TIMEOUT) as response:
            response.raise_for_status()
            names = await response.json()

        # 블랙리스트에 있는 테마를 제외하고 반환
        return [
            Theme(name=name, source="antwinner") for name in names if name not in self.blacklist
        ]

    async def extract_theme_stock(
        self, source_theme_id: int | None = None, theme_name: str | None = None
    ) -> list[Company]:
        params = {
            "period": "this-week",
            "rateFilter": "all",
            "sortBy": "rate",
            "themes": theme_name,
        }
        url = "https://antwinner.com/api/screener"
        companies: list[Company] = []

        try:
            session = http_client.get_session()
            async with session.get(
                url, params=params, headers=self.HEADERS, timeout=self.TIMEOUT
            ) as response:
                response.raise_for_status()
                data = await response.json()

            for stock in data.get("stocks", []):
                stock_name = stock["stock_name"]
                stock_code = stock["stock_code"]

                # 가스 테마로 검색했을때 가스 라는 키워드가 포함된 모든 테마에 대한 주식이 다 나옴
                # 석유가스, 천연가스 등등 따라서 걸러줘야함
                if theme_name not in stock.get("themes", []):
                    continue
                if not stock_code:
                    continue

                companies.append(Company(name=stock_name, ticker=stock_code, reason=None))

            logger.debug("[%s] %d개 종목 완료", theme_name, len(companies))

        except Exception:
            logger.exception("theme=%s 처리 중 에러 발생", theme_name)

        return companies

    async def extract(self) -> list[Theme]:
        themes: list[Theme] = await self.fetch_themes()
        for theme in themes:
            theme.companies = await self.extract_theme_stock(theme_name=theme.name)
            # 429 Client Error Rate Limiting 방지
            await asyncio.sleep(2)

        logger.info("총 %d개 테마 추출 완료", len(themes))
        return themes
