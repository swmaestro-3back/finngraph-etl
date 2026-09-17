from __future__ import annotations

import asyncio
from typing import Any

from pipelines.common.clients.http import http_client
from pipelines.common.logging import get_logger
from pipelines.themes.extractors.base import BaseExtractor
from pipelines.themes.models import Company, Theme

logger = get_logger(__name__)


class NaverExtractor(BaseExtractor):
    """
    finance.naver.com 의 EUC-KR HTML 테이블이 stock.naver.com(Next.js) 으로 리다이렉트되면서
    JSON API 로 전환했다.

    - 테마 목록: rankings/v2/domestic/themes — 커서 기반 페이지네이션.
      응답의 `cursor` 는 마지막 아이템 code 의 base64 이고 `hasNext` 가 False 가 될 때까지
      같은 파라미터에 `cursor` 만 붙여 다시 호출하면 전체 테마를 순회한다(size 최대 100).
    - 테마 설명: domestic/market/theme/{code}/info 의 `categoryInfo`.
    - 테마 종목·편입 이유: domestic/market/theme/{code}/stocklist — `startIdx` 는 오프셋이 아니라
      0-based 페이지 번호다(pageSize 최대 200). 빈 배열이 오면 끝.
    """

    source_name = "naver"
    blacklist = [
        "2026 상반기 신규상장",
        "2026 하반기 신규상장",
        "기업인수목적회사(SPAC)",
        "리츠(REITs)",
        "S7(삼성전자/SK하이닉스 등)",
        "S7",
    ]

    BASE_URL = "https://stock.naver.com/api"
    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json",
        "Referer": "https://stock.naver.com/market/stock/kr/theme",
    }

    # 테마 목록은 순위 정렬만 지원한다(changeRate|tradingVolume|tradingValue|marketCap).
    # 커서가 마지막 code 기준이라 정렬이 페이지 사이에 흔들리면 누락/중복이 생기므로
    # 장중에도 비교적 안정적인 marketCap 을 쓰고, 방어적으로 code 중복도 제거한다.
    THEME_LIST_SORT = "marketCap"
    THEME_LIST_PAGE_SIZE = 100
    STOCK_LIST_PAGE_SIZE = 200
    # 테마당 info 1회 + stocklist 1회 이상이라 동시 요청 수를 제한한다.
    CONCURRENCY = 5

    async def _get_json(self, url: str, params: dict[str, Any] | None = None) -> Any:
        session = http_client.get_session()
        async with session.get(
            url, params=params, headers=self.HEADERS, timeout=self.TIMEOUT
        ) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def fetch_themes(self) -> list[Theme]:
        """
        테마명 크롤링
        블랙리스트에 제거된 테마들만 크롤링
        """
        url = f"{self.BASE_URL}/stockSecurity/rankings/v2/domestic/themes"
        params: dict[str, Any] = {
            "sortType": self.THEME_LIST_SORT,
            "size": self.THEME_LIST_PAGE_SIZE,
            "period": "daily",
        }

        themes: list[Theme] = []
        seen: set[int] = set()
        cursor: str | None = None
        page = 0
        while True:
            page += 1
            data = await self._get_json(url, {**params, "cursor": cursor} if cursor else params)
            items = data.get("items") or []
            logger.debug("테마 목록 %d페이지: %d개 (cursor=%s)", page, len(items), cursor)

            for item in items:
                theme_name = str(item.get("name", "")).strip()
                code = item.get("code")
                if not theme_name or code is None:
                    continue

                source_theme_id = int(code)
                if source_theme_id in seen:
                    continue
                seen.add(source_theme_id)

                # 테마 이름이 블랙리스트에 있다면 스킵
                if theme_name in self.blacklist:
                    continue

                themes.append(
                    Theme(name=theme_name, source="naver", source_theme_id=source_theme_id)
                )

            cursor = data.get("cursor")
            if not data.get("hasNext") or not cursor or not items:
                break

        logger.info("테마 목록 %d개 수집 (%d페이지)", len(themes), page)
        return themes

    async def fetch_theme_description(self, source_theme_id: int) -> str:
        """테마 설명(categoryInfo) 조회. 실패하면 빈 문자열."""
        url = f"{self.BASE_URL}/domestic/market/theme/{source_theme_id}/info"
        try:
            data = await self._get_json(url, {"marketType": "ALL"})
            return str(data.get("categoryInfo") or "").strip()
        except Exception:
            logger.exception("themeCode=%s 설명 조회 중 에러 발생", source_theme_id)
            return ""

    async def extract_theme_stock(
        self, source_theme_id: int | None = None, theme_name: str | None = None
    ) -> list[Company]:
        url = f"{self.BASE_URL}/domestic/market/theme/{source_theme_id}/stocklist"
        companies: list[Company] = []
        seen: set[str] = set()

        try:
            page = 0
            while True:
                items = await self._get_json(
                    url,
                    {
                        "marketType": "ALL",
                        "orderType": "priceTop",
                        "startIdx": page,  # 0-based 페이지 번호
                        "pageSize": self.STOCK_LIST_PAGE_SIZE,
                    },
                )
                if not items:
                    break

                for item in items:
                    ticker = str(item.get("itemcode") or "").strip()
                    name = str(item.get("itemname") or "").strip()
                    if not ticker or not name or ticker in seen:
                        continue
                    seen.add(ticker)

                    # 테마 편입 이유
                    reason = str(item.get("itemInfo") or "").strip() or None
                    companies.append(Company(name=name, ticker=ticker, reason=reason))

                if len(items) < self.STOCK_LIST_PAGE_SIZE:
                    break
                page += 1

            logger.debug("[%s] %d개 종목 완료", theme_name, len(companies))

        except Exception:
            logger.exception(
                "themeCode=%s, theme=%s 처리 중 에러 발생", source_theme_id, theme_name
            )

        return companies

    async def _fill_theme(self, theme: Theme, semaphore: asyncio.Semaphore) -> None:
        assert theme.source_theme_id is not None
        async with semaphore:
            theme.description = await self.fetch_theme_description(theme.source_theme_id)
            theme.companies = await self.extract_theme_stock(
                source_theme_id=theme.source_theme_id, theme_name=theme.name
            )

    async def extract(self) -> list[Theme]:
        themes: list[Theme] = await self.fetch_themes()

        semaphore = asyncio.Semaphore(self.CONCURRENCY)
        await asyncio.gather(*(self._fill_theme(theme, semaphore) for theme in themes))

        logger.info("총 %d개 테마 추출 완료", len(themes))
        return themes
