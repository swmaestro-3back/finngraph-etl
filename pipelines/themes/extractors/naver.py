from __future__ import annotations

import re

from bs4 import BeautifulSoup

from pipelines.common.http import http_client
from pipelines.common.logging import get_logger
from pipelines.themes.extractors.base import BaseExtractor
from pipelines.themes.models import Company, Theme

logger = get_logger(__name__)


class NaverExtractor(BaseExtractor):
    """
    네이버 증권은 UTF-8이 아닌 2000년대 초반 EUC-KR로 인코딩하여 보내줌
    따라서 받은 Response를 현대적인 UTF-8로 인코딩을 하면 안되고 EUC-KR로 디코딩을 해야함
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

    BASE_URL = "https://finance.naver.com"
    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/56.0.2924.76 Safari/537.36"
        ),
        "Upgrade-Insecure-Requests": "1",
        "DNT": "1",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate",
    }

    async def fetch_themes(self) -> list[Theme]:
        """
        테마명 크롤링
        블랙리스트에 제거된 테마들만 크롤링
        """
        themes: list[Theme] = []
        session = http_client.get_session()
        for pagenum in range(1, 8):
            url = f"{self.BASE_URL}/sise/theme.naver?&page={pagenum}"

            # EUC-KR로 인코딩해서 보내주므로 EUC-KR로 디코딩해야함
            async with session.get(url, headers=self.HEADERS, timeout=self.TIMEOUT) as resp:
                text = await resp.text(encoding="euc-kr")

            soup = BeautifulSoup(text, "lxml")
            for a in soup.select("#contentarea_left > table.type_1.theme > tr > td.col_type1 > a"):
                theme_name = a.text.strip()

                # 테마 이름이 블랙리스트에 있다면 스킵
                if theme_name in self.blacklist:
                    continue

                href = str(a["href"])
                source_theme_id = None
                match = re.search(r"no=(\d+)", href)
                if match:
                    source_theme_id = int(match.group(1))

                description = ""
                try:
                    async with session.get(
                        f"{self.BASE_URL}{href}", headers=self.HEADERS
                    ) as detail_resp:
                        detail_text = await detail_resp.text(encoding="euc-kr")
                    detail_soup = BeautifulSoup(detail_text, "lxml")
                    info_tag = detail_soup.select_one("div.info_layer_wrap > p.info_txt")
                    if info_tag:
                        description = info_tag.text.strip()
                except Exception:
                    pass

                themes.append(
                    Theme(
                        name=theme_name,
                        source="naver",
                        source_theme_id=source_theme_id,
                        description=description,
                    )
                )
                logger.debug("[%s] 테마 추출 완료", theme_name)
        return themes

    async def extract_theme_stock(
        self, source_theme_id: int | None = None, theme_name: str | None = None
    ) -> list[Company]:
        url = f"{self.BASE_URL}/sise/sise_group_detail.naver?type=theme&no={source_theme_id}"
        companies: list[Company] = []

        try:
            session = http_client.get_session()
            async with session.get(url, headers=self.HEADERS, timeout=self.TIMEOUT) as resp:
                text = await resp.text(encoding="euc-kr")

            soup = BeautifulSoup(text, "lxml")
            # 전체 테이블의 tr 파싱
            for tr in soup.select("#contentarea > div:nth-child(5) > table > tbody > tr"):
                # 주식명과 종목코드 파싱
                a = tr.select_one("td.name > div > a")
                if not a:
                    continue
                href = str(a.get("href", ""))
                if "code=" not in href:
                    continue

                name = a.text.strip()
                srtn = href.split("code=")[-1].strip()
                if not srtn:
                    continue

                # 테마 편입 이유 파싱
                reason_tag = tr.select_one("div.info_layer_wrap > p.info_txt")
                reason = reason_tag.text.strip() if reason_tag else None

                companies.append(Company(name=name, ticker=srtn, reason=reason))

            logger.debug("[%s] %d개 종목 완료", theme_name, len(companies))

        except Exception:
            logger.exception(
                "themeCode=%s, theme=%s 처리 중 에러 발생", source_theme_id, theme_name
            )

        return companies

    async def extract(self) -> list[Theme]:
        themes: list[Theme] = await self.fetch_themes()

        for theme in themes:
            theme.companies = await self.extract_theme_stock(
                source_theme_id=theme.source_theme_id, theme_name=theme.name
            )

        logger.info("총 %d개 테마 추출 완료", len(themes))
        return themes
