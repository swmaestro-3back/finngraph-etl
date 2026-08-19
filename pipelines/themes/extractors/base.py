from __future__ import annotations

import json
from abc import ABC, abstractmethod
from datetime import date
from pathlib import Path

import aiohttp

from pipelines.common.logging import get_logger
from pipelines.themes.models import Company, Theme

logger = get_logger(__name__)

# parents[1]은 `pipelines/themes` — 이 파일이 extractors/ 아래에 있어 한 단계 올라간다.
# .gitignore가 `pipelines/themes/data/*`를 고정하므로 위치가 어긋나면 산출물이 커밋된다.
DATA_ROOT = Path(__file__).parents[1] / "data"


def today_folder() -> Path:
    """`pipelines/themes/data/{YYYYMMDD}`를 만들고 반환한다."""

    folder = DATA_ROOT / date.today().strftime("%Y%m%d")
    folder.mkdir(parents=True, exist_ok=True)
    return folder


class BaseExtractor(ABC):
    source_name: str  # 크롤링 도메인 이름
    blacklist: list[str]  # 테마 블랙리스트

    HEADERS: dict[str, str] = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )
    }
    TIMEOUT: aiohttp.ClientTimeout = aiohttp.ClientTimeout(total=10)

    @abstractmethod
    async def fetch_themes(self) -> list[Theme]:
        """
        테마명 크롤링
        블랙리스트에 제거된 테마들만 크롤링
        """

    @abstractmethod
    async def extract_theme_stock(
        self, source_theme_id: int | None = None, theme_name: str | None = None
    ) -> list[Company]:
        """
        특정 테마에 대한 종목 크롤링
        fetch_themes 이후에 실행된다
        """

    @abstractmethod
    async def extract(self) -> list[Theme]:
        """
        fetch_themes와 extract_theme_stock을 실행하는 extract 메서드
        """

    def save(self, themes: list[Theme]) -> Path:
        """수집된 테마 데이터를 JSON 파일로 저장한다.

        저장 경로: pipelines/themes/data/{오늘날짜}/{source_name}.json

        Args:
            themes: 저장할 list[Theme]. extract()의 반환값을 그대로 전달한다.

        Returns:
            저장된 파일의 절대 경로(Path).
        """
        output = today_folder() / f"{self.source_name}.json"
        output.write_text(
            json.dumps([t.model_dump() for t in themes], ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        logger.info("[%s] %d개 테마 저장 완료: %s", self.source_name, len(themes), output)
        return output

    async def run(self) -> Path:
        """파이프라인 진입점. 수집 후 저장까지 순서를 보장한다.

        Returns:
            저장된 파일의 절대 경로(Path).
        """
        themes = await self.extract()
        return self.save(themes)
