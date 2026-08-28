"""소스 이름 → Extractor 구현 생성.

실제로 돌리는 소스 목록은 `dags/themes/init.py`의 SOURCES가 단일 출처다.
소스를 추가할 때는 거기와 이 팩토리 두 곳을 함께 고친다.
"""

from __future__ import annotations

from pipelines.themes.extractors.antwinner import AntWinnerExtractor
from pipelines.themes.extractors.base import BaseExtractor
from pipelines.themes.extractors.judal import JudalExtractor
from pipelines.themes.extractors.naver import NaverExtractor


class ExtractorFactory:
    @staticmethod
    def get_extractor(source_name: str) -> BaseExtractor:
        if source_name == "naver":
            return NaverExtractor()
        elif source_name == "judal":
            return JudalExtractor()
        elif source_name == "antwinner":
            return AntWinnerExtractor()
        else:
            raise ValueError("Not Available Source Name")
