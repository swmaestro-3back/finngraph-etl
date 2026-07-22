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
