from flashtext import KeywordProcessor

from pipelines.triples.models import Entity, EntityLabel
from pipelines.triples.ontology.gazetteers import (
    COMMODITY_DICT,
    COMPANY_DICT,
    COUNTRY_DICT,
    PRODUCT_DICT,
)

GAZETTEERS: dict[EntityLabel, dict[str, list[str]]] = {
    "COMPANY": COMPANY_DICT,
    "COUNTRY": COUNTRY_DICT,
    "PRODUCT": PRODUCT_DICT,
    "COMMODITY": COMMODITY_DICT,
}


class EntityExtractor:
    def __init__(self):
        # 정규식 치환용
        self._normalizer = KeywordProcessor(case_sensitive=True)
        # NER용
        self._processors: dict[EntityLabel, KeywordProcessor] = {}

        # normalizer와 processors에 gazetteer 등록
        for label, gazetteer in GAZETTEERS.items():
            processor = KeywordProcessor(case_sensitive=True)
            processor.add_keywords_from_dict(gazetteer)
            self._normalizer.add_keywords_from_dict(gazetteer)
            self._processors[label] = processor

    def normalize(self, text: str) -> str:
        """
        본문에서 gazetteer에 존재하는 키워드들을 표준 명칭으로 대체한 텍스트를 반환
        """
        return self._normalizer.replace_keywords(text)

    def extract_entities(self, text: str) -> list[Entity]:
        """
        본문에서 gazetteer에 등록된 키워드들을 추출하여 반환
        """
        entities: list[Entity] = []
        for label, processor in self._processors.items():
            for canonical in processor.extract_keywords(text):
                entities.append(Entity(text=canonical, label=label))
        return entities
