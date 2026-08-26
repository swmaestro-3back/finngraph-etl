from flashtext import KeywordProcessor

from pipelines.triples.models import Entity
from pipelines.triples.ontology.gazetteers import COMPANY_DICT

# Pre-built knowledge base dict. Only companies are gazetteer-anchored; products stay as the
# free text the LLM copied out of the article.
GAZETTEERS: dict[str, dict[str, list[str]]] = {
    "COMPANY": COMPANY_DICT,
}


class EntityExtractor:
    def __init__(self):

        self._canonicalizer = KeywordProcessor(case_sensitive=True)
        self._processors: dict[str, KeywordProcessor] = {}

        for label, gazetteer in GAZETTEERS.items():
            processor = KeywordProcessor(case_sensitive=True)
            processor.add_keywords_from_dict(gazetteer)
            self._canonicalizer.add_keywords_from_dict(gazetteer)
            self._processors[label] = processor

    def canonicalize(self, text: str) -> str:
        """
        Replace gazetteer surface forms with their canonical names
        """
        return self._canonicalizer.replace_keywords(text)

    def extract(self, text: str) -> list[Entity]:
        """
        Extract entities using gazetteer
        """
        entities: list[Entity] = []
        for processor in self._processors.values():
            for canonical in processor.extract_keywords(text):
                entities.append(Entity(text=canonical))
        return entities
