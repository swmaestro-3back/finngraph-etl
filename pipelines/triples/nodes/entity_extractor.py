from flashtext import KeywordProcessor
from langchain_aws import ChatBedrockConverse

from pipelines.common.clients.bedrock import ensure_bedrock_token
from pipelines.common.config import get_settings
from pipelines.triples.models import Entity, RawEntityJudgement, RawEntityJudgementList
from pipelines.triples.ontology.gazetteers import COMPANY_DICT
from pipelines.triples.prompts.entity_verification import PROMPT

# Pre-built knowledge base dict. Only companies are gazetteer-anchored; products stay as the
# free text the LLM copied out of the article.
GAZETTEERS: dict[str, dict[str, list[str]]] = {
    "COMPANY": COMPANY_DICT,
}


def filter_verified(
    entities: list[Entity],
    judgements: list[RawEntityJudgement],
) -> list[Entity]:
    """
    Keep the gazetteer entities the LLM did not reject

    Pure function, kept apart from the LLM call so it can be unit tested without an API key.
    Judgements naming an entity outside the input are ignored, so the LLM cannot add entities.
    An entity the LLM skipped is kept: it already matched the gazetteer, and dropping it would
    lose every relation it takes part in.
    """

    keep_by_text: dict[str, bool] = {}
    for judgement in judgements:
        text = judgement.entity.strip()
        if text in keep_by_text:
            continue
        keep_by_text[text] = judgement.keep

    return [entity for entity in entities if keep_by_text.get(entity.text.strip(), True)]


class EntityExtractor:
    def __init__(self):

        self._canonicalizer = KeywordProcessor(case_sensitive=True)
        self._processors: dict[str, KeywordProcessor] = {}

        for label, gazetteer in GAZETTEERS.items():
            processor = KeywordProcessor(case_sensitive=True)
            processor.add_keywords_from_dict(gazetteer)
            self._canonicalizer.add_keywords_from_dict(gazetteer)
            self._processors[label] = processor

        ensure_bedrock_token()
        settings = get_settings()
        self._model = ChatBedrockConverse(
            model=settings.bedrock_chat_model,
            region_name=settings.bedrock_region,
            temperature=0,
        )

        self._chain = PROMPT | self._model.with_structured_output(
            schema=RawEntityJudgementList,
            method="json_schema",
        )

    def canonicalize(self, text: str) -> str:
        """
        Replace gazetteer surface forms with their canonical names

        Not used by the triples workflow, which keeps the article as written. The events
        pipeline calls it (events/transformers/candidates.py).
        """
        return self._canonicalizer.replace_keywords(text)

    def extract(self, text: str) -> list[Entity]:
        """
        Extract entities using gazetteer, each as the article spells it plus its canonical name
        """
        entities: list[Entity] = []
        for processor in self._processors.values():
            for canonical, start, end in processor.extract_keywords(text, span_info=True):
                entities.append(Entity(text=text[start:end], canonical=canonical))
        return entities

    async def verify(self, text: str, entities: list[Entity]) -> list[Entity]:
        """
        Drop gazetteer false hits and background-only mentions using the LLM
        """
        invoke_input = {
            "text": text,
            "entities": "\n".join(f"- {entity.text}" for entity in entities),
        }

        result = await self._chain.ainvoke(invoke_input)
        return filter_verified(entities, result.judgements)
