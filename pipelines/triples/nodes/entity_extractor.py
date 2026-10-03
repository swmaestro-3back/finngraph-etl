from dataclasses import asdict

from langchain_aws import ChatBedrockConverse

from pipelines.common.clients.bedrock import ensure_bedrock_token
from pipelines.common.config import get_settings
from pipelines.common.gazetteer import CompanyMatcher, get_company_matcher
from pipelines.triples.models import Entity, RawEntityJudgement, RawEntityJudgementList
from pipelines.triples.prompts.entity_verification import PROMPT


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
    def __init__(self, matcher: CompanyMatcher | None = None):

        # Only companies are gazetteer-anchored; products stay as the free text the LLM copied
        # out of the article. The gazetteer is the entity_gazetteer table (common/gazetteer.py).
        self._matcher = matcher if matcher is not None else get_company_matcher()

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

    def extract(self, text: str) -> list[Entity]:
        """
        Extract entities using gazetteer, each as the article spells it plus the company it names
        """
        return [
            Entity(text=match.text, **asdict(match.entry)) for match in self._matcher.extract(text)
        ]

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
