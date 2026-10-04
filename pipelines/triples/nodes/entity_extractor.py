from collections.abc import Iterable
from dataclasses import asdict

from pipelines.common.gazetteer import CompanyMatcher, get_company_matcher
from pipelines.triples.models import Entity


def linked_entities(
    text: str,
    company_ids: Iterable[int],
    matcher: CompanyMatcher | None = None,
) -> list[Entity]:
    """
    Build the entity list for relation extraction from the companies already linked to the article

    The news pipeline decides which companies an article is about (news_companies): the title
    relevance filter plus the body entity filter. This only recovers how the article spells them,
    because the relation prompt works on surface forms and news_companies holds ids only.
    The gazetteer matcher is deterministic, so it finds the same spellings the news pipeline
    judged; matches for companies outside company_ids are dropped. No LLM call.
    Entities are deduped by surface form, in article order, so two spellings of one company stay
    separate entities. The gazetteer is not loaded when there is no linked company.
    """

    wanted = {int(company_id) for company_id in company_ids}
    if not wanted:
        return []

    if matcher is None:
        matcher = get_company_matcher()

    seen: set[str] = set()
    entities: list[Entity] = []
    for match in matcher.extract(text):
        if match.entry.company_id not in wanted or match.text in seen:
            continue
        seen.add(match.text)
        entities.append(Entity(text=match.text, **asdict(match.entry)))

    return entities
