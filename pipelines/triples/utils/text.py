import re


# Strip every whitespace character before comparing, so a sentence still matches when the
# LLM breaks its verbatim instruction with a stray space or newline. Shared, because
# RelationExtractor's source check and FrameAnnotator's evidence grounding must agree.
def normalize_whitespace(s: str) -> str:
    return re.sub(r"\s+", "", s)
