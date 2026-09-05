"""
새 EVENT 생성 시 사용되는 제목/당사자 추출용 LLM 스테이지
"""

from __future__ import annotations

from datetime import date

from pipelines.common.clients.bedrock import ensure_bedrock_token
from pipelines.common.config import get_settings
from pipelines.common.logging import get_logger
from pipelines.events.models import EventDraft
from pipelines.news.utils.text_utils import remove_leading_title_brackets

logger = get_logger(__name__)

DEFAULT_MAX_TOKENS = 256
RETRY_ATTEMPTS = 2


def build_prompt_input(dated_texts: list[tuple[date | None, str]], candidates: list[str]) -> str:
    """LLM에게 입력할 Context 생성"""

    blocks: list[str] = []
    for index, (day, text) in enumerate(dated_texts, start=1):
        head = day.isoformat() if day else "날짜 미상"
        title, _, body = text.partition("\n")
        blocks.append(f"[기사 {index}] {head} | {title}\n{body}".rstrip())

    candidate_lines = "\n".join(f"- {name}" for name in candidates) if candidates else "- (없음)"
    return "\n\n".join(blocks) + "\n\n[후보 기업]\n" + candidate_lines


def validate_draft(draft: EventDraft, candidates: list[str], max_chars: int) -> EventDraft:
    """
    LLM이 출력한 EventDarft 검증
    """

    allowed = set(candidates)
    companies: list[str] = []
    for name in draft.companies:
        # 후보에 존재하지 않았던 기업명은 드랍
        if name not in allowed:
            logger.warning("후보 밖 기업명 드랍: %s", name)
            continue
        if name in companies:
            continue
        companies.append(name)

    title = remove_leading_title_brackets(" ".join(draft.title.split()))
    if not title:
        raise ValueError("빈 제목")
    if len(title) > max_chars:
        raise ValueError(f"제목 길이 초과: {len(title)} > {max_chars}")

    return EventDraft(companies=companies, title=title)


class EventGenerator:
    def __init__(self, max_tokens: int = DEFAULT_MAX_TOKENS):
        from langchain_aws import ChatBedrockConverse

        from pipelines.events.prompts.event import PROMPT

        ensure_bedrock_token()
        settings = get_settings()
        model = ChatBedrockConverse(
            model=settings.bedrock_chat_model,
            region_name=settings.bedrock_region,
            temperature=0,
            max_tokens=max_tokens,
            timeout=settings.bedrock_request_timeout,
        )

        # 출력스키마 강제
        structured = model.with_structured_output(schema=EventDraft, method="json_schema")

        self._chain = (PROMPT | structured).with_retry(stop_after_attempt=RETRY_ATTEMPTS)

    async def draft(
        self, dated_texts: list[tuple[date | None, str]], candidates: list[str]
    ) -> EventDraft:
        result = await self._chain.ainvoke(
            {"articles": build_prompt_input(dated_texts, candidates)}
        )
        return result
