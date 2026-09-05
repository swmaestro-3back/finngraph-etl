"""클러스터 제목·당사자 생성.

render_prompt_input / validate_draft 는 순수 함수라 프롬프트 없이 import 된다.
EventTitler 는 생성자 안에서 프롬프트와 langchain-aws 를 지연 import 한다 — prompts/ 가
gitignore 라 CI 에는 없기 때문이다(triples 노드와 같은 사정).
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


def render_prompt_input(dated_texts: list[tuple[date | None, str]], candidates: list[str]) -> str:
    """`[기사 N] 날짜 | 제목\\n본문` 블록들과 후보 목록을 한 문자열로 만든다."""

    blocks: list[str] = []
    for index, (day, text) in enumerate(dated_texts, start=1):
        head = day.isoformat() if day else "날짜 미상"
        title, _, body = text.partition("\n")
        blocks.append(f"[기사 {index}] {head} | {title}\n{body}".rstrip())

    candidate_lines = "\n".join(f"- {name}" for name in candidates) if candidates else "- (없음)"
    return "\n\n".join(blocks) + "\n\n[후보 기업]\n" + candidate_lines


def validate_draft(draft: EventDraft, candidates: list[str], max_chars: int) -> EventDraft:
    """LLM 출력을 후보 집합과 제목 규칙으로 검증한 새 EventDraft 를 돌려준다.

    후보 밖 이름은 버리고 경고만 남긴다(실패 아님). 제목은 공백 정리와 선두 브라켓 태그
    제거 뒤 비어 있거나 max_chars 를 넘으면 ValueError 다.
    """

    allowed = set(candidates)
    companies: list[str] = []
    for name in draft.companies:
        if name not in allowed:
            logger.warning("후보 밖 기업명을 버림: %s", name)
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


class EventTitler:
    def __init__(self, max_tokens: int = DEFAULT_MAX_TOKENS):
        from langchain_aws import ChatBedrockConverse

        from pipelines.events.prompts.event_title import PROMPT

        # langchain-aws 는 AWS_BEARER_TOKEN_BEDROCK 을 os.environ 에서 읽고 Settings 는
        # .env 만 읽는다. 로컬 run_job 처럼 .env 가 export 되지 않은 환경에서 필요하다.
        ensure_bedrock_token()
        settings = get_settings()
        model = ChatBedrockConverse(
            model=settings.bedrock_chat_model,
            region_name=settings.bedrock_region,
            temperature=0,
            max_tokens=max_tokens,
            # 넘기지 않으면 Settings 의 타임아웃은 boto3 경로에만 적용되고 여기서는 botocore
            # 기본값이 쓰인다.
            timeout=settings.bedrock_request_timeout,
        )
        structured = model.with_structured_output(schema=EventDraft, method="json_schema")
        # 러너블 수준 재시도. botocore 재시도와는 별개다.
        self._chain = (PROMPT | structured).with_retry(stop_after_attempt=RETRY_ATTEMPTS)

    async def draft(
        self, dated_texts: list[tuple[date | None, str]], candidates: list[str]
    ) -> EventDraft:
        result = await self._chain.ainvoke(
            {"articles": render_prompt_input(dated_texts, candidates)}
        )
        return result
