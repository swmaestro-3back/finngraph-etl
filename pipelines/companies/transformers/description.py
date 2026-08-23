"""기업 설명 생성.

사업보고서 원문에서 사업 설명 구간을 잘라내고, LLM으로 1~2문장 요약을 만든다.

**LLM이 사실을 지어내지 않게 하는 것이 설계의 핵심이다.** 요약 대상이 회사가 직접 쓴
공시 원문이라, 모델은 압축만 하면 된다. 그래서 프롬프트가 "원문에 없는 내용을 넣지 말라"를
반복해서 요구하고, 실패하면 요약을 버린다(빈 설명이 틀린 설명보다 낫다).

폴백은 KRX 업종·주요제품이다. 사업보고서를 제출하지 않는 법인, 원문 구간을 못 찾은 법인은
여기로 떨어지고 description_source로 구분된다.
"""

from __future__ import annotations

import re

from pipelines.common.clients.bedrock import extract_bedrock_text, get_bedrock_client
from pipelines.common.config import get_settings
from pipelines.common.logging import get_logger

logger = get_logger(__name__)

DESCRIPTION_SOURCE_DART_LLM = "DART_LLM"
DESCRIPTION_SOURCE_INDUSTRY = "INDUSTRY_FALLBACK"

# 사업 설명이 시작되는 표제. 앞에 있는 것부터 시도한다.
#
# 세 건(비바리퍼블리카·삼성전자·무신사)의 사업보고서에서 모두 '주요 사업의 내용'이
# 문서 앞부분(1,500자 이내)에 있고 바로 뒤가 회사가 쓴 사업 설명 원문이었다.
SECTION_KEYWORDS = (
    "주요 사업의 내용",
    "사업의 개요",
    "회사의 개요",
)

# 요약에 넘길 원문 길이. 사업보고서 전문은 수십만 자라 그대로 넣을 수 없다.
SECTION_LENGTH = 2000

# 태그·공백 정리용.
_TAG_PATTERN = re.compile(r"<[^>]+>")
_WHITESPACE_PATTERN = re.compile(r"\s+")

SYSTEM_PROMPT = (
    "너는 기업 정보를 정리하는 애널리스트다. "
    "주어진 사업보고서 발췌만 근거로 회사가 무엇을 하는 회사인지 설명한다."
)

USER_PROMPT_TEMPLATE = """다음은 {name}의 사업보고서 발췌다.

이 회사가 무엇을 하는 회사인지 한국어 1~2문장으로 요약하라.

규칙:
- 발췌에 없는 내용은 절대 쓰지 마라. 추측·과장·수식어를 넣지 마라.
- 매출 수치나 연도별 실적은 넣지 마라. 사업의 내용만 쓴다.
- "~입니다" 체로 쓰고, 회사 이름으로 시작하라.
- 요약 문장만 출력하고 다른 말을 덧붙이지 마라.

발췌:
{excerpt}"""

MAX_TOKENS = 300


def extract_business_section(document_text: str) -> str:
    """사업보고서 원문에서 사업 설명 구간을 잘라낸다.

    Args:
        document_text (str): document.xml 본문(태그 포함).

    Returns:
        str: 표제 뒤 SECTION_LENGTH자. 구간을 찾지 못하면 빈 문자열.
    """

    plain = _WHITESPACE_PATTERN.sub(" ", _TAG_PATTERN.sub(" ", document_text)).strip()
    if not plain:
        return ""

    for keyword in SECTION_KEYWORDS:
        index = plain.find(keyword)
        if index >= 0:
            start = index + len(keyword)
            return plain[start : start + SECTION_LENGTH].strip()

    return ""


def summarize_business_section(name: str, excerpt: str) -> str:
    """발췌를 1~2문장 설명으로 요약한다.

    Args:
        name (str): 회사 이름. 요약문의 주어로 쓰인다.
        excerpt (str): 사업보고서 발췌.

    Returns:
        str: 요약문. 실패하거나 응답이 비면 빈 문자열.
    """

    if not excerpt.strip():
        return ""

    settings = get_settings()
    if not settings.bedrock_chat_model:
        logger.warning("BEDROCK_CHAT_MODEL이 비어 있어 설명 생성을 건너뛴다")
        return ""

    client = get_bedrock_client(settings.bedrock_region, settings.bedrock_request_timeout)
    response = client.converse(
        modelId=settings.bedrock_chat_model,
        system=[{"text": SYSTEM_PROMPT}],
        messages=[
            {
                "role": "user",
                "content": [{"text": USER_PROMPT_TEMPLATE.format(name=name, excerpt=excerpt)}],
            }
        ],
        inferenceConfig={"maxTokens": MAX_TOKENS, "temperature": 0.0},
    )

    return _clean(extract_bedrock_text(response))


def build_industry_description(
    name: str,
    industry: str | None,
    products: str | None,
) -> str:
    """업종·주요제품으로 만드는 폴백 설명.

    LLM을 부르지 않는다. 사실만 조합하므로 틀릴 여지가 없고, 사업보고서가 없는 법인도
    최소한의 설명을 갖게 된다(요구사항 5 — 모든 기업에 설명이 있어야 함).
    """

    industry_text = (industry or "").strip()
    products_text = (products or "").strip()

    if industry_text and products_text:
        return (
            f"{name}는 {industry_text} 업종의 기업으로, 주요 제품·서비스는 {products_text}입니다."
        )
    if industry_text:
        return f"{name}는 {industry_text} 업종의 기업입니다."
    if products_text:
        return f"{name}의 주요 제품·서비스는 {products_text}입니다."
    return ""


def _clean(text: str) -> str:
    """모델이 붙이는 군더더기를 떼어낸다."""

    cleaned = _WHITESPACE_PATTERN.sub(" ", text or "").strip()
    # 따옴표로 감싸 오는 경우가 있다.
    if len(cleaned) >= 2 and cleaned[0] in "\"'“" and cleaned[-1] in "\"'”":
        cleaned = cleaned[1:-1].strip()
    return cleaned
