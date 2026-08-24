"""기업 설명 생성.

정기공시 원문에서 사업 설명 구간을 잘라내고, LLM으로 3~5문장 요약을 만든다. 프롬프트가
"원문에 없는 내용을 넣지 말라"를 반복해 요구하고, 실패하면 요약을 버린다.
"""

from __future__ import annotations

import copy
import re

from lxml import etree

from pipelines.common.clients.bedrock import extract_bedrock_text, get_bedrock_client
from pipelines.common.config import get_settings
from pipelines.common.logging import get_logger

logger = get_logger(__name__)

DESCRIPTION_SOURCE_DART_LLM = "DART_LLM"

# document.xml 은 절마다 <SECTION-2> 가 있고 그 첫 자식이 <TITLE> 이라 이름으로 집는다.
# 앞에 적힌 표제를 우선한다 — '회사의 개요'는 명칭·주소라 설명 재료로는 그다음이다.
SECTION_TITLES = (
    "사업의 개요",
    "회사의 개요",
)

# 구조 파싱이 실패했을 때 쓰는 평문 검색 표제. 옛 서식은 SECTION-2 구조가 없다.
# 평문 검색은 문서 맨 앞 '목 차'에 걸릴 수 있어 구조를 먼저 본다.
SECTION_KEYWORDS = (
    "주요 사업의 내용",
    "사업의 개요",
    "회사의 개요",
)

# 요약에 넘길 원문 길이. 표를 걷어낸 산문 기준 표본 20곳의 75%가 4,000자에 들어간다.
# 더 키우지 않는 이유는 「사업의 개요」가 뒤로 갈수록 산업 일반론으로 흐르기 때문이다.
SECTION_LENGTH = 4000

# 태그·공백 정리용.
_TAG_PATTERN = re.compile(r"<[^>]+>")
_WHITESPACE_PATTERN = re.compile(r"\s+")

# document.xml 은 well-formed 하지 않아 ElementTree 로는 못 읽는다(표본 30건 전부
# ParseError). recover=True 로 깨진 구간을 넘긴다. 본문이 수 MB 라 huge_tree 가 필요하다.
_XML_PARSER = etree.XMLParser(recover=True, huge_tree=True)

SYSTEM_PROMPT = (
    "너는 기업 정보를 정리하는 애널리스트다. "
    "주어진 사업보고서 발췌만 근거로 회사가 무엇을 하는 회사인지 설명한다."
)

USER_PROMPT_TEMPLATE = """다음은 {name}의 사업보고서 발췌다.

이 회사가 무엇을 하는 회사인지 한국어 3~5문장으로 요약하라.

규칙:
- 발췌에 없는 내용은 절대 쓰지 마라. 추측·과장·수식어를 넣지 마라.
- **이 회사에 대해서만 써라.** 발췌 뒤쪽에는 산업 일반론·시장 전망·기술 교과서 설명이
  섞여 있는데, 그것은 이 회사의 사업이 아니므로 넣지 마라.
- 사업부문이 여럿이면 각각 무엇을 만들어 어디에 파는지 밝혀라.
- 매출 수치나 연도별 실적은 넣지 마라. 사업의 내용만 쓴다.
- "~입니다" 체로 쓰고, 회사 이름으로 시작하라.
- 요약 문장만 출력하고 다른 말을 덧붙이지 마라.

발췌:
{excerpt}"""

MAX_TOKENS = 800


def _plain(fragment: str) -> str:
    """태그를 지우고 공백을 접는다."""

    return _WHITESPACE_PATTERN.sub(" ", _TAG_PATTERN.sub(" ", fragment)).strip()


def _drop(element) -> None:
    """엘리먼트를 지우되 꼬리 텍스트는 앞으로 넘긴다.

    lxml에서 remove()는 그 엘리먼트의 tail(닫는 태그 뒤 텍스트)까지 함께 지운다. 표를
    걷어낼 때 표 뒤에 이어지는 문장이 같이 사라지므로 앞 형제나 부모에 붙여 준다.
    """

    tail = element.tail or ""
    parent = element.getparent()
    if parent is None:
        return
    previous = element.getprevious()
    if previous is not None:
        previous.tail = (previous.tail or "") + tail
    else:
        parent.text = (parent.text or "") + tail
    parent.remove(element)


def _section_text(section) -> str:
    """절에서 제목과 표를 걷어낸 산문.

    **표를 빼는 이유는 절의 절반이 표이기 때문이다.** 세미티에스 「사업의 개요」는 3,819자
    중 2,098자(55%)가 표였고, 내용도 증권사 리서치 인용과 업계 일반론이라 "이 회사가 무엇을
    하는가"와 무관하다. 태그를 지워 평문화하면 표의 셀이 문장 사이에 섞여 들어가 요약을
    흐리고, 발췌 예산도 절반을 먹는다.
    """

    body = copy.deepcopy(section)
    for tag in ("TITLE", "TABLE"):
        for element in body.findall(f".//{tag}"):
            _drop(element)
    return _plain("".join(body.itertext()))


def extract_business_section(document_text: str) -> str:
    """사업보고서 원문에서 사업 설명 구간을 잘라낸다.

    **구조를 먼저 본다.** document.xml 은 절마다 <SECTION-2> 와 <TITLE> 을 갖고 있어
    원하는 절을 이름으로 집을 수 있다. 평문에서 문구를 찾으면 문서 앞 '목 차'에 걸리거나
    절 경계가 없어 다음 절을 침범한다.

    구조가 없는 서식을 위해 평문 검색을 폴백으로 남긴다.

    Args:
        document_text (str): document.xml 본문(태그 포함).

    Returns:
        str: 사업 설명 구간 앞 SECTION_LENGTH자. 찾지 못하면 빈 문자열.
    """

    if not document_text:
        return ""

    root = etree.fromstring(document_text.encode("utf-8"), _XML_PARSER)
    if root is not None:
        sections = root.findall(".//SECTION-2")
        for wanted in SECTION_TITLES:
            for section in sections:
                title_element = section.find("TITLE")
                if title_element is None:
                    continue
                if wanted not in _plain("".join(title_element.itertext())):
                    continue
                excerpt = _section_text(section)
                if excerpt:
                    return excerpt[:SECTION_LENGTH]

    plain = _plain(document_text)
    if not plain:
        return ""

    for keyword in SECTION_KEYWORDS:
        index = plain.find(keyword)
        if index >= 0:
            start = index + len(keyword)
            return plain[start : start + SECTION_LENGTH].strip()

    return ""


def summarize_business_section(name: str, excerpt: str) -> str:
    """발췌를 3~5문장 설명으로 요약한다.

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


def _clean(text: str) -> str:
    """모델이 붙이는 군더더기를 떼어낸다."""

    cleaned = _WHITESPACE_PATTERN.sub(" ", text or "").strip()
    # 따옴표로 감싸 오는 경우가 있다.
    if len(cleaned) >= 2 and cleaned[0] in "\"'“" and cleaned[-1] in "\"'”":
        cleaned = cleaned[1:-1].strip()
    return cleaned
