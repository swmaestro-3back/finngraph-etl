"""멤버 기사를 LLM 입력 텍스트로 바꾼다.

요약(news.summary)은 삼중항이 있는 기사에만 생기므로(summarize_articles 의 조건) 없으면
본문 앞부분(리드)을 쓴다. news.text 는 저장 시 이미 정제돼 있어 공백만 정리하고 자른다.
"""

from __future__ import annotations

from datetime import date

from pipelines.events.models import MemberArticle
from pipelines.news.utils.date_utils import SEOUL_TIMEZONE


def _collapse(text: str) -> str:
    return " ".join((text or "").split())


def member_source_text(member: MemberArticle, lead_chars: int) -> str:
    """`제목\\n본문` 형태. 본문은 요약이 있으면 요약, 없으면 리드 lead_chars 자."""

    title = _collapse(member.title)
    summary = _collapse(member.summary or "")
    body = summary if summary else _collapse(member.text)[:lead_chars].rstrip()
    return f"{title}\n{body}".strip()


def member_date(member: MemberArticle) -> date | None:
    """보도 시각을 KST 날짜로. 클러스터링의 local_date 와 같은 기준이다."""

    if member.published_at is None:
        return None
    return member.published_at.astimezone(SEOUL_TIMEZONE).date()
