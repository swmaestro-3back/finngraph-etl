"""뉴스 수집 경계 모델.

헤드라인·검색 두 수집 소스가 같은 형태의 기사를 반환한다는 계약을 타입으로
고정한다. 수집기 내부는 dict로 동작하고, 수집 모듈(collect_headline·
collect_search)의 반환 경계에서 to_articles()로 검증된 뒤 병합 시점에
to_pipeline_item()으로 다운스트림(필터·클러스터링·저장) dict로 돌아간다.
"""

from __future__ import annotations

import logging
from typing import Any, Literal

from pydantic import BaseModel, Field, ValidationError

SourceType = Literal["headline", "search"]


class NewsArticle(BaseModel):
    title: str = Field(..., min_length=1, description="기사 제목")
    description: str = Field(default="", description="검색 API 요약. 헤드라인 수집은 빈 문자열")
    link: str = Field(..., min_length=1, description="기사 URL (news 테이블 UNIQUE 키)")
    originallink: str = Field(default="", description="언론사 원문 URL")
    pub_date: str = Field(default="", description="발행 시각 원문 문자열 (저장 시점에 파싱)")
    source_type: SourceType = Field(..., description="수집 경로 (news.source_type에 기록)")
    search_keyword: str | None = Field(None, description="search 소스: 수집에 사용된 검색 쿼리")
    category_id: int | None = Field(None, description="headline 소스: 카테고리 ID")
    category_name: str | None = Field(None, description="headline 소스: 카테고리명")

    @classmethod
    def from_collected_item(cls, item: dict[str, Any], source_type: SourceType) -> NewsArticle:
        """수집기가 만든 raw dict를 검증된 모델로 변환한다."""

        return cls(
            title=item.get("title", ""),
            description=item.get("description", ""),
            link=item.get("link", ""),
            originallink=item.get("originallink", ""),
            pub_date=item.get("pubDate", ""),
            source_type=source_type,
            search_keyword=item.get("_search_keyword") or None,
            category_id=item.get("_category_id"),
            category_name=item.get("_category_name"),
        )

    def to_pipeline_item(self) -> dict[str, Any]:
        """다운스트림 단계들이 소비하는 dict 계약을 생성한다."""

        return {
            "title": self.title,
            "description": self.description,
            "link": self.link,
            "originallink": self.originallink,
            "pubDate": self.pub_date,
            "_source_type": self.source_type,
        }


def to_articles(items: list[dict[str, Any]], source_type: SourceType) -> list[NewsArticle]:
    """수집기 raw dict 목록을 검증된 모델 목록으로 변환한다. 계약 위반 항목은 경고 후 건너뛴다."""

    articles: list[NewsArticle] = []

    for item in items:
        try:
            articles.append(NewsArticle.from_collected_item(item, source_type=source_type))
        except ValidationError as e:
            logging.warning(
                "수집 항목이 기사 계약을 위반해 건너뜀 (source=%s, title=%r): %s",
                source_type,
                item.get("title", ""),
                e,
            )

    return articles
