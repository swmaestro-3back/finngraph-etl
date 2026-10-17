from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field, model_validator


class Company(BaseModel):
    name: str = Field(..., description="주식 종목명", examples=["삼성전자"])
    ticker: str = Field(..., min_length=6, max_length=7, description="KRX 거래소 단축 코드")
    reason: str | None = Field(
        None, description="테마 편입 이유 (추후 테마-주식 간 Relationship 설정 시 사용)"
    )


class Theme(BaseModel):
    name: str = Field(..., description="테마명", examples=["철강"])
    source: Literal["naver", "judal", "antwinner"]
    sources: list[str] = Field(default=[], description="추출된 소스 목록")
    source_theme_id: int | None = Field(None, description="크롤링 시점 source별 원본 테마 ID")
    description: str = Field(default="", description="테마 설명 및 개요")
    companies: list[Company] = Field(default=[], description="해당 테마에 속한 주식 리스트")

    @model_validator(mode="after")
    def _default_sources(self) -> Theme:
        if not self.sources:
            self.sources = [self.source]
        return self
