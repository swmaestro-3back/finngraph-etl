"""기업 개체 사전(gazetteer) 매처 — 본문 표기를 상장 기업 id 로 잇는 공통 모듈.

사전은 `entity_gazetteer` 테이블이 원천이고 companies_sync_gazetteer DAG 가 마스터 동기화
뒤에 매번 재생성한다. 여기서는 그 스냅샷을 프로세스당 한 번 읽어 FlashText 로 매칭한다.

매치 결과가 company_id·stock_id·ticker 를 직접 들고 나오므로, 소비자는 이름으로 그래프나
DB 를 다시 조회하지 않는다. LLM·Bedrock 의존이 없어 어느 파이프라인에서든 가볍게 쓸 수 있다.

경계 동작은 FlashText 기본값 그대로다 — 한글은 단어 경계로 보지 않아 '삼전동' 안의 '삼전'도
잡힌다. 오탐 정리는 소비자 몫이다(triples 는 LLM verify_entities 단계가 맡는다).
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import cache

from flashtext import KeywordProcessor
from sqlalchemy import text
from sqlalchemy.orm import Session

from pipelines.common.clients.postgres import session_scope


@dataclass(frozen=True)
class GazetteerEntry:
    """별칭이 가리키는 상장 기업. canonical 은 companies.name 이다."""

    company_id: int
    stock_id: int
    ticker: str
    canonical: str


@dataclass(frozen=True)
class CompanyMatch:
    """본문 매치 1건. text 는 본문에 적힌 그대로의 표기, start·end 는 그 위치다."""

    text: str
    start: int
    end: int
    entry: GazetteerEntry

    @property
    def canonical(self) -> str:
        return self.entry.canonical


class CompanyMatcher:
    def __init__(self, entries: dict[str, GazetteerEntry]):
        """
        Args:
            entries (dict[str, GazetteerEntry]): 별칭 → 기업.
        """

        # FlashText 의 clean_name 자리에 기업을 그대로 넣어, 매치가 곧 기업으로 나온다.
        self._processor = KeywordProcessor(case_sensitive=True)
        for alias, entry in entries.items():
            self._processor.add_keyword(alias, entry)

    def extract(self, text: str) -> list[CompanyMatch]:
        """본문의 기업 표기를 등장 순서대로 돌려준다. 겹치면 FlashText 최장 일치다."""

        return [
            CompanyMatch(text=text[start:end], start=start, end=end, entry=entry)
            for entry, start, end in self._processor.extract_keywords(text, span_info=True)
        ]

    def canonicalize(self, text: str) -> str:
        """본문의 기업 표기를 정식명(companies.name)으로 바꾼 텍스트를 돌려준다."""

        parts: list[str] = []
        cursor = 0
        for match in self.extract(text):
            parts.append(text[cursor : match.start])
            parts.append(match.canonical)
            cursor = match.end
        parts.append(text[cursor:])
        return "".join(parts)


SELECT_GAZETTEER_SQL = text(
    """
    SELECT alias, company_id, stock_id, ticker, canonical_name AS canonical
      FROM entity_gazetteer
    """
)


def load_gazetteer_entries(session: Session) -> dict[str, GazetteerEntry]:
    rows = session.execute(SELECT_GAZETTEER_SQL).all()
    return {
        row.alias: GazetteerEntry(
            company_id=row.company_id,
            stock_id=row.stock_id,
            ticker=row.ticker,
            canonical=row.canonical,
        )
        for row in rows
    }


@cache
def get_company_matcher() -> CompanyMatcher:
    """프로세스당 한 번 사전을 읽어 매처를 만든다.

    Airflow 태스크는 런마다 새 프로세스라, 사전이 재생성되면 다음 런에 자연히 반영된다.
    사전이 비어 있으면 예외를 던진다 — 빈 매처로 돌면 triples 가 모든 기사를 "관계없음"으로
    조용히 마킹해 버린다.
    """

    with session_scope() as session:
        entries = load_gazetteer_entries(session)

    if not entries:
        raise RuntimeError("entity_gazetteer 가 비어 있음 — companies_sync_gazetteer 를 먼저 실행")

    return CompanyMatcher(entries)


def reset_company_matcher() -> None:
    """캐시된 매처를 버린다 (테스트·장기 실행 프로세스용)."""

    get_company_matcher.cache_clear()
