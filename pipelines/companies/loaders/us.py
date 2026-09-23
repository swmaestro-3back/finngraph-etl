"""미국 상장사 Postgres 적재 — companies(법인) · stocks(종목) · company_aliases(영문명).

스키마를 바꾸지 않고 KR과 같은 자리에 나눠 넣는다. 칼럼이 없는 값(영문명·종업원 수·
reutersCode)은 stocks.raw_attributes(JSONB)에 둔다.

키:
  companies → 활성 티커 부분 유니크(companies_active_ticker_uk)
  stocks    → 활성 티커 부분 유니크(stocks_active_ticker_uk). US 종목은 standard_code가
              없다(NULL, V3 마이그레이션) — 원천이 위키피디아 인덱스라 표준코드가 없다.

companies.name은 한경 한글명에서 괄호 부분과 공백을 전부 뺀 값이다("메타 플랫폼스(페이스북)"
→ "메타플랫폼스"). Neo4j Company.name 키가 이 값을 따른다(load_us_neo4j가 Postgres에서 읽는다).
stocks.name은 한경 표기를 그대로 둔다.

COALESCE인 이유: 네이버 호출이 실패한 주에 지난 설명·대표를 NULL로 덮지 않기 위해서다.

지수 이탈 종목의 stocks 행은 내리지 않는다 — 계속 활성으로 남아 목록에 보인다. Neo4j
노드도 US 시드에 삭제 단계가 없어 그대로 남는다. 지수 편입 플래그는 Neo4j 전용 속성이라
여기(Postgres)에는 없다 — 매 회차 인덱스 원본으로 다시 계산된다.
"""

from __future__ import annotations

import json
import re

from sqlalchemy import text
from sqlalchemy.orm import Session

from pipelines.companies.models import UsCompany, UsSyncResult

STOCK_SOURCE_WIKIPEDIA = "WIKIPEDIA"
ALIAS_SOURCE_WIKIPEDIA = "WIKIPEDIA"
DESCRIPTION_SOURCE_NAVER = "NAVER"

_PARENTHESIZED = re.compile(r"[(（][^()（）]*[)）]")
_WHITESPACE = re.compile(r"\s+")


def clean_company_name(name: str) -> str:
    """한경 한글명 → companies.name. 괄호 묶음(반각·전각)을 지우고 공백을 전부 없앤다."""

    return _WHITESPACE.sub("", _PARENTHESIZED.sub("", name))


UPSERT_COMPANY_SQL = text(
    """
    INSERT INTO companies (name, ticker, is_listed, country, description, description_source,
                           ceo_name, industry_code, homepage, address, fiscal_month)
    VALUES (:name, :ticker, true, 'US', :description,
            CASE WHEN CAST(:description AS text) IS NULL THEN NULL ELSE :description_source END,
            :ceo_name, :industry_code, :homepage, :address, :fiscal_month)
    ON CONFLICT (ticker) WHERE delisted_at IS NULL DO UPDATE
       SET name = EXCLUDED.name,
           is_listed = true,
           description = COALESCE(EXCLUDED.description, companies.description),
           description_source = CASE WHEN EXCLUDED.description IS NULL
                                     THEN companies.description_source
                                     ELSE EXCLUDED.description_source END,
           ceo_name = COALESCE(EXCLUDED.ceo_name, companies.ceo_name),
           industry_code = COALESCE(EXCLUDED.industry_code, companies.industry_code),
           homepage = COALESCE(EXCLUDED.homepage, companies.homepage),
           address = COALESCE(EXCLUDED.address, companies.address),
           fiscal_month = COALESCE(EXCLUDED.fiscal_month, companies.fiscal_month),
           updated_at = now()
    RETURNING id
    """
)

# standard_code·source도 갱신한다. 예전 회차가 남긴 reutersCode·'US_INDEX' 행을 NULL·'WIKIPEDIA'로
# 맞추기 위해서다.
UPSERT_STOCK_SQL = text(
    """
    INSERT INTO stocks (name, ticker, market, company_id, standard_code, listed_date,
                        listed_shares, source, raw_attributes, synced_at)
    VALUES (:name, :ticker, :market, :company_id, NULL, :listed_date,
            :listed_shares, :source, CAST(:raw_attributes AS jsonb), now())
    ON CONFLICT (ticker) WHERE is_active DO UPDATE
       SET name = EXCLUDED.name,
           market = EXCLUDED.market,
           company_id = EXCLUDED.company_id,
           standard_code = NULL,
           source = EXCLUDED.source,
           listed_date = COALESCE(EXCLUDED.listed_date, stocks.listed_date),
           listed_shares = COALESCE(EXCLUDED.listed_shares, stocks.listed_shares),
           raw_attributes = COALESCE(stocks.raw_attributes, '{}'::jsonb) || EXCLUDED.raw_attributes,
           is_active = true,
           synced_at = now(),
           updated_at = now()
    """
)

INSERT_ALIAS_SQL = text(
    """
    INSERT INTO company_aliases (company_id, alias, lang, source)
    VALUES (:company_id, :alias, 'en', :source)
    ON CONFLICT (alias, company_id) DO NOTHING
    """
)


def load_us_companies(session: Session, companies: list[UsCompany]) -> UsSyncResult:
    company_count = stock_count = alias_count = 0

    for company in companies:
        company_id = session.execute(
            UPSERT_COMPANY_SQL,
            {
                "name": clean_company_name(company.name),
                "ticker": company.ticker,
                "description": company.description,
                "description_source": DESCRIPTION_SOURCE_NAVER,
                "ceo_name": company.ceo_name,
                "industry_code": company.industry_code,
                "homepage": company.homepage,
                "address": company.address,
                "fiscal_month": company.fiscal_month,
            },
        ).scalar_one()
        company_count += 1

        stock_count += (
            session.execute(
                UPSERT_STOCK_SQL,
                {
                    "name": company.name,
                    "ticker": company.ticker,
                    "market": company.market,
                    "company_id": company_id,
                    "listed_date": company.listed_date,
                    "listed_shares": company.listed_shares,
                    "source": STOCK_SOURCE_WIKIPEDIA,
                    "raw_attributes": json.dumps(company.raw_attributes, ensure_ascii=False),
                },
            ).rowcount
            or 0
        )

        alias_count += (
            session.execute(
                INSERT_ALIAS_SQL,
                {
                    "company_id": company_id,
                    "alias": company.name_eng,
                    "source": ALIAS_SOURCE_WIKIPEDIA,
                },
            ).rowcount
            or 0
        )

    return UsSyncResult(
        company_count=company_count, stock_count=stock_count, alias_count=alias_count
    )
