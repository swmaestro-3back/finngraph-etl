"""companies 테이블 조회·적재 — 상장 토글, DART 매핑·개요, 기업 설명, 미국 상장사.

법인 행과 한 흐름으로 묶이는 stocks·company_aliases 쓰기(sync_listed_companies,
load_us_companies)도 여기 둔다.

DART 매핑
---------
매핑 규칙이 DART 적재의 핵심이다(task.md 3-14 실측 근거).

    상장사   → stock_code 매칭            중복 0건, 활성 보통주의 96.9%가 붙는다
    비상장   → corp_code 단위로 전량 적재  이름으로 고르지 않으니 동명 문제가 없다

**법인명으로 상장사를 찾으면 안 된다.** `카카오`는 DART에 2건(035720을 가진 00258801과
상장코드 없는 00918444)이고, 정규화 후 정확 매칭을 해도 둘 다 걸린다. 단축코드로 하면
하나로 확정된다.

비상장은 이름이 아니라 corp_code를 키로 적재하므로 동명(5,431개 이름) 문제가 적재
단계에서는 생기지 않는다. 문제는 "뉴스에 나온 이 표기가 어느 법인이냐"를 고를 때인데,
그건 별칭 사전과 매칭 로직의 몫이다. 그래서 **비상장 법인에는 별칭을 자동으로 붙이지
않는다** — 붙이는 순간 `신한`·`디에스피` 같은 표기가 여러 법인에 걸린다.

미국 상장사
-----------
companies(법인) · stocks(종목) · company_aliases(영문명)에 스키마를 바꾸지 않고 KR과 같은
자리에 나눠 넣는다. 칼럼이 없는 값(영문명·종업원 수·reutersCode)은
stocks.raw_attributes(JSONB)에 둔다.

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
from collections.abc import Sequence

from sqlalchemy import text
from sqlalchemy.orm import Session

from pipelines.companies.models import (
    CompanyProfile,
    CompanySyncResult,
    DartCorp,
    UsCompany,
    UsSyncResult,
)
from pipelines.companies.transformers.us import clean_company_name

ALIAS_SOURCE_KIS_MASTER = "KIS_MASTER"

# 상장 표시 켜기
MARK_LISTED_SQL = text(
    """
    UPDATE companies AS c
       SET is_listed = true,
           delisted_at = NULL,
           name = s.name,
           updated_at = now()
      FROM stocks AS s
     WHERE c.ticker = s.ticker
       -- US 법인은 companies_load_us가 직접 관리한다. 티커 충돌로 덮어쓰지 않게 KR만 본다.
       AND c.country = 'KR'
       AND s.is_active
       AND BTRIM(s.name) <> ''
       AND (
             c.is_listed IS DISTINCT FROM true
          OR c.delisted_at IS NOT NULL
          OR c.name IS DISTINCT FROM s.name
       )
    """
)

# 상장 표시 끄기 (상장폐지)
MARK_DELISTED_SQL = text(
    """
    UPDATE companies AS c
       SET is_listed = false,
           delisted_at = COALESCE(c.delisted_at, CURRENT_DATE),
           updated_at = now()
     WHERE c.country = 'KR'
       AND c.ticker IS NOT NULL
       AND c.is_listed
       AND NOT EXISTS (
             SELECT 1
               FROM stocks AS s
              WHERE s.ticker = c.ticker
                AND s.is_active
           )
    """
)

# stocks.company_id 연결
# 상장 종목의 단축코드인, companies.ticker로 매핑
LINK_STOCKS_TO_COMPANIES_SQL = text(
    """
    UPDATE stocks AS s
       SET company_id = c.id,
           updated_at = now()
      FROM companies AS c
     WHERE c.ticker = s.ticker
       AND c.delisted_at IS NULL
       AND s.is_active
       AND s.company_id IS DISTINCT FROM c.id
    """
)

# 종목명을 별칭으로 적재
# companies_alias에 종목명 저장
# US 종목(source='WIKIPEDIA')은 자체 로더가 영문 별칭을 넣는다.
# 여기 섞이면 출처가 KIS_MASTER로 잘못 찍힌다.
INSERT_COMPANY_ALIASES_SQL = text(
    """
    INSERT INTO company_aliases (company_id, alias, lang, source)
    SELECT s.company_id, s.name, 'ko', :source
      FROM stocks AS s
     WHERE s.is_active
       AND s.source = 'KIS_MASTER'
       AND s.company_id IS NOT NULL
       AND BTRIM(s.name) <> ''
    ON CONFLICT (alias, company_id) DO NOTHING
    """
)


# 국내 상장 법인 상태 조회
def sync_listed_companies(session: Session) -> CompanySyncResult:

    listed = session.execute(MARK_LISTED_SQL)
    delisted = session.execute(MARK_DELISTED_SQL)
    linked = session.execute(LINK_STOCKS_TO_COMPANIES_SQL)
    aliases = session.execute(
        INSERT_COMPANY_ALIASES_SQL,
        {"source": ALIAS_SOURCE_KIS_MASTER},
    )

    return CompanySyncResult(
        listed_count=listed.rowcount or 0,
        delisted_count=delisted.rowcount or 0,
        linked_count=linked.rowcount or 0,
        alias_count=aliases.rowcount or 0,
    )


# 상장 법인 적재 — 법인의 존재 여부는 DART가 정한다
#
# **corpCode.xml에 없으면 법인이 아니다.** 예전에는 stocks에서 종목을 걸러 법인을 만들었는데,
# "법인이 아닌 것"을 플래그로 열거하는 방식이라 목록에 없는 종류가 계속 새어 들어왔다.
# 공모펀드(F701…)와 신주인수권(J…WR)이 우선주·ETP·스팩 어디에도 해당하지 않아 통과했다.
# corpCode.xml은 법인등록 기준 목록이라 그런 상품이 애초에 들어 있지 않다.
#
# is_listed는 여기서 정하지 않는다. corpCode.xml은 **폐지된 종목의 stock_code도 그대로**
# 들고 있어서(카카오엠 016170) 이 값만 보면 이미 없는 종목이 상장으로 들어온다.
# 지금 상장 중인지는 매일 오는 KIS 마스터가 판단한다 — sync_listed_companies가 토글한다.
#
# 두 문장으로 나눈 이유는 유니크 인덱스가 둘이기 때문이다. ticker는 활성 상장 기준
# (companies_active_ticker_uk), corp_code는 값이 있을 때 기준(companies_corp_code_uk)이라
# 한 문장의 ON CONFLICT로는 둘 다 추론할 수 없다.

# ① 이미 ticker로 존재하는 행에 corp_code를 붙인다.
ATTACH_CORP_CODE_BY_TICKER_SQL = text(
    """
    UPDATE companies AS c
       SET corp_code = :corp_code,
           updated_at = now()
     WHERE c.ticker = :stock_code
       AND c.delisted_at IS NULL
       AND c.corp_code IS NULL
    """
)

# ② 없으면 만든다. corpCode 의 stock_code 를 그대로 ticker 로 쓴다.
#
# stocks 를 읽지 않는다. 예전에는 "지금 거래되는가"를 여기서 조인으로 판정했는데, 그러면
# 이 잡이 stocks 보다 뒤에 돌아야 해서 순서가 얽혔다. 상장 여부는 sync_listed_companies 가
# is_listed 로 관리하므로 판정이 두 곳에 있을 이유가 없다.
#
# 폐지된 종목의 stock_code 도 corpCode 에 남아 있어 그 법인들까지 ticker 를 갖는다(실측
# 3,986 중 1,332). 활성 종목이 없어 is_listed 가 켜지지 않고 연결도 되지 않는다.
# stock_code 는 중복이 0건이라 활성 티커 유니크에 걸릴 일도 없다.
UPSERT_LISTED_CORP_SQL = text(
    """
    INSERT INTO companies (
      corp_code, ticker, name, country, is_listed, created_at, updated_at
    )
    SELECT CAST(:corp_code AS TEXT), CAST(:stock_code AS VARCHAR), CAST(:name AS TEXT),
           'KR', false, now(), now()
     WHERE NOT EXISTS (
             SELECT 1 FROM companies AS x
              WHERE x.ticker = CAST(:stock_code AS VARCHAR) AND x.delisted_at IS NULL
           )
    ON CONFLICT (corp_code) WHERE corp_code IS NOT NULL DO UPDATE SET
      ticker = COALESCE(companies.ticker, EXCLUDED.ticker),
      updated_at = now()
    """
)

# 비상장 법인 적재
#
# 상장 법인 행을 덮어쓰지 않도록 name은 비상장일 때만 갱신한다. 상장사의 정규 이름은
# KIS master가 주는 종목명이고, DART 법인명("(주)…")과 표기가 다르다.
#
# conflict target이 부분 유니크 인덱스(companies_corp_code_uk)라 인덱스 조건인
# WHERE corp_code IS NOT NULL을 그대로 적어야 Postgres가 추론한다.
UPSERT_UNLISTED_COMPANY_SQL = text(
    """
    INSERT INTO companies (corp_code, name, country, is_listed, created_at, updated_at)
    VALUES (:corp_code, :name, 'KR', false, now(), now())
    ON CONFLICT (corp_code) WHERE corp_code IS NOT NULL DO UPDATE SET
      name = CASE WHEN companies.is_listed THEN companies.name ELSE EXCLUDED.name END,
      updated_at = now()
    """
)

UPDATE_COMPANY_PROFILE_SQL = text(
    """
    UPDATE companies
       SET ceo_name = COALESCE(:ceo_name, ceo_name),
           industry_code = COALESCE(:industry_code, industry_code),
           established_on = COALESCE(:established_on, established_on),
           fiscal_month = COALESCE(:fiscal_month, fiscal_month),
           homepage = COALESCE(:homepage, homepage),
           address = COALESCE(:address, address),
           updated_at = now()
     WHERE corp_code = :corp_code
    """
)

# 개요 수집 대상 — corp_code가 붙었고 개요가 아직 비어 있는 법인부터
#
# **service_companies로 대상을 좁힌다.** 목록이 법인 단위라 여기서는 종목을 거치지 않고
# 곧바로 조인한다 — DART가 corp_code로 조회하므로 같은 축이다.
#
# 비상장 법인도 목록에 넣을 수 있다. 반도체·2차전지 테마에 비상장이 들어가는 경우가 있고,
# 그 법인은 종목이 없어 시세는 자연히 빠지고 DART 개요·재무만 수집된다.
SELECT_PROFILE_TARGETS_SQL = text(
    """
    SELECT c.corp_code
      FROM companies AS c
     WHERE c.corp_code IS NOT NULL
       AND EXISTS (SELECT 1 FROM service_companies AS u WHERE u.company_id = c.id)
     ORDER BY (c.ceo_name IS NOT NULL), c.updated_at
     LIMIT :limit
    """
)

# DART 재무 수집 대상 — 갱신이 오래된 순
SELECT_DART_FINANCIAL_TARGETS_SQL = text(
    """
    SELECT c.id, c.corp_code, c.fiscal_month
      FROM companies AS c
      LEFT JOIN (
             SELECT company_id, MAX(updated_at) AS updated_at
               FROM company_financials
              WHERE source = 'DART'
              GROUP BY company_id
           ) AS f ON f.company_id = c.id
     WHERE c.corp_code IS NOT NULL
       AND EXISTS (SELECT 1 FROM service_companies AS u WHERE u.company_id = c.id)
     ORDER BY f.updated_at ASC NULLS FIRST, c.corp_code
     LIMIT :limit
    """
)

# 서비스 대상인데 수집 경로가 이어지지 않는 법인
#
# 목록은 법인 단위지만 실제 수집은 두 축으로 갈린다.
#
#     DART 개요·재무   companies.corp_code 가 필요하다
#     시세·수급·배당   활성 보통주(stocks)가 필요하다
#
# 어느 쪽이 비어도 **조인에서 조용히 빠진다.** 에러가 안 나니 "왜 이 법인 데이터가 없지"를
# 나중에야 알게 된다. 그래서 배치마다 끊긴 지점을 세어 로그로 남긴다.
#
# **상장 법인만 종목을 요구한다.** 비상장은 종목이 없는 것이 정상이고 DART만 수집된다.
# 서비스 대상인데 수집 경로가 끊긴 법인 — 경고 로그용 진단
#
# 조인이 조용히 걸러버리는 종류라 결과 건수만 봐서는 드러나지 않는다. 두 가지를 본다.
#
#   corp_code 없음        → DART 개요·재무를 조회할 키가 없다
#   활성 보통주 없음      → 시세·수급·배당이 붙을 종목이 없다
#
# **s.ticker가 아니라 s.company_id로 확인한다.** 실제 수집이 그 컬럼으로 대상을 고르기
# 때문이다(SELECT_SERVICEABLE_TICKERS_SQL). ticker로 보면 "시장에 있으니 괜찮다"가 나오지만,
# 연결이 끊긴 종목은 수집에서 그대로 빠진다 — delisted_at이 안 지워졌거나 종목 마스터 동기화가
# 실패한 경우가 그렇다. 진단은 수집과 같은 경로를 봐야 의미가 있다.
#
# is_listed로 감싸는 이유는 비상장 법인 오탐을 막기 위해서다. 테마에 비상장이 들어갈 수 있고,
# 그 법인은 종목이 없는 것이 정상이라 매 실행마다 경고가 뜨면 진짜 고장과 구분되지 않는다.
#
# 우선주·ETP·스팩은 다시 거르지 않는다. company_id는 companies.ticker로만 붙고 그 컬럼에는
# 보통주만 들어간다 — 실측으로 연결된 활성 종목 2,581건 중 비보통주는 0건이다.
SELECT_UNRESOLVED_UNIVERSE_SQL = text(
    """
    SELECT c.id,
           c.name,
           CASE
             WHEN c.corp_code IS NULL THEN 'corp_code 없음 — DART 수집 불가'
             ELSE '활성 보통주 없음 — 시세 수집 불가'
           END AS reason
      FROM service_companies AS u
      JOIN companies AS c ON c.id = u.company_id
     WHERE c.corp_code IS NULL
        OR (c.is_listed
            AND NOT EXISTS (
                  SELECT 1
                    FROM stocks AS s
                   WHERE s.company_id = c.id
                     AND s.is_active
                ))
     ORDER BY c.id
    """
)


def fetch_unresolved_universe(session: Session) -> list[tuple[int, str, str]]:
    """수집 경로가 끊긴 서비스 대상 법인 (company_id, name, 이유)."""

    rows = session.execute(SELECT_UNRESOLVED_UNIVERSE_SQL)
    return [(row.id, row.name, row.reason) for row in rows]


def link_listed_corp_codes(session: Session, corps: list[DartCorp]) -> int:
    """상장 법인을 적재하고 corp_code·ticker를 붙인다.

    corpCode.xml에 있는 법인만 만든다. 목록에 없는 상품(공모펀드·신주인수권)은 여기서
    걸러지므로 stocks 쪽에서 종류를 열거해 뺄 필요가 없다.

    상장 여부는 정하지 않는다. corpCode.xml은 폐지된 종목의 stock_code도 들고 있어서
    이 값만으로는 지금 거래되는지 알 수 없다. 그 판단은 sync_listed_companies가 한다.

    Args:
        session (Session): DB 세션.
        corps (list[DartCorp]): corpCode.xml 전체.

    Returns:
        int: 새로 만들어지거나 값이 바뀐 법인 수.
    """

    listed = [corp for corp in corps if corp.stock_code]
    if not listed:
        return 0

    touched = 0
    for corp in listed:
        params = {
            "corp_code": corp.corp_code,
            "stock_code": corp.stock_code,
            "name": corp.name,
        }
        attached = session.execute(ATTACH_CORP_CODE_BY_TICKER_SQL, params)
        if attached.rowcount:
            touched += attached.rowcount
            continue

        created = session.execute(UPSERT_LISTED_CORP_SQL, params)
        touched += created.rowcount or 0

    return touched


def upsert_unlisted_companies(session: Session, corps: list[DartCorp]) -> int:
    """비상장 법인을 corp_code 단위로 적재한다.

    Returns:
        int: 적재를 시도한 법인 수.
    """

    unlisted = [corp for corp in corps if not corp.stock_code]
    if not unlisted:
        return 0

    session.execute(
        UPSERT_UNLISTED_COMPANY_SQL,
        [{"corp_code": corp.corp_code, "name": corp.name} for corp in unlisted],
    )
    return len(unlisted)


def update_company_profile(session: Session, profile: CompanyProfile) -> int:
    """기업개황을 반영한다. 이미 값이 있는 컬럼은 덮어쓰지 않는다."""

    result = session.execute(
        UPDATE_COMPANY_PROFILE_SQL,
        {
            "corp_code": profile.corp_code,
            "ceo_name": profile.ceo_name,
            "industry_code": profile.industry_code,
            "established_on": profile.established_on,
            "fiscal_month": profile.fiscal_month,
            "homepage": profile.homepage,
            "address": profile.address,
        },
    )
    return result.rowcount or 0


def fetch_profile_targets(session: Session, limit: int) -> list[str]:
    """개요를 채울 corp_code 목록. 아직 비어 있는 법인이 먼저 온다."""

    rows = session.execute(SELECT_PROFILE_TARGETS_SQL, {"limit": limit})
    return [row.corp_code for row in rows]


def fetch_dart_financial_targets(
    session: Session,
    limit: int,
) -> list[tuple[int, str, str | None]]:
    """DART 재무를 채울 (company_id, corp_code, fiscal_month) 목록."""

    rows = session.execute(SELECT_DART_FINANCIAL_TARGETS_SQL, {"limit": limit})
    return [(row.id, row.corp_code, row.fiscal_month) for row in rows]


UPDATE_DESCRIPTION_SQL = text(
    """
    UPDATE companies
       SET description = :description,
           description_source = :description_source,
           description_rcept_no = :description_rcept_no,
           updated_at = now()
     WHERE id = :company_id
    """
)

# 설명 생성 대상 — 국내 서비스 대상 법인
#
# 새 보고서가 올라왔는지는 DART 목록을 봐야 알 수 있어 전량을 넘긴다. 넘길지 말지는
# 잡이 description_rcept_no와 대조해 정한다.
SELECT_DESCRIPTION_TARGETS_SQL = text(
    """
    SELECT c.id, c.name, c.corp_code, c.description_rcept_no
      FROM companies AS c
     WHERE c.country = 'KR'
       AND c.corp_code IS NOT NULL
       AND EXISTS (SELECT 1 FROM service_companies AS u WHERE u.company_id = c.id)
     ORDER BY (c.description IS NOT NULL), c.id
     LIMIT :limit
    """
)


def update_description(
    session: Session,
    company_id: int,
    description: str,
    description_source: str,
    description_rcept_no: str,
) -> int:
    """법인 설명을 갱신한다."""

    result = session.execute(
        UPDATE_DESCRIPTION_SQL,
        {
            "company_id": company_id,
            "description": description,
            "description_source": description_source,
            "description_rcept_no": description_rcept_no,
        },
    )
    return result.rowcount or 0


def fetch_description_targets(
    session: Session, limit: int | None = None
) -> list[tuple[int, str, str, str | None]]:
    """설명 후보 법인 (id, name, corp_code, 직전 원천 접수번호)."""

    rows = session.execute(SELECT_DESCRIPTION_TARGETS_SQL, {"limit": limit})
    return [(row.id, row.name, row.corp_code, row.description_rcept_no) for row in rows]


STOCK_SOURCE_WIKIPEDIA = "WIKIPEDIA"
ALIAS_SOURCE_WIKIPEDIA = "WIKIPEDIA"
DESCRIPTION_SOURCE_NAVER = "NAVER"

UPSERT_US_COMPANY_SQL = text(
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
UPSERT_US_STOCK_SQL = text(
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

INSERT_US_ALIAS_SQL = text(
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
            UPSERT_US_COMPANY_SQL,
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
                UPSERT_US_STOCK_SQL,
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
                INSERT_US_ALIAS_SQL,
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


SELECT_LISTED_TICKERS_SQL = text(
    """
    SELECT DISTINCT ON (c.corp_code) c.corp_code, s.ticker
      FROM companies AS c
      JOIN stocks AS s ON s.company_id = c.id AND s.is_active
     WHERE c.corp_code = ANY(CAST(:codes AS text[]))
     ORDER BY c.corp_code, s.ticker
    """
)


def select_listed_tickers(session: Session, codes: Sequence[str]) -> dict[str, str]:
    """corp_code 로 지정한 법인의 활성 종목 단축코드. 종목이 여럿이면 코드가 가장 앞선 것."""

    if not codes:
        return {}
    rows = session.execute(SELECT_LISTED_TICKERS_SQL, {"codes": list(codes)})
    return {row.corp_code: row.ticker for row in rows}
