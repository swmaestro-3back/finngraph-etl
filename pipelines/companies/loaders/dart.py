"""DART 법인 매핑·개요 적재.

매핑 규칙이 이 파일의 핵심이다(task.md 3-14 실측 근거).

    상장사   → stock_code 매칭            중복 0건, 활성 보통주의 96.9%가 붙는다
    비상장   → corp_code 단위로 전량 적재  이름으로 고르지 않으니 동명 문제가 없다

**법인명으로 상장사를 찾으면 안 된다.** `카카오`는 DART에 2건(035720을 가진 00258801과
상장코드 없는 00918444)이고, 정규화 후 정확 매칭을 해도 둘 다 걸린다. 단축코드로 하면
하나로 확정된다.

비상장은 이름이 아니라 corp_code를 키로 적재하므로 동명(5,431개 이름) 문제가 적재
단계에서는 생기지 않는다. 문제는 "뉴스에 나온 이 표기가 어느 법인이냐"를 고를 때인데,
그건 별칭 사전과 매칭 로직의 몫이다. 그래서 **비상장 법인에는 별칭을 자동으로 붙이지
않는다** — 붙이는 순간 `신한`·`디에스피` 같은 표기가 여러 법인에 걸린다.
"""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.orm import Session

from pipelines.companies.models import CompanyProfile, DartCorp
from pipelines.companies.transformers.dart import normalize_corp_name

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

# DART 법인명 별칭 적재 — 상장 법인만
#
# 상장사의 companies.name은 sync_listed_companies가 매일 KIS 종목명("현대차")으로 덮으므로
# DART 법인명("현대자동차")은 여기 별칭이 유일한 보존처다. 공시 계약상대 역매칭이
# source='DART' 별칭을 마스터명과 함께 조회한다.
#
# 비상장에는 붙이지 않는다(모듈 docstring 참고) — 비상장은 companies.name이 이미 DART
# 법인명이라 별칭이 필요 없고, 붙이는 순간 동명 표기가 여러 법인에 걸린다.
ALIAS_SOURCE_DART = "DART"

INSERT_DART_NAME_ALIAS_SQL = text(
    """
    INSERT INTO company_aliases (company_id, alias, lang, source)
    SELECT c.id, CAST(:name AS TEXT), 'ko', :source
      FROM companies AS c
     WHERE c.corp_code = :corp_code
    ON CONFLICT (alias, company_id) DO NOTHING
    """
)

# 수동 관리 별칭 시드 — 사명변경·통용표기
#
# DART 법인명으로도 흡수되지 않는 표기다. 포스코건설은 포스코이앤씨의 **옛 사명**이라
# 현행 등록부 어디에도 없고, 통용 한글표기는 공시 원문에만 나온다. (별칭, 마스터명)
# 쌍으로 두고 마스터명이 **유일하게** 해석될 때만 넣는다 — 같은 이름의 법인이 둘이면
# 어느 쪽인지 알 수 없으므로 넣지 않는다.
CURATED_ALIASES: list[tuple[str, str]] = [
    ("엘지씨엔에스", "LG CNS"),
    ("포스코건설", "포스코이앤씨"),
    ("엘지화학", "LG화학"),
    ("엘지전자", "LG전자"),
    ("엘지유플러스", "LG유플러스"),
    ("엘지디스플레이", "LG디스플레이"),
    ("에스케이하이닉스", "SK하이닉스"),
    ("에스케이텔레콤", "SK텔레콤"),
    ("에스케이이노베이션", "SK이노베이션"),
    ("지에스건설", "GS건설"),
    ("케이씨씨", "KCC"),
]

INSERT_CURATED_ALIAS_SQL = text(
    """
    INSERT INTO company_aliases (company_id, alias, lang, source)
    SELECT c.id, CAST(:alias AS TEXT), 'ko', 'CURATED'
      FROM companies AS c
     WHERE c.name = :name
       AND c.corp_code IS NOT NULL
       AND (SELECT count(*) FROM companies AS x
             WHERE x.name = c.name AND x.corp_code IS NOT NULL) = 1
    ON CONFLICT (alias, company_id) DO NOTHING
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

SELECT_COMPANY_BY_CORP_CODE_SQL = text(
    "SELECT id, fiscal_month FROM companies WHERE corp_code = :corp_code"
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

SELECT_COMPANY_IDS_BY_NORMALIZED_NAME_SQL = text(
    """
    SELECT id, name
      FROM companies
     WHERE country = 'KR'
       AND NOT is_listed
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


def insert_dart_name_aliases(session: Session, corps: list[DartCorp]) -> int:
    """상장 법인의 DART 법인명을 별칭으로 적재한다.

    법인 행이 아직 없는 corp_code(동기화 순서상 뒤에 생기는 경우)는 조용히 건너뛴다 —
    다음 주기 실행이 채운다.

    Returns:
        int: 새로 추가된 별칭 수.
    """

    added = 0
    for corp in corps:
        if not corp.stock_code or not corp.name.strip():
            continue
        result = session.execute(
            INSERT_DART_NAME_ALIAS_SQL,
            {"corp_code": corp.corp_code, "name": corp.name, "source": ALIAS_SOURCE_DART},
        )
        added += result.rowcount or 0
    return added


def seed_curated_aliases(
    session: Session,
    seed: list[tuple[str, str]] | None = None,
) -> int:
    """수동 관리 별칭(CURATED_ALIASES)을 적재한다.

    마스터명이 아직 없거나 모호한 항목은 조용히 건너뛴다 — 다음 주기 실행이 채운다.

    Args:
        session (Session): DB 세션.
        seed (list[tuple[str, str]] | None): (별칭, 마스터명) 쌍. 없으면 CURATED_ALIASES.

    Returns:
        int: 새로 추가된 별칭 수.
    """

    added = 0
    for alias, name in CURATED_ALIASES if seed is None else seed:
        result = session.execute(INSERT_CURATED_ALIAS_SQL, {"alias": alias, "name": name})
        added += result.rowcount or 0
    return added


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


def fetch_company_by_corp_code(session: Session, corp_code: str) -> tuple[int, str | None] | None:
    """corp_code로 (company_id, fiscal_month)를 찾는다."""

    row = session.execute(SELECT_COMPANY_BY_CORP_CODE_SQL, {"corp_code": corp_code}).first()
    return (row.id, row.fiscal_month) if row else None


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


def find_unlisted_company_by_name(session: Session, name: str) -> int | None:
    """정규화한 법인명이 **유일하게** 일치하는 비상장 법인 id.

    2건 이상 걸리면 None을 준다. 잘못 붙는 것보다 안 붙는 게 낫다 — 재무가 엉뚱한
    법인에 붙으면 조용히 틀린 값이 서비스에 나간다.
    """

    target = normalize_corp_name(name)
    if not target:
        return None

    matches = [
        row.id
        for row in session.execute(SELECT_COMPANY_IDS_BY_NORMALIZED_NAME_SQL)
        if normalize_corp_name(row.name) == target
    ]
    return matches[0] if len(matches) == 1 else None
