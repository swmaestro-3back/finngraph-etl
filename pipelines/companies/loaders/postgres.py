from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.orm import Session

from pipelines.companies.models import CompanySyncResult

ALIAS_SOURCE_KIS_MASTER = "KIS_MASTER"

# 상장 표시 켜기
#
# **법인 행은 여기서 만들지 않는다.** 존재 여부는 DART corpCode.xml이 정하고(loaders/dart.py),
# 여기서는 그중 지금 거래되는 것을 켜기만 한다. 예전에는 stocks에서 종목을 걸러 법인을
# 만들었는데, "법인이 아닌 것"을 플래그로 열거하는 방식이라 목록에 없는 종류가 새어 들어왔다
# — 공모펀드(F701…)와 신주인수권(J…WR)이 우선주·ETP·스팩 어디에도 걸리지 않아 107건이 쌓였다.
#
# delisted_at을 지우는 이유: 폐지됐다 다시 거래되면 활성 상태로 되돌려야 한다. 활성 ticker
# 부분 유니크(companies_active_ticker_uk)와 종목 연결이 모두 이 컬럼을 조건으로 쓴다. 안 지우면
# 행이 남아 있는데도 양쪽 모두에서 안 보여, stocks.company_id가 붙지 않고 재무·개요가 통째로
# 빠진다 — 시세만 쌓이므로 눈에 잘 띄지 않는다.
#
# 진짜 재상장은 드물지만 이 경로는 매일 쓰인다. 폐지 판정이 "오늘 KIS 마스터에 없으면"이라,
# 마스터가 부분 수신되면 멀쩡한 종목이 폐지로 찍힌다. 그 복구도 여기가 맡는다.
#
# **우선주·ETP·스팩을 여기서 다시 거르지 않는다.** companies.ticker는 그 필터를 통과한 종목에만
# 채워지고(loaders/dart.py의 UPSERT_LISTED_CORP_SQL), 활성 종목은 ticker당 하나뿐이라
# (stocks_active_ticker_uk) 조인이 다른 분류의 행을 집을 수 없다. 실측으로 조건을 넣든 빼든
# 대상이 2,581건으로 같고, companies.ticker가 비보통주를 가리키는 경우는 0건이다.
MARK_LISTED_SQL = text(
    """
    UPDATE companies AS c
       SET is_listed = true,
           delisted_at = NULL,
           name = s.name,
           updated_at = now()
      FROM stocks AS s
     WHERE c.ticker = s.ticker
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
#
# 활성 종목 목록에서 사라진 ticker를 내린다. 상장 여부를 매일 오는 KIS 마스터가 판단하므로
# 폐지도 같은 경로로 잡힌다 — 별도 폐지 목록을 받아올 필요가 없다.
#
# delisted_at은 폐지 시점을 남기고, 동시에 활성 ticker 부분 유니크
# (companies_active_ticker_uk, WHERE delisted_at IS NULL)에서 이 행을 빼낸다. 인덱스를 부분으로
# 둔 것은 단축코드가 다른 법인에 재발급될 여지를 남기기 위해서다 — 다만 DART 등록부에서
# stock_code가 붙은 3,984건(폐지 흔적 1,332건 포함)을 훑어도 중복은 0건이라, 재사용은 아직
# 실증되지 않았다. 인덱스가 부분이어서 손해 볼 것은 없으므로 그대로 둔다.
#
# 상장 표시와 마찬가지로 우선주·ETP·스팩을 다시 거르지 않는다(위 MARK_LISTED_SQL 주석 참고).
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
#
# 단축코드로 잇는다. companies.ticker는 활성 상장 종목의 단축코드이고, 상장폐지된
# 법인 행은 delisted_at이 채워져 조인에서 빠진다. 종목코드가 재사용되어 다른 법인이
# 같은 단축코드를 갖게 되면, 활성 법인 쪽으로만 붙는다.
#
# IS DISTINCT FROM으로 이미 같은 법인을 가리키는 행은 건드리지 않는다. updated_at이
# 매일 무의미하게 갱신되는 것을 막고, linked_count가 "실제로 바뀐 수"를 뜻하게 된다.
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

# 종목명·단축코드를 별칭으로 적재
#
# 뉴스 본문에는 '삼성전자'처럼 종목명이 그대로 나오기도 하고 '005930'이 나오기도 한다.
# 둘 다 같은 법인을 가리키므로 별칭 사전에 함께 넣는다.
#
# UNION(UNION ALL이 아니다)으로 중복을 먼저 없앤다. 종목명과 단축코드가 같은
# 이상 데이터가 들어오면 한 INSERT에서 같은 (alias, company_id)를 두 번 건드려
# "ON CONFLICT DO UPDATE command cannot affect row a second time"가 난다.
INSERT_COMPANY_ALIASES_SQL = text(
    """
    INSERT INTO company_aliases (company_id, alias, lang, source)
    SELECT s.company_id, s.name, 'ko', :source
      FROM stocks AS s
     WHERE s.is_active
       AND s.company_id IS NOT NULL
       AND BTRIM(s.name) <> ''
     UNION
    SELECT s.company_id, s.ticker, NULL::text, :source
      FROM stocks AS s
     WHERE s.is_active
       AND s.company_id IS NOT NULL
    ON CONFLICT (alias, company_id) DO NOTHING
    """
)


def sync_listed_companies(session: Session) -> CompanySyncResult:
    """적재된 stocks를 기준으로 국내 상장 법인의 상태를 맞춘다.

    **법인 행을 만들지 않는다.** 존재 여부는 DART corpCode.xml이 정하고(sync_dart_corp_codes),
    여기서는 그 목록 위에서 지금 거래되는 것을 켜고 사라진 것을 내린다.

    네 단계를 순서대로 실행한다. 뒤 단계가 앞 단계 결과에 의존한다 — 상장 표시가 맞아야
    연결할 대상이 정해지고, 연결이 끝나야 별칭에 붙일 company_id가 생긴다.

    1. 활성 보통주에 해당하는 법인을 상장으로 표시 (이름·시장도 갱신)
    2. 목록에서 사라진 ticker를 상장폐지로 표시
    3. stocks.company_id를 단축코드로 연결
    4. 종목명·단축코드를 company_aliases에 적재

    DART가 아직 반영하지 못한 신규 상장은 법인 행이 없어 1단계에서 걸리지 않는다.
    그 종목은 company_id가 NULL로 남고 다음 corpCode 동기화 때 붙는다.

    Args:
        session (Session): DB 세션.

    Returns:
        CompanySyncResult: 단계별 영향 행 수.
    """

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
