from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.orm import Session

from pipelines.companies.models import CompanySyncResult

ALIAS_SOURCE_KIS_MASTER = "KIS_MASTER"

# 국내 상장 법인 upsert
#
# 원천이 KIS master 파일이 아니라 이미 적재된 stocks다.
# 마스터파일 파싱은 이미 stocks의 sync_master가 끝낸 상태이므로,
# 여기서는 그 결과를 company 축으로 옮기기만 한다.
#
# 증권그룹구분코드가 주권('ST')인 것만 법인으로 본다.
# 화이트리스트인 이유: 마스터에는 주권 말고도 수익증권(BC), 리츠(RT), 신주인수권(SR/SW),
# 예탁증서(DR), 외국주권(FS), 인프라투자회사(IF), 뮤추얼펀드(MF)가 함께 실려 온다.
# etp/spac 플래그로 ETF·ETN·스팩은 걸러지지만 나머지는 플래그가 비어 있어 그대로 통과했다
# ('한투한미핵심성장포커스2(A)' 같은 공모펀드가 법인으로 올라갔다). 블랙리스트로 두면
# 신종 상품이 생길 때마다 또 뚫리므로 주권만 통과시킨다.
#
# 우선주는 ST면서 별도 법인이 아니라 같은 법인의 다른 주식이므로 계속 제외한다.
# 제외된 종목은 stocks에는 남아 있고 company_id만 NULL로 둔다(수집 범위 ≠ 제공 범위).
#
# conflict target이 부분 유니크 인덱스(companies_active_ticker_uk)라 인덱스 조건인
# WHERE delisted_at IS NULL을 그대로 적어 추론시킨다.
UPSERT_LISTED_COMPANIES_SQL = text(
    """
    INSERT INTO companies (ticker, name, country, is_listed, created_at, updated_at)
    SELECT s.ticker, s.name, 'KR', true, now(), now()
      FROM stocks AS s
     WHERE s.is_active
       AND s.security_group = 'ST'
       AND NOT s.preferred_stock
       AND NOT s.etp
       AND NOT s.spac
       AND BTRIM(s.name) <> ''
    ON CONFLICT (ticker) WHERE delisted_at IS NULL DO UPDATE SET
      name = EXCLUDED.name,
      country = EXCLUDED.country,
      is_listed = EXCLUDED.is_listed,
      updated_at = now()
    """
)

# stocks.company_id 연결
#
# 단축코드로 잇는다. companies.ticker는 활성 상장 종목의 단축코드이고, 상장폐지된
# 법인 행은 delisted_at이 채워져 조인에서 빠진다. 종목코드가 재사용되어 다른 법인이
# 같은 단축코드를 갖게 되면, 활성 법인 쪽으로만 붙는다.
#
# upsert와 같은 주권('ST') 조건을 다시 건다. 없으면 종목코드가 겹치는 비주권 종목이
# 활성 법인에 다시 붙어, 정리해둔 연결이 다음 동기화에서 조용히 되살아난다.
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
       AND s.security_group = 'ST'
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
    """적재된 stocks를 기준으로 국내 상장 법인을 companies에 동기화한다.

    세 단계를 순서대로 실행한다. 순서가 고정인 이유는 뒤 단계가 앞 단계의 결과에
    의존하기 때문이다 — 법인이 있어야 연결할 수 있고, 연결이 끝나야 별칭에 붙일
    company_id가 생긴다.

    1. 활성 보통주를 companies에 upsert
    2. stocks.company_id를 단축코드로 연결
    3. 종목명·단축코드를 company_aliases에 적재

    Args:
        session (Session): DB 세션.

    Returns:
        CompanySyncResult: 단계별 영향 행 수.
    """

    upserted = session.execute(UPSERT_LISTED_COMPANIES_SQL)
    linked = session.execute(LINK_STOCKS_TO_COMPANIES_SQL)
    aliases = session.execute(
        INSERT_COMPANY_ALIASES_SQL,
        {"source": ALIAS_SOURCE_KIS_MASTER},
    )

    return CompanySyncResult(
        upserted_count=upserted.rowcount or 0,
        linked_count=linked.rowcount or 0,
        alias_count=aliases.rowcount or 0,
    )
