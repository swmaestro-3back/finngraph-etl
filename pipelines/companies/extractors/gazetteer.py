"""개체 사전(entity_gazetteer)의 원천이 되는 별칭 후보 조회(Postgres).

사전 입장에서 companies·stocks·company_aliases 는 **원천**이라 extractors 에 둔다.
"""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.orm import Session

from pipelines.companies.models import GazetteerAlias

# 사전에 넣는 company_aliases 출처. WIKIPEDIA(영문 법인명, 'Apple Inc.')는 뺀다 — 한국어
# 기사에 그 표기로 나오는 일이 드물고, Visa·Target 처럼 일반 단어와 겹치는 짧은 영문명이
# 오탐만 늘린다. 통용 영문 표기(구글 등)가 필요하면 CURATED 로 넣는다.
ALIAS_SOURCES = ("KIS_MASTER", "DART", "CURATED")

# 대상은 활성 종목이 연결된 상장 기업뿐이다. 비상장 11만여 건은 동명 법인이 많아 넣지 않는다.
# 활성 종목은 기업당 하나라(국내는 보통주만 수집, 미국은 1:1) 조인이 행을 불리지 않는다.
#
# 별칭 후보(출처 의미는 GazetteerAlias.source)
# - NAME: companies.name. 정식명이 자기 표기로 매칭돼야 정식명으로 적힌 본문(events 의 치환
#   텍스트 포함)에서도 잡힌다.
# - STOCK_NAME: 종목명에서 괄호 묶음을 뺀 값. 미국은 한경 표기의 띄어쓰기('메타 플랫폼스')가
#   살아 있어 companies.name('메타플랫폼스')과 다른 별칭이 되고, 끝의 '홀딩스'를 뗀 표기도
#   넣는다 — 기사는 '크라우드스트라이크 홀딩스'가 아니라 '크라우드스트라이크'로 쓴다.
# - company_aliases 의 ALIAS_SOURCES 출처.
SELECT_ALIAS_CANDIDATES_SQL = text(
    """
    WITH listed AS (
        SELECT c.id AS company_id, s.id AS stock_id, s.ticker, c.name AS canonical_name,
               c.country,
               BTRIM(REGEXP_REPLACE(s.name, '[(（][^()（）]*[)）]', '', 'g')) AS stock_alias
          FROM companies AS c
          JOIN stocks AS s
            ON s.company_id = c.id
           AND s.is_active
         WHERE c.delisted_at IS NULL
    ), candidates AS (
        SELECT company_id, canonical_name AS alias, 'NAME' AS source FROM listed
        UNION ALL
        SELECT company_id, stock_alias, 'STOCK_NAME' FROM listed
        UNION ALL
        SELECT company_id, BTRIM(REGEXP_REPLACE(stock_alias, ' *홀딩스$', '')), 'STOCK_NAME'
          FROM listed
         WHERE country = 'US'
           AND stock_alias LIKE '%홀딩스'
        UNION ALL
        SELECT company_id, alias, source
          FROM company_aliases
         WHERE source = ANY(:sources)
    )
    SELECT cand.alias, l.company_id, l.stock_id, l.ticker, l.canonical_name, cand.source
      FROM candidates AS cand
      JOIN listed AS l USING (company_id)
    """
)


def fetch_alias_candidates(session: Session) -> list[GazetteerAlias]:
    rows = session.execute(SELECT_ALIAS_CANDIDATES_SQL, {"sources": list(ALIAS_SOURCES)}).mappings()
    return [GazetteerAlias(**row) for row in rows]
