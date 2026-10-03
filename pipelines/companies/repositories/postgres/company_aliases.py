"""company_aliases 테이블 적재 — DART 법인명 별칭과 수동 별칭 시드.

KIS 종목명 별칭(KIS_MASTER)과 미국 영문명 별칭(WIKIPEDIA)은 법인 적재와 한 흐름이라
companies.py 의 sync_listed_companies·load_us_companies 가 쓴다.
"""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.orm import Session

from pipelines.companies.models import DartCorp

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


# 수동 관리 별칭 — 기사에서 통용되지만 마스터(KIS·DART·한경) 어디에도 없는 표기
#
# (별칭, ticker) 쌍이다. dart.py 의 CURATED_ALIASES 는 마스터명으로 법인을 찾아 비상장까지
# 덮는 공시 매칭용이고, 여기는 상장사 사전용이라 사명이 바뀌어도 그대로인 ticker 로 찾는다.
# 같은 표기가 여러 기업에 걸리면 CURATED 가 우선한다(transformers/gazetteer.py) — '구글'이
# GOOGL·GOOG 중 GOOGL 로 가는 이유다.
CURATED_ALIASES_BY_TICKER: list[tuple[str, str]] = [
    # 국내
    ("삼전", "005930"),
    ("현대 자동차", "005380"),
    ("LG 에너지솔루션", "373220"),
    ("LG엔솔", "373220"),
    ("LG 엔솔", "373220"),
    ("기아차", "000270"),
    ("기아자동차", "000270"),
    ("기아 자동차", "000270"),
    ("신한금융지주", "055550"),
    ("두산중공업", "034020"),
    ("네이버", "035420"),
    ("LSELECTRIC", "010120"),
    ("LS일렉트릭", "010120"),
    ("LS 일렉트릭", "010120"),
    ("LS일렉", "010120"),
    ("LS 일렉", "010120"),
    ("대우조선해양", "042660"),
    ("포스코", "005490"),
    ("POSCO", "005490"),
    ("한전", "015760"),
    ("SKT", "017670"),
    ("soil", "010950"),
    ("SOIL", "010950"),
    ("에쓰오일", "010950"),
    ("에스오일", "010950"),
    ("KAKAO", "035720"),
    ("삼성SDS", "018260"),
    ("삼성 SDS", "018260"),
    ("APR", "278470"),
    ("포스코케미칼", "003670"),
    ("카뱅", "323410"),
    ("한국타이어", "161390"),
    ("LG CNS", "064400"),
    ("LGCNS", "064400"),
    ("LGU+", "032640"),
    ("LG U+", "032640"),
    ("LG 유플러스", "032640"),
    ("엔씨소프트", "036570"),
    ("엔씨", "036570"),
    ("아시아나", "020560"),
    ("SILICON2", "257720"),
    ("JYP", "035900"),
    ("JYP 엔터테인먼트", "035900"),
    ("JYP엔터테인먼트", "035900"),
    ("SM 엔터테인먼트", "041510"),
    ("SM엔터테인먼트", "041510"),
    ("한글과컴퓨터", "030520"),
    ("하이닉스", "000660"),
    ("삼바", "207940"),
    ("삼성바이오", "207940"),
    ("한화에어로", "012450"),
    ("KAI", "047810"),
    ("현대중공업", "329180"),
    ("한국조선해양", "009540"),
    ("현대일렉트릭", "267260"),
    ("대우조선", "042660"),
    ("포스코홀딩스", "005490"),
    ("KB금융지주", "105560"),
    ("하나금융", "086790"),
    ("우리금융", "316140"),
    ("메리츠금융", "138040"),
    ("에코프로BM", "247540"),
    ("두산에너빌", "034020"),
    ("SK이노", "096770"),
    ("L&F", "066970"),
    ("HYBE", "352820"),
    ("현대상선", "011200"),
    ("IBK기업은행", "024110"),
    ("가스공사", "036460"),
    ("아모레", "090430"),
    ("LIG넥스원", "079550"),
    ("삼성 SDI", "006400"),
    ("LG 전자", "066570"),
    # 미국
    ("구글", "GOOGL"),
    ("아마존", "AMZN"),
    ("페이스북", "META"),
    ("NVIDIA", "NVDA"),
    ("팔란티어", "PLTR"),
    ("SpaceX", "SPCX"),
    ("T모바일", "TMUS"),
    ("T 모바일", "TMUS"),
    ("Paypal", "PYPL"),
    # 정식 한글명 '시스코'는 Sysco(SYY)지만 기사의 '시스코'는 대개 Cisco 다.
    # CURATED 가 NAME 을 이긴다.
    ("시스코", "CSCO"),
    ("MS", "MSFT"),
    ("알파벳", "GOOGL"),
    ("마이크론", "MU"),
    ("코스트코", "COST"),
    ("ASML", "ASML"),
    ("AMAT", "AMAT"),
    ("마벨", "MRVL"),
    ("ARM", "ARM"),
    ("슈퍼마이크로", "SMCI"),
    ("코인베이스", "COIN"),
    ("마이크로스트래티지", "MSTR"),
    ("팔로알토", "PANW"),
    ("로빈후드", "HOOD"),
    ("텍사스인스트루먼트", "TXN"),
    ("온세미", "ON"),
    ("길리어드", "GILD"),
]

# ticker 로 기업을 찾는다. 상폐 기업에는 붙이지 않는다 — 같은 ticker 가 재사용될 수 있다.
#
# 같은 (별칭, 기업)이 WIKIPEDIA 영문명으로 이미 있으면 CURATED 로 올린다. 사전은 WIKIPEDIA 를
# 읽지 않으므로(extractors/gazetteer.py), 그대로 두면 'SpaceX' 같은 지정 표기가 빠진다.
# 미국 적재는 별칭을 ON CONFLICT DO NOTHING 으로 넣어 되돌리지 않는다.
INSERT_CURATED_ALIAS_BY_TICKER_SQL = text(
    """
    INSERT INTO company_aliases (company_id, alias, lang, source)
    SELECT c.id, CAST(:alias AS TEXT), 'ko', 'CURATED'
      FROM companies AS c
     WHERE c.ticker = :ticker
       AND c.delisted_at IS NULL
    ON CONFLICT (alias, company_id) DO UPDATE
       SET source = 'CURATED'
     WHERE company_aliases.source = 'WIKIPEDIA'
    """
)


def seed_curated_aliases_by_ticker(
    session: Session, seed: list[tuple[str, str]] | None = None
) -> int:
    """수동 별칭을 company_aliases(source='CURATED')에 넣는다. 새로 들어간 행 수를 돌려준다.

    사전 생성마다 먼저 돌아 빈 DB 에서도 재현된다. 이미 있는 행은 ON CONFLICT 로 건너뛴다.

    Args:
        seed (list[tuple[str, str]] | None): (별칭, ticker) 쌍. 없으면 CURATED_ALIASES_BY_TICKER.
    """

    inserted = 0
    for alias, ticker in CURATED_ALIASES_BY_TICKER if seed is None else seed:
        result = session.execute(
            INSERT_CURATED_ALIAS_BY_TICKER_SQL, {"alias": alias, "ticker": ticker}
        )
        inserted += result.rowcount or 0
    return inserted
