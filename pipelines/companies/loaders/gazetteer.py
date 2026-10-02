"""개체 사전(entity_gazetteer) 적재와 사전 전용 수동 별칭 시드."""

from __future__ import annotations

from dataclasses import asdict

from sqlalchemy import text
from sqlalchemy.orm import Session

from pipelines.companies.models import GazetteerAlias

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

COUNT_GAZETTEER_SQL = text("SELECT count(*) FROM entity_gazetteer")

# 전량 교체는 한 트랜잭션 안의 DELETE + INSERT 다. TRUNCATE 는 읽는 쪽까지 잠그지만, DELETE 는
# 커밋 전까지 읽는 쪽이 이전 스냅샷을 그대로 본다.
DELETE_GAZETTEER_SQL = text("DELETE FROM entity_gazetteer")

INSERT_GAZETTEER_SQL = text(
    """
    INSERT INTO entity_gazetteer (alias, company_id, stock_id, ticker, canonical_name, alias_source)
    VALUES (:alias, :company_id, :stock_id, :ticker, :canonical_name, :source)
    """
)

# 새 사전이 기존의 이 비율보다 작으면 교체하지 않는다. 마스터가 반쯤 비어 들어온 날 사전이
# 같이 쪼그라들면, 그날 수집된 뉴스의 기업 연결이 조용히 빠진다.
MIN_RETAIN_RATIO = 0.5


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


def replace_gazetteer(session: Session, entries: list[GazetteerAlias]) -> int:
    """entity_gazetteer 를 entries 로 통째로 바꾸고 이전 행 수를 돌려준다.

    새 사전이 이전의 MIN_RETAIN_RATIO 미만이면 바꾸지 않고 예외를 던진다. 커밋은
    호출자(session_scope)가 한다.
    """

    previous = int(session.execute(COUNT_GAZETTEER_SQL).scalar_one())
    if len(entries) < previous * MIN_RETAIN_RATIO:
        raise RuntimeError(
            f"개체 사전이 {previous}건 → {len(entries)}건으로 줄어 교체하지 않는다 — "
            "마스터 동기화 결과를 먼저 확인"
        )

    session.execute(DELETE_GAZETTEER_SQL)
    if entries:
        session.execute(INSERT_GAZETTEER_SQL, [asdict(entry) for entry in entries])
    return previous
