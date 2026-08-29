"""disclosures 참조 조회 통합 테스트 — DART 법인명 별칭.

실제 Postgres에 붙는다. `integration` 마커가 붙어 CI unit-test job에서는 제외된다.
로컬은 `docker compose up -d db` 후 `pytest -m integration`.

fetch_counterparty_aliases 는 계약상대 역매칭에 들어가는 입력이라 두 성질이 중요하다.

1. source가 DART(법인명)·CURATED(수동 시드)인 별칭만 돌려준다 — KIS 종목명·단축코드
   별칭(KIS_MASTER)이나 향후 뉴스 유래 별칭이 섞이면 "틀린 식별자는 null보다 나쁘다"
   원칙이 깨진다.
2. corp_code 없는 법인의 별칭은 돌려주지 않는다 — 마스터(fetch_corp_master)에 없는
   company_id 는 리졸버가 어차피 버리므로 애초에 나가지 않아야 한다.
"""

from __future__ import annotations

import pytest
from sqlalchemy import text

from pipelines.common.clients.postgres import session_scope
from pipelines.disclosures.references.companies import fetch_counterparty_aliases

pytestmark = pytest.mark.integration

# 운영 데이터와 섞이지 않도록 테스트 전용 대역. corp_code는 99로 시작하는 8자리.
TEST_CORP_CODE = "99999911"
TEST_ALIASES = ("테스트자동차주식회사", "테스트차량공업")


@pytest.fixture(autouse=True)
def _seed():
    with session_scope() as session:
        company_id = session.execute(
            text(
                """
                INSERT INTO companies (name, corp_code, is_listed, country)
                VALUES ('테스트차', :corp_code, TRUE, 'KR')
                RETURNING id
                """
            ),
            {"corp_code": TEST_CORP_CODE},
        ).scalar_one()
        no_corp_id = session.execute(
            text(
                """
                INSERT INTO companies (name, is_listed, country)
                VALUES ('코드없는법인', FALSE, 'KR')
                RETURNING id
                """
            )
        ).scalar_one()
        session.execute(
            text(
                """
                INSERT INTO company_aliases (company_id, alias, lang, source)
                VALUES (:id, :dart_name, 'ko', 'DART'),
                       (:id, '구테스트차공업', 'ko', 'CURATED'),
                       (:id, '테스트차', 'ko', 'KIS_MASTER'),
                       (:no_corp_id, :orphan_name, 'ko', 'DART')
                """
            ),
            {
                "id": company_id,
                "dart_name": TEST_ALIASES[0],
                "no_corp_id": no_corp_id,
                "orphan_name": TEST_ALIASES[1],
            },
        )
    yield company_id
    with session_scope() as session:
        # company_aliases 는 ON DELETE CASCADE 라 companies 와 함께 사라진다.
        session.execute(
            text("DELETE FROM companies WHERE id IN (:a, :b)"),
            {"a": company_id, "b": no_corp_id},
        )


def test_fetch_counterparty_aliases_returns_only_dart_source_with_corp_code(_seed: int) -> None:
    with session_scope() as session:
        aliases = dict(fetch_counterparty_aliases(session))

    assert aliases.get(TEST_ALIASES[0]) == _seed
    assert aliases.get("구테스트차공업") == _seed, "수동 관리(CURATED) 별칭도 역매칭 입력이다"
    assert "테스트차" not in aliases, "KIS_MASTER 별칭은 역매칭 입력이 아니다"
    assert TEST_ALIASES[1] not in aliases, "corp_code 없는 법인의 별칭은 나가지 않는다"
