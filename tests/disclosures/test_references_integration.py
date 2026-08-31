"""disclosures 참조 조회 통합 테스트 — 계약상대 역매칭 별칭."""

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


def test_fetch_counterparty_aliases_returns_every_source(_seed: int) -> None:
    with session_scope() as session:
        aliases = dict(fetch_counterparty_aliases(session))

    assert aliases.get(TEST_ALIASES[0]) == _seed
    assert aliases.get("구테스트차공업") == _seed, "수동 관리(CURATED) 별칭도 역매칭 입력이다"
    assert aliases.get("테스트차") == _seed, "KIS 종목명(KIS_MASTER)도 역매칭 입력이다"
    # corp_code 없는 법인의 별칭도 나간다. 마스터에 없는 company_id 를 버리는 것은
    # CorpResolver 의 몫이라 조회에서 미리 좁히지 않는다.
    assert TEST_ALIASES[1] in aliases
