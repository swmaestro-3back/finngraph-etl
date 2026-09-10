"""DB 스모크 통합 테스트.

DB 컨테이너가 요구하는 확장이 실제로 설치되어 올라오는지 검증한다.
- vector: 테마 임베딩(themes.embedding) 등 pgvector 컬럼 용

로컬 DB 컨테이너가 떠 있어야 하며(`docker compose up -d db`), 프로젝트의 실제 DB 레이어
(`pipelines.common.clients.postgres`)를 그대로 사용하므로 연결 설정/DATABASE_URL 경로까지 함께 스모크된다.

이 테스트는 `integration` 마커가 붙어 있어 CI unit-test job에서는 `-m "not integration"`으로
제외되고, db-integration job에서만 실행된다. 로컬에서 DB 없이 유닛만 돌리려면 동일하게
`pytest -m "not integration"` 을 쓰면 된다.
"""

from __future__ import annotations

import pytest
from sqlalchemy import text

from pipelines.common.clients.postgres import session_scope

REQUIRED_EXTENSIONS = ("vector",)


@pytest.mark.integration
def test_required_extensions_installed() -> None:
    with session_scope() as session:
        rows = session.execute(
            text("SELECT extname FROM pg_extension WHERE extname = ANY(:names)"),
            {"names": list(REQUIRED_EXTENSIONS)},
        ).all()

    found = {row[0] for row in rows}
    missing = set(REQUIRED_EXTENSIONS) - found
    assert not missing, f"필수 DB 확장이 설치되어 있지 않음: {sorted(missing)}"
