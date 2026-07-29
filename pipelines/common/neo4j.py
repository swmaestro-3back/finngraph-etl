from __future__ import annotations

from typing import LiteralString

from neo4j import AsyncGraphDatabase, Record

from pipelines.common.config import get_settings

# BoltDriver
# It addresses a single database machine. This may be a standalone server or could be a
# specific member of a cluster. Connections established by a BoltDriver are always made
# to the exact host and port detailed in the URI.

# Neo4jDriver
# The routing behaviour works in tandem with Neo4j's Causal Clustering feature by
# directing read and write behaviour to appropriate cluster members.


class Neo4jDatabase:
    # constructor
    def __init__(self) -> None:
        self._driver = None

    # URI에 맞는 Driver 생성 (BoltDriver 또는 Neo4jDriver)
    # settings는 여기서 지연 로드: import 시점(env 미주입 상태)에 설정을 읽지 않는다.
    def init_driver(self):
        settings = get_settings()
        self._driver = AsyncGraphDatabase.driver(
            uri=settings.neo4j_uri, auth=(settings.neo4j_username, settings.neo4j_password)
        )

    async def close(self):
        if self._driver:
            await self._driver.close()

    async def execute(self, query: LiteralString, parameters: dict | None = None) -> list[Record]:
        if not self._driver:
            raise RuntimeError("Neo4j Driver is not initialized. Call init_driver first.")

        records, _, _ = await self._driver.execute_query(
            query, parameters_=parameters, database_=get_settings().neo4j_database
        )

        # 원래 records, summary, keys 이렇게 3개 주는데 지금은 쿼리 결과인 records만
        # 사용하니 나머지는 버리는 용으로 _ 표기
        return records

    # Airflow에서는 태스크마다 별개 프로세스라 드라이버를 태스크 단위로 만들고 닫아야 한다.
    # 실행 단위 진입점에서 `async with neo4j_database:`로 감싸 수명을 관리한다.
    async def __aenter__(self) -> Neo4jDatabase:
        self.init_driver()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()


# 전역적으로 하나의 객체만 사용
# 싱글톤 패턴 적용
neo4j_database = Neo4jDatabase()
