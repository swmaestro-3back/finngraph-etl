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

settings = get_settings()


class Neo4jDatabase:
    # constructor
    def __init__(self) -> None:
        self._driver = None

    # URI에 맞는 Driver 생성 (BoltDriver 또는 Neo4jDriver)
    def init_driver(self):
        try:
            self._driver = AsyncGraphDatabase.driver(
                uri=settings.NEO4J_URI, auth=(settings.NEO4J_USERNAME, settings.NEO4J_PASSWORD)
            )
        except Exception:
            raise

    async def close(self):
        if self._driver:
            await self._driver.close()

    async def execute(self, query: LiteralString, parameters: dict | None = None) -> list[Record]:
        if not self._driver:
            raise RuntimeError("Neo4j Driver is not initialized. Call init_driver first.")

        records, _, _ = await self._driver.execute_query(
            query, parameters_=parameters, database_=settings.NEO4J_DATABASE
        )

        # 원래 records, summary, keys 이렇게 3개 주는데 지금은 쿼리 결과인 records만
        # 사용하니 나머지는 버리는 용으로 _ 표기
        return records


# 전역적으로 하나의 객체만 사용
# 싱글톤 패턴 적용
neo4j_database = Neo4jDatabase()
