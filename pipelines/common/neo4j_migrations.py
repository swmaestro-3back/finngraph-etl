from __future__ import annotations

import os
from pathlib import Path

from neo4j import GraphDatabase

from pipelines.common.config import get_settings

DEFAULT_DIR = "/opt/airflow/migrations/neo4j"


def split_statements(text: str) -> list[str]:
    lines = [line for line in text.splitlines() if not line.lstrip().startswith("//")]
    return [stmt.strip() for stmt in "\n".join(lines).split(";") if stmt.strip()]


def apply(directory: Path) -> None:
    files = sorted(directory.glob("*.cypher"))
    if not files:
        raise SystemExit(f"cypher 파일이 없습니다: {directory}")

    settings = get_settings()
    with GraphDatabase.driver(
        settings.neo4j_uri, auth=(settings.neo4j_username, settings.neo4j_password)
    ) as driver:
        driver.verify_connectivity()
        for path in files:
            for stmt in split_statements(path.read_text(encoding="utf-8")):
                driver.execute_query(stmt, database_=settings.neo4j_database)
            print(f"applied {path.name}")


if __name__ == "__main__":
    apply(Path(os.environ.get("NEO4J_MIGRATIONS_DIR", DEFAULT_DIR)))
