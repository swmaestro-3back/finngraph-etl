from pathlib import Path

from pipelines.common.neo4j_migrations import split_statements

MIGRATIONS = Path(__file__).resolve().parents[2] / "migrations" / "neo4j"


def test_split_drops_comments_and_blank_statements():
    text = """// 주석
CREATE CONSTRAINT a IF NOT EXISTS
FOR (c:Company) REQUIRE c.name IS UNIQUE;

  // 들여쓴 주석
CREATE INDEX b IF NOT EXISTS FOR (e:Event) ON (e.x);
"""
    assert split_statements(text) == [
        "CREATE CONSTRAINT a IF NOT EXISTS\nFOR (c:Company) REQUIRE c.name IS UNIQUE",
        "CREATE INDEX b IF NOT EXISTS FOR (e:Event) ON (e.x)",
    ]


def test_repository_migrations_are_idempotent_statements():
    files = sorted(MIGRATIONS.glob("*.cypher"))
    assert files
    for path in files:
        statements = split_statements(path.read_text(encoding="utf-8"))
        assert statements, path.name
        for stmt in statements:
            assert stmt.startswith("CREATE "), (path.name, stmt)
            assert "IF NOT EXISTS" in stmt, (path.name, stmt)
            assert "//" not in stmt, (path.name, stmt)
