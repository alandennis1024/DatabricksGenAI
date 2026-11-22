"""Load operating_views.sql and fill in the catalog and schema names."""
from __future__ import annotations

from pathlib import Path

from cobalt_common import check_identifier, quote

SQL_FILE = Path(__file__).with_name("operating_views.sql")
# The names written in the SQL file; the notebook swaps in its widget values.
SQL_FILE_PREFIX = "cobalt_ops.future"


def split_statements(text: str) -> list[str]:
    """Drop full-line comments, then split on semicolons."""
    lines = [ln for ln in text.splitlines() if not ln.lstrip().startswith("--")]
    parts = "\n".join(lines).split(";")
    return [part.strip() for part in parts if part.strip()]


def load_statements(catalog: str, schema: str, path: Path = SQL_FILE) -> list[str]:
    catalog = check_identifier(catalog, "catalog")
    schema = check_identifier(schema, "schema")
    target = f"{quote(catalog)}.{quote(schema)}"
    text = Path(path).read_text(encoding="utf-8")
    return [s.replace(SQL_FILE_PREFIX, target) for s in split_statements(text)]


def kind(statement: str) -> str:
    """Return schema, table, view, or query."""
    head = " ".join(statement.split()).upper()
    if head.startswith("CREATE SCHEMA"):
        return "schema"
    if head.startswith("CREATE TABLE"):
        return "table"
    if head.startswith("CREATE OR REPLACE VIEW"):
        return "view"
    if head.startswith(("SELECT", "WITH")):
        return "query"
    raise ValueError(f"Unexpected statement: {statement[:60]!r}")


def alert_query(statements: list[str], view_name: str) -> str:
    """The alert query from the file that reads the given view."""
    for statement in statements:
        if kind(statement) == "query" and f".{view_name}\n" in statement + "\n":
            return statement
    raise KeyError(f"No alert query reads {view_name}")
